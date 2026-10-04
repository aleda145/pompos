package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pompos/internal/agent"
)

func TestWebChatUsesCustomAgentRegardlessOfMCP(t *testing.T) {
	app, service, _ := setupFixture(t)
	calls := 0
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		call := agent.Call{ID: "question", Type: "function"}
		call.Function.Name = "ask_user"
		call.Function.Arguments = `{"kind":"question","prompt":"Which source?","options":[]}`
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": agent.Message{Role: "assistant", Calls: []agent.Call{call}}}}})
	}))
	defer model.Close()
	if err := service.SaveSettings(agent.Settings{Mode: "mcp", Endpoint: model.URL, Model: "fixture"}); err != nil {
		t.Fatal(err)
	}
	// This is the empty session shape previously created by /ingestions/new.
	legacy, err := service.NewMCPChat("New ingestion")
	if err != nil {
		t.Fatal(err)
	}
	if response := setupRequest(app, "GET", "/chat/"+legacy.ID, nil); response.Code != http.StatusOK {
		t.Fatal("old web chat did not reopen", response.Code)
	}
	repaired, err := service.Load(legacy.ID)
	if err != nil || repaired.External || len(repaired.Messages) != 0 {
		t.Fatal("empty web placeholder was not restored", err)
	}
	post := func(id string) {
		t.Helper()
		r := httptest.NewRequest("POST", "/chat/"+id, strings.NewReader(`{"message":"Continue with this source"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		app.Handler().ServeHTTP(w, r)
		var result struct {
			Session agent.Session `json:"session"`
			Error   string        `json:"error"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.Error != "" || result.Session.External || result.Session.Pending == nil {
			t.Fatalf("web turn did not reach custom agent: %v %s", err, w.Body.String())
		}
	}
	post(legacy.ID)
	for _, enabled := range []bool{true, false} {
		if err := service.SetMCPEnabled(enabled); err != nil {
			t.Fatal(err)
		}
		post(legacy.ID) // Existing custom-agent conversations survive toggling MCP.
		response := setupRequest(app, "GET", "/ingestions/new", nil)
		if response.Code != http.StatusSeeOther || !strings.HasPrefix(response.Header().Get("Location"), "/chat/") {
			t.Fatal("new chat did not route to the web agent")
		}
		id := strings.TrimPrefix(response.Header().Get("Location"), "/chat/")
		post(id)
	}
	if calls != 5 {
		t.Fatalf("custom agent called %d times, want 5", calls)
	}
	// Actual MCP sessions must not be converted or processed by web actions.
	external, err := service.NewMCPChat("ECB rates")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/chat/" + external.ID, "/chat/" + external.ID + "/publish", "/chat/" + external.ID + "/secret"} {
		if response := setupRequest(app, "POST", path, nil); response.Code != http.StatusNotFound {
			t.Fatal("web action accepted an MCP conversation", path, response.Code)
		}
	}
	if response := setupRequest(app, "GET", "/chat/"+external.ID, nil); response.Code != http.StatusNotFound {
		t.Fatal("web chat accepted an MCP conversation", response.Code)
	}
	after, err := service.Load(external.ID)
	if err != nil || !after.External || len(after.Messages) != len(external.Messages) || calls != 5 {
		t.Fatal("web access changed MCP conversation or called the model", err)
	}
}

func TestWebChatRequiresProviderEvenWhenMCPSetupIsComplete(t *testing.T) {
	app, service, _ := setupFixture(t)
	if err := service.SelectMode("mcp"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/ingestions/new", "/chat/existing"} {
		assertRedirect(t, setupRequest(app, "GET", path, nil), "/settings")
	}
}
