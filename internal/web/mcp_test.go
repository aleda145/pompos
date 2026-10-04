package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pompos/internal/agent"
	"pompos/internal/compiler"
	"pompos/internal/execution"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/scheduler"
	"pompos/internal/spec"
)

func TestMCPSetupPersistsWithoutCredentials(t *testing.T) {
	app, service, db := setupFixture(t)
	assertRedirect(t, setupRequest(app, "POST", "/setup/mode", url.Values{"mode": {"mcp"}}), "/settings#mcp")
	cfg, _ := service.Settings()
	if cfg.Mode != "mcp" || cfg.Endpoint != "" || cfg.APIKeyRef != "" || cfg.MCPEnabled {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	entries, _ := db.Secrets().List(context.Background())
	if len(entries) != 0 {
		t.Fatal("MCP setup created credentials")
	}
	app.Agent = &agent.Service{Dir: service.Dir, Secrets: db.Secrets(), Destinations: db}
	for _, path := range []string{"/", "/settings"} {
		if w := setupRequest(app, "GET", path, nil); w.Code != 200 {
			t.Fatalf("page %s: %d", path, w.Code)
		}
	}
	connection := setupRequest(app, "GET", "/settings", nil)
	for _, want := range []string{`id="mcp"`, `<strong class="mcp-state">Disabled</strong>`, `aria-label="Enable MCP">Enable</button>`, "http://127.0.0.1:8080/mcp", "codex mcp add pompos --url", "claude mcp add --transport http pompos"} {
		if !strings.Contains(connection.Body.String(), want) {
			t.Fatalf("missing MCP setting: %s", want)
		}
	}
	for _, removed := range []string{"Generate token", "POMPOS_MCP_TOKEN", "bearer-token", "make build", `href="/settings/mcp"`, "Try:", "<textarea", "copy-mcp", "tool_timeout_sec", "checked"} {
		if strings.Contains(connection.Body.String(), removed) {
			t.Fatalf("obsolete MCP UI: %s", removed)
		}
	}
	assertRedirect(t, setupRequest(app, "GET", "/settings/mcp", nil), "/settings#mcp")

	assertRedirect(t, setupRequest(app, "GET", "/setup", nil), "/")
	assertRedirect(t, setupRequest(app, "POST", "/settings/mcp", url.Values{"enabled": {"on"}}), "/settings#mcp")
	restarted := &agent.Service{Dir: service.Dir}
	cfg, err := restarted.Settings()
	if err != nil || !cfg.MCPEnabled || cfg.Mode != "mcp" {
		t.Fatal("MCP switch did not persist", err)
	}
	connection = setupRequest(app, "GET", "/settings", nil)
	if !strings.Contains(connection.Body.String(), `<strong class="mcp-state">Enabled</strong>`) || !strings.Contains(connection.Body.String(), `aria-label="Disable MCP">Disable</button>`) {
		t.Fatal("enabled status and disable action missing on reload")
	}
	assertRedirect(t, setupRequest(app, "POST", "/setup/mode", url.Values{"mode": {"agent"}}), "/setup")
	page := setupRequest(app, "GET", "/settings", nil)
	if !strings.Contains(page.Body.String(), `<strong class="mcp-state">Disabled</strong>`) || !strings.Contains(page.Body.String(), `aria-label="Enable MCP">Enable</button>`) {
		t.Fatal("switching to agent mode did not show disabled status and enable action")
	}
	if page := setupRequest(app, "GET", "/setup", nil); !strings.Contains(page.Body.String(), "Configure agent") {
		t.Fatal("agent setup missing")
	}
}

func TestMCPLocalhostWithoutAuthenticationAndDisable(t *testing.T) {
	app, service, _ := setupFixture(t)
	if err := service.SelectMode("mcp"); err != nil {
		t.Fatal(err)
	}
	handler := app.Handler()
	request := func(host, origin, fetchSite, authorization string) int {
		r := httptest.NewRequest("POST", "http://"+host+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json, text/event-stream")
		r.Header.Set("Origin", origin)
		r.Header.Set("Sec-Fetch-Site", fetchSite)
		r.Header.Set("Authorization", authorization)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	if got := request("localhost:8080", "", "", ""); got != 503 {
		t.Fatal("MCP should be off by default, even in MCP mode", got)
	}
	assertRedirect(t, setupRequest(app, "POST", "/settings/mcp", url.Values{"enabled": {"on"}}), "/settings#mcp")
	for _, tc := range []struct {
		host, origin, fetchSite, authorization string
		status                                 int
	}{
		{"127.0.0.1:8080", "", "", "", 200},
		{"localhost:8080", "", "", "", 200},
		{"[::1]:8080", "", "", "", 200},
		{"localhost:8080", "", "", "Bearer unused-legacy-token", 200},
		{"evil.example", "", "", "", 403},
		{"192.168.1.2:8080", "", "", "", 403},
		{"localhost:8080", "https://evil.example", "", "", 403},
		{"localhost:8080", "http://localhost:8080", "", "", 403},
		{"localhost:8080", "", "cross-site", "", 403},
	} {
		if got := request(tc.host, tc.origin, tc.fetchSite, tc.authorization); got != tc.status {
			t.Fatalf("%+v: got %d", tc, got)
		}
	}
	if invalid := setupRequest(app, "POST", "/settings/mcp", url.Values{"enabled": {"invalid"}}); invalid.Code != http.StatusBadRequest {
		t.Fatal("invalid switch state accepted")
	}
	assertRedirect(t, setupRequest(app, "POST", "/settings/mcp", url.Values{}), "/settings#mcp")
	if got := request("localhost:8080", "", "", ""); got != 503 {
		t.Fatal("disabled endpoint remained usable", got)
	}
	r := httptest.NewRequest("POST", "/settings/mcp", strings.NewReader("enabled=on"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cross-origin MCP enable allowed")
	}
}

func TestMCPClientCompletesWorkflowWithoutWebApprovalsOrModel(t *testing.T) {
	ctx := context.Background()
	app, service, db := setupFixture(t)
	service.Python = chatValidationRunner{Runner: runnerpython.Runner{Binary: "python3", Secrets: app.Secrets}}
	if binary := os.Getenv("POMPOS_TEST_PYTHON"); binary != "" {
		service.Python = runnerpython.Runner{Binary: binary, Secrets: app.Secrets}
	}
	tracker := &trackedMCPPython{PythonRunner: service.Python}
	service.Python = tracker
	schedules := &scheduleManagerStub{enqueue: func(ctx context.Context, id string) error { return db.EnqueueRun(ctx, id, time.Now()) }}
	app.Scheduler = mcpTestScheduler{schedules}
	if err := service.SelectMode("mcp"); err != nil {
		t.Fatal(err)
	}
	if err := service.SetMCPEnabled(true); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(app.Handler())
	defer httpServer.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	listed, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range listed.Tools {
		names[tool.Name] = true
	}
	if !names["write_script"] || !names["get_chat"] || !names["configure_loading"] || !names["validate_ingestion"] || !names["save_ingestion"] || !names["run_ingestion"] {
		t.Fatalf("tools: %v", names)
	}
	for _, removed := range []string{"respond", "ask_user", "propose_loading", "propose_validation", "finish", "set_secret"} {
		if names[removed] {
			t.Fatalf("MCP exposes web-only or secret tool %s", removed)
		}
	}
	call := func(name string, args map[string]any, wantError bool) map[string]any {
		t.Helper()
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(name, err)
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), "private-mcp-source-fixture") {
			t.Fatal("secret value leaked to MCP")
		}
		if result.IsError != wantError {
			t.Fatalf("%s: error=%v: %+v", name, result.IsError, result.Content)
		}
		if wantError {
			return nil
		}
		bytes, _ := json.Marshal(result.StructuredContent)
		var data map[string]any
		if err := json.Unmarshal(bytes, &data); err != nil {
			t.Fatal(err)
		}
		if name != "get_chat" && (data["session"] != nil || strings.Contains(string(bytes), `"tool_calls"`) || strings.Contains(string(bytes), `"code"`)) {
			t.Fatalf("%s returned full history or code", name)
		}
		return data
	}
	created := call("new_chat", map[string]any{"title": "Fixture rows"}, false)
	id := created["session_id"].(string)
	args := func() map[string]any { return map[string]any{"session_id": id} }
	connections := call("context", args(), false)
	if connections["credentials_path"] != "/secrets" {
		t.Fatal("missing credential entry link")
	}
	if _, ok := connections["result"].(map[string]any); !ok {
		t.Fatal("context must return structured data, not encoded JSON text")
	}
	call("write_script", map[string]any{"session_id": id, "code": 10}, true) // SDK schema validation.
	write := map[string]any{"session_id": id, "name": "Fixture rows", "source": "fixture/rows", "table": "fixture_rows", "schema": "raw", "destination": "local-duckdb", "strategy": "replace", "secret_refs": []string{}, "code": "def fetch(secret, limit):\n    yield {'id': 1, 'name': 'example'}\n"}
	call("write_script", write, false)
	call("validate_ingestion", args(), true)
	call("test_script", args(), false)
	loading := map[string]any{"session_id": id, "cron": "0 6 * * *", "strategy": "replace", "primary_key": []string{}}
	call("configure_loading", map[string]any{"session_id": id, "cron": "bad cron", "strategy": "replace", "primary_key": []string{}}, true)
	call("configure_loading", loading, false)
	call("save_ingestion", args(), true)
	validationArgs := map[string]any{"session_id": id, "limit": 5}
	for _, limit := range []int{-1, -10} {
		call("validate_ingestion", map[string]any{"session_id": id, "limit": limit}, true)
	}
	scriptPath := filepath.Join(service.Dir, id+".py")
	original, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(scriptPath, append(original, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	call("validate_ingestion", validationArgs, false) // Manual edits after the probe are accepted.
	if tracker.validations != 1 {
		t.Fatal("edited script did not execute validation")
	}
	if err := os.WriteFile(scriptPath, original, 0600); err != nil {
		t.Fatal(err)
	}
	tracker.fail = true
	call("validate_ingestion", validationArgs, true)
	call("save_ingestion", args(), true)
	tracker.fail = false
	validation := call("validate_ingestion", validationArgs, false)
	if _, ok := validation["result"].(map[string]any); !ok {
		t.Fatal("validation must return structured checks and preview")
	}
	if tracker.validations != 3 {
		t.Fatal("validation executed more than once per call")
	}
	state := call("get_chat", args(), false)["session"].(map[string]any)
	if len(state["messages"].([]any)) != 0 || state["draft"].(map[string]any)["code"] == "" {
		t.Fatal("get_chat should return the draft without history by default")
	}
	state = call("get_chat", map[string]any{"session_id": id, "include_history": true}, false)["session"].(map[string]any)
	if len(state["messages"].([]any)) == 0 {
		t.Fatal("explicit history request returned no messages")
	}
	// Changes invalidate validation; save cannot bypass the current fingerprint.
	call("configure_loading", loading, false)
	call("save_ingestion", args(), true)
	call("validate_ingestion", validationArgs, false)
	call("write_script", write, false)
	call("save_ingestion", args(), true)
	call("validate_ingestion", validationArgs, true)
	call("test_script", args(), false)
	call("validate_ingestion", validationArgs, false)
	v, _ := service.Load(id)
	if !v.Ready || v.Validation == nil {
		t.Fatal("validation did not finish")
	}
	saved := call("save_ingestion", args(), false)
	ingestionID := saved["ingestion_id"].(string)
	if retry := call("save_ingestion", args(), false); retry["ingestion_id"] != ingestionID {
		t.Fatal("save retry created another ingestion")
	}
	if schedules.item.Schedule != "0 6 * * *" || len(schedules.enqueued) != 0 {
		t.Fatal("save did not register the schedule or ran production early")
	}
	ingestionArgs := map[string]any{"ingestion_id": ingestionID}
	call("get_ingestion", ingestionArgs, false)
	call("list_ingestions", map[string]any{}, false)
	before, _, err := spec.Read(spec.ArtifactPath(app.SpecDir, ingestionID, ".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	call("set_schedule", map[string]any{"ingestion_id": ingestionID, "cron": "not cron"}, true)
	call("set_schedule", map[string]any{"ingestion_id": ingestionID, "cron": ""}, false)
	after, _, err := spec.Read(spec.ArtifactPath(app.SpecDir, ingestionID, ".yaml"))
	if err != nil || after.Schedule != nil || after.Runtime.Script != before.Runtime.Script {
		t.Fatal("schedule update changed code or failed to disable schedule", err)
	}
	if result := call("run_ingestion", ingestionArgs, false); result["queued"] != true {
		t.Fatal("run did not queue")
	}
	if len(schedules.enqueued) != 1 {
		t.Fatal("unexpected number of full loads")
	}
	call("run_ingestion", map[string]any{"ingestion_id": "missing"}, true)
	if binary := os.Getenv("POMPOS_TEST_PYTHON"); binary != "" {
		runner := runnerpython.Runner{Binary: binary, Secrets: app.Secrets}
		executor, err := execution.New(execution.Service{Store: db, Runner: runner, Logger: app.Logger})
		if err != nil {
			t.Fatal(err)
		}
		item, err := db.Get(ctx, ingestionID)
		if err != nil {
			t.Fatal(err)
		}
		if err := executor.Run(ctx, ingestion.Run{IngestionID: ingestionID, SpecPath: item.SpecPath, SpecDigest: item.SpecDigest}); err != nil {
			t.Fatal(err)
		}
		app.Previewer = runner
		result := call("preview_ingestion", ingestionArgs, false)
		if result["preview"].(map[string]any)["total_rows"] != float64(1) {
			t.Fatalf("preview: %#v", result)
		}
		state := call("get_ingestion", ingestionArgs, false)["ingestion"].(map[string]any)
		if state["Status"] != ingestion.StatusSucceeded {
			t.Fatalf("status: %v", state)
		}
	} else {
		call("preview_ingestion", ingestionArgs, true) // Explicit unavailable state without a previewer.
	}
	v, _ = service.Load(id)
	if len(v.SavedIngestions) != 1 {
		t.Fatal("ingestion not published")
	}
	if _, err := os.Stat(spec.ArtifactPath(app.SpecDir, v.SavedIngestions[0].ID, ".yaml")); err != nil {
		t.Fatal(err)
	}
	call("list_chats", map[string]any{}, false)
	write["table"] = "second_rows"
	call("write_script", write, false)
	v, _ = service.Load(id)
	if v.Loading != nil || v.Validation != nil || v.Ready || len(v.SavedIngestions) != 1 {
		t.Fatal("second ingestion reused approvals or lost publication")
	}
	write["secret_refs"] = []string{"source-fixture"}
	call("write_script", write, true)
	// Credential entry is the only non-MCP request in this workflow.
	body := url.Values{"name": {"source-fixture"}, "value": {"private-mcp-source-fixture"}}
	request := httptest.NewRequest("POST", "/secrets", strings.NewReader(body.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	app.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusSeeOther {
		t.Fatalf("credential entry failed: %s", recorder.Body)
	}
	call("context", args(), false)
	call("write_script", write, false)
	// An HTTP reconnect resumes the same durable chat, independent of MCP sessions.
	session.Close()
	session, err = client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	call("get_chat", args(), false)
}

type trackedMCPPython struct {
	agent.PythonRunner
	validations int
	fail        bool
}

func (r *trackedMCPPython) Validate(ctx context.Context, plan compiler.ExecutionPlan, limit int) (runnerpython.ValidationResult, string, error) {
	r.validations++
	if r.fail {
		return runnerpython.ValidationResult{}, "", errors.New("fixture validation failure")
	}
	return r.PythonRunner.Validate(ctx, plan, limit)
}

type mcpTestScheduler struct{ *scheduleManagerStub }

func (s mcpTestScheduler) Validate(cron string) error { return scheduler.ValidateCron(cron) }
