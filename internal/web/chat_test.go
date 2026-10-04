package web

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pompos/internal/agent"
	"pompos/internal/compiler"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestChatCreatesMultipleIngestionsAndPreservesThemThroughScheduling(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	destination := filepath.Join(dir, "out.duckdb")
	db, e := store.Open(ctx, filepath.Join(dir, "db.sqlite"), destination)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	step := 0
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := agent.Message{Role: "assistant"}
		call := agent.Call{ID: "call", Type: "function"}
		call.Function.Arguments = "{}"
		switch step % 6 {
		case 0:
			call.Function.Name = "write_script"
			group := "men"
			if step >= 6 {
				group = "women"
				var payload struct {
					Messages []agent.Message `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					return
				}
				var history strings.Builder
				for _, message := range payload.Messages {
					history.WriteString(message.Content)
					for _, call := range message.Calls {
						history.WriteString(call.Function.Arguments)
					}
				}
				if !strings.Contains(history.String(), "men_records") || !strings.Contains(history.String(), "Saved ingestion") || !strings.Contains(history.String(), "do the same for women") {
					t.Error("continuation lost the previous extractor or save status")
				}
			}
			b, _ := json.Marshal(agent.Draft{Schema: "raw", Name: group + " records", Source: "fixture/" + group, Table: group + "_records", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1, 'group': '" + group + "'}\n"})
			if step >= 12 {
				b, _ = json.Marshal(agent.Draft{Schema: "raw", Name: "Updated men records", Source: "fixture/men", Table: "men_records", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 2, 'group': 'men'}\n"})
			}
			call.Function.Arguments = string(b)
		case 1:
			call.Function.Name = "test_script"
		case 2:
			call.Function.Name = "propose_loading"
			call.Function.Arguments = `{"cron":"0 6 * * *","strategy":"replace","primary_key":[],"reason":"A daily snapshot keeps the current list up to date."}`
		case 3:
			call.Function.Name = "propose_validation"
		case 4:
			call.Function.Name = "finish"
		default:
			m.Content = "Ready to save."
		}
		if call.Function.Name != "" {
			m.Calls = []agent.Call{call}
		}
		step++
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": m}}})
	}))
	defer model.Close()
	service := &agent.Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db, Python: chatValidationRunner{Runner: runnerpython.Runner{Binary: "python3", Secrets: db.Secrets()}}}
	if binary := os.Getenv("POMPOS_TEST_PYTHON"); binary != "" {
		service.Python = runnerpython.Runner{Binary: binary, Secrets: db.Secrets()}
	}
	if e = service.SaveSettings(agent.Settings{Mode: "mcp", MCPEnabled: true, Endpoint: model.URL, Model: "test", ExaSkipped: true, ManualValidation: true}); e != nil {
		t.Fatal(e)
	}
	schedules := &scheduleManagerStub{enqueue: func(ctx context.Context, id string) error { return db.EnqueueRun(ctx, id, time.Now()) }}
	app, e := New(App{Agent: service, Store: db, Secrets: db.Secrets(), Scheduler: schedules, SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
	if e != nil {
		t.Fatal(e)
	}
	handler := app.Handler()
	request := func(method, path, body, contentType string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", contentType)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	w := request("GET", "/ingestions/new", "", "")
	if w.Code != 303 || !strings.HasPrefix(w.Header().Get("Location"), "/chat/") {
		t.Fatalf("new chat: %d %s", w.Code, w.Body)
	}
	path := w.Header().Get("Location")
	w = request("GET", path, "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "New ingestion") {
		t.Fatalf("chat page: %d %s", w.Code, w.Body)
	}
	w = request("POST", path+"/publish", "", "")
	if w.Code != 422 {
		t.Fatal("untested publish accepted")
	}
	w = request("POST", path, `{"message":"ingest men's high jump records"}`, "application/json")
	if w.Code != 200 {
		t.Fatalf("turn: %d %s", w.Code, w.Body)
	}
	var reply struct {
		Session agent.Session `json:"session"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Session.Pending == nil || reply.Session.Pending.Kind != "loading" {
		t.Fatalf("missing loading proposal: %s", w.Body)
	}
	if blocked := request("POST", path+"/publish", "", ""); blocked.Code != 422 {
		t.Fatal("unconfirmed loading settings published")
	}
	accepted, _ := json.Marshal(agent.Input{ActionID: "accept_loading", HandoffID: reply.Session.Pending.ID, Loading: &agent.Loading{Cron: "0 * * * *", Strategy: "merge", PrimaryKey: []string{"id"}}})
	w = request("POST", path, string(accepted), "application/json")
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Session.Pending == nil || reply.Session.Pending.Kind != "validation" || reply.Session.Ready {
		t.Fatalf("missing validation proposal: %s", w.Body)
	}
	if blocked := request("POST", path+"/publish", "", ""); blocked.Code != 422 {
		t.Fatal("unvalidated draft published")
	}
	accepted, _ = json.Marshal(agent.Input{ActionID: "accept_validation", HandoffID: reply.Session.Pending.ID})
	w = request("POST", path, string(accepted), "application/json")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ready":true`) {
		t.Fatalf("confirmation: %d %s", w.Code, w.Body)
	}
	w = request("POST", path+"/publish", "", "")
	if w.Code != 303 || w.Header().Get("Location") != path {
		t.Fatalf("publish: %d %s", w.Code, w.Body)
	}
	chatID := strings.TrimPrefix(path, "/chat/")
	saved, err := service.Load(chatID)
	if err != nil || len(saved.SavedIngestions) != 1 || saved.PublishedID != "local-duckdb/raw/men_records" {
		t.Fatalf("ingestion was not saved separately from its chat: %#v %v", saved.SavedIngestions, err)
	}
	detail := "/ingestions/" + saved.PublishedID
	initial, _, err := spec.Read(spec.ArtifactPath(app.SpecDir, strings.TrimPrefix(detail, "/ingestions/"), ".yaml"))
	if err != nil || initial.Schedule == nil || initial.Schedule.Cron != "0 * * * *" || initial.Schedule.Timezone != "UTC" || initial.Materialization.Strategy != "merge" || len(initial.Materialization.PrimaryKey) != 1 || initial.Materialization.PrimaryKey[0] != "id" {
		t.Fatalf("confirmed loading lost: %#v %v", initial, err)
	}
	if initial.Destination.Schema != "raw" || filepath.Base(initial.Runtime.Script) != "men_records.py" {
		t.Fatalf("script filename does not match the destination table: %q", initial.Runtime.Script)
	}
	if schedules.item.Schedule != "0 * * * *" {
		t.Fatal("schedule not registered on publish")
	}
	if len(schedules.enqueued) != 0 {
		t.Fatal("publish prematurely queued full load")
	}
	w = request("GET", detail, "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "fetch(secret, limit)") {
		t.Fatalf("detail: %d %s", w.Code, w.Body)
	}
	w = request("POST", detail+"/schedule", url.Values{"schedule": {"0 6 * * *"}}.Encode(), "application/x-www-form-urlencoded")
	if w.Code != 303 {
		t.Fatalf("schedule: %d %s", w.Code, w.Body)
	}
	id := strings.TrimPrefix(detail, "/ingestions/")
	doc, _, e := spec.Read(spec.ArtifactPath(app.SpecDir, id, ".yaml"))
	if e != nil {
		t.Fatal(e)
	}
	if doc.Destination.Schema != "raw" || doc.Runtime.Engine != "python" || doc.Runtime.Script == "" || doc.Schedule.Cron != "0 6 * * *" {
		t.Fatalf("lost Python spec fields: %#v", doc)
	}
	w = request("POST", detail+"/run", "", "")
	if w.Code != 303 || len(schedules.enqueued) != 1 {
		t.Fatalf("run: %d %s", w.Code, w.Body)
	}
	w = request("GET", "/settings", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), model.URL) {
		t.Fatalf("settings: %d %s", w.Code, w.Body)
	}
	// Save retries must not create another ingestion.
	w = request("POST", path+"/publish", "", "")
	repeated, err := service.Load(chatID)
	if w.Code != 303 || err != nil || len(repeated.SavedIngestions) != 1 || repeated.PublishedID != saved.PublishedID {
		t.Fatal("duplicate publication created another ingestion")
	}
	firstYAMLPath := spec.ArtifactPath(app.SpecDir, saved.PublishedID, ".yaml")
	firstYAML, err := os.ReadFile(firstYAMLPath)
	if err != nil {
		t.Fatal(err)
	}
	firstScript, err := os.ReadFile(initial.Runtime.Script)
	if err != nil {
		t.Fatal(err)
	}
	// A restarted service can continue the same conversation with a new draft.
	app.Agent = &agent.Service{Dir: service.Dir, Secrets: service.Secrets, Destinations: service.Destinations, Python: service.Python}
	w = request("POST", path, `{"message":"do the same for women"}`, "application/json")
	reply.Session = agent.Session{}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Session.Pending == nil || reply.Session.Pending.Kind != "loading" {
		t.Fatalf("could not continue saved chat: %s", w.Body)
	}
	if reply.Session.PublishedID != "" || reply.Session.DraftID != "" || reply.Session.Loading != nil || reply.Session.Validation != nil || reply.Session.Ready || reply.Session.Draft.Table != "women_records" || len(reply.Session.SavedIngestions) != 1 {
		t.Fatal("new draft reused publication or approvals from the previous ingestion")
	}
	if blocked := request("POST", path+"/publish", "", ""); blocked.Code != 422 {
		t.Fatal("new draft published without fresh approvals")
	}
	accepted, _ = json.Marshal(agent.Input{ActionID: "accept_loading", HandoffID: reply.Session.Pending.ID})
	w = request("POST", path, string(accepted), "application/json")
	reply.Session = agent.Session{}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Session.Pending == nil || reply.Session.Pending.Kind != "validation" {
		t.Fatalf("second validation proposal: %s", w.Body)
	}
	accepted, _ = json.Marshal(agent.Input{ActionID: "accept_validation", HandoffID: reply.Session.Pending.ID})
	w = request("POST", path, string(accepted), "application/json")
	if !strings.Contains(w.Body.String(), `"ready":true`) {
		t.Fatalf("second validation: %s", w.Body)
	}
	w = request("POST", path+"/publish", "", "")
	if w.Code != 303 || w.Header().Get("Location") != path {
		t.Fatalf("second publish: %d %s", w.Code, w.Body)
	}
	both, err := app.Agent.Load(chatID)
	if err != nil || len(both.SavedIngestions) != 2 || both.PublishedID != "local-duckdb/raw/women_records" {
		t.Fatalf("expected two independent ingestions: %#v %v", both.SavedIngestions, err)
	}
	second, _, err := spec.Read(spec.ArtifactPath(app.SpecDir, both.PublishedID, ".yaml"))
	if err != nil || second.Source.Table != "women_records" || second.Destination.Object != "women_records" || second.Runtime.Script == initial.Runtime.Script {
		t.Fatalf("second ingestion is not independent: %#v %v", second, err)
	}
	for _, ingestion := range both.SavedIngestions {
		if _, err := db.Get(ctx, ingestion.ID); err != nil {
			t.Fatal(err)
		}
	}
	unchangedYAML, _ := os.ReadFile(firstYAMLPath)
	unchangedScript, _ := os.ReadFile(initial.Runtime.Script)
	if string(unchangedYAML) != string(firstYAML) || string(unchangedScript) != string(firstScript) {
		t.Fatal("second ingestion modified the first ingestion's files or schedule")
	}
	w = request("GET", path, "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "saved-ingestions") || !strings.Contains(w.Body.String(), both.PublishedID) || !strings.Contains(w.Body.String(), saved.PublishedID) || !strings.Contains(w.Body.String(), "New chat") {
		t.Fatal("chat reload lost saved ingestions or the new-chat action")
	}
	// Editing an earlier ingestion updates its files and schedule in place.
	beforeUpdate, err := db.Get(ctx, saved.PublishedID)
	if err != nil {
		t.Fatal(err)
	}
	w = request("POST", path, `{"message":"update the men's records extractor"}`, "application/json")
	reply.Session = agent.Session{}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Session.Pending == nil || reply.Session.Pending.Kind != "loading" {
		t.Fatalf("update loading proposal: %s", w.Body)
	}
	if blocked := request("POST", path+"/publish", "", ""); blocked.Code != 422 {
		t.Fatal("update published without fresh validation")
	}
	accepted, _ = json.Marshal(agent.Input{ActionID: "accept_loading", HandoffID: reply.Session.Pending.ID, Loading: &agent.Loading{Strategy: "replace"}})
	w = request("POST", path, string(accepted), "application/json")
	reply.Session = agent.Session{}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || reply.Session.Pending == nil || reply.Session.Pending.Kind != "validation" {
		t.Fatalf("update validation proposal: %s", w.Body)
	}
	accepted, _ = json.Marshal(agent.Input{ActionID: "accept_validation", HandoffID: reply.Session.Pending.ID})
	w = request("POST", path, string(accepted), "application/json")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ready":true`) {
		t.Fatalf("update validation: %d %s", w.Code, w.Body)
	}
	for attempt := 0; attempt < 2; attempt++ {
		w = request("POST", path+"/publish", "", "")
		if w.Code != 303 {
			t.Fatalf("update publish: %d %s", w.Code, w.Body)
		}
	}
	updated, err := app.Agent.Load(chatID)
	if err != nil || updated.PublishedID != saved.PublishedID || len(updated.SavedIngestions) != 2 || updated.SavedIngestions[0].Name != "Updated men records" {
		t.Fatalf("update duplicated or lost saved ingestion: %#v %v", updated, err)
	}
	updatedDoc, updatedYAML, err := spec.Read(firstYAMLPath)
	if err != nil || updatedDoc.Metadata.Name != "Updated men records" || updatedDoc.Schedule != nil || updatedDoc.Materialization.Strategy != "replace" {
		t.Fatalf("update did not persist configuration: %#v %v", updatedDoc, err)
	}
	updatedScript, err := os.ReadFile(initial.Runtime.Script)
	if err != nil || !strings.Contains(string(updatedScript), "'id': 2") {
		t.Fatalf("update did not persist script: %s %v", updatedScript, err)
	}
	afterUpdate, err := db.Get(ctx, saved.PublishedID)
	if err != nil || afterUpdate.SpecDigest != spec.Digest(updatedYAML) || afterUpdate.Status != beforeUpdate.Status || afterUpdate.LastError != beforeUpdate.LastError {
		t.Fatalf("update lost run state or spec reference: %#v %v", afterUpdate, err)
	}
	if schedules.item.ID != saved.PublishedID || schedules.item.Schedule != "" || len(schedules.enqueued) != 1 {
		t.Fatal("update did not disable schedule or unexpectedly queued a run")
	}
}

type chatTransport func(*http.Request) (*http.Response, error)

func (f chatTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestChatNavigationListsAndReopensSavedConversation(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(context.Background(), filepath.Join(dir, "metadata.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service := &agent.Service{Dir: filepath.Join(dir, "agent")}
	if err := service.SaveSettings(agent.Settings{Endpoint: "http://model.test", Model: "test", ExaSkipped: true}); err != nil {
		t.Fatal(err)
	}
	app, err := New(App{Agent: service, Store: db, Secrets: db.Secrets(), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		app.Handler().ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		return response
	}
	empty := get("/chat")
	if empty.Code != 200 || !strings.Contains(empty.Body.String(), "No chats.") || !strings.Contains(empty.Body.String(), `href="/ingestions/new"`) {
		t.Fatalf("empty list: %d %s", empty.Code, empty.Body)
	}
	v := agent.Session{ID: "high_jump", Messages: []agent.Message{{Role: "user", Content: "High jump <script>alert(1)</script>"}}, SavedIngestions: []agent.SavedIngestion{{ID: "men", Name: "Men", Table: "men_records"}, {ID: "women", Name: "Women", Table: "women_records"}}}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.WriteFile(filepath.Join(service.Dir, v.ID+".json"), data); err != nil {
		t.Fatal(err)
	}
	listed := get("/chat")
	if listed.Code != 200 || !strings.Contains(listed.Body.String(), `href="/chat/high_jump"`) || !strings.Contains(listed.Body.String(), `<td class="mono">2</td>`) || !strings.Contains(listed.Body.String(), "High jump &lt;script&gt;") || strings.Contains(listed.Body.String(), "<script>alert(1)</script>") {
		t.Fatalf("chat list: %d %s", listed.Code, listed.Body)
	}
	reopened := get("/chat/high_jump")
	if reopened.Code != 200 || !strings.Contains(reopened.Body.String(), `data-id="high_jump"`) || !strings.Contains(reopened.Body.String(), "women_records") {
		t.Fatalf("reopen conversation: %d %s", reopened.Code, reopened.Body)
	}
	for _, path := range []string{"/", "/chat", "/chat/high_jump"} {
		if page := get(path); page.Code != 200 || !strings.Contains(page.Body.String(), `<a href="/chat">Chat</a>`) {
			t.Fatalf("Chat missing from header on %s", path)
		}
	}
}

func TestChatFlushesStepsBeforeModelCompletesAndOffersSecretActions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "db.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	requests := 0
	service := &agent.Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db}
	service.Client = &http.Client{Transport: chatTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		data, _ := io.ReadAll(r.Body)
		if strings.Contains(string(data), "private-github-value") {
			t.Error("secret leaked to model")
		}
		response := `{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"key","type":"function","function":{"name":"ask_user","arguments":"{\"kind\":\"secret\",\"prompt\":\"Add a GitHub key to retry.\",\"secret_name\":\"github_token\"}"}}]}}]}`
		if requests == 1 {
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		} else {
			response = `{"choices":[{"message":{"role":"assistant","content":"Continuing with the saved key."}}]}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
	})}
	if err = service.SaveSettings(agent.Settings{Endpoint: "http://model.test/v1", Model: "test", ExaSkipped: true}); err != nil {
		t.Fatal(err)
	}
	app, err := New(App{Agent: service, Store: db, Secrets: db.Secrets(), SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequest("POST", server.URL+"/chat/stream", strings.NewReader(`{"message":"Ingest stars </script><img src=x>"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("response was not flushed before model completion: %v", err)
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var event struct {
		Type    string        `json:"type"`
		Session agent.Session `json:"session"`
		Message agent.Message `json:"message"`
	}
	if err = decoder.Decode(&event); err != nil || event.Type != "message" || event.Message.Role != "user" {
		t.Fatalf("first streamed event: %#v %v", event, err)
	}
	if err = decoder.Decode(&event); err != nil || event.Type != "thinking" {
		t.Fatalf("thinking not streamed: %#v %v", event, err)
	}
	close(release)
	var types []string
	for {
		if err = decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		types = append(types, event.Type)
		if event.Type == "done" {
			break
		}
	}
	if strings.Join(types, ",") != "message,tool_start,message,done" || event.Session.Pending == nil {
		t.Fatalf("events %v, session %#v", types, event.Session)
	}
	for _, message := range event.Session.Messages {
		if message.Role == "system" {
			t.Fatal("system instructions leaked to UI")
		}
	}
	pending := event.Session.Pending
	request := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		app.Handler().ServeHTTP(w, r)
		return w
	}
	page := request("GET", "/chat/stream", "")
	if strings.Contains(page.Body.String(), "</script><img src=x>") {
		t.Fatal("unsafe chat bootstrap HTML")
	}
	marker := `<script id="chat-state" type="application/json">`
	parts := strings.SplitN(page.Body.String(), marker, 2)
	if len(parts) != 2 {
		t.Fatal("missing conversation bootstrap")
	}
	stateJSON := strings.SplitN(parts[1], "</script>", 2)[0]
	var boot agent.Session
	if err = json.Unmarshal([]byte(stateJSON), &boot); err != nil || boot.Pending == nil {
		t.Fatalf("invalid bootstrap JSON: %s, %v", stateJSON, err)
	}
	body, _ := json.Marshal(map[string]string{"name": "github_token", "value": "private-github-value", "handoff_id": pending.ID})
	saved := request("POST", "/chat/stream/secret", string(body))
	if saved.Code != 204 || saved.Body.Len() != 0 {
		t.Fatalf("save secret: %d %s", saved.Code, saved.Body)
	}
	body, _ = json.Marshal(agent.Input{ActionID: "retry_secret", HandoffID: pending.ID})
	resumed := request("POST", "/chat/stream", string(body))
	if resumed.Code != 200 || !strings.Contains(resumed.Body.String(), "Continuing with the saved key.") || strings.Contains(resumed.Body.String(), "private-github-value") {
		t.Fatalf("resume: %d %s", resumed.Code, resumed.Body)
	}
}

// Loading itself is covered by the runner's real dlt integration tests.
type chatValidationRunner struct{ runnerpython.Runner }

func (r chatValidationRunner) Validate(ctx context.Context, plan compiler.ExecutionPlan, limit int) (runnerpython.ValidationResult, string, error) {
	result := runnerpython.ValidationResult{SampleCount: 1, FirstLoadRows: 1, SecondLoadRows: 1}
	b, _ := json.Marshal(result)
	return result, "POMPOS_VALIDATION_RESULT=" + string(b), nil
}

func TestExaSettingsPersistSecretReference(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "db.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Secrets().Put(ctx, "exa_key", []byte("private-exa-value")); err != nil {
		t.Fatal(err)
	}
	service := &agent.Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets()}
	if err := service.SaveSettings(agent.Settings{Mode: "agent", Endpoint: "https://model.example/v1", Model: "test", APIKeyRef: "model_key"}); err != nil {
		t.Fatal(err)
	}
	app, err := New(App{Agent: service, Store: db, Secrets: db.Secrets(), SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	body := url.Values{"section": {"search"}, "exa_api_key_ref": {"exa_key"}}.Encode()
	request := httptest.NewRequest("POST", "/settings", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), "Exa API key secret") || !strings.Contains(response.Body.String(), `value="exa_key" selected`) || strings.Contains(response.Body.String(), "private-exa-value") {
		t.Fatalf("settings: %d %s", response.Code, response.Body)
	}
	restarted := &agent.Service{Dir: service.Dir}
	settings, err := restarted.Settings()
	if err != nil || settings.ExaAPIKeyRef != "exa_key" {
		t.Fatalf("Exa reference lost: %#v %v", settings, err)
	}
	if settings.Endpoint != "https://model.example/v1" || settings.Model != "test" || settings.APIKeyRef != "model_key" || settings.Mode != "agent" {
		t.Fatalf("saving search changed agent settings: %+v", settings)
	}
	response = setupRequest(app, "POST", "/settings", url.Values{"section": {"agent"}, "endpoint": {settings.Endpoint}, "model": {"updated"}, "api_key_ref": {settings.APIKeyRef}})
	settings, err = restarted.Settings()
	if response.Code != 200 || err != nil || settings.Model != "updated" || settings.ExaAPIKeyRef != "exa_key" {
		t.Fatalf("saving agent changed search settings: %+v %v", settings, err)
	}
	response = setupRequest(app, "POST", "/settings", url.Values{"section": {"invalid"}})
	if response.Code != http.StatusBadRequest {
		t.Fatal("unknown settings section accepted")
	}
}
