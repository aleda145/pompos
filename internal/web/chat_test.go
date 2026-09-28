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

func TestChatCreatesPythonIngestionAndPreservesItThroughScheduling(t *testing.T) {
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
		switch step {
		case 0:
			call.Function.Name = "write_script"
			b, _ := json.Marshal(agent.Draft{Name: "Stars", Source: "fixture", Table: "stars", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\ndef estimate(secret):\n    return {'rows': 42, 'kind': 'exact', 'basis': 'Fixture source total_count'}\n"})
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
	if e = service.SaveSettings(agent.Settings{Endpoint: model.URL, Model: "test"}); e != nil {
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
	if w.Code != 200 || !strings.Contains(w.Body.String(), "What do you want to bring in?") {
		t.Fatalf("chat page: %d %s", w.Code, w.Body)
	}
	w = request("POST", path+"/publish", "", "")
	if w.Code != 422 {
		t.Fatal("untested publish accepted")
	}
	w = request("POST", path, `{"message":"ingest fixture stars"}`, "application/json")
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
	if w.Code != 303 {
		t.Fatalf("publish: %d %s", w.Code, w.Body)
	}
	detail := w.Header().Get("Location")
	initial, _, err := spec.Read(filepath.Join(app.SpecDir, strings.TrimPrefix(detail, "/ingestions/")+".yaml"))
	if err != nil || initial.Schedule == nil || initial.Schedule.Cron != "0 * * * *" || initial.Schedule.Timezone != "UTC" || initial.Materialization.Strategy != "merge" || len(initial.Materialization.PrimaryKey) != 1 || initial.Materialization.PrimaryKey[0] != "id" {
		t.Fatalf("confirmed loading lost: %#v %v", initial, err)
	}
	if initial.Source.Estimate == nil || initial.Source.Estimate.Rows == nil || *initial.Source.Estimate.Rows != 42 || initial.Source.Estimate.ObservedAt == "" {
		t.Fatal("source estimate was not persisted")
	}
	if schedules.item.Schedule != "0 * * * *" {
		t.Fatal("schedule not registered on publish")
	}
	if len(schedules.enqueued) != 0 {
		t.Fatal("publish prematurely queued full load")
	}
	w = request("GET", detail, "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "fetch(secret, limit)") || !strings.Contains(w.Body.String(), "42 rows at observation time") {
		t.Fatalf("detail: %d %s", w.Code, w.Body)
	}
	w = request("POST", detail+"/schedule", url.Values{"schedule": {"0 6 * * *"}}.Encode(), "application/x-www-form-urlencoded")
	if w.Code != 303 {
		t.Fatalf("schedule: %d %s", w.Code, w.Body)
	}
	id := strings.TrimPrefix(detail, "/ingestions/")
	doc, _, e := spec.Read(filepath.Join(app.SpecDir, id+".yaml"))
	if e != nil {
		t.Fatal(e)
	}
	if doc.Runtime.Engine != "python" || doc.Runtime.Script == "" || doc.Runtime.ScriptDigest == "" || doc.Schedule.Cron != "0 6 * * *" || doc.Source.Estimate == nil || *doc.Source.Estimate.Rows != 42 {
		t.Fatalf("lost Python spec fields: %#v", doc)
	}
	w = request("POST", detail+"/run", "", "")
	if w.Code != 303 || len(schedules.enqueued) != 1 {
		t.Fatalf("run: %d %s", w.Code, w.Body)
	}
	w = request("GET", "/settings/agent", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), model.URL) {
		t.Fatalf("settings: %d %s", w.Code, w.Body)
	}
}

type chatTransport func(*http.Request) (*http.Response, error)

func (f chatTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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
	if err = service.SaveSettings(agent.Settings{Endpoint: "http://model.test/v1", Model: "test"}); err != nil {
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
	app, err := New(App{Agent: service, Store: db, Secrets: db.Secrets(), SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	body := url.Values{"endpoint": {"https://model.example/v1"}, "model": {"test"}, "exa_api_key_ref": {"exa_key"}}.Encode()
	request := httptest.NewRequest("POST", "/settings/agent", strings.NewReader(body))
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
}
