package web

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pompos/internal/agent"
	"pompos/internal/ingestion"
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
			b, _ := json.Marshal(agent.Draft{Name: "Stars", Source: "fixture", Table: "stars", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\n"})
			call.Function.Arguments = string(b)
		case 1:
			call.Function.Name = "test_script"
		case 2:
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
	service := &agent.Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db, Python: runnerpython.Runner{Binary: "python3", Secrets: db.Secrets()}}
	if e = service.SaveSettings(agent.Settings{Endpoint: model.URL, Model: "test"}); e != nil {
		t.Fatal(e)
	}
	schedules := &scheduleManagerStub{enqueue: func(ctx context.Context, id string) error { return db.EnqueueRun(ctx, id, time.Now()) }}
	app, e := New(App{Agent: service, Store: db, Secrets: db.Secrets(), Scheduler: schedules, Validator: validatorFunc(func(context.Context, ingestion.Source) error { return nil }), Destination: ingestion.Destination{Type: "duckdb", Path: destination}, SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
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
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ready":true`) {
		t.Fatalf("turn: %d %s", w.Code, w.Body)
	}
	w = request("POST", path+"/publish", "", "")
	if w.Code != 303 {
		t.Fatalf("publish: %d %s", w.Code, w.Body)
	}
	detail := w.Header().Get("Location")
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
	doc, _, e := spec.Read(filepath.Join(app.SpecDir, id+".yaml"))
	if e != nil {
		t.Fatal(e)
	}
	if doc.Runtime.Engine != "python" || doc.Runtime.Script == "" || doc.Runtime.ScriptDigest == "" || doc.Schedule.Cron != "0 6 * * *" {
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
