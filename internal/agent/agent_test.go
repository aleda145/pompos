package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestLoopRepairsFailedProbeAndPersistsTestedArtifact(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, e := store.Open(ctx, filepath.Join(dir, "db.sqlite"), filepath.Join(dir, "out.duckdb"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	service := &Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db, Python: &validationRunner{Runner: runnerpython.Runner{Binary: "python3", Secrets: db.Secrets()}}}
	if binary := os.Getenv("POMPOS_TEST_PYTHON"); binary != "" {
		service.Python = runnerpython.Runner{Binary: binary, Secrets: db.Secrets()}
	}
	good := Draft{Name: "Stars", Source: "fixture", Table: "stars", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\n"}
	bad := good
	bad.Code = "def fetch(secret, limit):\n    raise ValueError('retry this source')\n    yield {}\n"
	step := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path %s", r.URL.Path)
		}
		var request struct {
			Messages []Message `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&request)
		m := Message{Role: "assistant"}
		name := ""
		args := "{}"
		switch step {
		case 0:
			name = "context"
		case 1:
			name = "write_script"
			b, _ := json.Marshal(bad)
			args = string(b)
		case 2:
			name = "finish"
		case 3:
			if !strings.Contains(request.Messages[len(request.Messages)-1].Content, "must pass") {
				t.Error("finish before test accepted")
			}
			name = "test_script"
		case 4:
			if !strings.Contains(request.Messages[len(request.Messages)-1].Content, "retry this source") {
				t.Error("failure not fed back to model")
			}
			name = "write_script"
			b, _ := json.Marshal(good)
			args = string(b)
		case 5:
			name = "test_script"
		case 6:
			if !strings.Contains(request.Messages[len(request.Messages)-1].Content, "sample_count") {
				t.Error("probe result missing")
			}
			name = "finish"
		case 7:
			if !strings.Contains(request.Messages[len(request.Messages)-1].Content, "propose_loading") {
				t.Error("finish accepted unconfirmed loading settings")
			}
			name = "propose_loading"
			args = `{"cron":"0 6 * * *","strategy":"replace","primary_key":[],"reason":"A daily current-state snapshot is sufficient; replace reflects removed stars."}`
		case 8:
			name = "finish"
		case 9:
			if !strings.Contains(request.Messages[len(request.Messages)-1].Content, "propose_validation") {
				t.Error("finish accepted without validation")
			}
			name = "propose_validation"
		case 10:
			name = "finish"
		default:
			m.Content = "The source probe passed. Review and save."
		}
		if name != "" {
			c := Call{ID: fmt.Sprint(step), Type: "function"}
			c.Function.Name = name
			c.Function.Arguments = args
			m.Calls = []Call{c}
		}
		step++
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": m}}})
	}))
	defer server.Close()
	if e = service.SaveSettings(Settings{Endpoint: server.URL + "/v1", Model: "test"}); e != nil {
		t.Fatal(e)
	}
	v, e := service.Turn(ctx, "session1", "ingest stars")
	if e != nil {
		t.Fatal(e)
	}
	if v.Ready || v.Pending == nil || v.Pending.Kind != "loading" || v.Loading != nil {
		t.Fatal("loading settings did not pause for confirmation")
	}
	v, e = service.TurnWithEvents(ctx, v.ID, Input{ActionID: "accept_loading", HandoffID: v.Pending.ID}, nil)
	if e != nil {
		t.Fatal(e)
	}
	if v.Ready || v.Pending == nil || v.Pending.Kind != "validation" {
		t.Fatal("did not wait for validation approval")
	}
	v, e = service.TurnWithEvents(ctx, v.ID, Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !v.Ready || v.TestedDigest == "" || step != 12 {
		t.Fatalf("not ready: %#v, steps %d", v, step)
	}
	restarted := &Service{Dir: service.Dir, Secrets: db.Secrets(), Destinations: db}
	loaded, e := restarted.Load("session1")
	if e != nil || !loaded.Ready || len(loaded.Messages) != len(v.Messages) {
		t.Fatal("conversation was not persisted")
	}
	calls := 0
	persist := func(doc spec.Ingestion) error {
		calls++
		b, e := spec.Marshal(doc)
		if e != nil {
			return e
		}
		roundtrip, e := spec.Parse(b)
		if e != nil {
			return e
		}
		if roundtrip.Schedule == nil || roundtrip.Schedule.Cron != "0 6 * * *" || roundtrip.Schedule.Timezone != "UTC" || roundtrip.Materialization.Strategy != "replace" {
			t.Fatal("confirmed loading settings lost in YAML")
		}
		if roundtrip.Runtime.ScriptDigest != v.TestedDigest {
			t.Fatal("lost script digest")
		}
		return nil
	}
	path := filepath.Join(dir, "ingestions", "session1.py")
	if _, e = service.Publish(ctx, v.ID, path, persist); e != nil {
		t.Fatal(e)
	}
	if _, e = service.Publish(ctx, v.ID, path, persist); e != nil {
		t.Fatal(e)
	}
	if calls != 1 {
		t.Fatal("duplicate publish")
	}
	data, e := os.ReadFile(path)
	if e != nil || spec.Digest(data) != v.TestedDigest {
		t.Fatal("published different code")
	}
}
func TestStepLimitAndResume(t *testing.T) {
	dir := t.TempDir()
	s := &Service{Dir: dir}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := Call{ID: "call", Type: "function"}
		c.Function.Name = "unknown"
		c.Function.Arguments = "{}"
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": Message{Role: "assistant", Calls: []Call{c}}}}})
	}))
	defer server.Close()
	if e := s.SaveSettings(Settings{Endpoint: server.URL, Model: "test"}); e != nil {
		t.Fatal(e)
	}
	v, e := s.Turn(context.Background(), "bounded", "go")
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(v.Messages[len(v.Messages)-1].Content, "step limit") {
		t.Fatal("missing continuation")
	}
	v, e = s.Turn(context.Background(), "bounded", "continue")
	if e != nil || len(v.Messages) != 53 {
		t.Fatalf("resume: messages %d error %v", len(v.Messages), e)
	}
}

func TestToolSchemasUseArraysForRequired(t *testing.T) {
	data, err := json.Marshal(toolsDefinition())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"required":null`) {
		t.Fatal("JSON Schema required must be an array")
	}
}
