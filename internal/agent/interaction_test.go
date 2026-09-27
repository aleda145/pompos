package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/store"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestSecretHandoffStreamsPausesAndResumesWithoutLeakingKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db}
	s.SaveSettings(Settings{Endpoint: "http://model.test/v1", Model: "test", APIKeyRef: "provider"})
	db.Secrets().Put(ctx, "provider", []byte("provider-private"))
	var events []string
	requests := 0
	s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		data, _ := io.ReadAll(r.Body)
		if strings.Contains(string(data), "github-private") || strings.Contains(string(data), "provider-private") {
			t.Fatal("key leaked to model")
		}
		if len(events) == 0 || events[len(events)-1] != "thinking" {
			t.Fatal("thinking was not emitted before model request")
		}
		response := `{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"ask","type":"function","function":{"name":"ask_user","arguments":"{\"kind\":\"secret\",\"prompt\":\"GitHub rejected this key (401). Add a GitHub key to retry.\",\"secret_name\":\"github_token\"}"}}]}}]}`
		if requests > 0 {
			if !strings.Contains(string(data), `managed source secret \"github_token\"`) {
				t.Errorf("missing explicit credential reply: %s", data)
			}
			response = `{"choices":[{"message":{"role":"assistant","content":"Retrying with the source key."}}]}`
		}
		requests++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
	})}
	emit := func(event Event) { events = append(events, event.Type) }
	v, err := s.TurnWithEvents(ctx, "chat", Input{Message: "ingest stars"}, emit)
	if err != nil {
		t.Fatal(err)
	}
	if v.Pending == nil || v.Pending.Kind != "secret" || requests != 1 {
		t.Fatalf("did not pause: %#v", v.Pending)
	}
	if strings.Join(events, ",") != "message,thinking,message,tool_start,message" {
		t.Fatalf("events: %v", events)
	}
	loaded, err := s.Load(v.ID)
	if err != nil || loaded.Pending == nil || len(loaded.Pending.Actions) != 2 {
		t.Fatal("handoff was not persisted")
	}
	retry := Input{ActionID: "retry_secret", HandoffID: v.Pending.ID}
	if _, err = s.TurnWithEvents(ctx, v.ID, retry, nil); err == nil {
		t.Fatal("retried without key")
	}
	if err = s.SaveRequestedSecret(ctx, v.ID, v.Pending.ID, "provider", "overwrite"); err == nil {
		t.Fatal("provider key could be overwritten")
	}
	if err = s.SaveRequestedSecret(ctx, v.ID, v.Pending.ID, "github_token", "github-private"); err != nil {
		t.Fatal(err)
	}
	saved, err := s.Load(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	serialized, _ := json.Marshal(saved)
	if strings.Contains(string(serialized), "github-private") {
		t.Fatal("key leaked to conversation")
	}
	events = nil
	v, err = s.TurnWithEvents(ctx, v.ID, retry, emit)
	if err != nil {
		t.Fatal(err)
	}
	if v.Pending != nil || requests != 2 {
		t.Fatal("handoff did not resume")
	}
	if _, err = s.TurnWithEvents(ctx, v.ID, retry, nil); err == nil {
		t.Fatal("stale action accepted")
	}
}
func TestChoiceValidationAndProviderKeyExclusion(t *testing.T) {
	v := Session{Messages: []Message{{Role: "user", Content: "choose"}}}
	if _, err := askUser(&v, `{"kind":"choice","prompt":"Which entity?","options":[{"label":"Stargazers","message":"Load individual stargazers."},{"label":"Star count","message":"Load a star-count snapshot."}]}`); err != nil {
		t.Fatal(err)
	}
	if len(v.Pending.Actions) != 3 || v.Pending.Actions[0].Message != "Load individual stargazers." {
		t.Fatal("missing options")
	}
	if _, err := askUser(&Session{}, `{"kind":"choice","prompt":"Pick","options":[]}`); err == nil {
		t.Fatal("empty choices accepted")
	}
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "db.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Service{Dir: dir, Secrets: db.Secrets(), Destinations: db}
	s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test", APIKeyRef: "provider"})
	db.Secrets().Put(ctx, "provider", []byte("private"))
	db.Secrets().Put(ctx, "github_token", []byte("private"))
	call := Call{}
	call.Function.Name = "context"
	result, err := s.execute(ctx, &Session{}, call)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "provider") || !strings.Contains(result, "github_token") {
		t.Fatalf("source secret list: %s", result)
	}
	call.Function.Name = "ask_user"
	call.Function.Arguments = `{"kind":"secret","prompt":"Key needed","secret_name":"provider"}`
	if _, err = s.execute(ctx, &Session{}, call); err == nil {
		t.Fatal("provider key request accepted")
	}
}

func TestTellMeMoreKeepsChoicesAndRejectsOldActions(t *testing.T) {
	s := &Service{Dir: t.TempDir()}
	if err := s.SaveSettings(Settings{Endpoint: "http://model.test/v1", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	v := Session{ID: "choice", Messages: []Message{{Role: "system", Content: prompt}, {Role: "user", Content: "ingest stars"}}}
	if _, err := askUser(&v, `{"kind":"choice","prompt":"Which entity?","options":[{"label":"Stargazers","message":"Individual stargazers"},{"label":"Star count","message":"A star-count snapshot"}]}`); err != nil {
		t.Fatal(err)
	}
	if err := s.save(v); err != nil {
		t.Fatal(err)
	}
	oldID := v.Pending.ID
	s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"Stargazers gives one row per person; a count gives one snapshot row."}}]}`)), Header: make(http.Header)}, nil
	})}
	updated, err := s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "explain", HandoffID: oldID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Pending == nil || len(updated.Pending.Actions) != 3 || updated.Pending.ID == oldID {
		t.Fatal("explanation did not retain fresh choices")
	}
	if _, err = s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "option_0", HandoffID: oldID}, nil); err == nil {
		t.Fatal("old action accepted")
	}
	persisted, err := s.Load(v.ID)
	if err != nil || persisted.Pending == nil {
		t.Fatal("choices were not saved")
	}
}
