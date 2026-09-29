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

func TestHighJumpChoicesPersistAndSubmitPreciseReplies(t *testing.T) {
	options := []struct{ label, reply string }{
		{"A. Source fields only", "Use height, athlete name, country, venue and date only."},
		{"B. Add date of birth", "Also fetch date of birth from linked athlete pages when available."},
		{"C. Full athlete profile", "Discuss a separate World Athletics athlete-profile ingestion."},
	}
	for _, selected := range options {
		t.Run(selected.label, func(t *testing.T) {
			s := &Service{Dir: t.TempDir()}
			if err := s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test"}); err != nil {
				t.Fatal(err)
			}
			requests := 0
			s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				var request struct {
					ToolChoice string    `json:"tool_choice"`
					Messages   []Message `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				if request.ToolChoice != "required" {
					t.Fatal("model can end with prose choices instead of clickable options")
				}
				call := Call{ID: "ask", Type: "function"}
				call.Function.Name = "ask_user"
				switch requests {
				case 0:
					call.Function.Arguments = `{"kind":"choice","prompt":"Which high jump table first?","options":[{"label":"Men’s records","message":"Ingest men’s high jump world-record progression."},{"label":"Women’s records","message":"Ingest women’s high jump world-record progression."}]}`
				case 1:
					if request.Messages[len(request.Messages)-1].Content != "Ingest men’s high jump world-record progression." {
						t.Fatal("table selection did not reach the model")
					}
					choices := make([]map[string]string, 0, len(options))
					for _, option := range options {
						choices = append(choices, map[string]string{"label": option.label, "message": option.reply})
					}
					args, _ := json.Marshal(map[string]any{"kind": "choice", "prompt": "How much athlete detail? A uses source fields; B adds linked birth dates; C needs a separate ingestion.", "options": choices})
					call.Function.Arguments = string(args)
				case 2:
					if request.Messages[len(request.Messages)-1].Content != selected.reply {
						t.Fatal("detail selection did not send its precise reply")
					}
					call.Function.Arguments = `{"kind":"question","prompt":"What should this ingestion be called?"}`
				default:
					t.Fatal("agent continued past a question")
				}
				requests++
				data, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": Message{Role: "assistant", Calls: []Call{call}}}}})
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}, nil
			})}
			v, err := s.Turn(context.Background(), "high_jump", "Ingest high jump world records")
			if err != nil || v.Pending == nil || requests != 1 {
				t.Fatalf("missing table question: %v", err)
			}
			v, err = s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "option_0", HandoffID: v.Pending.ID}, nil)
			if err != nil || requests != 2 {
				t.Fatalf("missing detail question: %v", err)
			}
			// Loading the persisted session is also what restores buttons after reload.
			v, err = s.Load(v.ID)
			if err != nil || v.Pending == nil || len(v.Pending.Actions) != 4 {
				t.Fatalf("detail choices were not saved: %v", err)
			}
			var actionID string
			for i, option := range options {
				action := v.Pending.Actions[i]
				if action.Label != option.label || action.Message != option.reply {
					t.Fatalf("wrong choice: %#v", action)
				}
				if action.Label == selected.label {
					actionID = action.ID
				}
			}
			if _, err = s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: actionID, HandoffID: v.Pending.ID}, nil); err != nil || requests != 3 {
				t.Fatalf("could not select %s: %v", selected.label, err)
			}
		})
	}
}

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
		var request struct {
			ToolChoice string `json:"tool_choice"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.ToolChoice != "auto" {
			t.Fatal("explanations should allow prose while keeping the existing choices")
		}
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
