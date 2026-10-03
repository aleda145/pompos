package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestLoadingValidation(t *testing.T) {
	for _, options := range []Loading{
		{Cron: "0 6 * * *", Strategy: "replace"},
		{Cron: "", Strategy: "append"},
		{Cron: "0 * * * *", Strategy: "merge", PrimaryKey: []string{"id"}},
	} {
		if err := options.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, options := range []Loading{
		{Cron: "every day", Strategy: "replace"},
		{Cron: "0 0 6 * * *", Strategy: "replace"},
		{Cron: "90 * * * *", Strategy: "replace"},
		{Cron: "CRON_TZ=Europe/Stockholm 0 6 * * *", Strategy: "replace"},
		{Strategy: "merge"},
		{Strategy: "scd2", PrimaryKey: []string{"id"}},
		{Strategy: "merge", PrimaryKey: []string{"id", "id"}},
	} {
		if err := options.Validate(); err == nil {
			t.Errorf("invalid options accepted: %#v", options)
		}
	}
}

func TestLoadingEditsAreConfirmedPersistedAndAuthoritative(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "db.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db}
	if err = s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	v := Session{ID: "loading", Messages: []Message{{Role: "system", Content: prompt}}, Draft: &Draft{Name: "Stars", Source: "fixture", Table: "stars", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\n"}}
	data := []byte(runnerpython.Wrap(v.Draft.Code))
	if err = WriteFile(s.scriptPath(v.ID), data); err != nil {
		t.Fatal(err)
	}
	v.Probed = true
	if _, err = proposeLoading(&v, `{"cron":"0 6 * * *","strategy":"replace","primary_key":[],"reason":"Daily refresh of the current list."}`); err != nil {
		t.Fatal(err)
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"Settings confirmed."}}]}`)), Header: make(http.Header)}, nil
	})}
	input := Input{ActionID: "accept_loading", HandoffID: v.Pending.ID, Loading: &Loading{Cron: "not cron", Strategy: "replace"}}
	if _, err = s.TurnWithEvents(ctx, v.ID, input, nil); err == nil {
		t.Fatal("bad cron accepted")
	}
	persisted, _ := s.Load(v.ID)
	if persisted.Loading != nil || persisted.Pending == nil {
		t.Fatal("invalid settings changed saved state")
	}
	input.Loading = &Loading{Cron: "", Strategy: "merge", PrimaryKey: []string{"id"}}
	v, err = s.TurnWithEvents(ctx, v.ID, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Loading == nil || v.Draft.Schedule != "" || v.Draft.Strategy != "merge" || !v.Probed {
		t.Fatal("settings were not applied or unnecessarily discarded probe")
	}
	loaded, err := s.Load(v.ID)
	if err != nil || loaded.Loading == nil || loaded.Loading.Strategy != "merge" {
		t.Fatal("confirmed settings not persisted")
	}
	// Rewriting the same extractor must not quietly override the user's settings.
	draft := *v.Draft
	draft.Strategy = "append"
	draft.PrimaryKey = nil
	args, _ := json.Marshal(draft)
	call := Call{}
	call.Function.Name = "write_script"
	call.Function.Arguments = string(args)
	if _, err = s.execute(ctx, &v, call); err != nil {
		t.Fatal(err)
	}
	if v.Draft.Strategy != "merge" || len(v.Draft.PrimaryKey) != 1 {
		t.Fatal("model overrode confirmed settings")
	}
	data, _ = os.ReadFile(s.scriptPath(v.ID))
	v.Probed = true
	// This unit test focuses on settings persistence; the approval flow is tested separately.
	_, fingerprint, err := s.validationPlan(ctx, &v)
	if err != nil {
		t.Fatal(err)
	}
	v.Validation = &Validation{Fingerprint: fingerprint}
	call.Function.Name = "finish"
	call.Function.Arguments = "{}"
	if _, err = s.execute(ctx, &v, call); err != nil {
		t.Fatal(err)
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Publish(ctx, v.ID, filepath.Join(dir, "ingestions"), func(_ string, doc spec.Ingestion) error {
		if doc.Schedule != nil || doc.Materialization.Strategy != "merge" {
			t.Fatal("manual or merge choice lost")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A different source needs its own confirmation.
	draft.Source = "different-source"
	args, _ = json.Marshal(draft)
	call.Function.Name = "write_script"
	call.Function.Arguments = string(args)
	if _, err = s.execute(ctx, &v, call); err != nil {
		t.Fatal(err)
	}
	if v.Loading != nil {
		t.Fatal("reused confirmation for a different source")
	}
}

func TestRevisedLoadingProposalNeedsFreshConfirmation(t *testing.T) {
	v := Session{Ready: true, Loading: &Loading{Cron: "0 6 * * *", Strategy: "replace"}}
	if _, err := proposeLoading(&v, `{"cron":"0 * * * *","strategy":"append","primary_key":[],"reason":"Hourly history needs appending timestamped observations."}`); err != nil {
		t.Fatal(err)
	}
	if v.Ready || v.Loading != nil || v.Pending == nil || v.Pending.Loading.Cron != "0 * * * *" {
		t.Fatal("revised settings reused prior confirmation")
	}
}

func TestConfirmedLoadingProposalPreservesState(t *testing.T) {
	for _, loading := range []Loading{
		{Cron: "0 6 * * 1", Strategy: "replace"},
		{Strategy: "merge", PrimaryKey: []string{"id"}},
	} {
		t.Run(loading.Strategy, func(t *testing.T) {
			validation := &Validation{Fingerprint: "validated"}
			pending := &Handoff{ID: "validation", Kind: "validation"}
			v := Session{Ready: true, Probed: true, Loading: &loading, Validation: validation, Pending: pending}
			args, err := json.Marshal(struct {
				Loading
				Reason string `json:"reason"`
			}{loading, "Confirmed by user."})
			if err != nil {
				t.Fatal(err)
			}
			result, err := proposeLoading(&v, string(args))
			if err != nil || !strings.Contains(result, "already confirmed") {
				t.Fatalf("duplicate proposal did not acknowledge approval: %q, %v", result, err)
			}
			if !v.Ready || !v.Probed || v.Loading != &loading || v.Validation != validation || v.Pending != pending {
				t.Fatal("duplicate proposal changed approved state")
			}
		})
	}
}

func TestLoadingApprovalContinuesWithoutReconfirmation(t *testing.T) {
	for _, modelFailure := range []bool{false, true} {
		name := "repeated proposal"
		if modelFailure {
			name = "model failure"
		}
		t.Run(name, func(t *testing.T) {
			s, runner, v := validationFixture(t)
			v.Loading = nil
			if _, err := proposeLoading(&v, `{"cron":"0 6 * * *","strategy":"replace","primary_key":[],"reason":"Daily snapshot."}`); err != nil {
				t.Fatal(err)
			}
			if err := s.save(v); err != nil {
				t.Fatal(err)
			}
			calls := 0
			s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if modelFailure {
					return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("model unavailable")), Header: make(http.Header)}, nil
				}
				call := Call{ID: "repeat", Type: "function"}
				switch calls {
				case 1:
					call.Function.Name = "propose_loading"
					call.Function.Arguments = `{"cron":"0 6 * * 1","strategy":"replace","primary_key":[],"reason":"Confirmed by user: weekly full replace on Monday 06:00 UTC."}`
				case 2:
					call.Function.Name = "propose_validation"
					call.Function.Arguments = `{"limit":500}`
				default:
					t.Fatal("unexpected extra model request")
				}
				body, err := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": Message{Role: "assistant", Calls: []Call{call}}}}})
				if err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
			})}
			approval := Input{ActionID: "accept_loading", HandoffID: v.Pending.ID, Loading: &Loading{Cron: "0 6 * * 1", Strategy: "replace"}}
			_, err := s.TurnWithEvents(context.Background(), v.ID, approval, nil)
			if (err != nil) != modelFailure {
				t.Fatalf("unexpected turn error: %v", err)
			}
			v, err = s.Load(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if v.Loading == nil || v.Loading.Cron != "0 6 * * 1" || v.Draft.Schedule != "0 6 * * 1" || !v.Probed {
				t.Fatal("user-edited settings or source probe lost")
			}
			if modelFailure {
				if v.Pending != nil {
					t.Fatal("model failure restored consumed loading confirmation")
				}
			} else if calls != 2 || v.Pending == nil || v.Pending.Kind != "validation" || v.Pending.Validation.Limit != 500 {
				t.Fatal("approval did not advance directly to validation proposal")
			}
			if runner.calls != 0 {
				t.Fatal("loading approval ran validation without separate approval")
			}
			if _, err := s.TurnWithEvents(context.Background(), v.ID, approval, nil); err == nil {
				t.Fatal("consumed loading approval could be replayed")
			}
		})
	}
}
