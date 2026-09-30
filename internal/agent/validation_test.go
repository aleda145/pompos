package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/compiler"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

// Source probes use real Python; unit tests control the destination validation result.
type validationRunner struct {
	runnerpython.Runner
	calls int
	limit int
	err   error
}

func (r *validationRunner) Validate(ctx context.Context, plan compiler.ExecutionPlan, limit int) (runnerpython.ValidationResult, string, error) {
	r.calls++
	r.limit = limit
	if r.err != nil {
		return runnerpython.ValidationResult{}, "", r.err
	}
	result := runnerpython.ValidationResult{SampleCount: 1, FirstLoadRows: 1, SecondLoadRows: 1,
		Preview: &runnerpython.TablePreview{Columns: []string{"id"}, Rows: [][]string{{"1"}}}}
	if plan.Strategy == "append" {
		result.SecondLoadRows = 2
	}
	b, _ := json.Marshal(result)
	return result, "POMPOS_VALIDATION_RESULT=" + string(b), nil
}

func validationFixture(t *testing.T) (*Service, *validationRunner, Session) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	runner := &validationRunner{Runner: runnerpython.Runner{Binary: "python3", Secrets: db.Secrets()}}
	s := &Service{Dir: filepath.Join(dir, "agent"), Destinations: db, Secrets: db.Secrets(), Python: runner}
	if err = s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	v := Session{ID: "validation", Messages: []Message{{Role: "system", Content: prompt}}, Draft: &Draft{Name: "Rows", Source: "fixture", Table: "rows", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\n"}, Loading: &Loading{Strategy: "replace"}}
	data := []byte(runnerpython.Wrap(v.Draft.Code))
	if err = WriteFile(s.scriptPath(v.ID), data); err != nil {
		t.Fatal(err)
	}
	v.TestedDigest = spec.Digest(data)
	if _, err = s.proposeValidation(ctx, &v, `{"limit":25}`); err != nil {
		t.Fatal(err)
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	s.Client = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"Review the validation."}}]}`)), Header: make(http.Header)}, nil
	})}
	return s, runner, v
}

func TestValidationRequiresUserActionAndCannotBeReplayed(t *testing.T) {
	ctx := context.Background()
	s, runner, v := validationFixture(t)
	finish := Call{}
	finish.Function.Name = "finish"
	if _, err := s.execute(ctx, &v, finish); err == nil {
		t.Fatal("finish accepted without validation")
	}
	v.Ready = true // Even a forged ready flag must not bypass publication checks.
	if err := s.save(v); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, v.ID, t.TempDir(), func(string, spec.Ingestion) error { t.Fatal("published without validation"); return nil }); err == nil {
		t.Fatal("publish accepted")
	}
	for _, action := range []string{"explain", "defer_validation"} {
		var err error
		v, err = s.TurnWithEvents(ctx, v.ID, Input{ActionID: action, HandoffID: v.Pending.ID}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if runner.calls != 0 || v.Pending == nil || v.Ready {
			t.Fatal("non-approval ran validation or lost the proposal")
		}
	}
	approval := Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}
	var events []string
	var err error
	v, err = s.TurnWithEvents(ctx, v.ID, approval, func(e Event) { events = append(events, e.Type) })
	if err != nil {
		t.Fatal(err)
	}
	if runner.calls != 1 || runner.limit != 25 || v.Validation == nil {
		t.Fatalf("validation did not execute once: %#v", v)
	}
	if !strings.Contains(strings.Join(events, ","), "message,tool_start,message") {
		t.Fatal("missing validation events")
	}
	reloaded, err := s.Load(v.ID)
	if err != nil || reloaded.Validation == nil || reloaded.Validation.Result.Preview == nil {
		t.Fatalf("validation preview did not survive reload: %v", err)
	}
	foundPreview := false
	for _, message := range reloaded.Messages {
		if message.Role == "tool" && strings.Contains(message.Content, `"preview":{"columns":["id"],"rows":[["1"]]`) {
			foundPreview = true
		}
	}
	if !foundPreview {
		t.Fatal("validation preview missing from persistent chat activity")
	}
	if _, err = s.execute(ctx, &v, finish); err != nil {
		t.Fatal(err)
	}
	if _, err = s.TurnWithEvents(ctx, v.ID, approval, nil); err == nil {
		t.Fatal("approval replay accepted")
	}
	if runner.calls != 1 {
		t.Fatal("validation was replayed")
	}
	v.Draft.Code += "\n# changed"
	if err = WriteFile(s.scriptPath(v.ID), []byte(runnerpython.Wrap(v.Draft.Code))); err != nil {
		t.Fatal(err)
	}
	if err = s.requireValidation(ctx, &v); err == nil {
		t.Fatal("changed code accepted")
	}
}

func TestValidationFailureRequiresFreshApproval(t *testing.T) {
	s, runner, v := validationFixture(t)
	runner.err = errors.New("duplicate row keys")
	v, err := s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Validation != nil || v.Ready || runner.calls != 1 {
		t.Fatal("failed validation accepted")
	}
	if !strings.Contains(v.Messages[len(v.Messages)-2].Content, "duplicate row keys") {
		t.Fatal("failure not added to conversation")
	}
	if err = s.requireValidation(context.Background(), &v); err == nil {
		t.Fatal("failure passed gate")
	}
}

func TestPublicationRetryKeepsArtifactID(t *testing.T) {
	ctx := context.Background()
	s, _, v := validationFixture(t)
	v, err := s.TurnWithEvents(ctx, v.ID, Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	finish := Call{}
	finish.Function.Name = "finish"
	if _, err = s.execute(ctx, &v, finish); err != nil {
		t.Fatal(err)
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	artifactDir := t.TempDir()
	var firstID, firstPath string
	publicationErr := errors.New("scheduler unavailable")
	if _, err := s.Publish(ctx, v.ID, artifactDir, func(id string, doc spec.Ingestion) error {
		firstID, firstPath = id, doc.Runtime.Script
		return publicationErr
	}); !errors.Is(err, publicationErr) {
		t.Fatalf("expected publication failure, got %v", err)
	}
	restarted := &Service{Dir: s.Dir, Destinations: s.Destinations}
	savedID, err := restarted.Publish(ctx, v.ID, artifactDir, func(id string, doc spec.Ingestion) error {
		if id != firstID || doc.Runtime.Script != firstPath {
			t.Fatal("retry changed the ingestion ID or script path")
		}
		return nil
	})
	if err != nil || savedID != firstID || savedID == v.ID {
		t.Fatalf("retry failed: %q %v", savedID, err)
	}
	persisted, err := restarted.Load(v.ID)
	if err != nil || len(persisted.SavedIngestions) != 1 || persisted.SavedIngestions[0].ID != savedID {
		t.Fatal("retry did not save one publication")
	}
}

func TestValidationApprovalRejectsChangedDraftAndSettings(t *testing.T) {
	for _, change := range []string{"script", "loading", "destination"} {
		t.Run(change, func(t *testing.T) {
			s, runner, v := validationFixture(t)
			switch change {
			case "script":
				if err := os.WriteFile(s.scriptPath(v.ID), []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "loading":
				v.Loading.Strategy = "append"
			case "destination":
				v.Draft.Table = "different"
			}
			if err := s.save(v); err != nil {
				t.Fatal(err)
			}
			if _, err := s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}, nil); err == nil {
				t.Fatal("stale approval accepted")
			}
			if runner.calls != 0 {
				t.Fatal("stale approval ran validation")
			}
		})
	}
}
