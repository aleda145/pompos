package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/compiler"
	"pompos/internal/destination"
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
	return validationFixtureWithApproval(t, true)
}

func validationFixtureWithApproval(t *testing.T, manual bool) (*Service, *validationRunner, Session) {
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
	if err = s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test", ManualValidation: manual}); err != nil {
		t.Fatal(err)
	}
	v := Session{ID: "validation", Messages: []Message{{Role: "system", Content: prompt}}, Draft: &Draft{Name: "Rows", Source: "fixture", Table: "rows", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\n"}, Loading: &Loading{Strategy: "replace"}}
	data := []byte(runnerpython.Wrap(v.Draft.Code))
	if err = WriteFile(s.scriptPath(v.ID), data); err != nil {
		t.Fatal(err)
	}
	v.Probed = true
	if manual {
		if _, err = s.proposeValidation(ctx, &v, `{"limit":25}`); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	s.Client = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"Review the validation."}}]}`)), Header: make(http.Header)}, nil
	})}
	return s, runner, v
}

func TestAutomaticValidationContinuesWithoutApproval(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint("retry=", retry), func(t *testing.T) {
			ctx := context.Background()
			s, runner, v := validationFixtureWithApproval(t, false)
			if retry {
				runner.err = errors.New("sample load failed")
			}
			step := 0
			s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				var request struct{ Messages []Message }
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(request.Messages[0].Content, "Validation approval is disabled") {
					t.Fatal("model was not told validation runs automatically")
				}
				last := request.Messages[len(request.Messages)-1].Content
				name := "propose_validation"
				if step > 0 {
					if retry && step == 1 {
						if !strings.Contains(last, "Error: sample load failed") {
							t.Fatalf("failure did not reach model: %s", last)
						}
						runner.err = nil
					} else if strings.HasPrefix(last, "POMPOS_VALIDATION_RESULT=") {
						name = "finish"
					} else {
						name = ""
					}
				}
				step++
				message := Message{Role: "assistant", Content: "Validation complete."}
				if name != "" {
					call := Call{ID: fmt.Sprint(step), Type: "function"}
					call.Function.Name, call.Function.Arguments = name, `{"limit":25}`
					message.Calls = []Call{call}
				}
				data, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": message}}})
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data)))}, nil
			})}
			v, err := s.Turn(ctx, v.ID, "Continue")
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if retry {
				wantCalls = 2
			}
			if !v.Ready || v.Pending != nil || v.Validation == nil || runner.calls != wantCalls || runner.limit != 25 {
				t.Fatalf("automatic validation stalled: calls=%d session=%+v", runner.calls, v)
			}
			reloaded, err := s.Load(v.ID)
			if err != nil || reloaded.Validation == nil || reloaded.Validation.Result.Preview == nil {
				t.Fatalf("validation result not persisted: %v", err)
			}
			if err := s.requireValidation(ctx, &reloaded); err != nil {
				t.Fatal(err)
			}
			reloaded.Draft.Code += "\n# changed"
			if err := s.requireValidation(ctx, &reloaded); err == nil {
				t.Fatal("changed draft reused validation")
			}
		})
	}
}

func TestAutomaticValidationFailureBlocksFinishAndSave(t *testing.T) {
	ctx := context.Background()
	s, runner, v := validationFixtureWithApproval(t, false)
	if _, err := s.proposeValidation(ctx, &v, `{"limit":1001}`); err == nil || runner.calls != 0 {
		t.Fatal("automatic validation bypassed sample limits")
	}
	runner.err = errors.New("sample load failed")
	if _, err := s.proposeValidation(ctx, &v, `{"limit":25}`); err == nil {
		t.Fatal("validation failure lost")
	}
	finish := Call{}
	finish.Function.Name = "finish"
	if _, err := s.execute(ctx, &v, finish); err == nil || v.Ready || v.Validation != nil || v.Pending != nil {
		t.Fatal("failed validation was accepted or asked for approval")
	}
	v.Ready = true
	if err := s.save(v); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, v.ID, t.TempDir(), func(string, spec.Ingestion) error {
		t.Fatal("published failed validation")
		return nil
	}); err == nil {
		t.Fatal("publish bypassed failed validation")
	}
}

func TestDisableApprovalResumesPendingValidation(t *testing.T) {
	s, runner, v := validationFixture(t)
	if err := s.SetManualValidation(false); err != nil {
		t.Fatal(err)
	}
	v.Draft.Code += "\n# updated draft"
	if err := s.save(v); err != nil {
		t.Fatal(err)
	}
	v, err := s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}, nil)
	if err != nil || runner.calls != 1 || v.Pending != nil || v.Validation == nil {
		t.Fatalf("pending validation did not resume: calls=%d session=%+v err=%v", runner.calls, v, err)
	}
	if err := s.requireValidation(context.Background(), &v); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, message := range v.Messages {
		if message.Role == "tool" && strings.HasPrefix(message.Content, "POMPOS_VALIDATION_RESULT=") {
			found = true
		}
	}
	if !found {
		t.Fatal("resumed validation result missing from conversation")
	}
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
	if err != nil || savedID != firstID || savedID != v.Draft.Destination+"/main/"+v.Draft.Table {
		t.Fatalf("retry failed: %q %v", savedID, err)
	}
	persisted, err := restarted.Load(v.ID)
	if err != nil || len(persisted.SavedIngestions) != 1 || persisted.SavedIngestions[0].ID != savedID {
		t.Fatal("retry did not save one publication")
	}
}

func TestPublicationRejectsExistingTableFiles(t *testing.T) {
	for _, extension := range []string{".py", ".yaml", ".py.lock"} {
		t.Run(extension, func(t *testing.T) {
			ctx := context.Background()
			s, _, v := validationFixture(t)
			_, fingerprint, err := s.validationPlan(ctx, &v)
			if err != nil {
				t.Fatal(err)
			}
			v.Validation = &Validation{Fingerprint: fingerprint}
			v.Ready = true
			if err := s.save(v); err != nil {
				t.Fatal(err)
			}
			artifactDir := t.TempDir()
			path := spec.ArtifactPath(artifactDir, v.Draft.Destination+"/main/"+v.Draft.Table, extension)
			if err := WriteFile(path, []byte("existing ingestion")); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Publish(ctx, v.ID, artifactDir, func(string, spec.Ingestion) error {
				t.Fatal("duplicate ingestion was published")
				return nil
			}); err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("expected filename collision, got %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "existing ingestion" {
				t.Fatalf("existing file changed: %s, %v", data, err)
			}
		})
	}
}

func TestPublicationScopesTablesByDestinationAndSchema(t *testing.T) {
	ctx := context.Background()
	s, _, base := validationFixture(t)
	warehouse := destination.NewDuckDB("warehouse", filepath.Join(t.TempDir(), "warehouse.duckdb"))
	if err := s.Destinations.(*store.SQLite).PutDestination(ctx, warehouse); err != nil {
		t.Fatal(err)
	}
	artifactDir := t.TempDir()
	for _, target := range []struct{ destination, schema string }{
		{"local-duckdb", "main"}, {"local-duckdb", "raw"}, {"warehouse", "main"}, {"warehouse", "raw"},
	} {
		v := base
		draft := *base.Draft
		v.Draft = &draft
		v.ID = target.destination + "_" + target.schema
		v.Draft.Destination, v.Draft.Schema = target.destination, target.schema
		if err := WriteFile(s.scriptPath(v.ID), []byte(runnerpython.Wrap(v.Draft.Code))); err != nil {
			t.Fatal(err)
		}
		_, fingerprint, err := s.validationPlan(ctx, &v)
		if err != nil {
			t.Fatal(err)
		}
		v.Validation, v.Ready = &Validation{Fingerprint: fingerprint}, true
		if err := s.save(v); err != nil {
			t.Fatal(err)
		}
		wantID := target.destination + "/" + target.schema + "/rows"
		id, err := s.Publish(ctx, v.ID, artifactDir, func(id string, doc spec.Ingestion) error {
			if doc.Destination.Schema != target.schema || doc.Runtime.Script != spec.ArtifactPath(artifactDir, wantID, ".py") {
				t.Fatalf("wrong publication target: %#v", doc)
			}
			data, err := spec.Marshal(doc)
			if err != nil {
				return err
			}
			return WriteFile(spec.ArtifactPath(artifactDir, id, ".yaml"), data)
		})
		if err != nil || id != wantID {
			t.Fatalf("publish %s: %q, %v", wantID, id, err)
		}
	}
}

func TestValidationApprovalRejectsChangedDraftAndSettings(t *testing.T) {
	for _, change := range []string{"loading", "destination", "schema", "dependencies", "code"} {
		t.Run(change, func(t *testing.T) {
			s, runner, v := validationFixture(t)
			switch change {
			case "loading":
				v.Loading.Strategy = "append"
			case "destination":
				v.Draft.Table = "different"
			case "schema":
				v.Draft.Schema = "raw"
			case "dependencies":
				v.Draft.Dependencies = []string{"pyarrow"}
			case "code":
				v.Draft.Code += "\n# changed source code"
			}
			if err := s.save(v); err != nil {
				t.Fatal(err)
			}
			approval := Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}
			updated, err := s.TurnWithEvents(context.Background(), v.ID, approval, nil)
			if err != nil {
				t.Fatalf("couldn't refresh stale approval: %v", err)
			}
			if runner.calls != 0 {
				t.Fatal("stale approval ran validation")
			}
			if updated.Pending == nil || updated.Pending.Kind != "validation" || updated.Pending.ID == approval.HandoffID || updated.Ready || updated.Validation != nil {
				t.Fatal("stale approval did not produce a fresh confirmation")
			}
			if updated.Pending.Validation.Limit != v.Pending.Validation.Limit {
				t.Fatal("refresh changed the validation sample")
			}
			if _, err := s.TurnWithEvents(context.Background(), v.ID, approval, nil); err == nil || runner.calls != 0 {
				t.Fatal("old approval could be replayed after refresh")
			}
			// The replacement is persisted and usable without another chat message.
			updated, err = s.Load(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			updated, err = s.TurnWithEvents(context.Background(), v.ID, Input{ActionID: "accept_validation", HandoffID: updated.Pending.ID}, nil)
			if err != nil || runner.calls != 1 || updated.Validation == nil {
				t.Fatalf("fresh confirmation failed: calls=%d, err=%v", runner.calls, err)
			}
		})
	}
}

func TestValidationApprovalSurvivesEmptyDependenciesRoundTrip(t *testing.T) {
	for _, kind := range []string{"duckdb", "objects"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, runner, v := validationFixture(t)
			if kind == "objects" {
				if err := s.Destinations.(*store.SQLite).PutDestination(ctx, destination.Config{Name: "files", Type: "objects", Path: t.TempDir()}); err != nil {
					t.Fatal(err)
				}
				v.Draft.Data, v.Draft.Destination, v.Draft.Schema, v.Draft.Table = "files", "files", "reports", "objects"
				v.Loading.Strategy = "update"
			}
			// Models commonly emit dependencies: []; session JSON omits that field.
			v.Draft.Dependencies = []string{}
			v.Draft.SecretRefs = []string{}
			v.Loading.PrimaryKey = []string{}
			if _, err := s.proposeValidation(ctx, &v, `{"limit":2}`); err != nil {
				t.Fatal(err)
			}
			if err := s.save(v); err != nil {
				t.Fatal(err)
			}
			updated, err := s.TurnWithEvents(ctx, v.ID, Input{ActionID: "accept_validation", HandoffID: v.Pending.ID}, nil)
			if err != nil || runner.calls != 1 || updated.Validation == nil {
				t.Fatalf("unchanged draft rejected after reload: calls=%d, err=%v", runner.calls, err)
			}
			reloaded, err := s.Load(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.requireValidation(ctx, &reloaded); err != nil {
				t.Fatalf("successful validation lost after reload: %v", err)
			}
		})
	}
}
