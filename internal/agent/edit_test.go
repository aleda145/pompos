package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func editFixture(t *testing.T) (*Service, Session) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s := &Service{Dir: filepath.Join(dir, "agent"), Destinations: db, Secrets: db.Secrets(), Python: &validationRunner{Runner: runnerpython.Runner{Binary: "python3"}}}
	item := ingestion.Ingestion{ID: "local-duckdb/raw/rows", Name: "Rows", Source: ingestion.Source{Type: "python", URL: "fixture", Table: "source_rows"},
		Destination:     ingestion.Destination{Type: "duckdb", Path: filepath.Join(dir, "out.duckdb"), Schema: "raw", Table: "rows"},
		Runtime:         ingestion.Runtime{Engine: "python", Orchestrator: "direct", Script: filepath.Join(dir, "extract.py")},
		Materialization: ingestion.Materialization{Strategy: "replace"}, Schedule: "0 6 * * *"}
	item.SpecPath, err = spec.Write(dir, item)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(item.Runtime.Script, []byte(runnerpython.Wrap("def fetch(secret, limit):\n    yield {'id': 1}\n"))); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(item.Runtime.Script+".lock", []byte("original lock")); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	v, err := s.StartEdit(ctx, item, &ingestion.Run{ID: 7, IngestionID: item.ID, Status: "failed", Log: "source changed"}, true)
	if err != nil {
		t.Fatal(err)
	}
	return s, v
}

func validateEdit(t *testing.T, s *Service, v Session) EditReview {
	t.Helper()
	d := *v.Draft
	d.Code = "def fetch(secret, limit):\n    yield {'id': 2}\n"
	data, _ := json.Marshal(d)
	for _, call := range []struct {
		name string
		data []byte
	}{{"write_script", data}, {"test_script", []byte("{}")}, {"validate_ingestion", []byte(`{"limit":5}`)}} {
		if _, err := s.MCPCall(context.Background(), v.ID, call.name, call.data); err != nil {
			t.Fatal(call.name, err)
		}
	}
	review, err := s.ReviewEdit(context.Background(), v.ID)
	if err != nil {
		t.Fatal(err)
	}
	return review
}

func TestEditLoadsFilesAndAppliesReviewedRevision(t *testing.T) {
	s, v := editFixture(t)
	ctx := context.Background()
	if v.Probed || v.Ready || v.Validation != nil || v.Edit.Run.Log != "source changed" || strings.Contains(v.Draft.Code, "Pompos entrypoint") {
		t.Fatalf("invalid initial edit: %+v", v)
	}
	if v.Loading.Cron != "0 6 * * *" {
		t.Fatal("lost saved schedule")
	}
	before, _ := readEditFiles(v.Edit.SpecPath, v.Edit.ScriptPath)
	if _, err := s.Publish(ctx, v.ID, t.TempDir(), nil); err == nil {
		t.Fatal("edit bypassed review")
	}
	review := validateEdit(t, s, v)
	afterDraft, _ := readEditFiles(v.Edit.SpecPath, v.Edit.ScriptPath)
	if before.digest() != afterDraft.digest() {
		t.Fatal("draft modified publication")
	}
	if !strings.Contains(review.BeforePython, "'id': 1") || !strings.Contains(review.AfterPython, "'id': 2") {
		t.Fatal("review does not show actual change")
	}
	if _, err := s.ApplyEdit(ctx, v.ID, "wrong", nil); err == nil {
		t.Fatal("accepted stale review")
	}
	persisted := 0
	persist := func(id string, doc spec.Ingestion) error {
		persisted++
		if id != v.Edit.IngestionID || doc.Source.Table != "source_rows" || doc.Schedule.Cron != "0 6 * * *" {
			t.Fatal("changed identity or source settings")
		}
		return nil
	}
	for range 2 {
		id, err := s.ApplyEdit(ctx, v.ID, review.Fingerprint, persist)
		if err != nil || id != v.Edit.IngestionID {
			t.Fatalf("apply: %s %v", id, err)
		}
	}
	if persisted != 1 {
		t.Fatal("retry repeated publication")
	}
	published, _ := os.ReadFile(v.Edit.ScriptPath)
	if !strings.Contains(string(published), "'id': 2") {
		t.Fatal("repair not published")
	}
}

func TestEditRejectsConflictsAndChangedTargets(t *testing.T) {
	for _, change := range []string{"yaml", "script", "lock", "target", "draft", "validation"} {
		t.Run(change, func(t *testing.T) {
			s, v := editFixture(t)
			review := validateEdit(t, s, v)
			ctx := context.Background()
			switch change {
			case "yaml", "script", "lock":
				path := v.Edit.SpecPath
				if change != "yaml" {
					path = v.Edit.ScriptPath
				}
				if change == "lock" {
					path += ".lock"
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := WriteFile(path, append(data, []byte("\n# external edit\n")...)); err != nil {
					t.Fatal(err)
				}
			case "target":
				d := *v.Draft
				d.Table = "another_table"
				data, _ := json.Marshal(d)
				if _, err := s.MCPCall(ctx, v.ID, "write_script", data); err == nil {
					t.Fatal("target change accepted")
				}
				return
			case "draft":
				if err := WriteFile(s.scriptPath(v.ID), []byte(runnerpython.Wrap("def fetch(secret, limit):\n    yield {'id': 9}\n"))); err != nil {
					t.Fatal(err)
				}
			case "validation":
				data, _ := json.Marshal(v.Draft)
				if _, err := s.MCPCall(ctx, v.ID, "write_script", data); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.ApplyEdit(ctx, v.ID, review.Fingerprint, func(string, spec.Ingestion) error { t.Fatal("published despite conflict"); return nil }); err == nil {
				t.Fatal("conflict accepted")
			}
		})
	}
}

func TestEditRollsBackFailedPublication(t *testing.T) {
	s, v := editFixture(t)
	review := validateEdit(t, s, v)
	before, _ := readEditFiles(v.Edit.SpecPath, v.Edit.ScriptPath)
	calls := 0
	_, err := s.ApplyEdit(context.Background(), v.ID, review.Fingerprint, func(string, spec.Ingestion) error {
		calls++
		if calls == 1 {
			return errors.New("metadata unavailable")
		}
		return nil
	})
	if err == nil || calls != 2 {
		t.Fatal("failed publication did not restore metadata")
	}
	after, _ := readEditFiles(v.Edit.SpecPath, v.Edit.ScriptPath)
	if before.digest() != after.digest() {
		t.Fatal("failed publication changed saved files")
	}
	if _, err := s.ReviewEdit(context.Background(), v.ID); err != nil {
		t.Fatal("rolled-back edit is not retryable", err)
	}
}
