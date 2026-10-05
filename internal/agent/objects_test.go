package agent

import (
	"context"
	"encoding/json"
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

func TestMCPObjectsPublishAndRun(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for object ingestion integration test")
	}
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "state.sqlite"), filepath.Join(dir, "rows.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root := filepath.Join(dir, "files")
	if err := db.PutDestination(ctx, destination.Config{Name: "media", Type: "objects", Path: root}); err != nil {
		t.Fatal(err)
	}
	runner := runnerpython.Runner{Binary: binary, Secrets: db.Secrets()}
	s := &Service{Dir: filepath.Join(dir, "agent"), Destinations: db, Secrets: db.Secrets(), Python: runner}
	v, err := s.NewMCPChat("Download a report")
	if err != nil {
		t.Fatal(err)
	}
	call := func(name string, input any) string {
		t.Helper()
		data, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		out, err := s.MCPCall(ctx, v.ID, name, data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return out
	}
	draft := Draft{Name: "Annual reports", Source: "fixture://reports", Schema: "company_reports", Collection: "reports", Table: "objects", Destination: "media", Strategy: "update", Code: `def fetch(secret, limit):
    yield {'object_id': '2026', 'source_uri': 'fixture://report', 'filename': 'report.pdf', 'source_version': '1', 'metadata': {'year': 2026}}
def download(obj, target_path, secret):
    from pathlib import Path
    Path(target_path).write_bytes(b'fixture PDF')
`}
	call("write_script", draft)
	call("test_script", map[string]any{})
	if _, err := s.MCPCall(ctx, v.ID, "configure_loading", json.RawMessage(`{"strategy":"replace"}`)); err == nil {
		t.Fatal("row replacement accepted for objects")
	}
	call("configure_loading", Loading{Strategy: "update"})
	if out := call("validate_ingestion", map[string]any{}); !strings.Contains(out, `"data":"files"`) {
		t.Fatalf("no file validation result: %s", out)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("preparation wrote production files")
	}
	var saved spec.Ingestion
	id, err := s.Publish(ctx, v.ID, filepath.Join(dir, "ingestions"), func(id string, document spec.Ingestion) error {
		saved = document
		_, err := spec.Write(filepath.Join(dir, "ingestions"), spec.ToProjection(document, id, "", ""))
		return err
	})
	if err != nil || id != "media/company_reports/objects" || saved.Data != "files" || saved.Source.Collection != "reports" || saved.Source.Table != "" {
		t.Fatalf("publication: %s, %#v, %v", id, saved, err)
	}
	plan, err := compiler.Compile(saved)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Run(ctx, plan); err != nil {
		t.Fatal(err)
	}
	preview, err := runner.Preview(ctx, plan)
	if err != nil || preview.TotalRows != 1 || preview.Columns[0] != "object_id" {
		t.Fatalf("published file ingestion: %#v, %v", preview, err)
	}
	// A new chat can replace the ingestion at the same schema/table.
	v, err = s.NewMCPChat("Update reports")
	if err != nil {
		t.Fatal(err)
	}
	call("write_script", draft)
	call("test_script", map[string]any{})
	call("configure_loading", Loading{Strategy: "skip"})
	call("validate_ingestion", map[string]any{"limit": 1, "max_bytes": 1024, "timeout_seconds": 30})
	updatedID, err := s.Publish(ctx, v.ID, filepath.Join(dir, "ingestions"), func(id string, document spec.Ingestion) error {
		_, err := spec.Write(filepath.Join(dir, "ingestions"), spec.ToProjection(document, id, "", ""))
		return err
	})
	if err != nil || updatedID != id {
		t.Fatalf("replace object ingestion: %q, %v", updatedID, err)
	}
	updated, _, err := spec.Read(spec.ArtifactPath(filepath.Join(dir, "ingestions"), id, ".yaml"))
	if err != nil || updated.Materialization.Strategy != "skip" {
		t.Fatalf("replacement lost loading settings: %#v, %v", updated, err)
	}
}
