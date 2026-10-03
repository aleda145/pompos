package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/ingestion"
	"pompos/internal/spec"
	"pompos/internal/store"
	"pompos/internal/testutil"
)

func commandFixture(t *testing.T, code string) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	dir := t.TempDir()
	t.Setenv("POMPOS_DATA_DIR", dir)
	t.Setenv("POMPOS_METADATA_PATH", filepath.Join(dir, "metadata.sqlite"))
	t.Setenv("POMPOS_DESTINATION_PATH", filepath.Join(dir, "out.duckdb"))
	t.Setenv("POMPOS_PYTHON_BINARY", python)
	t.Setenv("POMPOS_UV_BINARY", testutil.FakeUV(t))
	document, _, err := spec.Read("../../internal/spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document.Runtime.Script = filepath.Join(dir, "customers.py")
	document.Runtime.ScriptDigest = spec.Digest([]byte(code))
	if err = os.WriteFile(document.Runtime.Script, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Open(context.Background(), filepath.Join(dir, "metadata.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.Secrets().Put(context.Background(), "source-key", []byte("private-fixture-key")); err != nil {
		t.Fatal(err)
	}
	metadata.Close()
	data, err := spec.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "customers.yaml")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestRunCommandRunsPythonSpecAndReportsSuccess(t *testing.T) {
	path := commandFixture(t, `import json, os
assert json.loads(os.environ["POMPOS_CONFIG"])["table"] == "customers"
assert json.loads(os.environ["POMPOS_SECRETS"])["source-key"] == "private-fixture-key"
`)
	var stdout bytes.Buffer
	if err := runCommandIO([]string{"run", path}, &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Running customers\n") || !strings.Contains(stdout.String(), "Succeeded customers in ") {
		t.Fatalf("stdout: %s", &stdout)
	}
}
func TestRunCommandReportsPythonFailure(t *testing.T) {
	path := commandFixture(t, "raise RuntimeError('fixture failed')\n")
	var stdout bytes.Buffer
	err := runCommandIO([]string{"run", path}, &stdout)
	if err == nil || !strings.Contains(err.Error(), `run "customers" failed`) || !strings.Contains(err.Error(), "fixture failed") {
		t.Fatalf("error: %v", err)
	}
	if strings.Contains(stdout.String(), "Succeeded") {
		t.Fatal("failed run reported success")
	}
}
func TestRebuildSpecProjectionsFromFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	specDir := filepath.Join(dir, "ingestions")
	os.MkdirAll(specDir, 0755)
	input, err := os.ReadFile("../../internal/spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(specDir, "analytics", "raw", "customers.yaml")
	input = bytes.Replace(input, []byte("destination:\n"), []byte("destination:\n  schema: raw\n"), 1)
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err = rebuildSpecProjections(ctx, metadata, specDir); err != nil {
		t.Fatal(err)
	}
	item, err := metadata.Get(ctx, "analytics/raw/customers")
	if err != nil {
		t.Fatal(err)
	}
	if item.SpecPath != path || item.SpecDigest != spec.Digest(input) {
		t.Fatalf("projection: %#v", item)
	}
	if err = os.WriteFile(path, []byte(strings.Replace(string(input), spec.APIVersion, "pompos.dev/v1", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if err = metadata.Finish(ctx, "analytics/raw/customers", ingestion.StatusFailed, "previous failure"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(specDir, "a-broken.yaml"), []byte("invalid: ["), 0600); err != nil {
		t.Fatal(err)
	}
	otherPath := filepath.Join(specDir, "reporting", "raw", "customers.yaml")
	if err = os.MkdirAll(filepath.Dir(otherPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(otherPath, input, 0600); err != nil {
		t.Fatal(err)
	}
	hidden := filepath.Join(filepath.Dir(path), ".dlt")
	if err = os.MkdirAll(hidden, 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(hidden, "state.yaml"), []byte("not an ingestion"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = rebuildSpecProjections(ctx, metadata, specDir); err != nil {
		t.Fatalf("broken spec blocked startup: %v", err)
	}
	items, err := metadata.List(ctx)
	if err != nil || len(items) != 3 {
		t.Fatalf("broken and healthy files must remain registered: %#v, %v", items, err)
	}
	if _, err = metadata.Get(ctx, "reporting/raw/customers"); err != nil {
		t.Fatalf("same table in another destination was not discovered: %v", err)
	}
	item, err = metadata.Get(ctx, "analytics/raw/customers")
	if err != nil || item.Status != ingestion.StatusFailed || item.LastError != "previous failure" {
		t.Fatalf("rebuild lost run history: %#v, %v", item, err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = rebuildSpecProjections(ctx, metadata, specDir); err != nil {
		t.Fatal(err)
	}
	if _, err = metadata.Get(ctx, "analytics/raw/customers"); err != nil {
		t.Fatalf("deleted YAML lost its registered identity: %v", err)
	}
}

func TestLegacyMCPTokenCommandDirectsToAgentSettings(t *testing.T) {
	var out bytes.Buffer
	err := runCommandIO([]string{"mcp-token"}, &out)
	if err == nil || !strings.Contains(err.Error(), "no longer requires a token") || out.Len() != 0 {
		t.Fatal("legacy token command should explain that authentication was removed")
	}
}
