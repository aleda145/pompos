package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/store"
)

func TestRunCommandRunsSelectedSpecAndReportsSuccess(t *testing.T) {
	t.Setenv("POMPOS_INGESTR_BINARY", "true")
	t.Setenv("POMPOS_DESTINATION_PATH", filepath.Join(t.TempDir(), "pompos.duckdb"))
	path := filepath.Join("..", "..", "internal", "spec", "testdata", "customers.yaml")
	var stdout, stderr bytes.Buffer
	if err := runCommandIO([]string{"run", path}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Running customers\n") || !strings.Contains(stdout.String(), "Succeeded customers in ") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunCommandReportsSelectedSpecFailure(t *testing.T) {
	t.Setenv("POMPOS_INGESTR_BINARY", "false")
	t.Setenv("POMPOS_DESTINATION_PATH", filepath.Join(t.TempDir(), "pompos.duckdb"))
	path := filepath.Join("..", "..", "internal", "spec", "testdata", "customers.yaml")
	var stdout, stderr bytes.Buffer
	err := runCommandIO([]string{"run", path}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), `run "customers" failed`) {
		t.Fatalf("run error = %v", err)
	}
	if strings.Contains(stdout.String(), "Succeeded") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRebuildSpecProjectionsFromFiles(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	specDir := filepath.Join(dataDir, "ingestions")
	if err := os.MkdirAll(specDir, 0o755); err != nil {
		t.Fatal(err)
	}
	input, err := os.ReadFile(filepath.Join("..", "..", "internal", "spec", "testdata", "customers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(specDir, "customers.yaml"), input, 0o644); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := rebuildSpecProjections(ctx, metadata, specDir, destination, log.New(io.Discard, "", 0)); err != nil {
		t.Fatal(err)
	}
	item, err := metadata.Get(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	if item.ID != "customers" || item.Name != "" || item.Source.URL != "" || item.Destination.Table != "" || item.SpecDigest == "" || item.SpecPath == "" {
		t.Fatalf("projection = %#v", item)
	}
}
