package main

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"pompos/internal/store"
)

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
