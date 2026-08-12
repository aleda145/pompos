package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"pompos/internal/ingestion"
)

func TestSQLiteLifecycle(t *testing.T) {
	ctx := context.Background()
	destination := filepath.Join(t.TempDir(), "pompos.duckdb")
	metadata, err := Open(ctx, filepath.Join(t.TempDir(), "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{
		ID: "abc", Name: "customers", Status: ingestion.StatusPending,
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "customers"},
	}
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Microsecond)
	if err := metadata.MarkRunning(ctx, item.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Finish(ctx, item.ID, ingestion.StatusSucceeded, ""); err != nil {
		t.Fatal(err)
	}
	got, err := metadata.Get(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ingestion.StatusSucceeded || got.LastRun == nil || !got.LastRun.Equal(now.UTC()) {
		t.Fatalf("Get() = %#v", got)
	}
}
