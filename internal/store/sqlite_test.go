package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/secrets"
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

func TestSQLiteSecretsLifecycle(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(ctx, filepath.Join(t.TempDir(), "pompos.sqlite"), filepath.Join(t.TempDir(), "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	secretStore := metadata.Secrets()
	if err := secretStore.Put(ctx, "github/example", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := secretStore.Put(ctx, "github/example", []byte("updated")); err != nil {
		t.Fatal(err)
	}
	value, err := secretStore.Get(ctx, "github/example")
	if err != nil || string(value) != "updated" {
		t.Fatalf("Get() = %q, %v", value, err)
	}
	entries, err := secretStore.List(ctx)
	if err != nil || len(entries) != 1 || entries[0].Key != "github/example" {
		t.Fatalf("List() = %#v, %v", entries, err)
	}
	if err := secretStore.Delete(ctx, "github/example"); err != nil {
		t.Fatal(err)
	}
	if _, err := secretStore.Get(ctx, "github/example"); !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("Get() after delete = %v", err)
	}
}
