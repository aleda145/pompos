package scheduler

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestDurablePollerRunsPersistedDueSchedule(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), filepath.Join(dataDir, "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{
		ID: "abc", Name: "customers", Status: ingestion.StatusSucceeded, Schedule: "*/5 * * * *",
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: filepath.Join(dataDir, "pompos.duckdb"), Table: "customers"},
	}
	persistSpec(t, dataDir, &item)
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	var ran string
	manager, err := New(log.New(io.Discard, "", 0), metadata, func(_ context.Context, run ingestion.Run) error { ran = run.IngestionID; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	now := time.Date(2026, time.August, 13, 12, 2, 0, 0, time.UTC)
	manager.now = func() time.Time { return now }
	due := now.Add(-2 * time.Minute)
	if err := metadata.UpdateNextRun(ctx, item.ID, &due); err != nil {
		t.Fatal(err)
	}
	manager.poll(ctx)
	if ran != item.ID {
		t.Fatalf("ran ingestion %q", ran)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextRun == nil || !stored.NextRun.After(now) {
		t.Fatalf("next run = %v", stored.NextRun)
	}
}

func TestValidateAndDisable(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), filepath.Join(dataDir, "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{ID: "abc", Name: "abc", Status: ingestion.StatusPending, Source: ingestion.Source{Type: "csv", URL: "https://example.com/abc.csv"}, Destination: ingestion.Destination{Type: "duckdb", Path: filepath.Join(dataDir, "pompos.duckdb"), Table: "abc"}}
	persistSpec(t, dataDir, &item)
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	manager, err := New(log.New(io.Discard, "", 0), metadata, func(context.Context, ingestion.Run) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	if err := manager.Validate("not a cron"); err == nil {
		t.Fatal("invalid cron was accepted")
	}
	item.Schedule = "*/5 * * * *"
	if err := manager.Upsert(item); err != nil {
		t.Fatal(err)
	}
	item.Schedule = ""
	if err := manager.Upsert(item); err != nil {
		t.Fatal(err)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Schedule != "" || stored.NextRun != nil {
		t.Fatalf("stored = %#v, error = %v", stored, err)
	}
}

func TestWorkerRunsManualQueuePersistedBeforeStartup(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), filepath.Join(dataDir, "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{ID: "manual", Name: "manual", Status: ingestion.StatusSucceeded, Source: ingestion.Source{Type: "csv"}}
	item.Source.URL = "https://example.com/manual.csv"
	item.Destination = ingestion.Destination{Type: "duckdb", Path: filepath.Join(dataDir, "pompos.duckdb"), Table: "manual"}
	persistSpec(t, dataDir, &item)
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := metadata.EnqueueRun(ctx, item.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := metadata.ClaimRun(ctx, time.Now(), time.Now().Add(-time.Hour)); err != nil || !ok {
		t.Fatalf("simulate crashed claim: ok=%v, error=%v", ok, err)
	}
	ran := make(chan string, 1)
	manager, err := New(log.New(io.Discard, "", 0), metadata, func(_ context.Context, run ingestion.Run) error {
		ran <- run.IngestionID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	select {
	case id := <-ran:
		if id != item.ID {
			t.Fatalf("ran ingestion %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("persisted manual run was not claimed after worker startup")
	}
}

func persistSpec(t *testing.T, directory string, item *ingestion.Ingestion) {
	t.Helper()
	path, err := spec.Write(filepath.Join(directory, "ingestions"), *item)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	item.SpecPath, item.SpecDigest = path, spec.Digest(data)
}
