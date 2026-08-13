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
	nextRun := time.Date(2026, time.August, 14, 6, 0, 0, 0, time.UTC)
	if err := metadata.UpdateSchedule(ctx, item.ID, "0 6 * * *", &nextRun); err != nil {
		t.Fatal(err)
	}
	got, err = metadata.Get(ctx, item.ID)
	if err != nil || got.Schedule != "0 6 * * *" || got.NextRun == nil || !got.NextRun.Equal(nextRun) {
		t.Fatalf("schedule after update = %q, %v", got.Schedule, err)
	}
}

func TestRunCanBeReclaimedAfterWorkerCrash(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(ctx, filepath.Join(t.TempDir(), "pompos.sqlite"), filepath.Join(t.TempDir(), "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{ID: "scheduled", Name: "scheduled", Status: ingestion.StatusPending, Schedule: "* * * * *", Source: ingestion.Source{Type: "csv"}}
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	if err := metadata.UpdateSchedule(ctx, item.ID, item.Schedule, &due); err != nil {
		t.Fatal(err)
	}
	enqueued, err := metadata.EnqueueScheduledRun(ctx, item.ID, due, now.Add(time.Minute))
	if err != nil || !enqueued {
		t.Fatalf("enqueue = %v, %v", enqueued, err)
	}
	first, ok, err := metadata.ClaimRun(ctx, now, now.Add(-time.Hour))
	if err != nil || !ok || first.Attempts != 1 {
		t.Fatalf("first claim = %#v, %v, %v", first, ok, err)
	}
	if _, ok, err := metadata.ClaimRun(ctx, now.Add(30*time.Minute), now.Add(-30*time.Minute)); err != nil || ok {
		t.Fatalf("fresh claim was reclaimed: %v, %v", ok, err)
	}
	second, ok, err := metadata.ClaimRun(ctx, now.Add(2*time.Hour), now.Add(time.Hour))
	if err != nil || !ok || second.ID != first.ID || second.Attempts != 2 {
		t.Fatalf("reclaimed = %#v, %v, %v", second, ok, err)
	}
	if err := metadata.ReleaseRun(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	third, ok, err := metadata.ClaimRun(ctx, now.Add(2*time.Hour), now.Add(-time.Hour))
	if err != nil || !ok || third.ID != first.ID || third.Attempts != 3 {
		t.Fatalf("released claim = %#v, %v, %v", third, ok, err)
	}
}

func TestManualRunIsPersistedBeforeClaim(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(ctx, filepath.Join(t.TempDir(), "pompos.sqlite"), filepath.Join(t.TempDir(), "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{ID: "manual", Name: "manual", Status: ingestion.StatusSucceeded, Source: ingestion.Source{Type: "csv"}}
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	if err := metadata.EnqueueRun(ctx, item.ID, now); err != nil {
		t.Fatal(err)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Status != ingestion.StatusPending {
		t.Fatalf("stored = %#v, error = %v", stored, err)
	}
	run, ok, err := metadata.ClaimRun(ctx, now, now.Add(-time.Hour))
	if err != nil || !ok || run.IngestionID != item.ID || run.Trigger != "manual" {
		t.Fatalf("claim = %#v, %v, %v", run, ok, err)
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
