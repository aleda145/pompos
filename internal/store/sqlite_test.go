package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"pompos/internal/destination"
	"pompos/internal/ingestion"
	"pompos/internal/secrets"
)

func TestIngestionProjectionSchemaIsOperationalOnly(t *testing.T) {
	metadata, err := Open(context.Background(), filepath.Join(t.TempDir(), "pompos.sqlite"), "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	rows, err := metadata.db.Query(`PRAGMA table_info(ingestions)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var position, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&position, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	want := []string{"id", "status", "last_run_at", "last_error", "next_run_at", "spec_path", "spec_digest"}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %v, want %v", columns, want)
	}
}

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
		Source:      ingestion.Source{Type: "python", URL: "https://example.com/customers.csv"},
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
	if err := metadata.UpdateNextRun(ctx, item.ID, &nextRun); err != nil {
		t.Fatal(err)
	}
	got, err = metadata.Get(ctx, item.ID)
	if err != nil || got.NextRun == nil || !got.NextRun.Equal(nextRun) {
		t.Fatalf("next run after update = %v, %v", got.NextRun, err)
	}
}

func TestRunCanBeReclaimedAfterWorkerCrash(t *testing.T) {
	ctx := context.Background()
	metadata, err := Open(ctx, filepath.Join(t.TempDir(), "pompos.sqlite"), filepath.Join(t.TempDir(), "pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{ID: "scheduled", Name: "scheduled", Status: ingestion.StatusPending, Schedule: "* * * * *", Source: ingestion.Source{Type: "python"}}
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	if err := metadata.UpdateNextRun(ctx, item.ID, &due); err != nil {
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
	item := ingestion.Ingestion{ID: "manual", Name: "manual", Status: ingestion.StatusSucceeded, Source: ingestion.Source{Type: "python"}, SpecPath: "/data/specs/manual.yaml", SpecDigest: "sha256:abc"}
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
	if err != nil || !ok || run.IngestionID != item.ID || run.Trigger != "manual" || run.SpecPath != item.SpecPath || run.SpecDigest != item.SpecDigest {
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

func TestSQLiteDestinationsLifecycle(t *testing.T) {
	ctx := context.Background()
	defaultPath := filepath.Join(t.TempDir(), "pompos.duckdb")
	metadata, err := Open(ctx, filepath.Join(t.TempDir(), "pompos.sqlite"), defaultPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	local, err := metadata.GetDestination(ctx, "local-duckdb")
	if err != nil || local.Path != defaultPath {
		t.Fatalf("default destination = %#v, %v", local, err)
	}
	warehouse := destination.NewDuckDB("warehouse", filepath.Join(t.TempDir(), "warehouse.duckdb"))
	if err := metadata.PutDestination(ctx, warehouse); err != nil {
		t.Fatal(err)
	}
	warehouse.Path = filepath.Join(t.TempDir(), "updated.duckdb")
	if err := metadata.PutDestination(ctx, warehouse); err != nil {
		t.Fatal(err)
	}
	stored, err := metadata.GetDestination(ctx, "warehouse")
	if err != nil || stored.Path != warehouse.Path {
		t.Fatalf("stored destination = %#v, %v", stored, err)
	}
	configs, err := metadata.ListDestinations(ctx)
	if err != nil || len(configs) != 2 || configs[0].Name != "local-duckdb" || configs[1].Name != "warehouse" {
		t.Fatalf("destinations = %#v, %v", configs, err)
	}
}

func TestDeleteDestinationPreservesFilesAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "metadata.sqlite")
	destinationPath := filepath.Join(dir, "data.duckdb")
	contents := []byte("destination data")
	if err := os.WriteFile(destinationPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	metadata, err := Open(ctx, databasePath, destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := metadata.DeleteDestination(ctx, "missing"); !errors.Is(err, destination.ErrNotFound) {
		t.Fatalf("delete missing destination = %v", err)
	}
	if err := metadata.DeleteDestination(ctx, "local-duckdb"); err != nil {
		t.Fatal(err)
	}
	metadata.Close()
	reopened, err := Open(ctx, databasePath, destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	configs, err := reopened.ListDestinations(ctx)
	if err != nil || len(configs) != 0 {
		t.Fatalf("destinations after reopening = %#v, %v", configs, err)
	}
	data, err := os.ReadFile(destinationPath)
	if err != nil || string(data) != string(contents) {
		t.Fatalf("destination file changed: %q, %v", data, err)
	}
}

func TestUnsupportedSchemaIsRejectedWithoutDeletingData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	metadata, err := Open(ctx, path, "unused")
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.Secrets().Put(ctx, "keep-me", []byte("untouched")); err != nil {
		t.Fatal(err)
	}
	if _, err = metadata.db.Exec("PRAGMA user_version = 5"); err != nil {
		t.Fatal(err)
	}
	metadata.Close()
	if reopened, err := Open(ctx, path, "unused"); err == nil {
		reopened.Close()
		t.Fatal("unsupported schema accepted")
	}
	// Inspect directly, without invoking initialization.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err = db.QueryRow("SELECT value FROM secrets WHERE key = 'keep-me'").Scan(&value); err != nil || value != "untouched" {
		t.Fatalf("existing data was changed: %q %v", value, err)
	}
}
