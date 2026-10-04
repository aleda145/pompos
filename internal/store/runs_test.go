package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pompos/internal/ingestion"
)

func TestRunHistoryAndLogsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := Open(ctx, path, "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	for _, id := range []string{"one", "two"} {
		if err := db.Create(ctx, ingestion.Ingestion{ID: id, Status: ingestion.StatusPending}); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		if err := db.EnqueueRun(ctx, "one", now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := db.ClaimRun(ctx, now, now.Add(-time.Hour))
	if err != nil || !ok {
		t.Fatalf("claim: %v, %v", ok, err)
	}
	if err := db.AppendRunLog(ctx, first.ID, "hello\n"); err != nil {
		t.Fatal(err)
	}
	if err := db.FinishRun(ctx, first.ID, "source unavailable"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetRun(ctx, "two", first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-ingestion read: %v", err)
	}
	page, err := db.ListRuns(ctx, "one", 0, 2)
	if err != nil || len(page) != 2 || page[0].ID <= page[1].ID {
		t.Fatalf("page = %+v, %v", page, err)
	}
	older, err := db.ListRuns(ctx, "one", page[1].ID, 2)
	if err != nil || len(older) != 1 || older[0].ID != first.ID {
		t.Fatalf("older = %+v, %v", older, err)
	}
	if active, err := db.HasActiveRuns(ctx, "one"); err != nil || !active {
		t.Fatalf("active = %v, %v", active, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path, "unused")
	if err != nil {
		t.Fatal(err)
	}
	run, err := db.GetRun(ctx, "one", first.ID)
	if err != nil || run.Status != ingestion.StatusFailed || run.LastError != "source unavailable" || run.Log != "hello\n" || run.StartedAt == nil || run.FinishedAt == nil || run.Attempts != 1 {
		t.Fatalf("persisted run = %+v, %v", run, err)
	}
	for _, output := range []string{strings.Repeat("界", 70000), "\ntail"} {
		if err := db.AppendRunLog(ctx, first.ID, output); err != nil {
			t.Fatal(err)
		}
	}
	run, err = db.GetRun(ctx, "one", first.ID)
	if err != nil || !run.LogTruncated || len([]rune(run.Log)) != 65536 || !strings.HasSuffix(run.Log, "\ntail") {
		t.Fatalf("bounded log: truncated=%v, length=%d, err=%v", run.LogTruncated, len([]rune(run.Log)), err)
	}
}

func TestRunLogTableAddedToExistingMetadata(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := Open(ctx, path, "unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(ctx, ingestion.Ingestion{ID: "existing", Status: ingestion.StatusPending}); err != nil {
		t.Fatal(err)
	}
	if err := db.EnqueueRun(ctx, "existing", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`DROP TABLE ingestion_run_logs`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(ctx, path, "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	runs, err := db.ListRuns(ctx, "existing", 0, 25)
	if err != nil || len(runs) != 1 {
		t.Fatalf("history = %+v, %v", runs, err)
	}
	if err := db.AppendRunLog(ctx, runs[0].ID, "upgraded\n"); err != nil {
		t.Fatal(err)
	}
	run, err := db.GetRun(ctx, "existing", runs[0].ID)
	if err != nil || run.Log != "upgraded\n" {
		t.Fatalf("log = %q, %v", run.Log, err)
	}
}
