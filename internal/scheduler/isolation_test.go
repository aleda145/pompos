package scheduler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/execution"
	"pompos/internal/ingestion"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func isolationStore(t *testing.T) (*store.SQLite, []ingestion.Ingestion) {
	t.Helper()
	dir := t.TempDir()
	destination := filepath.Join(dir, "out.duckdb")
	metadata, err := store.Open(context.Background(), filepath.Join(dir, "metadata.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metadata.Close() })
	var items []ingestion.Ingestion
	for _, id := range []string{"a-broken", "z-healthy"} {
		item := ingestion.Ingestion{ID: id, Name: id, Status: ingestion.StatusPending, Schedule: "*/5 * * * *",
			Source:      ingestion.Source{Type: "python", URL: "fixture"},
			Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "rows"}}
		persistSpec(t, dir, &item)
		if err := metadata.Create(context.Background(), item); err != nil {
			t.Fatal(err)
		}
		items = append(items, item)
	}
	return metadata, items
}

func TestBrokenScheduleIsSkippedAndRecovers(t *testing.T) {
	for _, failure := range []string{"missing", "malformed", "invalid cron"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			metadata, items := isolationStore(t)
			broken, healthy := items[0], items[1]
			original, err := os.ReadFile(broken.SpecPath)
			if err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "missing":
				err = os.Remove(broken.SpecPath)
			case "malformed":
				err = os.WriteFile(broken.SpecPath, []byte("invalid: ["), 0600)
			case "invalid cron":
				broken.Schedule = "not a cron"
				_, err = spec.Write(filepath.Dir(broken.SpecPath), broken)
			}
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 10, 2, 12, 2, 0, 0, time.UTC)
			due := now.Add(-time.Minute)
			for _, item := range items {
				if err := metadata.UpdateNextRun(ctx, item.ID, &due); err != nil {
					t.Fatal(err)
				}
			}
			var ran []string
			var logs bytes.Buffer
			manager, err := New(log.New(&logs, "", 0), metadata, func(_ context.Context, run ingestion.Run) error {
				ran = append(ran, run.IngestionID)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Shutdown(ctx)
			manager.now = func() time.Time { return now }
			manager.poll(ctx)
			manager.poll(ctx)
			if len(ran) != 1 || ran[0] != healthy.ID {
				t.Fatalf("runs = %v", ran)
			}
			stored, err := metadata.Get(ctx, broken.ID)
			if err != nil || stored.NextRun != nil {
				t.Fatalf("broken schedule still active: %#v, %v", stored, err)
			}
			if strings.Count(logs.String(), "schedule reconciliation failed") != 1 {
				t.Fatalf("repeated failure should log once: %s", &logs)
			}
			if err := os.WriteFile(broken.SpecPath, original, 0600); err != nil {
				t.Fatal(err)
			}
			manager.poll(ctx)
			stored, err = metadata.Get(ctx, broken.ID)
			if err != nil || stored.NextRun == nil || !stored.NextRun.After(now) {
				t.Fatalf("restored schedule did not recover: %#v, %v", stored, err)
			}
			now = *stored.NextRun
			manager.poll(ctx)
			if len(ran) != 3 || ran[1] != broken.ID || ran[2] != healthy.ID {
				t.Fatalf("restored schedule runs = %v", ran)
			}
		})
	}
}

func TestWorkerContinuesAfterIngestionFailure(t *testing.T) {
	for _, failure := range []string{"missing YAML", "runner error", "runner panic"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			metadata, items := isolationStore(t)
			broken, healthy := items[0], items[1]
			now := time.Now().UTC()
			for i, item := range items {
				if err := metadata.EnqueueRun(ctx, item.ID, now.Add(time.Duration(i)*time.Nanosecond)); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "missing YAML" {
				if err := os.Remove(broken.SpecPath); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			service, err := execution.New(execution.Service{Store: metadata, Logger: log.New(io.Discard, "", 0),
				Runner: isolationRunner(func(context.Context, compiler.ExecutionPlan) error {
					calls++
					if calls == 1 {
						switch failure {
						case "runner error":
							return errors.New("fixture failure")
						case "runner panic":
							panic("fixture failure")
						}
					}
					return nil
				})})
			if err != nil {
				t.Fatal(err)
			}
			manager, err := New(log.New(io.Discard, "", 0), metadata, service.Run)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Shutdown(ctx)
			manager.poll(ctx)
			for _, item := range items {
				stored, err := metadata.Get(ctx, item.ID)
				if err != nil {
					t.Fatal(err)
				}
				if item.ID == broken.ID && (stored.Status != ingestion.StatusFailed || stored.LastError == "") {
					t.Fatalf("failure not persisted: %#v", stored)
				}
				if item.ID == healthy.ID && stored.Status != ingestion.StatusSucceeded {
					t.Fatalf("healthy run blocked: %#v", stored)
				}
			}
			// A failed run must be finished, not left running for later reclamation.
			if run, ok, err := metadata.ClaimRun(ctx, now.Add(2*time.Hour), now.Add(time.Hour)); err != nil || ok {
				t.Fatalf("queue not drained: %#v, %v, %v", run, ok, err)
			}
		})
	}
}

type isolationRunner func(context.Context, compiler.ExecutionPlan) error

func (f isolationRunner) Run(ctx context.Context, plan compiler.ExecutionPlan) error {
	return f(ctx, plan)
}
