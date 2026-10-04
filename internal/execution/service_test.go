package execution

import (
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
	"pompos/internal/ingestion"
	"pompos/internal/runner"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestRunPersistsRunnerFailure(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{
		ID: "customers", Name: "customers", Status: ingestion.StatusPending,
		Source:      ingestion.Source{Type: "python", URL: "https://example.com/customers.csv", Table: "customers"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "customers"},
	}
	item.Runtime = ingestion.Runtime{Engine: "python", Script: "customers.py"}
	document := spec.FromIngestion(item)
	data, err := spec.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	item.SpecPath = filepath.Join(dataDir, "customers.yaml")
	item.SpecDigest = spec.Digest(data)
	if err := os.WriteFile(item.SpecPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := metadata.EnqueueRun(ctx, item.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	queued, ok, err := metadata.ClaimRun(ctx, time.Now(), time.Now().Add(-time.Hour))
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	service, err := New(Service{
		Store: metadata,
		Runner: runnerFunc(func(ctx context.Context, plan compiler.ExecutionPlan) error {
			if plan.Strategy != "append" {
				t.Fatalf("runner did not use the edited YAML: %#v", plan)
			}
			runner.Log(ctx, "Fetched 42 rows\n")
			running, err := metadata.GetRun(ctx, item.ID, queued.ID)
			if err != nil || !strings.Contains(running.Log, "Fetched 42 rows") || running.Status != ingestion.StatusRunning {
				t.Fatalf("live log = %+v, %v", running, err)
			}
			return errors.New("Python failed")
		}),
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The queued digest must not prevent manual edits before execution.
	document.Materialization.Strategy = "append"
	data, err = spec.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(item.SpecPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := service.Run(ctx, queued); err == nil || err.Error() != "ingestion failed: Python failed" {
		t.Fatalf("run error = %v", err)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Status != ingestion.StatusFailed || stored.LastError != "Python failed" {
		t.Fatalf("stored = %#v, error = %v", stored, err)
	}
	run, err := metadata.GetRun(ctx, item.ID, queued.ID)
	if err != nil || !strings.Contains(run.Log, "FAILED: ingestion failed: Python failed") {
		t.Fatalf("failure log = %+v, %v", run, err)
	}
}

type runnerFunc func(context.Context, compiler.ExecutionPlan) error

func (f runnerFunc) Run(ctx context.Context, plan compiler.ExecutionPlan) error {
	return f(ctx, plan)
}
