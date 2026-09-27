package execution

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"pompos/internal/compiler"
	"pompos/internal/ingestion"
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
	item.Runtime = ingestion.Runtime{Engine: "python", Script: "customers.py", ScriptDigest: spec.Digest([]byte("fixture"))}
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
	service, err := New(Service{
		Store: metadata,
		Runner: runnerFunc(func(context.Context, compiler.ExecutionPlan) error {
			return errors.New("Python failed")
		}),
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	queued := ingestion.Run{IngestionID: item.ID, SpecPath: item.SpecPath, SpecDigest: item.SpecDigest}
	if err := service.Run(ctx, queued); err == nil || err.Error() != "ingestion failed: Python failed" {
		t.Fatalf("run error = %v", err)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Status != ingestion.StatusFailed || stored.LastError != "Python failed" {
		t.Fatalf("stored = %#v, error = %v", stored, err)
	}
}

type runnerFunc func(context.Context, compiler.ExecutionPlan) error

func (f runnerFunc) Run(ctx context.Context, plan compiler.ExecutionPlan) error {
	return f(ctx, plan)
}
