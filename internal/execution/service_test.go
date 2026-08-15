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
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "customers"},
	}
	document := spec.FromLegacy(item)
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
		Store: metadata, Blueprint: compiler.LocalDuckDB(destination),
		Runner: runnerFunc(func(context.Context, string, compiler.ExecutionPlan, string) error {
			return errors.New("ingestr exploded")
		}),
		Secrets: metadata.Secrets(), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	queued := ingestion.Run{IngestionID: item.ID, SpecPath: item.SpecPath, SpecDigest: item.SpecDigest}
	if err := service.Run(ctx, queued); err == nil || err.Error() != "ingestion failed: ingestr exploded" {
		t.Fatalf("run error = %v", err)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Status != ingestion.StatusFailed || stored.LastError != "ingestr exploded" {
		t.Fatalf("stored = %#v, error = %v", stored, err)
	}
}

type runnerFunc func(context.Context, string, compiler.ExecutionPlan, string) error

func (f runnerFunc) Run(ctx context.Context, id string, plan compiler.ExecutionPlan, credential string) error {
	return f(ctx, id, plan, credential)
}
