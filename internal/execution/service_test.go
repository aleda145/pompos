package execution

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"testing"

	"pompos/internal/ingestion"
	"pompos/internal/policy"
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
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	service, err := New(Service{
		Store: metadata, Policy: policy.DefaultEngine{DestinationPath: destination},
		Runner:  runnerFunc(func(context.Context, ingestion.Ingestion) error { return errors.New("ingestr exploded") }),
		Secrets: metadata.Secrets(), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Run(ctx, item.ID); err == nil || err.Error() != "ingestion failed: ingestr exploded" {
		t.Fatalf("run error = %v", err)
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Status != ingestion.StatusFailed || stored.LastError != "ingestr exploded" {
		t.Fatalf("stored = %#v, error = %v", stored, err)
	}
}

type runnerFunc func(context.Context, ingestion.Ingestion) error

func (f runnerFunc) Run(ctx context.Context, item ingestion.Ingestion) error { return f(ctx, item) }
