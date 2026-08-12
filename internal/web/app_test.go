package web

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/ingestion"
	"pompos/internal/policy"
	"pompos/internal/store"
)

func TestCreateIngestionRunsAndRendersDetail(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()

	var ran ingestion.Ingestion
	app, err := New(App{
		Store:     metadata,
		Policy:    policy.DefaultEngine{DestinationPath: destination},
		Runner:    runnerFunc(func(_ context.Context, item ingestion.Ingestion) error { ran = item; return nil }),
		Validator: validatorFunc(func(context.Context, string) error { return nil }),
		Destination: ingestion.Destination{
			Type: "duckdb", Path: destination,
		},
		SpecDir: filepath.Join(dataDir, "ingestions"),
		Logger:  log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{"csv_url": {"https://example.com/customers.csv"}, "table_name": {"customers"}}
	request := httptest.NewRequest(http.MethodPost, "/ingestions", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
	if ran.Destination.Table != "customers" {
		t.Fatalf("runner received %#v", ran)
	}

	item, err := metadata.Get(ctx, ran.ID)
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != ingestion.StatusSucceeded || item.LastRun == nil {
		t.Fatalf("stored ingestion = %#v", item)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "ingestions", ran.ID+".yaml")); err != nil {
		t.Fatal(err)
	}

	detailRequest := httptest.NewRequest(http.MethodGet, response.Header().Get("Location"), nil)
	detailResponse := httptest.NewRecorder()
	app.Handler().ServeHTTP(detailResponse, detailRequest)
	if detailResponse.Code != http.StatusOK || !strings.Contains(detailResponse.Body.String(), "succeeded") {
		t.Fatalf("GET detail status = %d, body = %s", detailResponse.Code, detailResponse.Body.String())
	}
}

func TestCreateIngestionPersistsRunnerFailure(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()

	var id string
	app, err := New(App{
		Store:  metadata,
		Policy: policy.DefaultEngine{DestinationPath: destination},
		Runner: runnerFunc(func(_ context.Context, item ingestion.Ingestion) error {
			id = item.ID
			return errors.New("ingestr exploded")
		}),
		Validator:   validatorFunc(func(context.Context, string) error { return nil }),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"),
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"csv_url": {"https://example.com/customers.csv"}, "table_name": {"customers"}}
	request := httptest.NewRequest(http.MethodPost, "/ingestions", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
	item, err := metadata.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != ingestion.StatusFailed || item.LastError != "ingestr exploded" {
		t.Fatalf("stored ingestion = %#v", item)
	}
}

type runnerFunc func(context.Context, ingestion.Ingestion) error

func (f runnerFunc) Run(ctx context.Context, item ingestion.Ingestion) error { return f(ctx, item) }

type validatorFunc func(context.Context, string) error

func (f validatorFunc) Validate(ctx context.Context, rawURL string) error { return f(ctx, rawURL) }
