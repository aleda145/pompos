package web

import (
	"bytes"
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
	var logs bytes.Buffer
	app, err := New(App{
		Store:     metadata,
		Policy:    policy.DefaultEngine{DestinationPath: destination},
		Runner:    runnerFunc(func(_ context.Context, item ingestion.Ingestion) error { ran = item; return nil }),
		Secrets:   metadata.Secrets(),
		Validator: validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
		Destination: ingestion.Destination{
			Type: "duckdb", Path: destination,
		},
		SpecDir: filepath.Join(dataDir, "ingestions"),
		Logger:  log.New(&logs, "", 0),
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
	for _, message := range []string{"request started", "metadata persisted", "spec written", "ingestion succeeded", "request completed"} {
		if !strings.Contains(logs.String(), message) {
			t.Errorf("logs do not contain %q:\n%s", message, logs.String())
		}
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
		Secrets:     metadata.Secrets(),
		Validator:   validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
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

func TestCreateGitHubIngestionsForSelectedTables(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()

	var ran []ingestion.Ingestion
	app, err := New(App{
		Store:  metadata,
		Policy: policy.DefaultEngine{DestinationPath: destination},
		Runner: runnerFunc(func(_ context.Context, item ingestion.Ingestion) error {
			ran = append(ran, item)
			return nil
		}),
		Secrets:     metadata.Secrets(),
		Validator:   validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"),
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{
		"source_type": {"github"}, "repository": {"https://github.com/OpenAI/codex"},
		"new_secret_name": {"github-codex"}, "access_token": {"github_pat_secret"}, "tables": {"issues", "stargazers"},
	}
	request := httptest.NewRequest(http.MethodPost, "/ingestions", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(ran) != 2 {
		t.Fatalf("runner calls = %d, want 2", len(ran))
	}
	if ran[0].Destination.Table != "openai_codex_issues" || ran[1].Destination.Table != "openai_codex_stargazers" {
		t.Fatalf("destination tables = %q, %q", ran[0].Destination.Table, ran[1].Destination.Table)
	}
	stored, err := metadata.Get(ctx, ran[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Source.AccessToken != "" || stored.Source.SecretKey != "github-codex" || stored.Source.Table != "issues" {
		t.Fatalf("stored source = %#v", stored.Source)
	}
	savedToken, err := metadata.Secrets().Get(ctx, stored.Source.SecretKey)
	if err != nil || string(savedToken) != "github_pat_secret" {
		t.Fatalf("saved token = %q, error = %v", savedToken, err)
	}
	specContent, err := os.ReadFile(filepath.Join(dataDir, "ingestions", ran[0].ID+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(specContent), "github_pat_secret") {
		t.Fatal("GitHub token was written to the ingestion spec")
	}
}

func TestCreateGitHubIngestionUsesSelectedSecret(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if err := metadata.Secrets().Put(ctx, "github-codex", []byte("saved-token")); err != nil {
		t.Fatal(err)
	}

	var ran ingestion.Ingestion
	app, err := New(App{
		Store: metadata, Policy: policy.DefaultEngine{DestinationPath: destination},
		Runner:  runnerFunc(func(_ context.Context, item ingestion.Ingestion) error { ran = item; return nil }),
		Secrets: metadata.Secrets(), Validator: validatorFunc(func(_ context.Context, source ingestion.Source) error {
			if source.AccessToken != "saved-token" {
				t.Fatalf("validator token = %q", source.AccessToken)
			}
			return nil
		}),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"source_type": {"github"}, "repository": {"openai/codex"}, "secret_key": {"github-codex"}, "tables": {"issues"}}
	request := httptest.NewRequest(http.MethodPost, "/ingestions", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || ran.Source.AccessToken != "saved-token" {
		t.Fatalf("status = %d, runner source = %#v, body = %s", response.Code, ran.Source, response.Body.String())
	}
}

func TestCreateGitHubIngestionRequiresExplicitCredentialChoice(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	app, err := New(App{
		Store: metadata, Policy: policy.DefaultEngine{DestinationPath: destination},
		Runner:  runnerFunc(func(context.Context, ingestion.Ingestion) error { t.Fatal("runner should not run"); return nil }),
		Secrets: metadata.Secrets(), Validator: validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"source_type": {"github"}, "repository": {"openai/codex"}, "tables": {"issues"}}
	request := httptest.NewRequest(http.MethodPost, "/ingestions", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity || !strings.Contains(response.Body.String(), "Select a saved secret") {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSecretsPageAddsAndListsNamesWithoutValues(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	app, err := New(App{
		Store: metadata, Policy: policy.DefaultEngine{DestinationPath: destination},
		Runner:  runnerFunc(func(context.Context, ingestion.Ingestion) error { return nil }),
		Secrets: metadata.Secrets(), Validator: validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"name": {"github-production"}, "value": {"never-render-this"}}
	request := httptest.NewRequest(http.MethodPost, "/secrets", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
	if err := metadata.Create(ctx, ingestion.Ingestion{
		ID: "uses-secret", Name: "Codex issues", Status: ingestion.StatusSucceeded,
		Source:      ingestion.Source{Type: "github", Owner: "openai", Repository: "codex", SecretKey: "github-production", Table: "issues"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "codex_issues"},
	}); err != nil {
		t.Fatal(err)
	}
	listResponse := httptest.NewRecorder()
	app.Handler().ServeHTTP(listResponse, httptest.NewRequest(http.MethodGet, "/secrets", nil))
	body := listResponse.Body.String()
	if listResponse.Code != http.StatusOK || !strings.Contains(body, "github-production") || strings.Contains(body, "never-render-this") ||
		!strings.Contains(body, "source-logo-github") || !strings.Contains(body, "Codex issues") || !strings.Contains(body, "future runs") {
		t.Fatalf("GET status = %d, body = %s", listResponse.Code, body)
	}
	deleteForm := url.Values{"key": {"github-production"}}
	deleteRequest := httptest.NewRequest(http.MethodPost, "/secrets/delete", strings.NewReader(deleteForm.Encode()))
	deleteRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	deleteResponse := httptest.NewRecorder()
	app.Handler().ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusSeeOther {
		t.Fatalf("delete status = %d, body = %s", deleteResponse.Code, deleteResponse.Body.String())
	}
	entries, err := metadata.Secrets().List(ctx)
	if err != nil || len(entries) != 0 {
		t.Fatalf("secrets after delete = %#v, %v", entries, err)
	}
}

type runnerFunc func(context.Context, ingestion.Ingestion) error

func (f runnerFunc) Run(ctx context.Context, item ingestion.Ingestion) error { return f(ctx, item) }

type validatorFunc func(context.Context, ingestion.Source) error

func (f validatorFunc) Validate(ctx context.Context, source ingestion.Source) error {
	return f(ctx, source)
}
