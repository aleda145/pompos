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
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/policy"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestCreateIngestionEnqueuesAndRendersPendingDetail(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()

	schedules := &scheduleManagerStub{
		persist: func(item ingestion.Ingestion) error { return nil },
		enqueue: func(ctx context.Context, id string) error { return metadata.EnqueueRun(ctx, id, time.Now()) },
	}
	var logs bytes.Buffer
	app, err := New(App{
		Store:     metadata,
		Policy:    policy.DefaultEngine{DestinationPath: destination},
		Secrets:   metadata.Secrets(),
		Scheduler: schedules,
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

	form := url.Values{"csv_url": {"https://example.com/customers.csv"}, "table_name": {"customers"}, "schedule": {"0 6 * * *"}}
	request := httptest.NewRequest(http.MethodPost, "/ingestions", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(schedules.enqueued) != 1 {
		t.Fatalf("enqueued = %#v", schedules.enqueued)
	}

	item, err := metadata.Get(ctx, schedules.enqueued[0])
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != ingestion.StatusPending || item.LastRun != nil || item.Schedule != "" {
		t.Fatalf("stored ingestion = %#v", item)
	}
	document, _, err := spec.Read(item.SpecPath)
	if err != nil || document.Schedule == nil || document.Schedule.Cron != "0 6 * * *" {
		t.Fatalf("document = %#v, error = %v", document, err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "ingestions", item.ID+".yaml")); err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{"request started", "metadata persisted", "spec written", "request completed"} {
		if !strings.Contains(logs.String(), message) {
			t.Errorf("logs do not contain %q:\n%s", message, logs.String())
		}
	}

	detailRequest := httptest.NewRequest(http.MethodGet, response.Header().Get("Location"), nil)
	detailResponse := httptest.NewRecorder()
	app.Handler().ServeHTTP(detailResponse, detailRequest)
	if detailResponse.Code != http.StatusOK || !strings.Contains(detailResponse.Body.String(), "pending") || !strings.Contains(detailResponse.Body.String(), "Run queued") || !strings.Contains(detailResponse.Body.String(), "engine: ingestr") {
		t.Fatalf("GET detail status = %d, body = %s", detailResponse.Code, detailResponse.Body.String())
	}
}

func TestUpdateSchedulePersistsAndRegisters(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	item := ingestion.Ingestion{
		ID: "scheduled", Name: "customers", Status: ingestion.StatusSucceeded,
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "customers"},
	}
	path, err := spec.Write(filepath.Join(dataDir, "ingestions"), item)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	item.SpecPath, item.SpecDigest = path, spec.Digest(data)
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	schedules := &scheduleManagerStub{}
	app, err := New(App{
		Store: metadata, Policy: policy.DefaultEngine{DestinationPath: destination},
		Secrets: metadata.Secrets(), Scheduler: schedules,
		Validator:   validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"schedule": {"15 * * * *"}}
	request := httptest.NewRequest(http.MethodPost, "/ingestions/scheduled/schedule", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil || stored.Schedule != "" || schedules.item.Schedule != "15 * * * *" {
		t.Fatalf("stored = %#v, scheduled = %#v, error = %v", stored, schedules.item, err)
	}
	specContent, err := os.ReadFile(filepath.Join(dataDir, "ingestions", item.ID+".yaml"))
	if err != nil || !strings.Contains(string(specContent), `cron: 15 * * * *`) {
		t.Fatalf("spec = %s, error = %v", specContent, err)
	}
}

func TestRunAgainOnlyEnqueuesDurableWork(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	destination := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()

	item := ingestion.Ingestion{
		ID: "rerun", Name: "customers", Status: ingestion.StatusSucceeded,
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "customers"},
	}
	path, err := spec.Write(filepath.Join(dataDir, "ingestions"), item)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	item.SpecPath, item.SpecDigest = path, spec.Digest(data)
	if err := metadata.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	schedules := &scheduleManagerStub{enqueue: func(ctx context.Context, id string) error {
		return metadata.EnqueueRun(ctx, id, time.Now())
	}}
	app, err := New(App{
		Store:   metadata,
		Policy:  policy.DefaultEngine{DestinationPath: destination},
		Secrets: metadata.Secrets(), Scheduler: schedules,
		Validator:   validatorFunc(func(context.Context, ingestion.Source) error { return nil }),
		Destination: ingestion.Destination{Type: "duckdb", Path: destination},
		SpecDir:     filepath.Join(dataDir, "ingestions"),
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/ingestions/rerun/run", nil)
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Location") != "/ingestions/rerun?run=queued" {
		t.Fatalf("redirect = %q", response.Header().Get("Location"))
	}
	stored, err := metadata.Get(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(schedules.enqueued) != 1 || schedules.enqueued[0] != item.ID || stored.Status != ingestion.StatusPending {
		t.Fatalf("enqueued = %#v, stored ingestion = %#v", schedules.enqueued, stored)
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

	schedules := &scheduleManagerStub{enqueue: func(ctx context.Context, id string) error {
		return metadata.EnqueueRun(ctx, id, time.Now())
	}}
	app, err := New(App{
		Store:   metadata,
		Policy:  policy.DefaultEngine{DestinationPath: destination},
		Secrets: metadata.Secrets(), Scheduler: schedules,
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
	if len(schedules.enqueued) != 2 {
		t.Fatalf("queued runs = %d, want 2", len(schedules.enqueued))
	}
	first, err := metadata.Get(ctx, schedules.enqueued[0])
	if err != nil {
		t.Fatal(err)
	}
	first, err = app.hydrate(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := metadata.Get(ctx, schedules.enqueued[1])
	if err != nil {
		t.Fatal(err)
	}
	second, err = app.hydrate(second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Destination.Table != "openai_codex_issues" || second.Destination.Table != "openai_codex_stargazers" {
		t.Fatalf("destination tables = %q, %q", first.Destination.Table, second.Destination.Table)
	}
	stored := first
	if stored.Source.AccessToken != "" || stored.Source.SecretKey != "github-codex" || stored.Source.Table != "issues" {
		t.Fatalf("stored source = %#v", stored.Source)
	}
	savedToken, err := metadata.Secrets().Get(ctx, stored.Source.SecretKey)
	if err != nil || string(savedToken) != "github_pat_secret" {
		t.Fatalf("saved token = %q, error = %v", savedToken, err)
	}
	specContent, err := os.ReadFile(filepath.Join(dataDir, "ingestions", first.ID+".yaml"))
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

	schedules := &scheduleManagerStub{enqueue: func(ctx context.Context, id string) error {
		return metadata.EnqueueRun(ctx, id, time.Now())
	}}
	app, err := New(App{
		Store: metadata, Policy: policy.DefaultEngine{DestinationPath: destination},
		Secrets: metadata.Secrets(), Scheduler: schedules, Validator: validatorFunc(func(_ context.Context, source ingestion.Source) error {
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
	if response.Code != http.StatusSeeOther || len(schedules.enqueued) != 1 {
		t.Fatalf("status = %d, queued = %#v, body = %s", response.Code, schedules.enqueued, response.Body.String())
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
	secretItem := ingestion.Ingestion{
		ID: "uses-secret", Name: "Codex issues", Status: ingestion.StatusSucceeded,
		Source:      ingestion.Source{Type: "github", Owner: "openai", Repository: "codex", SecretKey: "github-production", Table: "issues"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "codex_issues"},
	}
	secretPath, err := spec.Write(filepath.Join(dataDir, "ingestions"), secretItem)
	if err != nil {
		t.Fatal(err)
	}
	secretData, err := os.ReadFile(secretPath)
	if err != nil {
		t.Fatal(err)
	}
	secretItem.SpecPath, secretItem.SpecDigest = secretPath, spec.Digest(secretData)
	if err := metadata.Create(ctx, secretItem); err != nil {
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

type validatorFunc func(context.Context, ingestion.Source) error

func (f validatorFunc) Validate(ctx context.Context, source ingestion.Source) error {
	return f(ctx, source)
}

type scheduleManagerStub struct {
	item     ingestion.Ingestion
	persist  func(ingestion.Ingestion) error
	enqueue  func(context.Context, string) error
	enqueued []string
}

func (s *scheduleManagerStub) Validate(value string) error {
	if value == "invalid" {
		return errors.New("invalid cron schedule")
	}
	return nil
}
func (s *scheduleManagerStub) Upsert(item ingestion.Ingestion) error {
	s.item = item
	if s.persist != nil {
		return s.persist(item)
	}
	return nil
}
func (s *scheduleManagerStub) Enqueue(ctx context.Context, id string) error {
	s.enqueued = append(s.enqueued, id)
	if s.enqueue != nil {
		return s.enqueue(ctx, id)
	}
	return nil
}
func (s *scheduleManagerStub) NextRun(string) *time.Time { return nil }
