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
	"pompos/internal/ingestion"
	"pompos/internal/spec"
	"pompos/internal/store"
	"strings"
	"testing"
	"time"
)

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
		Source:      ingestion.Source{Type: "python", URL: "https://example.com/customers.csv", Table: "customers"},
		Runtime:     ingestion.Runtime{Engine: "python", Script: "customers.py", ScriptDigest: spec.Digest([]byte("fixture"))},
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
		Store:   metadata,
		Secrets: metadata.Secrets(), Scheduler: schedules,
		SpecDir: filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
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

func TestDestinationsPageSavesSQLiteDestination(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	defaultPath := filepath.Join(dataDir, "pompos.duckdb")
	metadata, err := store.Open(ctx, filepath.Join(dataDir, "pompos.sqlite"), defaultPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	app, err := New(App{
		Store: metadata, Secrets: metadata.Secrets(), Destinations: metadata,
		SpecDir: filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{"name": {"warehouse"}, "type": {"duckdb"}, "path": {filepath.Join(dataDir, "warehouse.duckdb")}}
	request := httptest.NewRequest(http.MethodPost, "/destinations", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/destinations?saved=1" {
		t.Fatalf("status = %d, location = %q, body = %s", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	stored, err := metadata.GetDestination(ctx, "warehouse")
	if err != nil || stored.Path != filepath.Join(dataDir, "warehouse.duckdb") {
		t.Fatalf("destination = %#v, error = %v", stored, err)
	}
	page := httptest.NewRecorder()
	app.Handler().ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/destinations", nil))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "warehouse") || !strings.Contains(page.Body.String(), "local-duckdb") {
		t.Fatalf("destinations page = %d, body = %s", page.Code, page.Body.String())
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
		Source:      ingestion.Source{Type: "python", URL: "https://example.com/customers.csv", Table: "customers"},
		Runtime:     ingestion.Runtime{Engine: "python", Script: "customers.py", ScriptDigest: spec.Digest([]byte("fixture"))},
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
		Store: metadata,

		Secrets: metadata.Secrets(), Scheduler: schedules,
		SpecDir: filepath.Join(dataDir, "ingestions"),
		Logger:  log.New(io.Discard, "", 0),
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
		Store:   metadata,
		Secrets: metadata.Secrets(),
		SpecDir: filepath.Join(dataDir, "ingestions"), Logger: log.New(io.Discard, "", 0),
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
		Source:      ingestion.Source{Type: "python", URL: "https://github.com/openai/codex", Table: "issues"},
		Runtime:     ingestion.Runtime{Engine: "python", Script: "issues.py", ScriptDigest: spec.Digest([]byte("fixture")), SecretRefs: []string{"github-production"}},
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
		!strings.Contains(body, "Codex issues") || !strings.Contains(body, "future runs") {
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

func TestRemovedSourceCreationEndpointsAreUnavailable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metadata, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	app, err := New(App{Store: metadata, Secrets: metadata.Secrets(), SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/ingestions", "/ingestions/preview", "/sources/columns"} {
		response := httptest.NewRecorder()
		app.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader("source_type=csv")))
		if response.Code != http.StatusNotFound && response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("removed route %s returned %d", path, response.Code)
		}
	}
	items, err := metadata.List(ctx)
	if err != nil || len(items) != 0 {
		t.Fatal("removed endpoints created an ingestion")
	}
}
