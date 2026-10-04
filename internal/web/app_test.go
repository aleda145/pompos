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
	"pompos/internal/destination"
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
		Runtime:     ingestion.Runtime{Engine: "python", Script: "customers.py"},
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
	specContent, err := os.ReadFile(spec.ArtifactPath(filepath.Join(dataDir, "ingestions"), item.ID, ".yaml"))
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
	form.Set("type", "objects")
	form.Set("path", filepath.Join(dataDir, "objects"))
	update := httptest.NewRequest(http.MethodPost, "/destinations", strings.NewReader(form.Encode()))
	update.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	updated := httptest.NewRecorder()
	app.Handler().ServeHTTP(updated, update)
	if updated.Code != http.StatusSeeOther {
		t.Fatalf("update status = %d, body = %s", updated.Code, updated.Body.String())
	}
	stored, err = metadata.GetDestination(ctx, "warehouse")
	if err != nil || stored.Type != "objects" || stored.Path != form.Get("path") {
		t.Fatalf("updated destination = %#v, error = %v", stored, err)
	}
	for _, test := range []struct {
		name   string
		status int
	}{
		{"", http.StatusUnprocessableEntity},
		{"missing", http.StatusNotFound},
		{"warehouse", http.StatusSeeOther},
		{"warehouse", http.StatusNotFound},
	} {
		deleteForm := url.Values{"name": {test.name}}
		request := httptest.NewRequest(http.MethodPost, "/destinations/delete", strings.NewReader(deleteForm.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		app.Handler().ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("delete %q: status = %d, body = %s", test.name, response.Code, response.Body.String())
		}
		if test.status == http.StatusSeeOther && response.Header().Get("Location") != "/destinations?deleted=1" {
			t.Fatalf("delete redirect = %q", response.Header().Get("Location"))
		}
	}
	if _, err := metadata.GetDestination(ctx, "warehouse"); !errors.Is(err, destination.ErrNotFound) {
		t.Fatalf("deleted destination still exists: %v", err)
	}
	if _, err := metadata.GetDestination(ctx, "local-duckdb"); err != nil {
		t.Fatalf("unrelated destination changed: %v", err)
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
		Runtime:     ingestion.Runtime{Engine: "python", Script: "customers.py"},
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
		Runtime:     ingestion.Runtime{Engine: "python", Script: "issues.py", SecretRefs: []string{"github-production"}},
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
	if !strings.Contains(body, `href="/secrets?edit=github-production#secret-form"`) {
		t.Fatal("missing secret update action")
	}
	editResponse := httptest.NewRecorder()
	app.Handler().ServeHTTP(editResponse, httptest.NewRequest(http.MethodGet, "/secrets?edit=github-production", nil))
	editBody := editResponse.Body.String()
	if editResponse.Code != http.StatusOK || !strings.Contains(editBody, `value="github-production"`) ||
		strings.Contains(editBody, "readonly") || !strings.Contains(editBody, "New value") ||
		!strings.Contains(editBody, "autofocus") || strings.Contains(editBody, "never-render-this") {
		t.Fatalf("edit status = %d, body = %s", editResponse.Code, editBody)
	}
	for _, value := range []string{"", "replacement-private-value"} {
		updateForm := url.Values{"name": {"github-production"}, "value": {value}}
		updateRequest := httptest.NewRequest(http.MethodPost, "/secrets", strings.NewReader(updateForm.Encode()))
		updateRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		updateResponse := httptest.NewRecorder()
		app.Handler().ServeHTTP(updateResponse, updateRequest)
		wantStatus, wantValue := http.StatusSeeOther, value
		if value == "" {
			wantStatus, wantValue = http.StatusUnprocessableEntity, "never-render-this"
			if strings.Contains(updateResponse.Body.String(), "readonly") || !strings.Contains(updateResponse.Body.String(), `value="github-production"`) || !strings.Contains(updateResponse.Body.String(), "New value") {
				t.Fatal("validation error lost the selected secret")
			}
		}
		stored, err := metadata.Secrets().Get(ctx, "github-production")
		if updateResponse.Code != wantStatus || err != nil || string(stored) != wantValue {
			t.Fatalf("update status = %d, error = %v", updateResponse.Code, err)
		}
		if strings.Contains(updateResponse.Body.String(), "never-render-this") || strings.Contains(updateResponse.Body.String(), "replacement-private-value") {
			t.Fatal("secret value leaked in update response")
		}
	}
	updatedEntries, err := metadata.Secrets().List(ctx)
	if err != nil || len(updatedEntries) != 1 || updatedEntries[0].Key != "github-production" {
		t.Fatal("updating a secret changed its identity or created another secret")
	}
	missingResponse := httptest.NewRecorder()
	app.Handler().ServeHTTP(missingResponse, httptest.NewRequest(http.MethodGet, "/secrets?edit=missing", nil))
	if missingResponse.Code != http.StatusNotFound {
		t.Fatalf("missing secret edit status = %d", missingResponse.Code)
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

func (s *scheduleManagerStub) WithPublication(apply func() error) error { return apply() }

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
