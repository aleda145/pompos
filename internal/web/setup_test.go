package web

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/agent"
	"pompos/internal/store"
)

func setupFixture(t *testing.T) (*App, *agent.Service, *store.SQLite) {
	t.Helper()
	dir := t.TempDir()
	db, err := store.Open(context.Background(), filepath.Join(dir, "db.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	service := &agent.Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db}
	app, err := New(App{Agent: service, Store: db, Secrets: db.Secrets(), SpecDir: filepath.Join(dir, "ingestions"), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	return app, service, db
}
func setupRequest(app *App, method, path string, values url.Values) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(values.Encode()))
	if method == "POST" {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, r)
	return w
}
func assertRedirect(t *testing.T, w *httptest.ResponseRecorder, path string) {
	t.Helper()
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != path {
		t.Fatalf("expected redirect to %s; got %d %s %s", path, w.Code, w.Header().Get("Location"), w.Body)
	}
}
func TestSetupRequiresAgentAndRemembersOptionalSearchSkip(t *testing.T) {
	app, service, db := setupFixture(t)
	for _, path := range []string{"/", "/ingestions/new", "/chat/draft"} {
		assertRedirect(t, setupRequest(app, "GET", path, nil), "/setup")
	}
	page := setupRequest(app, "GET", "/setup", nil)
	if page.Code != 200 || !strings.Contains(page.Body.String(), "Choose how to develop ingestions") || strings.Contains(page.Body.String(), "Skip for now") {
		t.Fatalf("required agent page: %d %s", page.Code, page.Body)
	}
	assertRedirect(t, setupRequest(app, "POST", "/setup/mode", url.Values{"mode": {"agent"}}), "/setup")
	assertRedirect(t, setupRequest(app, "POST", "/setup/search", url.Values{"action": {"skip"}}), "/setup")
	bad := setupRequest(app, "POST", "/setup/agent", url.Values{"endpoint": {"https://model.example/v1"}, "model": {"test"}})
	if bad.Code != 422 {
		t.Fatal("agent accepted without key or explicit no-auth choice")
	}
	assertRedirect(t, setupRequest(app, "POST", "/setup/agent", url.Values{"endpoint": {"http://localhost:11434/v1"}, "model": {"local-model"}, "no_auth": {"on"}}), "/setup")
	page = setupRequest(app, "GET", "/setup", nil)
	if page.Code != 200 || !strings.Contains(page.Body.String(), "Configure web search") || !strings.Contains(page.Body.String(), "Skip for now") {
		t.Fatalf("optional search page: %s", page.Body)
	}
	assertRedirect(t, setupRequest(app, "POST", "/setup/search", url.Values{"action": {"skip"}}), "/")
	// Recreate the service to exercise persisted setup state rather than in-memory flags.
	app.Agent = &agent.Service{Dir: service.Dir, Secrets: db.Secrets(), Destinations: db}
	cfg, err := app.Agent.Settings()
	if err != nil || !cfg.ExaSkipped || cfg.APIKeyRef != "" || cfg.Model != "local-model" {
		t.Fatalf("setup not persisted: %#v %v", cfg, err)
	}
	assertRedirect(t, setupRequest(app, "GET", "/setup", nil), "/")
	if w := setupRequest(app, "GET", "/", nil); w.Code != 200 {
		t.Fatalf("onboarded home: %d", w.Code)
	}
	// Editing agent settings must preserve the explicit skip choice.
	settings := setupRequest(app, "POST", "/settings", url.Values{"section": {"agent"}, "endpoint": {cfg.Endpoint}, "model": {"updated-model"}})
	if settings.Code != 200 {
		t.Fatal("settings update failed")
	}
	cfg, _ = app.Agent.Settings()
	if !cfg.ExaSkipped {
		t.Fatal("settings edit lost skip choice")
	}
}
func TestSetupStoresKeysWithoutExposingOrOverwritingSecrets(t *testing.T) {
	ctx := context.Background()
	app, service, db := setupFixture(t)
	db.Secrets().Put(ctx, "existing-source", []byte("source-value"))
	invalid := setupRequest(app, "POST", "/setup/agent", url.Values{"endpoint": {"file:///bad"}, "model": {"test"}, "api_key": {"private-model-key"}})
	if invalid.Code != 422 || strings.Contains(invalid.Body.String(), "private-model-key") {
		t.Fatal("invalid form leaked or accepted key")
	}
	entries, _ := db.Secrets().List(ctx)
	if len(entries) != 1 {
		t.Fatal("invalid configuration stored an unused key")
	}
	assertRedirect(t, setupRequest(app, "POST", "/setup/agent", url.Values{"endpoint": {"https://model.example/v1"}, "model": {"test"}, "api_key": {"private-model-key"}}), "/setup")
	cfg, err := service.Settings()
	if err != nil || !strings.HasPrefix(cfg.APIKeyRef, "pompos-model-") {
		t.Fatalf("model reference not saved: %#v %v", cfg, err)
	}
	value, _ := db.Secrets().Get(ctx, cfg.APIKeyRef)
	if string(value) != "private-model-key" {
		t.Fatal("model key not stored")
	}
	assertRedirect(t, setupRequest(app, "POST", "/setup/search", url.Values{"api_key": {"private-exa-key"}}), "/")
	cfg, _ = service.Settings()
	value, _ = db.Secrets().Get(ctx, cfg.ExaAPIKeyRef)
	if string(value) != "private-exa-key" || cfg.ExaSkipped {
		t.Fatal("search configuration not stored")
	}
	bytes, err := os.ReadFile(filepath.Join(service.Dir, "settings.json"))
	if err != nil || strings.Contains(string(bytes), "private-model-key") || strings.Contains(string(bytes), "private-exa-key") {
		t.Fatal("settings contain raw keys")
	}
	var stored map[string]any
	if err = json.Unmarshal(bytes, &stored); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/setup", "/settings", "/secrets"} {
		page := setupRequest(app, "GET", path, nil)
		if strings.Contains(page.Body.String(), "private-model-key") || strings.Contains(page.Body.String(), "private-exa-key") {
			t.Fatal("key value in HTML")
		}
	}
	value, _ = db.Secrets().Get(ctx, "existing-source")
	if string(value) != "source-value" {
		t.Fatal("setup overwrote another secret")
	}
	// A stale skip submission must not remove configuration completed in another tab.
	assertRedirect(t, setupRequest(app, "POST", "/setup/search", url.Values{"action": {"skip"}}), "/")
	after, _ := service.Settings()
	if after.ExaAPIKeyRef != cfg.ExaAPIKeyRef || after.ExaSkipped {
		t.Fatal("stale setup overwrote completed configuration")
	}
}
func TestSetupReusesExistingConfigurationAndRepairsMissingKeys(t *testing.T) {
	ctx := context.Background()
	app, service, db := setupFixture(t)
	db.Secrets().Put(ctx, "model-key", []byte("model-value"))
	db.Secrets().Put(ctx, "exa-key", []byte("exa-value"))
	cfg := agent.Settings{Endpoint: "https://model.example/v1", Model: "test", APIKeyRef: "model-key", ExaAPIKeyRef: "exa-key"}
	if err := service.SaveSettings(cfg); err != nil {
		t.Fatal(err)
	}
	if w := setupRequest(app, "GET", "/", nil); w.Code != 200 {
		t.Fatal("asked configured user to onboard")
	}
	db.Secrets().Delete(ctx, "exa-key")
	assertRedirect(t, setupRequest(app, "GET", "/", nil), "/setup")
	if w := setupRequest(app, "GET", "/setup", nil); !strings.Contains(w.Body.String(), "Configure web search") {
		t.Fatal("did not offer missing search setup")
	}
	db.Secrets().Delete(ctx, "model-key")
	if w := setupRequest(app, "GET", "/setup", nil); !strings.Contains(w.Body.String(), "Configure agent") {
		t.Fatal("missing model credential did not require agent setup")
	}
	db.Secrets().Put(ctx, "replacement", []byte("new-model-value"))
	assertRedirect(t, setupRequest(app, "POST", "/setup/agent", url.Values{"endpoint": {cfg.Endpoint}, "model": {cfg.Model}, "api_key_ref": {"replacement"}}), "/setup")
	repaired, _ := service.Settings()
	if repaired.APIKeyRef != "replacement" || repaired.ExaAPIKeyRef != "exa-key" {
		t.Fatal("repair lost unrelated configuration")
	}
	// Using the model credential as an Exa credential is a configuration mistake.
	if w := setupRequest(app, "POST", "/setup/search", url.Values{"exa_api_key_ref": {"replacement"}}); w.Code != 422 {
		t.Fatal("accepted model credential for search")
	}
	db.Secrets().Put(ctx, "exa-replacement", []byte("new-exa-value"))
	assertRedirect(t, setupRequest(app, "POST", "/setup/search", url.Values{"exa_api_key_ref": {"exa-replacement"}}), "/")
	repaired, _ = service.Settings()
	if repaired.ExaAPIKeyRef != "exa-replacement" {
		t.Fatal("existing search secret not saved")
	}
}
