package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"pompos/internal/agent"
	"pompos/internal/compiler"
	"pompos/internal/destination"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/secrets"
	"pompos/internal/spec"
	"pompos/internal/store"
	staticfiles "pompos/static"
	templatefiles "pompos/templates"
)

type MetadataStore interface {
	Create(context.Context, ingestion.Ingestion) error
	Get(context.Context, string) (ingestion.Ingestion, error)
	List(context.Context) ([]ingestion.Ingestion, error)
	UpdateSpecReference(context.Context, string, string, string) error
}

type ScheduleManager interface {
	Validate(string) error
	Upsert(ingestion.Ingestion) error
	Enqueue(context.Context, string) error
	NextRun(string) *time.Time
}

type DestinationCatalog interface {
	GetDestination(context.Context, string) (destination.Config, error)
	ListDestinations(context.Context) ([]destination.Config, error)
	PutDestination(context.Context, destination.Config) error
}

type App struct {
	Agent        *agent.Service
	Store        MetadataStore
	Secrets      secrets.Store
	Destinations DestinationCatalog
	Scheduler    ScheduleManager
	SpecDir      string
	Logger       *log.Logger
	Previewer    interface {
		Preview(context.Context, compiler.ExecutionPlan) (runnerpython.TablePreview, error)
	}
	templates map[string]*template.Template
}

func New(app App) (*App, error) {
	if app.Store == nil || app.Secrets == nil {
		return nil, errors.New("web app dependencies must not be nil")
	}
	if app.Logger == nil {
		app.Logger = log.Default()
	}
	if app.Scheduler == nil {
		app.Scheduler = noopScheduleManager{}
	}
	if app.Destinations == nil {
		catalog, ok := app.Store.(DestinationCatalog)
		if !ok {
			return nil, errors.New("web app destination store is required")
		}
		app.Destinations = catalog
	}
	app.templates = make(map[string]*template.Template, 5)
	for _, page := range []string{"home", "detail", "secrets", "destinations", "chats", "chat", "agent-settings", "setup"} {
		parsed, err := template.New(page).ParseFS(templatefiles.FS, "layout.html", page+".html")
		if err != nil {
			return nil, fmt.Errorf("parse %s template: %w", page, err)
		}
		app.templates[page] = parsed
	}
	return &app, nil
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.home)
	mux.HandleFunc("GET /setup", a.setupPage)
	mux.HandleFunc("POST /setup/agent", a.setupAgent)
	mux.HandleFunc("POST /setup/search", a.setupSearch)
	mux.HandleFunc("GET /ingestions/new", a.chatPage)
	mux.HandleFunc("GET /chat", a.listChats)
	mux.HandleFunc("GET /chat/{id}", a.chatPage)
	mux.HandleFunc("POST /chat/{id}", a.chatTurn)
	mux.HandleFunc("POST /chat/{id}/publish", a.publishChat)
	mux.HandleFunc("POST /chat/{id}/secret", a.chatSecret)
	mux.HandleFunc("GET /settings/agent", a.agentSettings)
	mux.HandleFunc("POST /settings/agent", a.agentSettings)
	mux.HandleFunc("GET /ingestions/{id}", a.ingestionDetail)
	mux.HandleFunc("GET /ingestions/{id}/preview", a.ingestionPreview)
	mux.HandleFunc("POST /ingestions/{id}/run", a.runIngestion)
	mux.HandleFunc("POST /ingestions/{id}/schedule", a.updateSchedule)
	mux.HandleFunc("GET /secrets", a.listSecrets)
	mux.HandleFunc("POST /secrets", a.createSecret)
	mux.HandleFunc("POST /secrets/delete", a.deleteSecret)
	mux.HandleFunc("GET /destinations", a.listDestinations)
	mux.HandleFunc("POST /destinations", a.saveDestination)
	assets, _ := fs.Sub(staticfiles.FS, ".")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(assets))))
	return a.logRequests(a.recover(a.requireSetup(mux)))
}

func (a *App) home(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	items, err := a.Store.List(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	for index := range items {
		items[index], err = a.hydrate(items[index])
		if err != nil {
			a.serverError(w, err)
			return
		}
	}
	a.render(w, http.StatusOK, "home", struct {
		Title      string
		Ingestions []ingestion.Ingestion
	}{Ingestions: items})
}

type destinationsPageData struct {
	Title        string
	Error        string
	Name         string
	Type         string
	Path         string
	Saved        bool
	Destinations []destination.Config
}

type secretsPageData struct {
	Title   string
	Error   string
	Name    string
	Editing bool
	Saved   bool
	Deleted bool
	Secrets []secretView
}

type secretView struct {
	Entry      secrets.Entry
	Ingestions []ingestion.Ingestion
}

type detailPageData struct {
	Title         string
	Ingestion     ingestion.Ingestion
	SpecPath      string
	NextRun       *time.Time
	ScheduleValue string
	ScheduleSaved bool
	ScheduleError string
	RunQueued     bool
	YAML          string
	Python        string
}

func (a *App) ingestionDetail(w http.ResponseWriter, r *http.Request) {
	item, err := a.Store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	item, err = a.hydrate(item)
	if err != nil {
		a.serverError(w, err)
		return
	}
	_, yamlData, err := spec.Read(item.SpecPath)
	if err != nil {
		a.serverError(w, err)
		return
	}
	code, err := os.ReadFile(item.Runtime.Script)
	if err != nil {
		a.serverError(w, err)
		return
	}
	page := detailPageData{
		Title: item.Name, Ingestion: item, SpecPath: item.SpecPath,
		NextRun: a.Scheduler.NextRun(item.ID), ScheduleValue: item.Schedule, ScheduleSaved: r.URL.Query().Get("schedule") == "saved",
		RunQueued: r.URL.Query().Get("run") == "queued",
		YAML:      string(yamlData),
		Python:    string(code),
	}
	a.render(w, http.StatusOK, "detail", page)
}

func (a *App) runIngestion(w http.ResponseWriter, r *http.Request) {
	item, err := a.Store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	_, data, err := spec.Read(item.SpecPath)
	if err != nil {
		a.serverError(w, err)
		return
	}
	digest := spec.Digest(data)
	if digest != item.SpecDigest {
		if err := a.Store.UpdateSpecReference(r.Context(), item.ID, item.SpecPath, digest); err != nil {
			a.serverError(w, err)
			return
		}
	}
	a.Logger.Printf("rerun requested ingestion_id=%s", item.ID)
	if err := a.Scheduler.Enqueue(r.Context(), item.ID); err != nil {
		a.serverError(w, err)
		return
	}
	a.Logger.Printf("rerun enqueued ingestion_id=%s", item.ID)
	http.Redirect(w, r, "/ingestions/"+item.ID+"?run=queued", http.StatusSeeOther)
}

func (a *App) updateSchedule(w http.ResponseWriter, r *http.Request) {
	item, err := a.Store.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	item, err = a.hydrate(item)
	if err != nil {
		a.serverError(w, err)
		return
	}
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	if err := a.Scheduler.Validate(schedule); err != nil {
		a.render(w, http.StatusUnprocessableEntity, "detail", detailPageData{
			Title: item.Name, Ingestion: item, SpecPath: filepath.Join(a.SpecDir, item.ID+".yaml"),
			NextRun: a.Scheduler.NextRun(item.ID), ScheduleValue: schedule, ScheduleError: err.Error(),
		})
		return
	}
	previous := item.Schedule
	item.Schedule = schedule
	if _, err := spec.Write(a.SpecDir, item); err != nil {
		a.serverError(w, err)
		return
	}
	data, err := os.ReadFile(filepath.Join(a.SpecDir, item.ID+".yaml"))
	if err != nil {
		a.serverError(w, err)
		return
	}
	if err := a.Store.UpdateSpecReference(r.Context(), item.ID, filepath.Join(a.SpecDir, item.ID+".yaml"), spec.Digest(data)); err != nil {
		a.serverError(w, err)
		return
	}
	if err := a.Scheduler.Upsert(item); err != nil {
		item.Schedule = previous
		_, _ = spec.Write(a.SpecDir, item)
		_ = a.Scheduler.Upsert(item)
		a.serverError(w, err)
		return
	}
	a.Logger.Printf("schedule updated ingestion_id=%s schedule=%q timezone=UTC", item.ID, schedule)
	http.Redirect(w, r, "/ingestions/"+item.ID+"?schedule=saved", http.StatusSeeOther)
}

func (a *App) listDestinations(w http.ResponseWriter, r *http.Request) {
	a.renderDestinations(w, http.StatusOK, destinationsPageData{Saved: r.URL.Query().Get("saved") == "1"})
}

func (a *App) saveDestination(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderDestinations(w, http.StatusBadRequest, destinationsPageData{Error: "Invalid form submission."})
		return
	}
	data := destinationsPageData{
		Name: strings.TrimSpace(r.FormValue("name")),
		Type: strings.TrimSpace(r.FormValue("type")),
		Path: strings.TrimSpace(r.FormValue("path")),
	}
	config := destination.NewDuckDB(data.Name, data.Path)
	if data.Type != "" {
		config.Type = data.Type
	}
	if err := a.Destinations.PutDestination(r.Context(), config); err != nil {
		data.Error = err.Error()
		a.renderDestinations(w, http.StatusUnprocessableEntity, data)
		return
	}
	a.Logger.Printf("destination saved destination=%s type=%s path=%s", config.Name, config.Type, config.Path)
	http.Redirect(w, r, "/destinations?saved=1", http.StatusSeeOther)
}

func (a *App) renderDestinations(w http.ResponseWriter, status int, data destinationsPageData) {
	configs, err := a.Destinations.ListDestinations(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}
	data.Title = "Destinations"
	data.Destinations = configs
	if data.Type == "" {
		data.Type = "duckdb"
	}
	a.render(w, status, "destinations", data)
}

func (a *App) listSecrets(w http.ResponseWriter, r *http.Request) {
	a.renderSecrets(w, r, http.StatusOK, secretsPageData{Name: r.URL.Query().Get("edit")})
}

func (a *App) createSecret(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderSecrets(w, r, http.StatusBadRequest, secretsPageData{Error: "Invalid form submission."})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	value := r.FormValue("value")
	if name == "" || len(name) > 200 {
		a.renderSecrets(w, r, http.StatusUnprocessableEntity, secretsPageData{Error: "Secret name must be between 1 and 200 characters.", Name: name})
		return
	}
	if value == "" {
		a.renderSecrets(w, r, http.StatusUnprocessableEntity, secretsPageData{Error: "Secret value is required.", Name: name})
		return
	}
	if err := a.Secrets.Put(r.Context(), name, []byte(value)); err != nil {
		a.serverError(w, err)
		return
	}
	a.Logger.Printf("secret saved secret_key=%s", name)
	http.Redirect(w, r, "/secrets?saved=1", http.StatusSeeOther)
}

func (a *App) deleteSecret(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderSecrets(w, r, http.StatusBadRequest, secretsPageData{Error: "Invalid form submission."})
		return
	}
	key := strings.TrimSpace(r.FormValue("key"))
	if key == "" {
		a.renderSecrets(w, r, http.StatusUnprocessableEntity, secretsPageData{Error: "Secret name is required."})
		return
	}
	if _, err := a.Secrets.Get(r.Context(), key); errors.Is(err, secrets.ErrNotFound) {
		http.NotFound(w, r)
		return
	} else if err != nil {
		a.serverError(w, err)
		return
	}
	if err := a.Secrets.Delete(r.Context(), key); err != nil {
		a.serverError(w, err)
		return
	}
	a.Logger.Printf("secret deleted secret_key=%s", key)
	http.Redirect(w, r, "/secrets?deleted=1", http.StatusSeeOther)
}

func (a *App) renderSecrets(w http.ResponseWriter, r *http.Request, status int, data secretsPageData) {
	entries, err := a.Secrets.List(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	ingestions, err := a.Store.List(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	for index := range ingestions {
		ingestions[index], err = a.hydrate(ingestions[index])
		if err != nil {
			a.serverError(w, err)
			return
		}
	}
	data.Title = "Secrets"
	for _, entry := range entries {
		if entry.Key == data.Name {
			data.Editing = true
			break
		}
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("edit") != "" && !data.Editing {
		http.NotFound(w, r)
		return
	}
	data.Secrets = describeSecrets(entries, ingestions)
	data.Saved = data.Saved || r.URL.Query().Get("saved") == "1"
	data.Deleted = data.Deleted || r.URL.Query().Get("deleted") == "1"
	a.render(w, status, "secrets", data)
}

func (a *App) hydrate(state ingestion.Ingestion) (ingestion.Ingestion, error) {
	document, data, err := spec.Read(state.SpecPath)
	if err != nil {
		return ingestion.Ingestion{}, err
	}
	projection := spec.ToProjection(document, state.ID, state.SpecPath, spec.Digest(data))
	projection.Status, projection.LastRun, projection.LastError, projection.NextRun = state.Status, state.LastRun, state.LastError, state.NextRun
	return projection, nil
}

func describeSecrets(entries []secrets.Entry, ingestions []ingestion.Ingestion) []secretView {
	views := make([]secretView, 0, len(entries))
	for _, entry := range entries {
		view := secretView{Entry: entry}
		for _, item := range ingestions {
			for _, ref := range item.Runtime.SecretRefs {
				if ref == entry.Key {
					view.Ingestions = append(view.Ingestions, item)
					break
				}
			}
		}
		views = append(views, view)
	}
	return views
}

func (a *App) render(w http.ResponseWriter, status int, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := a.templates[name].ExecuteTemplate(w, "layout", data); err != nil {
		a.Logger.Printf("render %s: %v", name, err)
	}
}

func (a *App) serverError(w http.ResponseWriter, err error) {
	a.Logger.Printf("request failed: %v", err)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func (a *App) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				a.serverError(w, fmt.Errorf("panic: %v", value))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type responseStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseStatusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseStatusWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (a *App) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		writer := &responseStatusWriter{ResponseWriter: w}
		a.Logger.Printf("request started method=%s path=%s", r.Method, r.URL.Path)
		next.ServeHTTP(writer, r)
		status := writer.status
		if status == 0 {
			status = http.StatusOK
		}
		a.Logger.Printf("request completed method=%s path=%s status=%d duration=%s",
			r.Method, r.URL.Path, status, time.Since(started).Round(time.Millisecond))
	})
}

func newID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		panic(fmt.Sprintf("generate ingestion ID: %v", err))
	}
	buffer[6] = (buffer[6] & 0x0f) | 0x40
	buffer[8] = (buffer[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(buffer)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

type noopScheduleManager struct{}

func (noopScheduleManager) Validate(string) error            { return nil }
func (noopScheduleManager) Upsert(ingestion.Ingestion) error { return nil }
func (noopScheduleManager) Enqueue(context.Context, string) error {
	return errors.New("run queue is unavailable")
}
func (noopScheduleManager) NextRun(string) *time.Time { return nil }
