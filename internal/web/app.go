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
	"slices"
	"strings"
	"sync"
	"time"

	"pompos/internal/agent"
	"pompos/internal/compiler"
	"pompos/internal/destination"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/scheduler"
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
	ListRuns(context.Context, string, int64, int) ([]ingestion.Run, error)
	GetRun(context.Context, string, int64) (ingestion.Run, error)
	HasActiveRuns(context.Context, string) (bool, error)
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
	DeleteDestination(context.Context, string) error
}

type App struct {
	publicationMu *sync.Mutex
	Agent         *agent.Service
	Store         MetadataStore
	Secrets       secrets.Store
	Destinations  DestinationCatalog
	Scheduler     ScheduleManager
	SpecDir       string
	Logger        *log.Logger
	Previewer     interface {
		Preview(context.Context, compiler.ExecutionPlan) (runnerpython.TablePreview, error)
	}
	templates map[string]*template.Template
}

func New(app App) (*App, error) {
	app.publicationMu = &sync.Mutex{}
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
	for _, page := range []string{"home", "detail", "secrets", "destinations", "chats", "chat", "settings", "setup", "edit_review"} {
		parsed, err := template.New(page).Funcs(template.FuncMap{
			"displayTimezone": app.displayTimezone,
		}).ParseFS(templatefiles.FS, "layout.html", page+".html")
		if err != nil {
			return nil, fmt.Errorf("parse %s template: %w", page, err)
		}
		app.templates[page] = parsed
	}
	return &app, nil
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.home)
	mux.HandleFunc("GET /setup", a.setupPage)
	mux.HandleFunc("POST /setup/mode", a.setupMode)
	mux.HandleFunc("GET /settings/mcp", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/settings#mcp", http.StatusSeeOther)
	})
	if a.Agent != nil {
		mux.Handle("/mcp", a.mcpHandler())
	}
	mux.HandleFunc("POST /setup/agent", a.setupAgent)
	mux.HandleFunc("POST /setup/search", a.setupSearch)
	mux.HandleFunc("GET /ingestions/new", a.chatPage)
	mux.HandleFunc("GET /chat", a.listChats)
	mux.HandleFunc("GET /chat/{id}", a.chatPage)
	mux.HandleFunc("POST /chat/{id}", a.chatTurn)
	mux.HandleFunc("POST /chat/{id}/publish", a.publishChat)
	mux.HandleFunc("GET /chat/{id}/review", a.reviewEdit)
	mux.HandleFunc("POST /chat/{id}/apply", a.applyEdit)
	mux.HandleFunc("POST /chat/{id}/secret", a.chatSecret)
	mux.HandleFunc("GET /settings", a.settings)
	mux.HandleFunc("POST /settings", a.settings)
	mux.HandleFunc("GET /settings/agent", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/settings", http.StatusSeeOther)
	})
	mux.HandleFunc("POST /settings/agent", a.settings)
	mux.HandleFunc("POST /settings/mcp", a.toggleMCP)
	mux.HandleFunc("POST /settings/agent/mcp", a.toggleMCP)
	mux.HandleFunc("GET /ingestions/{id}", a.ingestionDetail)
	mux.HandleFunc("GET /ingestions/{id}/preview", a.ingestionPreview)
	mux.HandleFunc("GET /ingestions/{id}/runs", a.ingestionRuns)
	mux.HandleFunc("POST /ingestions/{id}/run", a.runIngestion)
	mux.HandleFunc("POST /ingestions/{id}/schedule", a.updateSchedule)
	mux.HandleFunc("POST /ingestions/{id}/edit", a.editIngestion)
	for pattern, handler := range map[string]http.HandlerFunc{
		"GET /ingestions/{destination}/{schema}/{table}":           a.ingestionDetail,
		"GET /ingestions/{destination}/{schema}/{table}/preview":   a.ingestionPreview,
		"GET /ingestions/{destination}/{schema}/{table}/runs":      a.ingestionRuns,
		"POST /ingestions/{destination}/{schema}/{table}/run":      a.runIngestion,
		"POST /ingestions/{destination}/{schema}/{table}/schedule": a.updateSchedule,
		"POST /ingestions/{destination}/{schema}/{table}/edit":     a.editIngestion,
	} {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			r.SetPathValue("id", r.PathValue("destination")+"/"+r.PathValue("schema")+"/"+r.PathValue("table"))
			handler(w, r)
		})
	}
	mux.HandleFunc("GET /secrets", a.listSecrets)
	mux.HandleFunc("POST /secrets", a.createSecret)
	mux.HandleFunc("POST /secrets/delete", a.deleteSecret)
	mux.HandleFunc("GET /destinations", a.listDestinations)
	mux.HandleFunc("POST /destinations", a.saveDestination)
	mux.HandleFunc("POST /destinations/delete", a.deleteDestination)
	assets, _ := fs.Sub(staticfiles.FS, ".")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(assets))))
	return a.logRequests(a.recover(sameOriginWrites(a.requireSetup(mux))))
}

func (a *App) home(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	items, err := a.ingestionList(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
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
	Deleted      bool
	Editing      bool
	Destinations []destinationView
}

type destinationView struct {
	destination.Config
	Ingestions []destinationIngestion
}

type destinationIngestion struct {
	ID     string
	Target string
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
	YAML          string
	Python        string
	PythonError   string
	RunError      string
	RunView       runView
	RunsError     string
}

func (a *App) ingestionDetail(w http.ResponseWriter, r *http.Request) {
	item, err := a.getIngestion(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	page := a.detailData(r, item)
	page.ScheduleSaved = r.URL.Query().Get("schedule") == "saved"
	a.render(w, http.StatusOK, "detail", page)
}

func (a *App) detailData(r *http.Request, item ingestion.Ingestion) detailPageData {
	page := detailPageData{
		Title: item.Name, Ingestion: item, SpecPath: item.SpecPath,
		NextRun: item.NextRun, ScheduleValue: item.Schedule,
	}
	before, selected, historyErr := runQuery(r)
	if historyErr == nil {
		page.RunView, historyErr = a.runData(r.Context(), item.ID, before, selected)
	}
	if historyErr != nil {
		page.RunsError = "Run history unavailable."
		a.Logger.Printf("load run history ingestion_id=%s error=%q", item.ID, historyErr)
	}
	// Keep malformed YAML visible so the operator can diagnose it.
	yamlData, err := os.ReadFile(item.SpecPath)
	if err != nil && page.Ingestion.LoadError == "" {
		page.Ingestion.LoadError = err.Error()
		page.NextRun = nil
	}
	page.YAML = string(yamlData)
	if item.Runtime.Script != "" {
		code, err := os.ReadFile(item.Runtime.Script)
		if err != nil {
			page.PythonError = fmt.Sprintf("Read Python script %s: %v", item.Runtime.Script, err)
		} else {
			page.Python = string(code)
		}
	}
	return page
}

func (a *App) runIngestion(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.queueIngestion(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		item, loadErr := a.getIngestion(r.Context(), id)
		if loadErr != nil {
			a.serverError(w, loadErr)
			return
		}
		page := a.detailData(r, item)
		page.RunError = err.Error()
		a.render(w, http.StatusUnprocessableEntity, "detail", page)
		return
	}
	http.Redirect(w, r, "/ingestions/"+id+"?run=queued", http.StatusSeeOther)
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
		a.render(w, http.StatusUnprocessableEntity, "detail", a.detailData(r, item))
		return
	}
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	if err := a.Scheduler.Validate(schedule); err != nil {
		page := a.detailData(r, item)
		page.ScheduleValue, page.ScheduleError = schedule, err.Error()
		a.render(w, http.StatusUnprocessableEntity, "detail", page)
		return
	}
	if err := a.setIngestionSchedule(r.Context(), item.ID, schedule); err != nil {
		page := a.detailData(r, item)
		page.ScheduleValue, page.ScheduleError = schedule, err.Error()
		a.render(w, http.StatusUnprocessableEntity, "detail", page)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID+"?schedule=saved", http.StatusSeeOther)
}

func (a *App) listDestinations(w http.ResponseWriter, r *http.Request) {
	data := destinationsPageData{
		Saved:   r.URL.Query().Get("saved") == "1",
		Deleted: r.URL.Query().Get("deleted") == "1",
	}
	if name := r.URL.Query().Get("edit"); name != "" {
		config, err := a.Destinations.GetDestination(r.Context(), name)
		if errors.Is(err, destination.ErrNotFound) {
			http.NotFound(w, r)
			return
		} else if err != nil {
			a.serverError(w, err)
			return
		}
		data.Name, data.Type, data.Path = config.Name, config.Type, config.Path
	}
	a.renderDestinations(w, r, http.StatusOK, data)
}

func (a *App) saveDestination(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderDestinations(w, r, http.StatusBadRequest, destinationsPageData{Error: "Invalid form submission."})
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
		a.renderDestinations(w, r, http.StatusUnprocessableEntity, data)
		return
	}
	a.Logger.Printf("destination saved destination=%s type=%s path=%s", config.Name, config.Type, config.Path)
	http.Redirect(w, r, "/destinations?saved=1", http.StatusSeeOther)
}

func (a *App) deleteDestination(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderDestinations(w, r, http.StatusBadRequest, destinationsPageData{Error: "Invalid form submission."})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		a.renderDestinations(w, r, http.StatusUnprocessableEntity, destinationsPageData{Error: "Destination name is required."})
		return
	}
	if err := a.Destinations.DeleteDestination(r.Context(), name); errors.Is(err, destination.ErrNotFound) {
		http.NotFound(w, r)
		return
	} else if err != nil {
		a.serverError(w, err)
		return
	}
	a.Logger.Printf("destination deleted destination=%s", name)
	http.Redirect(w, r, "/destinations?deleted=1", http.StatusSeeOther)
}

func (a *App) renderDestinations(w http.ResponseWriter, r *http.Request, status int, data destinationsPageData) {
	configs, err := a.Destinations.ListDestinations(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	items, err := a.ingestionList(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	data.Title = "Destinations"
	for _, config := range configs {
		if config.Name == data.Name {
			data.Editing = true
		}
		view := destinationView{Config: config}
		for _, item := range items {
			parts := strings.Split(item.ID, "/")
			if len(parts) == 3 {
				if parts[0] != config.Name {
					continue
				}
			} else if item.Destination.Type != config.Type || item.Destination.Path != config.Path {
				continue
			}
			schema, table := destination.SchemaName(item.Destination.Schema), item.Destination.Table
			if item.LoadError != "" && len(parts) == 3 {
				schema, table = parts[1], parts[2]
			}
			view.Ingestions = append(view.Ingestions, destinationIngestion{ID: item.ID, Target: schema + "." + table})
		}
		slices.SortFunc(view.Ingestions, func(a, b destinationIngestion) int {
			if order := strings.Compare(strings.ToLower(a.Target), strings.ToLower(b.Target)); order != 0 {
				return order
			}
			return strings.Compare(a.ID, b.ID)
		})
		data.Destinations = append(data.Destinations, view)
	}
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
	ingestions, err := a.ingestionList(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
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

// Failed loads retain their identity and run history for read-only views.
// Callers that modify or execute the configuration must reject the error.
func (a *App) hydrate(state ingestion.Ingestion) (ingestion.Ingestion, error) {
	document, data, err := spec.Read(state.SpecPath)
	if err != nil {
		state.Name = state.ID
		state.LoadError, state.NextRun = err.Error(), nil
		return state, err
	}
	projection := spec.ToProjection(document, state.ID, state.SpecPath, spec.Digest(data))
	projection.Status, projection.LastRun, projection.LastError, projection.NextRun = state.Status, state.LastRun, state.LastError, state.NextRun
	if err := scheduler.ValidateCron(projection.Schedule); err != nil {
		projection.LoadError, projection.NextRun = err.Error(), nil
		return projection, err
	}
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
