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
	"path/filepath"
	"strings"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/policy"
	"pompos/internal/runner"
	"pompos/internal/spec"
	"pompos/internal/store"
	"pompos/internal/validation"
	staticfiles "pompos/static"
	templatefiles "pompos/templates"
)

type MetadataStore interface {
	Create(context.Context, ingestion.Ingestion) error
	Get(context.Context, string) (ingestion.Ingestion, error)
	List(context.Context) ([]ingestion.Ingestion, error)
	MarkRunning(context.Context, string, time.Time) error
	Finish(context.Context, string, string, string) error
}

type App struct {
	Store       MetadataStore
	Policy      policy.Engine
	Runner      runner.Runner
	Validator   validation.SourceValidator
	Destination ingestion.Destination
	SpecDir     string
	Logger      *log.Logger
	templates   map[string]*template.Template
}

func New(app App) (*App, error) {
	if app.Store == nil || app.Policy == nil || app.Runner == nil || app.Validator == nil {
		return nil, errors.New("web app dependencies must not be nil")
	}
	if app.Logger == nil {
		app.Logger = log.Default()
	}
	app.templates = make(map[string]*template.Template, 3)
	for _, page := range []string{"home", "new", "detail"} {
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
	mux.HandleFunc("GET /ingestions/new", a.newIngestion)
	mux.HandleFunc("POST /ingestions", a.createIngestion)
	mux.HandleFunc("GET /ingestions/{id}", a.ingestionDetail)
	mux.HandleFunc("POST /ingestions/{id}/run", a.runIngestion)
	assets, _ := fs.Sub(staticfiles.FS, ".")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(assets))))
	return a.recover(mux)
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
	a.render(w, http.StatusOK, "home", struct {
		Title      string
		Ingestions []ingestion.Ingestion
	}{Ingestions: items})
}

func (a *App) newIngestion(w http.ResponseWriter, _ *http.Request) {
	a.renderNew(w, http.StatusOK, "", "", "")
}

func (a *App) createIngestion(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderNew(w, http.StatusBadRequest, "Invalid form submission.", "", "")
		return
	}
	csvURL := strings.TrimSpace(r.FormValue("csv_url"))
	tableName := strings.TrimSpace(r.FormValue("table_name"))
	item := ingestion.Ingestion{
		ID:     newID(),
		Name:   tableName,
		Status: ingestion.StatusPending,
		Source: ingestion.Source{Type: "csv", URL: csvURL},
		Destination: ingestion.Destination{
			Type:  a.Destination.Type,
			Path:  a.Destination.Path,
			Table: tableName,
		},
	}
	if err := a.Policy.Validate(item); err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, err.Error(), csvURL, tableName)
		return
	}
	if err := a.Validator.Validate(r.Context(), csvURL); err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, err.Error(), csvURL, tableName)
		return
	}
	if err := a.Store.Create(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	if _, err := spec.Write(a.SpecDir, item); err != nil {
		if updateErr := a.finish(item.ID, ingestion.StatusFailed, err.Error()); updateErr != nil {
			a.serverError(w, errors.Join(err, updateErr))
			return
		}
		http.Redirect(w, r, "/ingestions/"+item.ID, http.StatusSeeOther)
		return
	}
	if err := a.execute(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID, http.StatusSeeOther)
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
	a.render(w, http.StatusOK, "detail", struct {
		Title     string
		Ingestion ingestion.Ingestion
		SpecPath  string
	}{Title: item.Name, Ingestion: item, SpecPath: filepath.Join(a.SpecDir, item.ID+".yaml")})
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
	if err := a.execute(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID, http.StatusSeeOther)
}

func (a *App) execute(ctx context.Context, item ingestion.Ingestion) error {
	if err := a.Policy.Validate(item); err != nil {
		return a.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	if err := a.Store.MarkRunning(ctx, item.ID, time.Now()); err != nil {
		return err
	}
	if err := a.Runner.Run(ctx, item); err != nil {
		return a.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	return a.finish(item.ID, ingestion.StatusSucceeded, "")
}

func (a *App) finish(id, status, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.Store.Finish(ctx, id, status, message)
}

func (a *App) renderNew(w http.ResponseWriter, status int, message, csvURL, tableName string) {
	a.render(w, status, "new", struct {
		Title     string
		Error     string
		CSVURL    string
		TableName string
	}{Title: "Add ingestion", Error: message, CSVURL: csvURL, TableName: tableName})
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
