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
	"regexp"
	"slices"
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
	return a.logRequests(a.recover(mux))
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

type githubTableOption struct {
	Value       string
	Name        string
	Description string
	Checked     bool
}

type newPageData struct {
	Title         string
	Error         string
	Source        string
	Sources       []ingestion.SourceDefinition
	CSVURL        string
	TableName     string
	Repository    string
	GitHubTables  []githubTableOption
	GitHubDocsURL string
}

var githubTableOptions = []githubTableOption{
	{Value: "issues", Name: "Issues", Description: "Issues, comments, and reactions.", Checked: true},
	{Value: "pull_requests", Name: "Pull requests", Description: "Pull requests, comments, and reactions."},
	{Value: "repo_events", Name: "Repository events", Description: "Recent repository activity from the past 30 days."},
	{Value: "stargazers", Name: "Stargazers", Description: "Restricted by GitHub to repository admins and collaborators."},
}

func (a *App) newIngestion(w http.ResponseWriter, r *http.Request) {
	sourceType := r.URL.Query().Get("source")
	if sourceType != "" && sourceType != "csv" && sourceType != "github" {
		http.NotFound(w, r)
		return
	}
	a.renderNew(w, http.StatusOK, newPageData{Source: sourceType})
}

func (a *App) createIngestion(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderNew(w, http.StatusBadRequest, newPageData{Error: "Invalid form submission."})
		return
	}
	if r.FormValue("source_type") == "github" {
		a.createGitHubIngestions(w, r)
		return
	}
	a.createCSVIngestion(w, r)
}

func (a *App) createCSVIngestion(w http.ResponseWriter, r *http.Request) {
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
	a.Logger.Printf("CSV ingestion requested ingestion_id=%s url=%s destination_table=%s", item.ID, item.Source.URL, item.Destination.Table)
	a.Logger.Printf("validating ingestion ingestion_id=%s source=csv", item.ID)
	if err := a.Policy.Validate(item); err != nil {
		a.Logger.Printf("validation failed ingestion_id=%s source=csv error=%q", item.ID, err)
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName})
		return
	}
	if err := a.Validator.Validate(r.Context(), item.Source); err != nil {
		a.Logger.Printf("connectivity validation failed ingestion_id=%s source=csv error=%q", item.ID, err)
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName})
		return
	}
	a.Logger.Printf("validation succeeded ingestion_id=%s source=csv", item.ID)
	if err := a.persistAndRun(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID, http.StatusSeeOther)
}

func (a *App) createGitHubIngestions(w http.ResponseWriter, r *http.Request) {
	repositoryInput := strings.TrimSpace(r.FormValue("repository"))
	owner, repository, err := validation.ParseGitHubRepository(repositoryInput)
	if err != nil {
		a.renderGitHubError(w, err.Error(), repositoryInput, r.Form["tables"])
		return
	}
	selectedTables := r.Form["tables"]
	if len(selectedTables) == 0 {
		a.renderGitHubError(w, "Choose at least one table to sync.", repositoryInput, nil)
		return
	}

	baseSource := ingestion.Source{
		Type:        "github",
		Owner:       owner,
		Repository:  repository,
		AccessToken: strings.TrimSpace(r.FormValue("access_token")),
	}
	a.Logger.Printf("GitHub ingestion requested repository=%s/%s tables=%s token_provided=%t",
		owner, repository, strings.Join(selectedTables, ","), baseSource.AccessToken != "")
	validationSource := baseSource
	if slices.Contains(selectedTables, "stargazers") {
		validationSource.Table = "stargazers"
	}
	a.Logger.Printf("validating GitHub repository repository=%s/%s stargazers=%t", owner, repository, validationSource.Table == "stargazers")
	if err := a.Validator.Validate(r.Context(), validationSource); err != nil {
		a.Logger.Printf("GitHub repository validation failed repository=%s/%s error=%q", owner, repository, err)
		a.renderGitHubError(w, err.Error(), repositoryInput, selectedTables)
		return
	}
	a.Logger.Printf("GitHub repository validation succeeded repository=%s/%s", owner, repository)

	items := make([]ingestion.Ingestion, 0, len(selectedTables))
	for _, sourceTable := range selectedTables {
		source := baseSource
		source.Table = sourceTable
		item := ingestion.Ingestion{
			ID:     newID(),
			Name:   owner + "/" + repository + " · " + githubTableName(sourceTable),
			Status: ingestion.StatusPending,
			Source: source,
			Destination: ingestion.Destination{
				Type:  a.Destination.Type,
				Path:  a.Destination.Path,
				Table: safeTableName(owner + "_" + repository + "_" + sourceTable),
			},
		}
		if err := a.Policy.Validate(item); err != nil {
			a.Logger.Printf("validation failed ingestion_id=%s source=github source_table=%s error=%q", item.ID, sourceTable, err)
			a.renderGitHubError(w, err.Error(), repositoryInput, selectedTables)
			return
		}
		a.Logger.Printf("prepared GitHub ingestion ingestion_id=%s source_table=%s destination_table=%s",
			item.ID, item.Source.Table, item.Destination.Table)
		items = append(items, item)
	}
	for index, item := range items {
		a.Logger.Printf("running selected GitHub table ingestion_id=%s source_table=%s position=%d total=%d",
			item.ID, item.Source.Table, index+1, len(items))
		if err := a.persistAndRun(r.Context(), item); err != nil {
			a.serverError(w, err)
			return
		}
	}
	http.Redirect(w, r, "/ingestions/"+items[0].ID, http.StatusSeeOther)
}

func (a *App) persistAndRun(ctx context.Context, item ingestion.Ingestion) error {
	a.Logger.Printf("persisting ingestion ingestion_id=%s name=%q", item.ID, item.Name)
	if err := a.Store.Create(ctx, item); err != nil {
		a.Logger.Printf("metadata persistence failed ingestion_id=%s error=%q", item.ID, err)
		return err
	}
	a.Logger.Printf("metadata persisted ingestion_id=%s", item.ID)
	specPath, err := spec.Write(a.SpecDir, item)
	if err != nil {
		a.Logger.Printf("spec write failed ingestion_id=%s error=%q", item.ID, err)
		if updateErr := a.finish(item.ID, ingestion.StatusFailed, err.Error()); updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return nil
	}
	a.Logger.Printf("spec written ingestion_id=%s path=%s", item.ID, specPath)
	return a.execute(ctx, item)
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
	a.Logger.Printf("rerun requested ingestion_id=%s source=%s source_table=%s", item.ID, item.Source.Type, item.Source.Table)
	if err := a.execute(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID, http.StatusSeeOther)
}

func (a *App) execute(ctx context.Context, item ingestion.Ingestion) error {
	started := time.Now()
	if err := a.Policy.Validate(item); err != nil {
		a.Logger.Printf("run policy failed ingestion_id=%s error=%q", item.ID, err)
		return a.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	a.Logger.Printf("marking ingestion running ingestion_id=%s destination_table=%s", item.ID, item.Destination.Table)
	if err := a.Store.MarkRunning(ctx, item.ID, time.Now()); err != nil {
		a.Logger.Printf("failed to mark ingestion running ingestion_id=%s error=%q", item.ID, err)
		return err
	}
	if err := a.Runner.Run(ctx, item); err != nil {
		a.Logger.Printf("ingestion failed ingestion_id=%s duration=%s error=%q", item.ID, time.Since(started).Round(time.Millisecond), err)
		return a.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	a.Logger.Printf("ingestion succeeded ingestion_id=%s duration=%s", item.ID, time.Since(started).Round(time.Millisecond))
	return a.finish(item.ID, ingestion.StatusSucceeded, "")
}

func (a *App) finish(id, status, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.Store.Finish(ctx, id, status, message)
}

func (a *App) renderNew(w http.ResponseWriter, status int, data newPageData) {
	data.Title = "Add ingestion"
	data.Sources = ingestion.DefaultSourceCatalog().List()
	data.GitHubDocsURL = "https://bruin-data.github.io/ingestr/supported-sources/github.html"
	if data.GitHubTables == nil {
		data.GitHubTables = append([]githubTableOption(nil), githubTableOptions...)
	}
	a.render(w, status, "new", data)
}

func (a *App) renderGitHubError(w http.ResponseWriter, message, repository string, selected []string) {
	selectedSet := make(map[string]bool, len(selected))
	for _, table := range selected {
		selectedSet[table] = true
	}
	tables := append([]githubTableOption(nil), githubTableOptions...)
	for index := range tables {
		tables[index].Checked = selectedSet[tables[index].Value]
	}
	a.renderNew(w, http.StatusUnprocessableEntity, newPageData{
		Source: "github", Error: message, Repository: repository, GitHubTables: tables,
	})
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

var unsafeTableCharacters = regexp.MustCompile(`[^a-z0-9_]+`)

func safeTableName(value string) string {
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, "-", "_")
	value = unsafeTableCharacters.ReplaceAllString(value, "_")
	return strings.Trim(value, "_")
}

func githubTableName(value string) string {
	for _, option := range githubTableOptions {
		if option.Value == value {
			return option.Name
		}
	}
	return value
}
