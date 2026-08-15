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
	"regexp"
	"slices"
	"strings"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/ingestion"
	"pompos/internal/policy"
	"pompos/internal/secrets"
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
	Finish(context.Context, string, string, string) error
	UpdateSpecReference(context.Context, string, string, string) error
}

type ScheduleManager interface {
	Validate(string) error
	Upsert(ingestion.Ingestion) error
	Enqueue(context.Context, string) error
	NextRun(string) *time.Time
}

type App struct {
	Store       MetadataStore
	Policy      policy.Engine // retained for source compatibility; compilation owns policy decisions
	Secrets     secrets.Store
	Scheduler   ScheduleManager
	Validator   validation.SourceValidator
	Destination ingestion.Destination
	SpecDir     string
	Logger      *log.Logger
	templates   map[string]*template.Template
}

func New(app App) (*App, error) {
	if app.Store == nil || app.Secrets == nil || app.Validator == nil {
		return nil, errors.New("web app dependencies must not be nil")
	}
	if app.Logger == nil {
		app.Logger = log.Default()
	}
	if app.Scheduler == nil {
		app.Scheduler = noopScheduleManager{}
	}
	app.templates = make(map[string]*template.Template, 4)
	for _, page := range []string{"home", "new", "detail", "secrets"} {
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
	mux.HandleFunc("POST /ingestions/{id}/schedule", a.updateSchedule)
	mux.HandleFunc("GET /secrets", a.listSecrets)
	mux.HandleFunc("POST /secrets", a.createSecret)
	mux.HandleFunc("POST /secrets/delete", a.deleteSecret)
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
	SavedSecrets  []secrets.Entry
	SecretKey     string
	NewSecretName string
	Schedule      string
}

type secretsPageData struct {
	Title   string
	Error   string
	Name    string
	Saved   bool
	Deleted bool
	Secrets []secretView
}

type secretView struct {
	Entry      secrets.Entry
	Type       string
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
	Plan          string
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
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	if err := a.Scheduler.Validate(schedule); err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule})
		return
	}
	item := ingestion.Ingestion{
		ID:       newID(),
		Name:     tableName,
		Status:   ingestion.StatusPending,
		Schedule: schedule,
		Source:   ingestion.Source{Type: "csv", URL: csvURL},
		Destination: ingestion.Destination{
			Type:  a.Destination.Type,
			Path:  a.Destination.Path,
			Table: tableName,
		},
	}
	a.Logger.Printf("CSV ingestion requested ingestion_id=%s url=%s destination_table=%s", item.ID, item.Source.URL, item.Destination.Table)
	a.Logger.Printf("validating ingestion ingestion_id=%s source=csv", item.ID)
	if _, err := compiler.Compile(spec.FromLegacy(item), compiler.LocalDuckDB(a.Destination.Path)); err != nil {
		a.Logger.Printf("validation failed ingestion_id=%s source=csv error=%q", item.ID, err)
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule})
		return
	}
	if err := a.Validator.Validate(r.Context(), item.Source); err != nil {
		a.Logger.Printf("connectivity validation failed ingestion_id=%s source=csv error=%q", item.ID, err)
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule})
		return
	}
	a.Logger.Printf("validation succeeded ingestion_id=%s source=csv", item.ID)
	if err := a.persistAndEnqueue(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID+"?run=queued", http.StatusSeeOther)
}

func (a *App) createGitHubIngestions(w http.ResponseWriter, r *http.Request) {
	repositoryInput := strings.TrimSpace(r.FormValue("repository"))
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	owner, repository, err := validation.ParseGitHubRepository(repositoryInput)
	if err != nil {
		a.renderGitHubError(w, err.Error(), repositoryInput, r.Form["tables"], r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	selectedTables := r.Form["tables"]
	if len(selectedTables) == 0 {
		a.renderGitHubError(w, "Choose at least one table to sync.", repositoryInput, nil, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	if err := a.Scheduler.Validate(schedule); err != nil {
		a.renderGitHubError(w, err.Error(), repositoryInput, selectedTables, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}

	secretKey := strings.TrimSpace(r.FormValue("secret_key"))
	newSecretName := strings.TrimSpace(r.FormValue("new_secret_name"))
	accessToken := strings.TrimSpace(r.FormValue("access_token"))
	if secretKey != "" && (newSecretName != "" || accessToken != "") {
		a.renderGitHubError(w, "Choose a saved secret or enter a new one, not both.", repositoryInput, selectedTables, secretKey, newSecretName, schedule)
		return
	}
	if secretKey == "" && accessToken == "" {
		a.renderGitHubError(w, "Select a saved secret or enter a new token.", repositoryInput, selectedTables, "", newSecretName, schedule)
		return
	}
	if accessToken != "" && newSecretName == "" {
		a.renderGitHubError(w, "Give the new token a secret name.", repositoryInput, selectedTables, "", "", schedule)
		return
	}
	newSecret := secretKey == ""
	if newSecret {
		secretKey = newSecretName
	} else {
		stored, err := a.Secrets.Get(r.Context(), secretKey)
		if errors.Is(err, secrets.ErrNotFound) {
			a.renderGitHubError(w, "The selected secret no longer exists.", repositoryInput, selectedTables, "", "", schedule)
			return
		}
		if err != nil {
			a.serverError(w, err)
			return
		}
		accessToken = string(stored)
	}
	baseSource := ingestion.Source{
		Type:        "github",
		Owner:       owner,
		Repository:  repository,
		AccessToken: accessToken,
		SecretKey:   secretKey,
	}
	a.Logger.Printf("GitHub ingestion requested repository=%s/%s tables=%s token_provided=%t",
		owner, repository, strings.Join(selectedTables, ","), newSecret)
	validationSource := baseSource
	if slices.Contains(selectedTables, "stargazers") {
		validationSource.Table = "stargazers"
	}
	a.Logger.Printf("validating GitHub repository repository=%s/%s stargazers=%t", owner, repository, validationSource.Table == "stargazers")
	if err := a.Validator.Validate(r.Context(), validationSource); err != nil {
		a.Logger.Printf("GitHub repository validation failed repository=%s/%s error=%q", owner, repository, err)
		a.renderGitHubError(w, err.Error(), repositoryInput, selectedTables, selectedSecretKey(secretKey, newSecret), newSecretName, schedule)
		return
	}
	a.Logger.Printf("GitHub repository validation succeeded repository=%s/%s", owner, repository)
	if newSecret {
		if err := a.Secrets.Put(r.Context(), secretKey, []byte(accessToken)); err != nil {
			a.serverError(w, err)
			return
		}
		a.Logger.Printf("GitHub token saved repository=%s/%s secret_key=%s", owner, repository, secretKey)
	} else {
		a.Logger.Printf("using selected GitHub token repository=%s/%s secret_key=%s", owner, repository, secretKey)
	}

	items := make([]ingestion.Ingestion, 0, len(selectedTables))
	for _, sourceTable := range selectedTables {
		source := baseSource
		source.Table = sourceTable
		item := ingestion.Ingestion{
			ID:       newID(),
			Name:     owner + "/" + repository + " · " + githubTableName(sourceTable),
			Status:   ingestion.StatusPending,
			Schedule: schedule,
			Source:   source,
			Destination: ingestion.Destination{
				Type:  a.Destination.Type,
				Path:  a.Destination.Path,
				Table: safeTableName(owner + "_" + repository + "_" + sourceTable),
			},
		}
		if _, err := compiler.Compile(spec.FromLegacy(item), compiler.LocalDuckDB(a.Destination.Path)); err != nil {
			a.Logger.Printf("validation failed ingestion_id=%s source=github source_table=%s error=%q", item.ID, sourceTable, err)
			a.renderGitHubError(w, err.Error(), repositoryInput, selectedTables, selectedSecretKey(secretKey, newSecret), newSecretName, schedule)
			return
		}
		a.Logger.Printf("prepared GitHub ingestion ingestion_id=%s source_table=%s destination_table=%s",
			item.ID, item.Source.Table, item.Destination.Table)
		items = append(items, item)
	}
	for index, item := range items {
		a.Logger.Printf("queueing selected GitHub table ingestion_id=%s source_table=%s position=%d total=%d",
			item.ID, item.Source.Table, index+1, len(items))
		if err := a.persistAndEnqueue(r.Context(), item); err != nil {
			a.serverError(w, err)
			return
		}
	}
	http.Redirect(w, r, "/ingestions/"+items[0].ID+"?run=queued", http.StatusSeeOther)
}

func (a *App) persistAndEnqueue(ctx context.Context, item ingestion.Ingestion) error {
	specPath, err := spec.Write(a.SpecDir, item)
	if err != nil {
		a.Logger.Printf("spec write failed ingestion_id=%s error=%q", item.ID, err)
		return err
	}
	data, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("read written spec: %w", err)
	}
	item.SpecPath, item.SpecDigest = specPath, spec.Digest(data)
	document, err := spec.Parse(data)
	if err != nil {
		return err
	}
	if _, err := compiler.Compile(document, compiler.LocalDuckDB(a.Destination.Path)); err != nil {
		return err
	}
	a.Logger.Printf("spec written ingestion_id=%s path=%s", item.ID, specPath)
	a.Logger.Printf("persisting ingestion ingestion_id=%s name=%q", item.ID, item.Name)
	if err := a.Store.Create(ctx, item); err != nil {
		a.Logger.Printf("metadata persistence failed ingestion_id=%s error=%q", item.ID, err)
		return err
	}
	a.Logger.Printf("metadata persisted ingestion_id=%s", item.ID)
	if err := a.Scheduler.Upsert(item); err != nil {
		a.Logger.Printf("schedule registration failed ingestion_id=%s schedule=%q error=%q", item.ID, item.Schedule, err)
		if updateErr := a.finish(item.ID, ingestion.StatusFailed, err.Error()); updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	if err := a.Scheduler.Enqueue(ctx, item.ID); err != nil {
		a.Logger.Printf("initial run enqueue failed ingestion_id=%s error=%q", item.ID, err)
		if updateErr := a.finish(item.ID, ingestion.StatusFailed, err.Error()); updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	return nil
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
	planPreview := ""
	if document, _, readErr := spec.Read(item.SpecPath); readErr == nil {
		if plan, compileErr := compiler.Compile(document, compiler.LocalDuckDB(a.Destination.Path)); compileErr == nil {
			if data, marshalErr := compiler.MarshalPlan(plan); marshalErr == nil {
				planPreview = string(data)
			}
		}
	}
	a.render(w, http.StatusOK, "detail", detailPageData{
		Title: item.Name, Ingestion: item, SpecPath: filepath.Join(a.SpecDir, item.ID+".yaml"),
		NextRun: a.Scheduler.NextRun(item.ID), ScheduleValue: item.Schedule, ScheduleSaved: r.URL.Query().Get("schedule") == "saved",
		RunQueued: r.URL.Query().Get("run") == "queued",
		Plan:      planPreview,
	})
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
	if data.Source == "github" {
		entries, err := a.Secrets.List(context.Background())
		if err != nil {
			a.serverError(w, err)
			return
		}
		data.SavedSecrets = entries
	}
	a.render(w, status, "new", data)
}

func (a *App) renderGitHubError(w http.ResponseWriter, message, repository string, selected []string, secretKey, newSecretName, schedule string) {
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
		SecretKey: secretKey, NewSecretName: newSecretName, Schedule: schedule,
	})
}

func (a *App) listSecrets(w http.ResponseWriter, r *http.Request) {
	a.renderSecrets(w, r, http.StatusOK, secretsPageData{})
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
	projection := spec.ToProjection(document, state.ID, state.SpecPath, spec.Digest(data), a.Destination.Path)
	projection.Status, projection.LastRun, projection.LastError, projection.NextRun = state.Status, state.LastRun, state.LastError, state.NextRun
	return projection, nil
}

func describeSecrets(entries []secrets.Entry, ingestions []ingestion.Ingestion) []secretView {
	views := make([]secretView, 0, len(entries))
	for _, entry := range entries {
		view := secretView{Entry: entry, Type: "generic"}
		inferredType := ""
		mixedTypes := false
		for _, item := range ingestions {
			if item.Source.SecretKey != entry.Key {
				continue
			}
			view.Ingestions = append(view.Ingestions, item)
			if inferredType == "" {
				inferredType = item.Source.Type
			} else if inferredType != item.Source.Type {
				mixedTypes = true
			}
		}
		if inferredType != "" && !mixedTypes {
			view.Type = inferredType
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

func selectedSecretKey(secretKey string, newSecret bool) string {
	if newSecret {
		return ""
	}
	return secretKey
}

type noopScheduleManager struct{}

func (noopScheduleManager) Validate(string) error            { return nil }
func (noopScheduleManager) Upsert(ingestion.Ingestion) error { return nil }
func (noopScheduleManager) Enqueue(context.Context, string) error {
	return errors.New("run queue is unavailable")
}
func (noopScheduleManager) NextRun(string) *time.Time { return nil }
