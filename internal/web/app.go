package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/destination"
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

type DestinationCatalog interface {
	GetDestination(context.Context, string) (destination.Config, error)
	ListDestinations(context.Context) ([]destination.Config, error)
	PutDestination(context.Context, destination.Config) error
}

type App struct {
	Store        MetadataStore
	Policy       policy.Engine // retained for source compatibility; compilation owns policy decisions
	Secrets      secrets.Store
	Destinations DestinationCatalog
	Scheduler    ScheduleManager
	Validator    validation.SourceValidator
	Inspector    validation.SourceInspector
	Destination  ingestion.Destination
	SpecDir      string
	Logger       *log.Logger
	templates    map[string]*template.Template
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
	if app.Inspector == nil {
		app.Inspector, _ = app.Validator.(validation.SourceInspector)
	}
	if app.Destination.Ref == "" {
		app.Destination.Ref = "local-duckdb"
	}
	if app.Destinations == nil {
		catalog, ok := app.Store.(DestinationCatalog)
		if !ok {
			return nil, errors.New("web app destination store is required")
		}
		app.Destinations = catalog
	}
	app.templates = make(map[string]*template.Template, 5)
	for _, page := range []string{"home", "new", "detail", "secrets", "destinations"} {
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
	mux.HandleFunc("POST /ingestions/preview", a.previewIngestionYAML)
	mux.HandleFunc("POST /sources/columns", a.previewSourceColumns)
	mux.HandleFunc("GET /ingestions/{id}", a.ingestionDetail)
	mux.HandleFunc("POST /ingestions/{id}/run", a.runIngestion)
	mux.HandleFunc("POST /ingestions/{id}/schedule", a.updateSchedule)
	mux.HandleFunc("GET /secrets", a.listSecrets)
	mux.HandleFunc("POST /secrets", a.createSecret)
	mux.HandleFunc("POST /secrets/delete", a.deleteSecret)
	mux.HandleFunc("GET /destinations", a.listDestinations)
	mux.HandleFunc("POST /destinations", a.saveDestination)
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
	Title               string
	Error               string
	Source              string
	Sources             []ingestion.SourceDefinition
	CSVURL              string
	TableName           string
	Repository          string
	GitHubTables        []githubTableOption
	GitHubDocsURL       string
	SavedSecrets        []secrets.Entry
	SecretKey           string
	NewSecretName       string
	Schedule            string
	RuntimeEngine       string
	RuntimeOrchestrator string
	Strategy            string
	PrimaryKey          string
	IncrementalKey      string
	Destinations        []destination.Config
	DestinationRef      string
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
	YAML          string
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
		a.createGitHubIngestion(w, r)
		return
	}
	a.createCSVIngestion(w, r)
}

type yamlPreviewResponse struct {
	YAML  string `json:"yaml,omitempty"`
	Error string `json:"error,omitempty"`
}

type columnPreviewResponse struct {
	Columns []string `json:"columns,omitempty"`
	Scope   string   `json:"scope,omitempty"`
	Error   string   `json:"error,omitempty"`
}

func (a *App) previewSourceColumns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if a.Inspector == nil {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: "Column discovery is unavailable."})
		return
	}
	if err := r.ParseMultipartForm(1 << 20); err != nil && !errors.Is(err, http.ErrNotMultipart) {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: "Could not read the source details."})
		return
	}
	if r.FormValue("source_type") != "github" {
		source := ingestion.Source{Type: "csv", URL: strings.TrimSpace(r.FormValue("csv_url"))}
		columns, err := a.Inspector.Columns(r.Context(), source)
		if err != nil {
			_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: err.Error()})
			return
		}
		a.Logger.Printf("CSV columns discovered url=%s columns=%d", source.URL, len(columns))
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Columns: columns, Scope: "the first CSV row"})
		return
	}

	owner, repository, err := validation.ParseGitHubRepository(r.FormValue("repository"))
	if err != nil {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: err.Error()})
		return
	}
	table, err := githubSourceTable(r)
	if err != nil {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: err.Error()})
		return
	}
	secretKey := strings.TrimSpace(r.FormValue("secret_key"))
	accessToken := strings.TrimSpace(r.FormValue("access_token"))
	if secretKey != "" && accessToken != "" {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: "Choose a saved secret or enter a new token, not both."})
		return
	}
	if secretKey != "" {
		stored, getErr := a.Secrets.Get(r.Context(), secretKey)
		if errors.Is(getErr, secrets.ErrNotFound) {
			_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: "The selected secret no longer exists."})
			return
		}
		if getErr != nil {
			a.serverError(w, getErr)
			return
		}
		accessToken = string(stored)
	}
	if accessToken == "" {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: "Choose a GitHub secret to discover columns."})
		return
	}

	columns, err := a.Inspector.Columns(r.Context(), ingestion.Source{
		Type: "github", Owner: owner, Repository: repository, Table: table, AccessToken: accessToken,
	})
	if err != nil {
		_ = json.NewEncoder(w).Encode(columnPreviewResponse{Error: fmt.Sprintf("%s: %v", githubTableName(table), err)})
		return
	}
	a.Logger.Printf("GitHub columns discovered repository=%s/%s table=%s columns=%d", owner, repository, table, len(columns))
	_ = json.NewEncoder(w).Encode(columnPreviewResponse{Columns: columns, Scope: githubTableName(table)})
}

func (a *App) previewIngestionYAML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := r.ParseMultipartForm(1 << 20); err != nil && !errors.Is(err, http.ErrNotMultipart) {
		_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: "Could not read the form."})
		return
	}
	var item ingestion.Ingestion
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	materialization, err := materializationFromForm(r)
	if err != nil {
		_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: err.Error()})
		return
	}
	destination, err := a.destinationFromForm(r)
	if err != nil {
		_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: err.Error()})
		return
	}
	runtime, err := runtimeFromForm(r)
	if err != nil {
		_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: err.Error()})
		return
	}
	if r.FormValue("source_type") == "github" {
		owner, repository, err := validation.ParseGitHubRepository(r.FormValue("repository"))
		if err != nil {
			_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: err.Error()})
			return
		}
		secretRef := strings.TrimSpace(r.FormValue("secret_key"))
		if secretRef == "" {
			secretRef = strings.TrimSpace(r.FormValue("new_secret_name"))
		}
		if secretRef == "" {
			_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: "Choose a saved secret or give the new token a secret name."})
			return
		}
		sourceTable, err := githubSourceTable(r)
		if err != nil {
			_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: err.Error()})
			return
		}
		item = ingestion.Ingestion{
			Name: owner + "/" + repository + " · " + githubTableName(sourceTable), Schedule: schedule,
			Source:          ingestion.Source{Type: "github", Owner: owner, Repository: repository, Table: sourceTable, SecretKey: secretRef},
			Destination:     ingestion.Destination{Ref: destination.Ref, Type: destination.Type, Path: destination.Path, Table: safeTableName(owner + "_" + repository + "_" + sourceTable)},
			Runtime:         runtime,
			Materialization: materialization,
		}
	} else {
		item = ingestion.Ingestion{
			Name: strings.TrimSpace(r.FormValue("table_name")), Schedule: schedule,
			Source:          ingestion.Source{Type: "csv", URL: strings.TrimSpace(r.FormValue("csv_url"))},
			Destination:     ingestion.Destination{Ref: destination.Ref, Type: destination.Type, Path: destination.Path, Table: strings.TrimSpace(r.FormValue("table_name"))},
			Runtime:         runtime,
			Materialization: materialization,
		}
	}
	data, err := spec.Marshal(spec.FromLegacy(item))
	if err != nil {
		_ = json.NewEncoder(w).Encode(yamlPreviewResponse{Error: err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(yamlPreviewResponse{YAML: string(data)})
}

func (a *App) createCSVIngestion(w http.ResponseWriter, r *http.Request) {
	csvURL := strings.TrimSpace(r.FormValue("csv_url"))
	tableName := strings.TrimSpace(r.FormValue("table_name"))
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	materialization, err := materializationFromForm(r)
	if err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule,
			Strategy: materialization.Strategy, PrimaryKey: strings.Join(materialization.PrimaryKey, ", "), IncrementalKey: materialization.IncrementalKey})
		return
	}
	destination, err := a.destinationFromForm(r)
	if err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule,
			DestinationRef: strings.TrimSpace(r.FormValue("destination_ref")), Strategy: materialization.Strategy,
			PrimaryKey: strings.Join(materialization.PrimaryKey, ", "), IncrementalKey: materialization.IncrementalKey})
		return
	}
	runtime, err := runtimeFromForm(r)
	if err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule,
			RuntimeEngine: runtime.Engine, RuntimeOrchestrator: runtime.Orchestrator, DestinationRef: destination.Ref,
			Strategy: materialization.Strategy, PrimaryKey: strings.Join(materialization.PrimaryKey, ", "), IncrementalKey: materialization.IncrementalKey})
		return
	}
	if err := a.Scheduler.Validate(schedule); err != nil {
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule,
			RuntimeEngine: runtime.Engine, RuntimeOrchestrator: runtime.Orchestrator, DestinationRef: destination.Ref,
			Strategy: materialization.Strategy, PrimaryKey: strings.Join(materialization.PrimaryKey, ", "), IncrementalKey: materialization.IncrementalKey})
		return
	}
	item := ingestion.Ingestion{
		ID:              newID(),
		Name:            tableName,
		Status:          ingestion.StatusPending,
		Schedule:        schedule,
		Runtime:         runtime,
		Materialization: materialization,
		Source:          ingestion.Source{Type: "csv", URL: csvURL},
		Destination: ingestion.Destination{
			Ref:   destination.Ref,
			Type:  destination.Type,
			Path:  destination.Path,
			Table: tableName,
		},
	}
	a.Logger.Printf("CSV ingestion requested ingestion_id=%s url=%s destination_table=%s", item.ID, item.Source.URL, item.Destination.Table)
	a.Logger.Printf("validating ingestion ingestion_id=%s source=csv", item.ID)
	if _, err := a.compile(spec.FromLegacy(item)); err != nil {
		a.Logger.Printf("validation failed ingestion_id=%s source=csv error=%q", item.ID, err)
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule,
			RuntimeEngine: runtime.Engine, RuntimeOrchestrator: runtime.Orchestrator, DestinationRef: destination.Ref,
			Strategy: materialization.Strategy, PrimaryKey: strings.Join(materialization.PrimaryKey, ", "), IncrementalKey: materialization.IncrementalKey})
		return
	}
	if err := a.Validator.Validate(r.Context(), item.Source); err != nil {
		a.Logger.Printf("connectivity validation failed ingestion_id=%s source=csv error=%q", item.ID, err)
		a.renderNew(w, http.StatusUnprocessableEntity, newPageData{Source: "csv", Error: err.Error(), CSVURL: csvURL, TableName: tableName, Schedule: schedule,
			RuntimeEngine: runtime.Engine, RuntimeOrchestrator: runtime.Orchestrator, DestinationRef: destination.Ref,
			Strategy: materialization.Strategy, PrimaryKey: strings.Join(materialization.PrimaryKey, ", "), IncrementalKey: materialization.IncrementalKey})
		return
	}
	a.Logger.Printf("validation succeeded ingestion_id=%s source=csv", item.ID)
	if err := a.persistAndEnqueue(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID+"?run=queued", http.StatusSeeOther)
}

func (a *App) createGitHubIngestion(w http.ResponseWriter, r *http.Request) {
	repositoryInput := strings.TrimSpace(r.FormValue("repository"))
	selectedTable := strings.TrimSpace(r.FormValue("source_table"))
	schedule := strings.TrimSpace(r.FormValue("schedule"))
	materialization, materializationErr := materializationFromForm(r)
	if materializationErr != nil {
		a.renderGitHubError(w, r, materializationErr.Error(), repositoryInput, selectedTable, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	destination, destinationErr := a.destinationFromForm(r)
	if destinationErr != nil {
		a.renderGitHubError(w, r, destinationErr.Error(), repositoryInput, selectedTable, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	runtime, runtimeErr := runtimeFromForm(r)
	if runtimeErr != nil {
		a.renderGitHubError(w, r, runtimeErr.Error(), repositoryInput, selectedTable, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	owner, repository, err := validation.ParseGitHubRepository(repositoryInput)
	if err != nil {
		a.renderGitHubError(w, r, err.Error(), repositoryInput, selectedTable, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	selectedTable, err = githubSourceTable(r)
	if err != nil {
		a.renderGitHubError(w, r, err.Error(), repositoryInput, selectedTable, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}
	if err := a.Scheduler.Validate(schedule); err != nil {
		a.renderGitHubError(w, r, err.Error(), repositoryInput, selectedTable, r.FormValue("secret_key"), r.FormValue("new_secret_name"), schedule)
		return
	}

	secretKey := strings.TrimSpace(r.FormValue("secret_key"))
	newSecretName := strings.TrimSpace(r.FormValue("new_secret_name"))
	accessToken := strings.TrimSpace(r.FormValue("access_token"))
	if secretKey != "" && (newSecretName != "" || accessToken != "") {
		a.renderGitHubError(w, r, "Choose a saved secret or enter a new one, not both.", repositoryInput, selectedTable, secretKey, newSecretName, schedule)
		return
	}
	if secretKey == "" && accessToken == "" {
		a.renderGitHubError(w, r, "Select a saved secret or enter a new token.", repositoryInput, selectedTable, "", newSecretName, schedule)
		return
	}
	if accessToken != "" && newSecretName == "" {
		a.renderGitHubError(w, r, "Give the new token a secret name.", repositoryInput, selectedTable, "", "", schedule)
		return
	}
	newSecret := secretKey == ""
	if newSecret {
		secretKey = newSecretName
	} else {
		stored, err := a.Secrets.Get(r.Context(), secretKey)
		if errors.Is(err, secrets.ErrNotFound) {
			a.renderGitHubError(w, r, "The selected secret no longer exists.", repositoryInput, selectedTable, "", "", schedule)
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
	a.Logger.Printf("GitHub ingestion requested repository=%s/%s table=%s token_provided=%t",
		owner, repository, selectedTable, newSecret)
	validationSource := baseSource
	if selectedTable == "stargazers" {
		validationSource.Table = "stargazers"
	}
	a.Logger.Printf("validating GitHub repository repository=%s/%s stargazers=%t", owner, repository, validationSource.Table == "stargazers")
	if err := a.Validator.Validate(r.Context(), validationSource); err != nil {
		a.Logger.Printf("GitHub repository validation failed repository=%s/%s error=%q", owner, repository, err)
		a.renderGitHubError(w, r, err.Error(), repositoryInput, selectedTable, selectedSecretKey(secretKey, newSecret), newSecretName, schedule)
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

	source := baseSource
	source.Table = selectedTable
	item := ingestion.Ingestion{
		ID:              newID(),
		Name:            owner + "/" + repository + " · " + githubTableName(selectedTable),
		Status:          ingestion.StatusPending,
		Schedule:        schedule,
		Runtime:         runtime,
		Materialization: materialization,
		Source:          source,
		Destination: ingestion.Destination{
			Ref:   destination.Ref,
			Type:  destination.Type,
			Path:  destination.Path,
			Table: safeTableName(owner + "_" + repository + "_" + selectedTable),
		},
	}
	if _, err := a.compile(spec.FromLegacy(item)); err != nil {
		a.Logger.Printf("validation failed ingestion_id=%s source=github source_table=%s error=%q", item.ID, selectedTable, err)
		a.renderGitHubError(w, r, err.Error(), repositoryInput, selectedTable, selectedSecretKey(secretKey, newSecret), newSecretName, schedule)
		return
	}
	a.Logger.Printf("prepared GitHub ingestion ingestion_id=%s source_table=%s destination_table=%s",
		item.ID, item.Source.Table, item.Destination.Table)
	if err := a.persistAndEnqueue(r.Context(), item); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/ingestions/"+item.ID+"?run=queued", http.StatusSeeOther)
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
	if _, err := a.compile(document); err != nil {
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
	_, yamlData, err := spec.Read(item.SpecPath)
	if err != nil {
		a.serverError(w, err)
		return
	}
	a.render(w, http.StatusOK, "detail", detailPageData{
		Title: item.Name, Ingestion: item, SpecPath: item.SpecPath,
		NextRun: a.Scheduler.NextRun(item.ID), ScheduleValue: item.Schedule, ScheduleSaved: r.URL.Query().Get("schedule") == "saved",
		RunQueued: r.URL.Query().Get("run") == "queued",
		YAML:      string(yamlData),
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
	if data.RuntimeEngine == "" {
		data.RuntimeEngine = "ingestr"
	}
	if data.RuntimeOrchestrator == "" {
		data.RuntimeOrchestrator = "direct"
	}
	if data.Strategy == "" {
		data.Strategy = "replace"
	}
	destinations, err := a.Destinations.ListDestinations(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}
	data.Destinations = destinations
	if data.DestinationRef == "" {
		data.DestinationRef = a.Destination.Ref
	}
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

func runtimeFromForm(r *http.Request) (ingestion.Runtime, error) {
	runtime := ingestion.Runtime{
		Engine:       strings.TrimSpace(r.FormValue("runtime_engine")),
		Orchestrator: strings.TrimSpace(r.FormValue("runtime_orchestrator")),
	}
	if runtime.Engine == "" {
		runtime.Engine = "ingestr"
	}
	if runtime.Orchestrator == "" {
		runtime.Orchestrator = "direct"
	}
	if runtime.Engine != "ingestr" {
		return runtime, fmt.Errorf("runtime engine %q is not available", runtime.Engine)
	}
	if runtime.Orchestrator != "direct" {
		return runtime, fmt.Errorf("runtime orchestrator %q is not available", runtime.Orchestrator)
	}
	return runtime, nil
}

func materializationFromForm(r *http.Request) (ingestion.Materialization, error) {
	materialization := ingestion.Materialization{
		Strategy:       strings.TrimSpace(r.FormValue("strategy")),
		IncrementalKey: strings.TrimSpace(r.FormValue("incremental_key")),
	}
	if materialization.Strategy == "" {
		materialization.Strategy = "replace"
	}
	for _, key := range strings.Split(r.FormValue("primary_key"), ",") {
		if key = strings.TrimSpace(key); key != "" {
			materialization.PrimaryKey = append(materialization.PrimaryKey, key)
		}
	}
	definition := spec.Materialization{Strategy: materialization.Strategy, PrimaryKey: materialization.PrimaryKey, IncrementalKey: materialization.IncrementalKey}
	if err := definition.Validate(); err != nil {
		return materialization, err
	}
	return materialization, nil
}

func (a *App) destinationFromForm(r *http.Request) (ingestion.Destination, error) {
	ref := strings.TrimSpace(r.FormValue("destination_ref"))
	if ref == "" {
		ref = a.Destination.Ref
	}
	config, err := a.Destinations.GetDestination(r.Context(), ref)
	if errors.Is(err, destination.ErrNotFound) {
		return ingestion.Destination{Ref: ref}, fmt.Errorf("destination %q does not exist", ref)
	}
	if err != nil {
		return ingestion.Destination{Ref: ref}, err
	}
	return ingestion.Destination{Ref: ref, Type: config.Type, Path: config.Path}, nil
}

func (a *App) compile(document spec.Ingestion) (compiler.ExecutionPlan, error) {
	return compiler.Compile(document, compiler.LocalDuckDB(a.Destination.Path))
}

func (a *App) renderGitHubError(w http.ResponseWriter, r *http.Request, message, repository, selected, secretKey, newSecretName, schedule string) {
	tables := append([]githubTableOption(nil), githubTableOptions...)
	if selected != "" {
		for index := range tables {
			tables[index].Checked = tables[index].Value == selected
		}
	}
	a.renderNew(w, http.StatusUnprocessableEntity, newPageData{
		Source: "github", Error: message, Repository: repository, GitHubTables: tables,
		SecretKey: secretKey, NewSecretName: newSecretName, Schedule: schedule,
		RuntimeEngine: strings.TrimSpace(r.FormValue("runtime_engine")), RuntimeOrchestrator: strings.TrimSpace(r.FormValue("runtime_orchestrator")),
		DestinationRef: strings.TrimSpace(r.FormValue("destination_ref")),
		Strategy:       strings.TrimSpace(r.FormValue("strategy")), PrimaryKey: strings.TrimSpace(r.FormValue("primary_key")), IncrementalKey: strings.TrimSpace(r.FormValue("incremental_key")),
	})
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

func isGitHubTable(value string) bool {
	for _, option := range githubTableOptions {
		if option.Value == value {
			return true
		}
	}
	return false
}

func githubSourceTable(r *http.Request) (string, error) {
	values := r.Form["source_table"]
	if len(values) != 1 {
		return "", errors.New("choose exactly one GitHub table")
	}
	table := strings.TrimSpace(values[0])
	if !isGitHubTable(table) {
		return table, errors.New("choose a supported GitHub table")
	}
	return table, nil
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
