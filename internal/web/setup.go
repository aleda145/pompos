package web

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"pompos/internal/agent"
	"pompos/internal/secrets"
)

type setupPageData struct {
	Title    string
	Step     string
	Error    string
	Settings agent.Settings
	Secrets  []string
	NoAuth   bool
}

func (a *App) setupStep(ctx context.Context, cfg agent.Settings) (string, error) {
	if cfg.Mode == "mcp" {
		return "", nil
	}
	if cfg.Mode == "" && cfg.Validate() != nil {
		return "mode", nil
	}
	if cfg.Validate() != nil {
		return "agent", nil
	}
	available, err := a.setupSecretAvailable(ctx, cfg.APIKeyRef)
	if err != nil {
		return "", err
	}
	if cfg.APIKeyRef != "" && !available {
		return "agent", nil
	}
	if cfg.ExaAPIKeyRef != "" {
		available, err = a.setupSecretAvailable(ctx, cfg.ExaAPIKeyRef)
		if err != nil {
			return "", err
		}
		if available {
			return "", nil
		}
	} else if cfg.ExaSkipped {
		return "", nil
	}
	return "search", nil
}
func (a *App) setupSecretAvailable(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, nil
	}
	value, err := a.Secrets.Get(ctx, name)
	if errors.Is(err, secrets.ErrNotFound) {
		return false, nil
	}
	return len(value) > 0, err
}

// Setup intercepts entry pages only. Settings, secrets and existing ingestion
// operations remain accessible so an operator can repair configuration.
func (a *App) requireSetup(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if a.Agent != nil && r.Method == "GET" && (path == "/" || path == "/ingestions/new" || path == "/chat" || strings.HasPrefix(path, "/chat/")) {
			cfg, err := a.Agent.Settings()
			if err != nil {
				a.serverError(w, err)
				return
			}
			step, err := a.setupStep(r.Context(), cfg)
			if err != nil {
				a.serverError(w, err)
				return
			}
			if step != "" {
				http.Redirect(w, r, "/setup", http.StatusSeeOther)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
func (a *App) setupPage(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	cfg, err := a.Agent.Settings()
	if err != nil {
		a.serverError(w, err)
		return
	}
	step, err := a.setupStep(r.Context(), cfg)
	if err != nil {
		a.serverError(w, err)
		return
	}
	if r.URL.Query().Get("step") == "mode" {
		step = "mode"
	}
	if step == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	a.renderSetup(w, r, 200, setupPageData{Step: step, Settings: cfg, NoAuth: cfg.APIKeyRef == "" && cfg.Validate() == nil})
}
func (a *App) renderSetup(w http.ResponseWriter, r *http.Request, status int, data setupPageData) {
	entries, err := a.Secrets.List(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	data.Title = "Set up Pompos"
	for _, entry := range entries {
		data.Secrets = append(data.Secrets, entry.Key)
	}
	w.Header().Set("Cache-Control", "no-store")
	a.render(w, status, "setup", data)
}
func (a *App) setupForm(w http.ResponseWriter, r *http.Request) (agent.Settings, bool) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return agent.Settings{}, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 20000)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid setup form", 400)
		return agent.Settings{}, false
	}
	cfg, err := a.Agent.Settings()
	if err != nil {
		a.serverError(w, err)
		return cfg, false
	}
	return cfg, true
}

// A newly entered key always gets its own name; existing secrets are never overwritten.
func (a *App) saveSetupKey(ctx context.Context, cfg agent.Settings, kind, key, existing string, noAuth bool) error {
	key = strings.TrimSpace(key)
	if noAuth {
		if key != "" || existing != "" {
			return errors.New("choose no authentication or a key, not both")
		}
	} else {
		if key == "" && existing == "" {
			return errors.New("enter an API key or select an existing secret")
		}
		if key != "" && existing != "" {
			return errors.New("enter a new key or select an existing secret, not both")
		}
	}
	if len(key) > 16000 {
		return errors.New("API key is too long")
	}
	if existing != "" {
		available, err := a.setupSecretAvailable(ctx, existing)
		if err != nil {
			return err
		}
		if !available {
			return errors.New("the selected secret is missing or empty; choose another or enter a new key")
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	name := existing
	if key != "" {
		name = "pompos-" + kind + "-" + newID()
	}
	if kind == "model" {
		cfg.APIKeyRef = name
	} else {
		cfg.ExaAPIKeyRef = name
		cfg.ExaSkipped = false
	}
	if key != "" {
		if err := a.Secrets.Put(ctx, name, []byte(key)); err != nil {
			return errors.New("could not save the API key")
		}
	}
	if err := a.Agent.SaveSettings(cfg); err != nil {
		if key != "" {
			_ = a.Secrets.Delete(ctx, name)
		}
		return err
	}
	return nil
}
func (a *App) setupAgent(w http.ResponseWriter, r *http.Request) {
	cfg, ok := a.setupForm(w, r)
	if !ok {
		return
	}
	step, err := a.setupStep(r.Context(), cfg)
	if err != nil {
		a.serverError(w, err)
		return
	}
	if step != "agent" && step != "mode" {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	cfg.Mode = "agent"
	cfg.Endpoint = strings.TrimRight(strings.TrimSpace(r.FormValue("endpoint")), "/")
	cfg.Model = strings.TrimSpace(r.FormValue("model"))
	cfg.APIKeyRef = r.FormValue("api_key_ref")
	noAuth := r.FormValue("no_auth") == "on"
	err = a.saveSetupKey(r.Context(), cfg, "model", r.FormValue("api_key"), cfg.APIKeyRef, noAuth)
	if err != nil {
		a.renderSetup(w, r, 422, setupPageData{Step: "agent", Settings: cfg, NoAuth: noAuth, Error: err.Error()})
		return
	}
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}
func (a *App) setupSearch(w http.ResponseWriter, r *http.Request) {
	cfg, ok := a.setupForm(w, r)
	if !ok {
		return
	}
	step, err := a.setupStep(r.Context(), cfg)
	if err != nil {
		a.serverError(w, err)
		return
	}
	if step == "agent" || step == "mode" {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	if step == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	if r.FormValue("action") == "skip" {
		cfg.ExaAPIKeyRef = ""
		cfg.ExaSkipped = true
		err = a.Agent.SaveSettings(cfg)
	} else {
		cfg.ExaAPIKeyRef = r.FormValue("exa_api_key_ref")
		if cfg.ExaAPIKeyRef != "" && cfg.ExaAPIKeyRef == cfg.APIKeyRef {
			err = errors.New("choose a separate Exa key, not the model provider secret")
		} else {
			err = a.saveSetupKey(r.Context(), cfg, "exa", r.FormValue("api_key"), cfg.ExaAPIKeyRef, false)
		}
	}
	if err != nil {
		a.renderSetup(w, r, 422, setupPageData{Step: "search", Settings: cfg, Error: err.Error()})
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (a *App) setupMode(w http.ResponseWriter, r *http.Request) {
	cfg, ok := a.setupForm(w, r)
	if !ok {
		return
	}
	mode := r.FormValue("mode")
	if err := a.Agent.SelectMode(mode); err != nil {
		a.renderSetup(w, r, 422, setupPageData{Step: "mode", Settings: cfg, Error: err.Error()})
		return
	}
	if mode == "mcp" {
		http.Redirect(w, r, "/settings/agent#mcp", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/setup", http.StatusSeeOther)
}
