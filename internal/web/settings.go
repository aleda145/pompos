package web

import (
	"net/http"

	"pompos/internal/agent"
)

func (a *App) settings(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", http.StatusServiceUnavailable)
		return
	}
	cfg, e := a.Agent.Settings()
	if e != nil {
		a.serverError(w, e)
		return
	}
	message := ""
	saved := r.URL.Query().Get("saved") == "1"
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 16000)
		if e = r.ParseForm(); e != nil {
			http.Error(w, "Invalid settings", http.StatusBadRequest)
			return
		}
		switch r.FormValue("section") {
		case "", "general":
			if e = a.Agent.SetManualValidation(r.FormValue("manual_validation") == "on"); e != nil {
				a.serverError(w, e)
				return
			}
			http.Redirect(w, r, "/settings?saved=1", http.StatusSeeOther)
			return
		case "agent":
			cfg.Endpoint = r.FormValue("endpoint")
			cfg.Model = r.FormValue("model")
			cfg.APIKeyRef = r.FormValue("api_key_ref")
			e = cfg.ValidateAgent()
		case "search":
			cfg.ExaAPIKeyRef = r.FormValue("exa_api_key_ref")
		default:
			http.Error(w, "Unknown settings section", http.StatusBadRequest)
			return
		}
		if e == nil {
			e = a.Agent.SaveSettings(cfg)
		}
		if e != nil {
			message = e.Error()
		} else {
			saved = true
		}
	}
	entries, e := a.Secrets.List(r.Context())
	if e != nil {
		a.serverError(w, e)
		return
	}
	names := []string{}
	for _, entry := range entries {
		names = append(names, entry.Key)
	}
	a.render(w, http.StatusOK, "settings", struct {
		Title       string
		Settings    agent.Settings
		Error       string
		Saved       bool
		Secrets     []string
		MCPEndpoint string
	}{"Settings", cfg, message, saved, names, mcpEndpoint(r.Host)})
}
