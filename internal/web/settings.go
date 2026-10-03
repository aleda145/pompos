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
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 16000)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Invalid settings", http.StatusBadRequest)
			return
		}
		if err := a.Agent.SetManualValidation(r.FormValue("manual_validation") == "on"); err != nil {
			a.serverError(w, err)
			return
		}
		http.Redirect(w, r, "/settings?saved=1", http.StatusSeeOther)
		return
	}
	cfg, err := a.Agent.Settings()
	if err != nil {
		a.serverError(w, err)
		return
	}
	a.render(w, http.StatusOK, "settings", struct {
		Title    string
		Settings agent.Settings
		Saved    bool
	}{"Settings", cfg, r.URL.Query().Get("saved") == "1"})
}
