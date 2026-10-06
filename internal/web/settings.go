package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"pompos/internal/agent"
	"pompos/internal/runner"
)

type runtimeSettingsData struct {
	WorkersValue string
	WorkersError string
	TimeoutValue string
	TimeoutError string
	TimeoutMax   int
}

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
	timeoutMinutes, e := a.Store.RunTimeoutMinutes(r.Context())
	if e != nil {
		a.serverError(w, e)
		return
	}
	runtime := runtimeSettingsData{
		WorkersValue: strconv.Itoa(a.Scheduler.Runtime().WorkerLimit),
		TimeoutValue: strconv.Itoa(timeoutMinutes), TimeoutMax: runner.MaxRunTimeoutMinutes,
	}
	message := ""
	saved := r.Method == http.MethodGet && r.URL.Query().Get("saved") == "1"
	status := http.StatusOK
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, 16000)
		if e = r.ParseForm(); e != nil {
			http.Error(w, "Invalid settings", http.StatusBadRequest)
			return
		}
		section := r.FormValue("section")
		switch section {
		case "", "general":
			if e = a.Agent.SetGeneralSettings(r.FormValue("display_timezone"), r.FormValue("manual_validation") == "on"); e == nil {
				http.Redirect(w, r, "/settings?saved=1", http.StatusSeeOther)
				return
			}
		case "agent":
			cfg.Endpoint = r.FormValue("endpoint")
			cfg.Model = r.FormValue("model")
			cfg.APIKeyRef = r.FormValue("api_key_ref")
			e = cfg.ValidateAgent()
		case "search":
			cfg.ExaAPIKeyRef = r.FormValue("exa_api_key_ref")
		case "runtime":
			runtime.WorkersValue = strings.TrimSpace(r.PostForm.Get("workers"))
			runtime.TimeoutValue = strings.TrimSpace(r.PostForm.Get("timeout_minutes"))
			workers, workersErr := strconv.Atoi(runtime.WorkersValue)
			minutes, timeoutErr := strconv.Atoi(runtime.TimeoutValue)
			if workersErr != nil || workers < 1 {
				runtime.WorkersError = "Worker count must be a positive whole number."
			}
			if timeoutErr != nil || minutes < 1 || minutes > runner.MaxRunTimeoutMinutes {
				runtime.TimeoutError = fmt.Sprintf("Run timeout must be a whole number from 1 to %d minutes.", runner.MaxRunTimeoutMinutes)
			}
			status = http.StatusUnprocessableEntity
			if runtime.WorkersError == "" && runtime.TimeoutError == "" {
				if err := a.Scheduler.SetRuntimeSettings(r.Context(), workers, minutes); err == nil {
					http.Redirect(w, r, "/settings?saved=1#runtime", http.StatusSeeOther)
					return
				} else {
					a.Logger.Printf("change runtime settings: %v", err)
					message = "Could not save runtime settings. Try again."
					status = http.StatusInternalServerError
				}
			}
		default:
			http.Error(w, "Unknown settings section", http.StatusBadRequest)
			return
		}
		if section != "runtime" {
			if e == nil {
				e = a.Agent.SaveSettings(cfg)
			}
			if e != nil {
				message = e.Error()
			} else {
				saved = true
			}
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
	a.render(w, status, "settings", struct {
		Title       string
		Settings    agent.Settings
		Error       string
		Saved       bool
		Secrets     []string
		MCPEndpoint string
		Runtime     runtimeSettingsData
	}{"Settings", cfg, message, saved, names, mcpEndpoint(r.Host), runtime})
}

func (a *App) displayTimezone() (string, error) {
	if a.Agent == nil {
		return "", nil
	}
	cfg, err := a.Agent.Settings()
	return cfg.DisplayTimezone, err
}
