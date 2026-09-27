package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"pompos/internal/agent"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func (a *App) chatPage(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.Redirect(w, r, "/chat/"+newID(), http.StatusSeeOther)
		return
	}
	v, e := a.Agent.Load(id)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	a.render(w, 200, "chat", struct {
		Title   string
		Session agent.Session
	}{"New ingestion", publicSession(v)})
}
func (a *App) chatTurn(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 20000)
	var input agent.Input
	if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
		http.Error(w, "Invalid message", 400)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if strings.Contains(r.Header.Get("Accept"), "application/x-ndjson") {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("X-Accel-Buffering", "no")
		encoder := json.NewEncoder(w)
		send := func(value any) {
			if encoder.Encode(value) == nil {
				_ = http.NewResponseController(w).Flush()
			}
		}
		v, e := a.Agent.TurnWithEvents(r.Context(), r.PathValue("id"), input, func(event agent.Event) { send(event) })
		message := ""
		if e != nil {
			message = e.Error()
		}
		send(struct {
			Type    string        `json:"type"`
			Session agent.Session `json:"session"`
			Error   string        `json:"error,omitempty"`
		}{"done", publicSession(v), message})
		return
	}
	v, e := a.Agent.TurnWithEvents(r.Context(), r.PathValue("id"), input, nil)
	w.Header().Set("Content-Type", "application/json")
	message := ""
	if e != nil {
		message = e.Error()
	}
	_ = json.NewEncoder(w).Encode(struct {
		Session agent.Session `json:"session"`
		Error   string        `json:"error,omitempty"`
	}{publicSession(v), message})
}
func publicSession(v agent.Session) agent.Session {
	if v.Loading == nil {
		v.Ready = false
	}
	visible := []agent.Message{}
	for _, m := range v.Messages {
		if m.Role != "system" {
			visible = append(visible, m)
		}
	}
	v.Messages = visible
	return v
}
func (a *App) chatSecret(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 20000)
	var input struct {
		Name      string `json:"name"`
		HandoffID string `json:"handoff_id"`
		Value     string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid secret", 400)
		return
	}
	if err := a.Agent.SaveRequestedSecret(r.Context(), r.PathValue("id"), input.HandoffID, input.Name, input.Value); err != nil {
		http.Error(w, err.Error(), 422)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) publishChat(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	id := r.PathValue("id")
	saved, e := a.Agent.Publish(r.Context(), id, filepath.Join(a.SpecDir, id+".py"), func(doc spec.Ingestion) error {
		data, e := spec.Marshal(doc)
		if e != nil {
			return e
		}
		path := filepath.Join(a.SpecDir, id+".yaml")
		item := spec.ToProjection(doc, id, path, spec.Digest(data), a.Destination.Path)
		if err := a.Scheduler.Validate(item.Schedule); err != nil {
			return err
		}
		if _, e = a.Store.Get(r.Context(), id); e == nil {
			return nil
		} else if !errors.Is(e, store.ErrNotFound) {
			return e
		}
		if e = agent.WriteFile(path, data); e != nil {
			return e
		}
		if e = a.Store.Create(r.Context(), item); e != nil {
			_ = os.Remove(path)
			return e
		}
		return a.Scheduler.Upsert(item)
	})
	if e != nil {
		http.Error(w, e.Error(), 422)
		return
	}
	http.Redirect(w, r, "/ingestions/"+saved, http.StatusSeeOther)
}
func (a *App) agentSettings(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	cfg, e := a.Agent.Settings()
	if e != nil {
		a.serverError(w, e)
		return
	}
	message := ""
	saved := false
	if r.Method == "POST" {
		r.Body = http.MaxBytesReader(w, r.Body, 16000)
		if e = r.ParseForm(); e != nil {
			http.Error(w, "Invalid settings", 400)
			return
		}
		cfg = agent.Settings{Endpoint: r.FormValue("endpoint"), Model: r.FormValue("model"), APIKeyRef: r.FormValue("api_key_ref")}
		e = a.Agent.SaveSettings(cfg)
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
	a.render(w, 200, "agent-settings", struct {
		Title    string
		Settings agent.Settings
		Error    string
		Saved    bool
		Secrets  []string
	}{"Agent settings", cfg, message, saved, names})
}
