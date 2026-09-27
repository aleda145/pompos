package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"

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
	}{"New ingestion", v})
}
func (a *App) chatTurn(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 20000)
	var input struct {
		Message string `json:"message"`
	}
	if e := json.NewDecoder(r.Body).Decode(&input); e != nil {
		http.Error(w, "Invalid message", 400)
		return
	}
	v, e := a.Agent.Turn(r.Context(), r.PathValue("id"), input.Message)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	message := ""
	if e != nil {
		message = e.Error()
	}
	// System prompts and raw tool arguments are not needed by the conversation UI.
	visible := []agent.Message{}
	for _, m := range v.Messages {
		if m.Role != "system" {
			m.Calls = nil
			visible = append(visible, m)
		}
	}
	v.Messages = visible
	_ = json.NewEncoder(w).Encode(struct {
		Session agent.Session `json:"session"`
		Error   string        `json:"error,omitempty"`
	}{v, message})
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
