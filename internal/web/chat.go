package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"pompos/internal/agent"
)

func (a *App) listChats(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", http.StatusServiceUnavailable)
		return
	}
	sessions, err := a.Agent.ListSessions()
	if err != nil {
		a.serverError(w, err)
		return
	}
	webChats := make([]agent.SessionSummary, 0, len(sessions))
	for _, session := range sessions {
		if !session.External {
			webChats = append(webChats, session)
		}
	}
	a.render(w, http.StatusOK, "chats", struct {
		Title string
		Chats []agent.SessionSummary
	}{"Chats", webChats})
}

func (a *App) chatPage(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	cfg, err := a.Agent.Settings()
	if err != nil {
		a.serverError(w, err)
		return
	}
	id := r.PathValue("id")
	if cfg.ValidateAgent() != nil {
		http.Redirect(w, r, "/settings/agent", http.StatusSeeOther)
		return
	}
	if id == "" {
		id = newID()
		http.Redirect(w, r, "/chat/"+id, http.StatusSeeOther)
		return
	}
	v, e := a.Agent.LoadWebChat(id)
	if errors.Is(e, agent.ErrMCPConversation) {
		http.NotFound(w, r)
		return
	}
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	a.render(w, 200, "chat", struct {
		Title   string
		Session agent.Session
	}{"Ingestion chat", publicSession(v)})
}
func (a *App) chatTurn(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	if !a.requireWebChat(w, r) {
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
	if v.Loading == nil || v.Validation == nil {
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
	if !a.requireWebChat(w, r) {
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
	if !a.requireWebChat(w, r) {
		return
	}
	id := r.PathValue("id")
	_, e := a.publishSession(r.Context(), id)
	if e != nil {
		http.Error(w, e.Error(), 422)
		return
	}
	http.Redirect(w, r, "/chat/"+id, http.StatusSeeOther)
}

func (a *App) requireWebChat(w http.ResponseWriter, r *http.Request) bool {
	_, err := a.Agent.LoadWebChat(r.PathValue("id"))
	if errors.Is(err, agent.ErrMCPConversation) {
		http.NotFound(w, r)
		return false
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
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
		switch r.FormValue("section") {
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
	a.render(w, 200, "agent-settings", struct {
		Title       string
		Settings    agent.Settings
		Error       string
		Saved       bool
		Secrets     []string
		MCPEndpoint string
	}{"Agent settings", cfg, message, saved, names, mcpEndpoint(r.Host)})
}
