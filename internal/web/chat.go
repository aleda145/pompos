package web

import (
	"encoding/json"
	"errors"
	"net/http"

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
		http.Redirect(w, r, "/settings", http.StatusSeeOther)
		return
	}
	if id == "" {
		id = newID()
		http.Redirect(w, r, "/chat/"+id, http.StatusSeeOther)
		return
	}
	status, e := a.Agent.ChatStatus(id)
	if errors.Is(e, agent.ErrMCPConversation) {
		http.NotFound(w, r)
		return
	}
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	status.Session = publicSession(status.Session)
	w.Header().Set("Cache-Control", "no-store")
	a.render(w, 200, "chat", struct {
		Title   string
		Session agent.Session
		Status  agent.ChatStatus
	}{"Ingestion chat", status.Session, status})
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
	status, err := a.Agent.StartTurn(r.PathValue("id"), input)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, agent.ErrSessionBusy) {
			code = http.StatusConflict
		} else if errors.Is(err, agent.ErrMCPConversation) {
			code = http.StatusNotFound
		}
		http.Error(w, err.Error(), code)
		return
	}
	status.Session = publicSession(status.Session)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(status)
}

func (a *App) chatStatus(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	status, err := a.Agent.ChatStatus(r.PathValue("id"))
	if errors.Is(err, agent.ErrMCPConversation) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	status.Session = publicSession(status.Session)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
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
