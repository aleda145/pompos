package web

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"pompos/internal/agent"
	"pompos/internal/ingestion"
	"pompos/internal/spec"
)

func (a *App) startEdit(ctx context.Context, id string, runID int64, external bool) (agent.Session, error) {
	a.publicationMu.Lock()
	defer a.publicationMu.Unlock()
	item, err := a.Store.Get(ctx, id)
	if err != nil {
		return agent.Session{}, err
	}
	var selected *ingestion.Run
	if runID == 0 {
		runs, err := a.Store.ListRuns(ctx, id, 0, 1)
		if err != nil {
			return agent.Session{}, err
		}
		if len(runs) > 0 {
			runID = runs[0].ID
		}
	}
	if runID != 0 {
		run, err := a.Store.GetRun(ctx, id, runID)
		if err != nil {
			return agent.Session{}, err
		}
		selected = &run
	}
	return a.Agent.StartEdit(ctx, item, selected, external)
}

func (a *App) editIngestion(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	cfg, err := a.Agent.Settings()
	if err != nil {
		a.serverError(w, err)
		return
	}
	if cfg.ValidateAgent() != nil {
		http.Redirect(w, r, "/settings", http.StatusSeeOther)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid edit request", 400)
		return
	}
	var runID int64
	if value := r.FormValue("run_id"); value != "" {
		runID, err = strconv.ParseInt(value, 10, 64)
		if err != nil || runID < 1 {
			http.Error(w, "Invalid run ID", 400)
			return
		}
	}
	v, err := a.startEdit(r.Context(), r.PathValue("id"), runID, false)
	if err != nil {
		item, loadErr := a.getIngestion(r.Context(), r.PathValue("id"))
		if loadErr != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		page := a.detailData(r, item)
		page.RunError = err.Error()
		a.render(w, http.StatusUnprocessableEntity, "detail", page)
		return
	}
	http.Redirect(w, r, "/chat/"+v.ID, http.StatusSeeOther)
}

func (a *App) reviewEdit(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	if !a.requireWebChat(w, r) {
		return
	}
	a.renderEditReview(w, r, "")
}

func (a *App) renderEditReview(w http.ResponseWriter, r *http.Request, message string) {
	id := r.PathValue("id")
	page := struct {
		Title, SessionID, IngestionID, Error string
		Review                               *agent.EditReview
	}{Title: "Review changes", SessionID: id, Error: message}
	v, err := a.Agent.Load(id)
	if err == nil && v.Edit != nil {
		page.IngestionID = v.Edit.IngestionID
	}
	review, err := a.Agent.ReviewEdit(r.Context(), id)
	if err == nil {
		page.Review = &review
	} else {
		page.Error = err.Error()
	}
	status := http.StatusOK
	if page.Error != "" {
		status = http.StatusUnprocessableEntity
	}
	w.Header().Set("Cache-Control", "no-store")
	a.render(w, status, "edit_review", page)
}

func (a *App) applySessionEdit(ctx context.Context, id, fingerprint string) (string, error) {
	a.publicationMu.Lock()
	defer a.publicationMu.Unlock()
	v, err := a.Agent.Load(id)
	if err != nil {
		return "", err
	}
	if v.Edit == nil {
		return "", errors.New("this conversation is not editing an ingestion")
	}
	// A repeated application does not touch files or need an idle scheduler.
	if v.PublishedID != "" && fingerprint != "" && v.Edit.AppliedReview == fingerprint {
		return v.PublishedID, nil
	}
	guard, ok := a.Scheduler.(interface{ WithPublication(func() error) error })
	if !ok {
		return "", errors.New("scheduler does not support applying edits")
	}
	var ingestionID string
	err = guard.WithPublication(func() error {
		active, err := a.Store.HasActiveRuns(ctx, v.Edit.IngestionID)
		if err != nil {
			return err
		}
		if active {
			return errors.New("this ingestion has queued or running work; wait for it to finish and apply again")
		}
		ingestionID, err = a.Agent.ApplyEdit(ctx, id, fingerprint, func(id string, doc spec.Ingestion) error {
			data, err := spec.Marshal(doc)
			if err != nil {
				return err
			}
			if err := a.Store.UpdateSpecReference(ctx, id, v.Edit.SpecPath, spec.Digest(data)); err != nil {
				return err
			}
			return a.Scheduler.Upsert(spec.ToProjection(doc, id, v.Edit.SpecPath, spec.Digest(data)))
		})
		return err
	})
	return ingestionID, err
}

func (a *App) applyEdit(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", 503)
		return
	}
	if !a.requireWebChat(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid review", 400)
		return
	}
	id, err := a.applySessionEdit(r.Context(), r.PathValue("id"), r.FormValue("fingerprint"))
	if err != nil {
		a.renderEditReview(w, r, err.Error())
		return
	}
	http.Redirect(w, r, "/ingestions/"+id, http.StatusSeeOther)
}
