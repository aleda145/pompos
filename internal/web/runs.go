package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"pompos/internal/ingestion"
	"pompos/internal/store"
)

type runView struct {
	IngestionID  string
	Runs         []ingestion.Run
	Selected     *ingestion.Run
	Active       bool
	Before       int64
	NextBefore   int64
	LatestID     int64
	LatestStatus string
}

func (a *App) runData(ctx context.Context, id string, before, selected int64) (runView, error) {
	view := runView{IngestionID: id, Before: before}
	runs, err := a.Store.ListRuns(ctx, id, before, 26)
	if err != nil {
		return view, err
	}
	if len(runs) > 25 {
		runs = runs[:25]
		view.NextBefore = runs[24].ID
	}
	view.Runs = runs
	latest := runs
	if before != 0 {
		latest, err = a.Store.ListRuns(ctx, id, 0, 1)
		if err != nil {
			return view, err
		}
	}
	if len(latest) > 0 {
		view.LatestID, view.LatestStatus = latest[0].ID, latest[0].Status
	}
	if selected == 0 && len(runs) > 0 {
		selected = runs[0].ID
	}
	if selected != 0 {
		run, err := a.Store.GetRun(ctx, id, selected)
		if err != nil {
			return view, err
		}
		view.Selected = &run
	}
	view.Active, err = a.Store.HasActiveRuns(ctx, id)
	return view, err
}

func runQuery(r *http.Request) (before, selected int64, err error) {
	for key, target := range map[string]*int64{"before": &before, "run_id": &selected} {
		if value := r.URL.Query().Get(key); value != "" {
			*target, err = strconv.ParseInt(value, 10, 64)
			if err != nil || *target < 1 {
				return 0, 0, fmt.Errorf("invalid %s", key)
			}
		}
	}
	return before, selected, nil
}

func (a *App) ingestionRuns(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	before, selected, err := runQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	if _, err := a.Store.Get(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			a.serverError(w, err)
		}
		return
	}
	view, err := a.runData(r.Context(), id, before, selected)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.templates["detail"].ExecuteTemplate(w, "runs", view); err != nil {
		a.Logger.Printf("render runs: %v", err)
	}
}
