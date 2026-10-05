package web

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/scheduler"
	"pompos/internal/store"
)

type runtimeQueueRow struct {
	ingestion.Run
	Waiting string
	Reason  string
}

type runtimePageData struct {
	Title          string
	Runtime        scheduler.Runtime
	Queue          []runtimeQueueRow
	Pending        int
	UpdatedAt      time.Time
	Uptime         string
	WorkersValue   string
	WorkersError   string
	WorkersSaved   bool
	History        store.RunHistory
	PreviousPage   int
	NextPage       int
	ingestionNames map[string]string
}

func (v runtimePageData) IngestionName(id string) string {
	if name := v.ingestionNames[id]; name != "" {
		return name
	}
	return id
}

func (a *App) runtimeData(ctx context.Context, page int) (runtimePageData, error) {
	view := runtimePageData{Title: "Runtime", Runtime: a.Scheduler.Runtime(), UpdatedAt: time.Now().UTC()}
	view.Uptime = runtimeElapsed(view.UpdatedAt, view.Runtime.StartedAt)
	view.WorkersValue = strconv.Itoa(view.Runtime.WorkerLimit)
	queue, err := a.Store.RunQueue(ctx)
	if err != nil {
		return view, err
	}
	view.Pending = queue.Total
	active := make(map[string]bool)
	for _, worker := range view.Runtime.Workers {
		if worker.Run.ID != 0 {
			active[worker.Run.IngestionID] = true
		}
	}
	for _, run := range queue.Runs {
		reason := "Waiting for worker"
		if active[run.IngestionID] {
			reason = "Ingestion already running"
		}
		view.Queue = append(view.Queue, runtimeQueueRow{Run: run, Waiting: runtimeElapsed(view.UpdatedAt, run.ScheduledFor), Reason: reason})
	}
	view.History, err = a.Store.RunHistory(ctx, page)
	if err != nil {
		return view, err
	}
	if view.History.Page > 1 {
		view.PreviousPage = view.History.Page - 1
	}
	if view.History.Page < view.History.Pages {
		view.NextPage = view.History.Page + 1
	}
	items, err := a.ingestionList(ctx)
	if err != nil {
		return view, err
	}
	view.ingestionNames = make(map[string]string, len(items))
	for _, item := range items {
		view.ingestionNames[item.ID] = item.Name
	}
	return view, nil
}

func runtimeHistoryPage(r *http.Request) (int, error) {
	value := r.URL.Query().Get("page")
	if value == "" {
		return 1, nil
	}
	page, err := strconv.Atoi(value)
	if err != nil || page < 1 {
		return 0, strconv.ErrSyntax
	}
	return page, nil
}

func runtimeElapsed(now, start time.Time) string {
	if start.IsZero() {
		return "—"
	}
	return max(now.Sub(start), 0).Round(time.Second).String()
}

func (a *App) runtimePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	page, err := runtimeHistoryPage(r)
	if err != nil {
		http.Error(w, "Invalid history page", http.StatusBadRequest)
		return
	}
	view, err := a.runtimeData(r.Context(), page)
	if err != nil {
		a.serverError(w, err)
		return
	}
	view.WorkersSaved = r.URL.Query().Get("saved") == "1"
	a.render(w, http.StatusOK, "runtime", view)
}

func (a *App) runtimeWorkers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid worker setting", http.StatusBadRequest)
		return
	}
	value := strings.TrimSpace(r.PostForm.Get("workers"))
	workers, err := strconv.Atoi(value)
	message := "Worker count must be a positive whole number."
	status := http.StatusUnprocessableEntity
	if err == nil && workers >= 1 {
		if err := a.Scheduler.SetWorkers(r.Context(), workers); err == nil {
			http.Redirect(w, r, "/runtime?saved=1", http.StatusSeeOther)
			return
		} else {
			a.Logger.Printf("change worker count: %v", err)
			message = "Could not save worker count. Try again."
			status = http.StatusInternalServerError
		}
	}
	view, err := a.runtimeData(r.Context(), 1)
	if err != nil {
		a.serverError(w, err)
		return
	}
	view.WorkersValue, view.WorkersError = value, message
	a.render(w, status, "runtime", view)
}

func (a *App) runtimeStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	page, err := runtimeHistoryPage(r)
	if err != nil {
		http.Error(w, "Invalid history page", http.StatusBadRequest)
		return
	}
	view, err := a.runtimeData(r.Context(), page)
	if err != nil {
		a.serverError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.templates["runtime"].ExecuteTemplate(w, "runtime-status", view); err != nil {
		a.Logger.Printf("render runtime: %v", err)
	}
}
