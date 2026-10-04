package web

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/store"
)

func TestRunDataTracksQueueRunningAndSuccess(t *testing.T) {
	ctx := context.Background()
	app, _ := newPreviewTestApp(t)
	db := app.Store.(*store.SQLite)
	view, err := app.runData(ctx, "rows", 0, 0)
	if err != nil || view.Selected != nil || view.Active {
		t.Fatalf("initial = %+v, %v", view, err)
	}
	now := time.Now().UTC()
	if err := db.EnqueueRun(ctx, "rows", now); err != nil {
		t.Fatal(err)
	}
	view, err = app.runData(ctx, "rows", 0, 0)
	if err != nil || !view.Active || view.Selected == nil || view.Selected.Status != ingestion.StatusPending {
		t.Fatalf("queued = %+v, %v", view, err)
	}
	queued, ok, err := db.ClaimRun(ctx, now, now.Add(-time.Hour))
	if err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if err := db.AppendRunLog(ctx, queued.ID, "Live output\n"); err != nil {
		t.Fatal(err)
	}
	view, err = app.runData(ctx, "rows", 0, 0)
	if err != nil || !view.Active || view.Selected.Status != ingestion.StatusRunning || view.Selected.Log != "Live output\n" || view.Selected.StartedAt == nil {
		t.Fatalf("running = %+v, %v", view, err)
	}
	if err := db.FinishRun(ctx, queued.ID, ""); err != nil {
		t.Fatal(err)
	}
	view, err = app.runData(ctx, "rows", 0, 0)
	if err != nil || view.Active || view.Selected.Status != ingestion.StatusSucceeded || view.Selected.FinishedAt == nil {
		t.Fatalf("success = %+v, %v", view, err)
	}
	for i := 1; i <= 26; i++ {
		if err := db.EnqueueRun(ctx, "rows", now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	view, err = app.runData(ctx, "rows", 0, queued.ID)
	if err != nil || len(view.Runs) != 25 || view.NextBefore == 0 || view.Selected.ID != queued.ID || !view.Active || view.LatestStatus != ingestion.StatusPending {
		t.Fatalf("selected history = %+v, %v", view, err)
	}
	older, err := app.runData(ctx, "rows", view.NextBefore, 0)
	if err != nil || len(older.Runs) != 2 || older.NextBefore != 0 || older.LatestID != view.LatestID {
		t.Fatalf("older = %+v, %v", older, err)
	}
	if _, err := app.runData(ctx, "other", 0, queued.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-ingestion run = %v", err)
	}
}

func TestRunPollingRoutesAndInvalidCursors(t *testing.T) {
	app, _ := newPreviewTestApp(t)
	if err := app.Store.Create(context.Background(), ingestion.Ingestion{ID: "local-duckdb/main/test", Status: ingestion.StatusPending}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path   string
		status int
	}{
		{"/ingestions/rows/runs", http.StatusOK},
		{"/ingestions/local-duckdb/main/test/runs", http.StatusOK},
		{"/ingestions/missing/runs", http.StatusNotFound},
		{"/ingestions/rows/runs?run_id=99999", http.StatusNotFound},
		{"/ingestions/rows/runs?before=-1", http.StatusBadRequest},
		{"/ingestions/rows/runs?run_id=no", http.StatusBadRequest},
	} {
		response := httptest.NewRecorder()
		app.Handler().ServeHTTP(response, httptest.NewRequest("GET", test.path, nil))
		if response.Code != test.status || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: status=%d, headers=%v", test.path, response.Code, response.Header())
		}
	}
}
