package web

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestBrokenIngestionIsVisibleAndDoesNotBlockOthers(t *testing.T) {
	for _, failure := range []string{"missing", "malformed", "invalid spec", "invalid cron"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			app, _ := newPreviewTestApp(t)
			metadata := app.Store.(*store.SQLite)
			healthy, err := app.getIngestion(ctx, "rows")
			if err != nil {
				t.Fatal(err)
			}
			broken := healthy
			broken.ID, broken.Name = "broken", "Broken"
			path, err := spec.Write(t.TempDir(), broken)
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			broken.SpecPath, broken.SpecDigest = path, spec.Digest(original)
			if err := metadata.Create(ctx, broken); err != nil {
				t.Fatal(err)
			}
			if err := metadata.Finish(ctx, broken.ID, ingestion.StatusFailed, "previous run failed"); err != nil {
				t.Fatal(err)
			}
			next := time.Now().Add(time.Hour)
			if err := metadata.UpdateNextRun(ctx, broken.ID, &next); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "missing":
				err = os.Remove(path)
			case "malformed":
				err = os.WriteFile(path, []byte("metadata: ["), 0600)
			case "invalid spec":
				err = os.WriteFile(path, []byte("apiVersion: unsupported"), 0600)
			case "invalid cron":
				broken.Schedule = "not a cron"
				_, err = spec.Write(filepath.Dir(path), broken)
			}
			if err != nil {
				t.Fatal(err)
			}
			items, err := app.ingestionList(ctx)
			if err != nil || len(items) != 2 || items[0].ID != "broken" || items[0].LoadError == "" || items[0].NextRun != nil || items[1].LoadError != "" {
				t.Fatalf("list = %#v, error = %v", items, err)
			}
			item, err := app.getIngestion(ctx, "broken")
			if err != nil || item.LoadError == "" || item.LastError != "previous run failed" {
				t.Fatalf("get = %#v, error = %v", item, err)
			}
			for _, route := range []string{"/", "/secrets", "/ingestions/rows", "/ingestions/broken", "/ingestions/broken/preview"} {
				w := httptest.NewRecorder()
				app.Handler().ServeHTTP(w, httptest.NewRequest("GET", route, nil))
				if w.Code != 200 {
					t.Fatalf("GET %s: %d %s", route, w.Code, w.Body.String())
				}
				if route == "/" && (!strings.Contains(w.Body.String(), "status-broken") || !strings.Contains(w.Body.String(), "/ingestions/rows")) {
					t.Fatal("home must show both the broken and healthy ingestion")
				}
				if route == "/ingestions/broken" && (!strings.Contains(w.Body.String(), path) || !strings.Contains(w.Body.String(), "configuration unavailable") || strings.Contains(w.Body.String(), "Next run:")) {
					t.Fatalf("broken detail missing diagnostic or showing runnable schedule: %s", w.Body.String())
				}
			}
			before, readErr := os.ReadFile(path)
			w := httptest.NewRecorder()
			app.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/ingestions/broken/schedule", nil))
			if w.Code != 422 {
				t.Fatalf("schedule status = %d", w.Code)
			}
			after, afterErr := os.ReadFile(path)
			if string(before) != string(after) || (readErr != nil) != (afterErr != nil) {
				t.Fatal("schedule update overwrote a broken or missing YAML")
			}
			w = httptest.NewRecorder()
			app.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/ingestions/broken/run", nil))
			if w.Code != 422 || !strings.Contains(w.Body.String(), "configuration unavailable") {
				t.Fatalf("run status = %d: %s", w.Code, w.Body.String())
			}
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			item, err = app.getIngestion(ctx, "broken")
			if err != nil || item.LoadError != "" || item.Name != "Broken" || item.Status != ingestion.StatusFailed || item.LastError != "previous run failed" {
				t.Fatalf("restored = %#v, error = %v", item, err)
			}
		})
	}
}

func TestMissingPythonScriptDoesNotBreakDetail(t *testing.T) {
	app, _ := newPreviewTestApp(t)
	item, err := app.getIngestion(context.Background(), "rows")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(item.Runtime.Script); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/rows", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Python unavailable") || !strings.Contains(w.Body.String(), item.Runtime.Script) {
		t.Fatalf("detail = %d %s", w.Code, w.Body.String())
	}
}
