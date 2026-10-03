package web

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

type previewFunc func(context.Context, compiler.ExecutionPlan) (runnerpython.TablePreview, error)

func (f previewFunc) Preview(ctx context.Context, plan compiler.ExecutionPlan) (runnerpython.TablePreview, error) {
	return f(ctx, plan)
}

func newPreviewTestApp(t *testing.T) (*App, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	destination := filepath.Join(dir, "out.duckdb")
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	script := filepath.Join(dir, "rows.py")
	if err := os.WriteFile(script, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	item := ingestion.Ingestion{ID: "rows", Name: "Rows", Source: ingestion.Source{Type: "python", URL: "fixture", Table: "rows"},
		Destination: ingestion.Destination{Type: "duckdb", Path: destination, Table: "rows"},
		Runtime:     ingestion.Runtime{Engine: "python", Script: script, ScriptDigest: spec.Digest([]byte("fixture"))}}
	path, err := spec.Write(dir, item)
	if err != nil {
		t.Fatal(err)
	}
	item.SpecPath = path
	if err := db.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	app, err := New(App{Store: db, Secrets: db.Secrets(), Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	return app, destination
}

func TestIngestionDetailDoesNotLoadPreview(t *testing.T) {
	app, _ := newPreviewTestApp(t)
	app.Previewer = previewFunc(func(context.Context, compiler.ExecutionPlan) (runnerpython.TablePreview, error) {
		t.Fatal("detail page must not wait for the destination database")
		return runnerpython.TablePreview{}, nil
	})
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/rows", nil))
	if w.Code != 200 {
		t.Fatalf("detail response: %d %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`data-preview-url="/ingestions/rows/preview"`, "Loading preview…", `/static/preview.js`, "Save schedule", "YAML", "Python"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("detail page missing %q", want)
		}
	}
}

func TestIngestionPreviewUsesSavedDestinationAndEscapesValues(t *testing.T) {
	app, destination := newPreviewTestApp(t)
	for _, tc := range []struct {
		name    string
		preview runnerpython.TablePreview
		err     error
		want    string
	}{
		{"rows", runnerpython.TablePreview{Columns: []string{"<script>"}, Rows: [][]string{{"<img src=x onerror=alert(1)>"}}, TotalRows: 1}, nil, "1 / 1 rows"},
		{"more", runnerpython.TablePreview{Columns: []string{"id"}, Rows: [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}, {"10"}}, TotalRows: 42, HasMore: true}, nil, "10 / 42 rows"},
		{"empty", runnerpython.TablePreview{Columns: []string{"id"}}, nil, "No rows."},
		{"truncated", runnerpython.TablePreview{Columns: []string{"id"}, Rows: [][]string{{"value…"}}, TotalRows: 1, Truncated: true}, nil, "Values truncated."},
		{"unavailable", runnerpython.TablePreview{}, errors.New("locked"), "database may be busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app.Previewer = previewFunc(func(_ context.Context, plan compiler.ExecutionPlan) (runnerpython.TablePreview, error) {
				if plan.DestinationPath != destination || plan.DestinationObject != "rows" || plan.Script != filepath.Join(filepath.Dir(destination), "rows.py") {
					t.Fatalf("preview did not use the saved destination and runtime: %#v", plan)
				}
				return tc.preview, tc.err
			})
			w := httptest.NewRecorder()
			app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/rows/preview?sql=DROP+TABLE+rows&table=other&limit=999", nil))
			body := w.Body.String()
			if w.Code != 200 || !strings.Contains(body, tc.want) || strings.Contains(body, "<img src=x") || strings.Contains(body, "<th scope=\"col\"><script>") {
				t.Fatalf("preview response: %d %s", w.Code, body)
			}
			if tc.name == "empty" && !strings.Contains(body, "0 / 0 rows") {
				t.Fatal("empty table count missing")
			}
			if strings.Contains(body, "<!doctype") || strings.Contains(body, "Save schedule") {
				t.Fatal("preview returned the full detail page")
			}
			if strings.Contains(body, "data-preview-retry") != (tc.err != nil) {
				t.Fatal("retry button should appear only when preview fails")
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "text/html; charset=utf-8" {
				t.Fatalf("unexpected preview headers: %v", w.Header())
			}
		})
	}
}

func TestIngestionPreviewUnavailableAndMissing(t *testing.T) {
	app, _ := newPreviewTestApp(t)
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/rows/preview", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Preview unavailable") || !strings.Contains(w.Body.String(), "data-preview-retry") {
		t.Fatalf("missing previewer response: %d %s", w.Code, w.Body.String())
	}
	app.Previewer = previewFunc(func(context.Context, compiler.ExecutionPlan) (runnerpython.TablePreview, error) {
		t.Fatal("must not query a destination for a missing ingestion")
		return runnerpython.TablePreview{}, nil
	})
	w = httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/missing/preview", nil))
	if w.Code != 404 {
		t.Fatalf("missing ingestion response: %d %s", w.Code, w.Body.String())
	}
}

func TestIngestionPreviewUsesRequestCancellationAndTimeout(t *testing.T) {
	app, _ := newPreviewTestApp(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := false
	app.Previewer = previewFunc(func(previewCtx context.Context, _ compiler.ExecutionPlan) (runnerpython.TablePreview, error) {
		called = true
		deadline, ok := previewCtx.Deadline()
		if !ok || time.Until(deadline) > runnerpython.EnvironmentPreparationTimeout+10*time.Second {
			t.Fatal("preview must have a bounded deadline")
		}
		cancel()
		if !errors.Is(previewCtx.Err(), context.Canceled) {
			t.Fatal("request cancellation must cancel the preview")
		}
		return runnerpython.TablePreview{}, previewCtx.Err()
	})
	w := httptest.NewRecorder()
	app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/rows/preview", nil).WithContext(ctx))
	if !called || !strings.Contains(w.Body.String(), "data-preview-retry") {
		t.Fatalf("cancelled preview response: %d %s", w.Code, w.Body.String())
	}
}
