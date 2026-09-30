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

func TestIngestionPreviewUsesSavedDestinationAndEscapesValues(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	destination := filepath.Join(dir, "out.duckdb")
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	for _, tc := range []struct {
		name    string
		preview runnerpython.TablePreview
		err     error
		want    string
	}{
		{"rows", runnerpython.TablePreview{Columns: []string{"<script>"}, Rows: [][]string{{"<img src=x onerror=alert(1)>"}}, TotalRows: 1}, nil, "1 / 1 rows"},
		{"more", runnerpython.TablePreview{Columns: []string{"id"}, Rows: [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}, {"10"}}, TotalRows: 42, HasMore: true}, nil, "10 / 42 rows"},
		{"empty", runnerpython.TablePreview{Columns: []string{"id"}}, nil, "No rows."},
		{"unavailable", runnerpython.TablePreview{}, errors.New("locked"), "database may be busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app.Previewer = previewFunc(func(_ context.Context, plan compiler.ExecutionPlan) (runnerpython.TablePreview, error) {
				if plan.DestinationPath != destination || plan.DestinationObject != "rows" || plan.Script != "" {
					t.Fatalf("preview used request parameters or an extractor: %#v", plan)
				}
				return tc.preview, tc.err
			})
			w := httptest.NewRecorder()
			app.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/ingestions/rows?sql=DROP+TABLE+rows&table=other&limit=999", nil))
			body := w.Body.String()
			if w.Code != 200 || !strings.Contains(body, tc.want) || strings.Contains(body, "<img src=x") || strings.Contains(body, "<th scope=\"col\"><script>") {
				t.Fatalf("preview response: %d %s", w.Code, body)
			}
			if tc.name == "empty" && !strings.Contains(body, "0 / 0 rows") {
				t.Fatal("empty table count missing")
			}
		})
	}
}
