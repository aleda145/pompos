package web

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pompos/internal/agent"
	"pompos/internal/compiler"
	"pompos/internal/execution"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/scheduler"
	"pompos/internal/spec"
)

// This exercises the complete repair loop against real Python, dlt and DuckDB.
func TestRepairIngestionEndToEnd(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON to an interpreter with dlt and DuckDB")
	}
	ctx := context.Background()
	app, service, db := setupFixture(t)
	if err := service.SaveSettings(agent.Settings{Mode: "mcp", MCPEnabled: true}); err != nil {
		t.Fatal(err)
	}
	python := runnerpython.Runner{Binary: binary, Secrets: db.Secrets()}
	service.Python = python
	executor, err := execution.New(execution.Service{Store: db, Runner: python, Logger: app.Logger})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := scheduler.New(app.Logger, db, executor.Run)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	app.Scheduler = manager
	dest, err := db.GetDestination(ctx, "local-duckdb")
	if err != nil {
		t.Fatal(err)
	}
	item := ingestion.Ingestion{ID: "local-duckdb/raw/customers", Name: "Customers",
		Source:          ingestion.Source{Type: "python", URL: "fixture://customers", Table: "customers"},
		Destination:     ingestion.Destination{Type: dest.Type, Path: dest.Path, Schema: "raw", Table: "customers"},
		Runtime:         ingestion.Runtime{Engine: "python", Script: spec.ArtifactPath(app.SpecDir, "local-duckdb/raw/customers", ".py")},
		Materialization: ingestion.Materialization{Strategy: "replace"}}
	broken := runnerpython.Wrap("def fetch(secret, limit):\n    raise ValueError('source field changed')\n    yield {}\n")
	if err := agent.WriteFile(item.Runtime.Script, []byte(broken)); err != nil {
		t.Fatal(err)
	}
	item.SpecPath, err = spec.Write(app.SpecDir, item)
	if err != nil {
		t.Fatal(err)
	}
	_, data, err := spec.Read(item.SpecPath)
	if err != nil {
		t.Fatal(err)
	}
	item.SpecDigest = spec.Digest(data)
	if err := db.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	runQueued := func(wantFailure bool) ingestion.Run {
		t.Helper()
		run, ok, err := db.ClaimRun(ctx, time.Now(), time.Now().Add(-time.Hour))
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		runErr := executor.Run(ctx, run)
		if (runErr != nil) != wantFailure {
			t.Fatalf("run error: %v", runErr)
		}
		message := ""
		if runErr != nil {
			message = runErr.Error()
		}
		if err := db.FinishRun(ctx, run.ID, message); err != nil {
			t.Fatal(err)
		}
		return run
	}
	if err := app.queueIngestion(ctx, item.ID); err != nil {
		t.Fatal(err)
	}
	failedRun := runQueued(true)
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "repair-test", Version: "1"}, nil)
	connection, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	call := func(name string, args any, result any) {
		t.Helper()
		response, err := connection.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(name, err)
		}
		if response.IsError {
			data, _ := json.Marshal(response.Content)
			t.Fatalf("%s: %s", name, data)
		}
		if result != nil {
			data, err := json.Marshal(response.StructuredContent)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, result); err != nil {
				t.Fatal(err)
			}
		}
	}
	var opened struct {
		Session agent.Session `json:"session"`
	}
	call("edit_ingestion", map[string]any{"ingestion_id": item.ID, "run_id": failedRun.ID}, &opened)
	v := opened.Session
	if v.Edit == nil || v.Edit.Run.ID != failedRun.ID || !strings.Contains(v.Edit.Run.Log, "source field changed") {
		t.Fatalf("missing failure context: %+v", v.Edit)
	}
	draft := *v.Draft
	draft.Code = "def fetch(secret, limit):\n    yield {'id': 1, 'name': 'Ada'}\n"
	encoded, _ := json.Marshal(draft)
	args := map[string]any{}
	json.Unmarshal(encoded, &args)
	args["session_id"] = v.ID
	// Tool schema accepts extractor fields, not internal draft state.
	delete(args, "schedule")
	args["primary_key"], args["secret_refs"] = []string{}, []string{}
	call("write_script", args, nil)
	call("test_script", map[string]any{"session_id": v.ID}, nil)
	call("validate_ingestion", map[string]any{"session_id": v.ID, "limit": 5}, nil)
	unchanged, _ := os.ReadFile(item.Runtime.Script)
	if string(unchanged) != broken {
		t.Fatal("validation changed production code")
	}
	var review agent.EditReview
	call("review_ingestion_changes", map[string]any{"session_id": v.ID}, &review)
	if !strings.Contains(review.AfterPython, "Ada") || review.Validation.Result.SecondLoadRows != 1 {
		t.Fatalf("invalid review: %+v", review)
	}
	call("apply_ingestion_changes", map[string]any{"session_id": v.ID, "fingerprint": review.Fingerprint}, nil)
	items, err := db.List(ctx)
	if err != nil || len(items) != 1 || items[0].ID != item.ID {
		t.Fatal("repair replaced ingestion identity", err)
	}
	call("run_ingestion", map[string]any{"ingestion_id": item.ID}, nil)
	runQueued(false)
	runs, err := db.ListRuns(ctx, item.ID, 0, 10)
	if err != nil || len(runs) != 2 || runs[0].Status != "succeeded" || runs[1].Status != "failed" {
		t.Fatalf("run history: %+v %v", runs, err)
	}
	preview, err := python.Preview(ctx, compiler.ExecutionPlan{DestinationType: "duckdb", DestinationPath: dest.Path, DestinationSchema: "raw", DestinationObject: "customers"})
	if err != nil || preview.TotalRows != 1 {
		t.Fatalf("preview: %+v %v", preview, err)
	}
	if filepath.Base(item.Runtime.Script) != "customers.py" {
		t.Fatal("unexpected artifact identity")
	}
}
