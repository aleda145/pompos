package agent

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"pompos/internal/ingestion"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestSchemaContextIncludesIngestionsFromOtherChatsBeforeLoading(t *testing.T) {
	ctx := context.Background()
	s, _, _ := validationFixture(t)
	dest, err := s.Destinations.GetDestination(ctx, "local-duckdb")
	if err != nil {
		t.Fatal(err)
	}
	item := ingestion.Ingestion{
		ID: "local-duckdb/election2022/results", Name: "Election 2022 results", Status: ingestion.StatusPending,
		Source:          ingestion.Source{Type: "python", URL: "https://example.com/election/2022/results", Table: "results"},
		Destination:     ingestion.Destination{Type: "duckdb", Path: dest.Path, Schema: "election2022", Table: "results"},
		Materialization: ingestion.Materialization{Strategy: "replace"},
		Runtime:         ingestion.Runtime{Engine: "python", Orchestrator: "direct", Script: filepath.Join(t.TempDir(), "results.py")},
	}
	item.SpecPath, err = spec.Write(t.TempDir(), item)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Destinations.(*store.SQLite).Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	chat, err := s.NewMCPChat("Add turnout for the 2022 election")
	if err != nil {
		t.Fatal(err)
	}
	check := func(output string, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Existing []existingIngestion `json:"existing_ingestions"`
			Saved    []SavedIngestion    `json:"saved_ingestions"`
		}
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Existing) != 1 || len(result.Saved) != 0 {
			t.Fatalf("missing cross-chat context: %s", output)
		}
		got := result.Existing[0]
		if got.Schema != "election2022" || got.Table != "results" || got.Destination != dest.Name || got.Source != item.Source.URL || got.Name != item.Name || got.LoadError != "" {
			t.Fatalf("incorrect grouping context: %#v", got)
		}
	}
	check(s.MCPCall(ctx, chat.ID, "context", json.RawMessage(`{}`)))
	call := Call{}
	call.Function.Name = "context"
	check(s.execute(ctx, &Session{ID: "new-web-chat"}, call))
	// Inspection of a not-yet-loaded destination must still work before a draft.
	output, err := s.MCPCall(ctx, chat.ID, "inspect_destination", json.RawMessage(`{"destination":"local-duckdb"}`))
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Exists bool `json:"exists"`
	}
	if err := json.Unmarshal([]byte(output), &catalog); err != nil || catalog.Exists {
		t.Fatalf("unloaded destination: %s, %v", output, err)
	}
	if _, err := s.MCPCall(ctx, chat.ID, "inspect_destination", json.RawMessage(`{"destination":"unknown"}`)); err == nil {
		t.Fatal("unconfigured destination accepted")
	}
}
