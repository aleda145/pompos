package agent

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPAccessDefaultsOffAndPreservesProviderSettings(t *testing.T) {
	s := &Service{Dir: t.TempDir()}
	cfg, err := s.Settings()
	if err != nil || cfg.MCPEnabled {
		t.Fatal("new configuration enabled MCP", err)
	}
	// Older configurations never implicitly enable the newly introduced flag.
	if err := WriteFile(filepath.Join(s.Dir, "settings.json"), []byte(`{"mode":"mcp","endpoint":"https://model.example/v1","model":"kept","api_key_ref":"model-key","exa_api_key_ref":"search-key"}`)); err != nil {
		t.Fatal(err)
	}
	before, err := s.Settings()
	if err != nil || before.MCPEnabled {
		t.Fatal("legacy configuration enabled MCP", err)
	}
	for _, enabled := range []bool{true, false} {
		if err := s.SetMCPEnabled(enabled); err != nil {
			t.Fatal(err)
		}
		restarted := &Service{Dir: s.Dir}
		after, err := restarted.Settings()
		if err != nil || after.MCPEnabled != enabled {
			t.Fatal("switch did not persist", err)
		}
		after.MCPEnabled = before.MCPEnabled
		if after != before {
			t.Fatal("MCP switch changed unrelated settings")
		}
	}
}

func TestMCPDirectValidationResumesLegacyPendingDraft(t *testing.T) {
	ctx := context.Background()
	s, runner, v := validationFixture(t)
	v.External = true // Simulate a draft from the previous MCP proposal workflow.
	if err := s.save(v); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"propose_validation", "propose_loading", "ask_user", "respond", "finish"} {
		if _, err := s.MCPCall(ctx, v.ID, name, json.RawMessage(`{}`)); err == nil {
			t.Fatalf("removed tool %s was callable", name)
		}
	}
	if runner.calls != 0 {
		t.Fatal("removed tools executed validation")
	}
	if _, err := s.MCPCall(ctx, v.ID, "configure_loading", json.RawMessage(`{"cron":"","strategy":"merge","primary_key":["id"]}`)); err != nil {
		t.Fatal(err)
	}
	updated, err := s.Load(v.ID)
	if err != nil || updated.Pending != nil || updated.Loading.Strategy != "merge" || updated.Ready {
		t.Fatalf("legacy handoff was not superseded by direct settings: %v", err)
	}
	if runner.calls != 0 {
		t.Fatal("configuring loading ran validation")
	}
	if _, err := s.MCPCall(ctx, v.ID, "validate_ingestion", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	updated, err = s.Load(v.ID)
	if err != nil || !updated.Ready || updated.Validation == nil || updated.Pending != nil || runner.limit != 0 || runner.calls != 1 {
		t.Fatalf("direct default validation did not make the draft ready: %v", err)
	}
	if !strings.HasPrefix(updated.Messages[len(updated.Messages)-1].Content, "POMPOS_VALIDATION_RESULT=") {
		t.Fatal("optional web review lost the validation preview")
	}
}

func TestMCPDirectToolsCannotBypassWebApprovals(t *testing.T) {
	s, runner, v := validationFixture(t)
	for _, name := range []string{"configure_loading", "validate_ingestion"} {
		if _, err := s.MCPCall(context.Background(), v.ID, name, json.RawMessage(`{}`)); err == nil {
			t.Fatalf("MCP tool %s accepted a built-in agent session", name)
		}
	}
	updated, err := s.Load(v.ID)
	if err != nil || updated.Pending == nil || updated.Pending.ID != v.Pending.ID || runner.calls != 0 {
		t.Fatal("MCP bypassed the built-in agent's pending approval")
	}
}
