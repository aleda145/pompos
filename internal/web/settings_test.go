package web

import (
	"net/url"
	"testing"

	"pompos/internal/agent"
)

func TestManualValidationSettingPersistsWithoutChangingProvider(t *testing.T) {
	app, service, _ := setupFixture(t)
	cfg, err := service.Settings()
	if err != nil || cfg.ManualValidation {
		t.Fatalf("manual validation must default off: %+v %v", cfg, err)
	}
	// The preference can also be changed before provider setup is complete.
	assertRedirect(t, setupRequest(app, "POST", "/settings", url.Values{"manual_validation": {"on"}}), "/settings?saved=1")
	restarted := &agent.Service{Dir: service.Dir}
	cfg, err = restarted.Settings()
	if err != nil || !cfg.ManualValidation {
		t.Fatalf("approval setting lost after restart: %+v %v", cfg, err)
	}
	cfg.Endpoint, cfg.Model, cfg.APIKeyRef = "https://model.example/v1", "test", "provider"
	cfg.ExaAPIKeyRef, cfg.MCPEnabled = "search", true
	if err := service.SaveSettings(cfg); err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		values := url.Values{"section": {"general"}}
		if enabled {
			values.Set("manual_validation", "on")
		}
		assertRedirect(t, setupRequest(app, "POST", "/settings", values), "/settings?saved=1")
		got, err := restarted.Settings()
		cfg.ManualValidation = enabled
		if err != nil || got != cfg {
			t.Fatalf("settings not preserved: got %+v, want %+v: %v", got, cfg, err)
		}
	}
}
