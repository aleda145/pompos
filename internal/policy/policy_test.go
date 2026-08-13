package policy

import (
	"strings"
	"testing"

	"pompos/internal/ingestion"
)

func TestDefaultEngine(t *testing.T) {
	base := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/data.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "./data/pompos.duckdb", Table: "customers"},
	}
	tests := []struct {
		name    string
		mutate  func(*ingestion.Ingestion)
		wantErr string
	}{
		{name: "valid HTTPS"},
		{name: "valid HTTP", mutate: func(i *ingestion.Ingestion) { i.Source.URL = "http://example.com/data.csv" }},
		{name: "invalid scheme", mutate: func(i *ingestion.Ingestion) { i.Source.URL = "ftp://example.com/data.csv" }, wantErr: "HTTP or HTTPS"},
		{name: "empty table", mutate: func(i *ingestion.Ingestion) { i.Destination.Table = "" }, wantErr: "cannot be empty"},
		{name: "unsafe table", mutate: func(i *ingestion.Ingestion) { i.Destination.Table = "customers;drop" }, wantErr: "letters, numbers"},
		{name: "wrong destination path", mutate: func(i *ingestion.Ingestion) { i.Destination.Path = "/tmp/other.duckdb" }, wantErr: "platform configuration"},
	}
	engine := DefaultEngine{DestinationPath: "./data/pompos.duckdb"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := base
			if test.mutate != nil {
				test.mutate(&item)
			}
			err := engine.Validate(item)
			if test.wantErr == "" && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestDefaultEngineGitHub(t *testing.T) {
	base := ingestion.Ingestion{
		Source: ingestion.Source{Type: "github", Owner: "openai", Repository: "codex", Table: "issues"},
		Destination: ingestion.Destination{
			Type: "duckdb", Path: "./data/pompos.duckdb", Table: "openai_codex_issues",
		},
	}
	engine := DefaultEngine{DestinationPath: "./data/pompos.duckdb"}
	if err := engine.Validate(base); err != nil {
		t.Fatalf("valid GitHub ingestion: %v", err)
	}
	base.Source.Table = "secrets"
	if err := engine.Validate(base); err == nil || !strings.Contains(err.Error(), "unsupported GitHub table") {
		t.Fatalf("invalid GitHub table error = %v", err)
	}
}
