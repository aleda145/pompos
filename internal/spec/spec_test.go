package spec

import (
	"testing"

	"pompos/internal/ingestion"
)

func TestGenerate(t *testing.T) {
	item := ingestion.Ingestion{
		Name:        "customers",
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "./data/pompos.duckdb", Table: "customers"},
	}
	want := `apiVersion: pompos.dev/v1
kind: Ingestion

metadata:
  name: "customers"

source:
  type: csv
  url: "https://example.com/customers.csv"

destination:
  type: duckdb
  table: "customers"
`
	if got := string(Generate(item)); got != want {
		t.Fatalf("Generate() =\n%s\nwant:\n%s", got, want)
	}
}

func TestGenerateGitHubOmitsToken(t *testing.T) {
	item := ingestion.Ingestion{
		Name: "openai/codex · Issues",
		Source: ingestion.Source{
			Type: "github", Owner: "openai", Repository: "codex", AccessToken: "secret", Table: "issues",
		},
		Destination: ingestion.Destination{Type: "duckdb", Path: "./data/pompos.duckdb", Table: "openai_codex_issues"},
	}
	want := `apiVersion: pompos.dev/v1
kind: Ingestion

metadata:
  name: "openai/codex · Issues"

source:
  type: github
  owner: "openai"
  repository: "codex"
  table: "issues"

destination:
  type: duckdb
  table: "openai_codex_issues"
`
	if got := string(Generate(item)); got != want {
		t.Fatalf("Generate() =\n%s\nwant:\n%s", got, want)
	}
}
