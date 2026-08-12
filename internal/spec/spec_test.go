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
