package ingestr

import (
	"reflect"
	"testing"

	"pompos/internal/ingestion"
)

func TestBuildArgs(t *testing.T) {
	item := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "./data/pompos.duckdb", Table: "customers"},
	}
	want := []string{
		"ingest",
		"--source-uri", "https://example.com/customers.csv",
		"--source-table", "data#csv",
		"--dest-uri", "duckdb://data/pompos.duckdb",
		"--dest-table", "customers",
		"--schema-naming", "direct",
	}
	got, err := BuildArgs(item)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuildArgs() = %#v, want %#v", got, want)
	}
}

func TestBuildArgsAbsoluteDestination(t *testing.T) {
	item := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "/data/pompos.duckdb", Table: "customers"},
	}
	args, err := BuildArgs(item)
	if err != nil {
		t.Fatal(err)
	}
	if args[6] != "duckdb:///data/pompos.duckdb" {
		t.Fatalf("destination URI = %q", args[6])
	}
}

func TestBuildArgsRepoRelativeDestination(t *testing.T) {
	item := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "data/pompos.duckdb", Table: "customers"},
	}
	args, err := BuildArgs(item)
	if err != nil {
		t.Fatal(err)
	}
	if args[6] != "duckdb://data/pompos.duckdb" {
		t.Fatalf("destination URI = %q", args[6])
	}
}
