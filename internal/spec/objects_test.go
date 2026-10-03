package spec

import (
	"reflect"
	"testing"
)

func TestObjectsSpecRoundTripAndPolicies(t *testing.T) {
	doc := Ingestion{APIVersion: APIVersion, Kind: Kind, Data: "files", Metadata: Metadata{Name: "Reports"},
		Source:          Source{Type: "python", URL: "https://example.com/reports", Collection: "annual_reports"},
		Destination:     Destination{Type: "objects", Path: "data/files", Schema: "company_reports", Object: "objects"},
		Materialization: Materialization{Strategy: "update"}, Runtime: Runtime{Engine: "python", Script: "reports.py"}}
	data, err := Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(data)
	if err != nil || !reflect.DeepEqual(parsed, doc) {
		t.Fatalf("file YAML roundtrip: %#v, %v", parsed, err)
	}
	projection := ToProjection(doc, "local/company_reports/objects", "reports.yaml", Digest(data))
	if got := FromIngestion(projection); !reflect.DeepEqual(got, doc) {
		t.Fatalf("projection lost object settings: %#v", got)
	}
	for _, strategy := range []string{"", "update", "skip"} {
		copy := doc
		copy.Materialization.Strategy = strategy
		if err := copy.Validate(); err != nil {
			t.Fatal(err)
		}
		if strategy == "" && copy.Strategy() != "update" {
			t.Fatal("wrong default file strategy")
		}
	}
	for _, change := range []func(*Ingestion){
		func(s *Ingestion) { s.Materialization.Strategy = "replace" },
		func(s *Ingestion) { s.Materialization.PrimaryKey = []string{"object_id"} },
		func(s *Ingestion) { s.Data = "rows" },
		func(s *Ingestion) { s.Source.Table = "reports" },
		func(s *Ingestion) { s.Source.Collection = "" },
		func(s *Ingestion) { s.Destination.Schema = "" },
		func(s *Ingestion) { s.Destination.Schema = "main" },
		func(s *Ingestion) { s.Destination.Schema = "MAIN" },
		func(s *Ingestion) { s.Destination.Schema = "pg_catalog" },
		func(s *Ingestion) { s.Destination.Object = "reports" },
		func(s *Ingestion) { s.Destination.Type = "duckdb" },
	} {
		copy := doc
		change(&copy)
		if err := copy.Validate(); err == nil {
			t.Fatalf("invalid objects spec accepted: %#v", copy)
		}
	}
}
