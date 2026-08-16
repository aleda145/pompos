package compiler

import (
	"os"
	"strings"
	"testing"

	"pompos/internal/spec"
)

func TestPlanGoldenAndDefaults(t *testing.T) {
	input, err := os.ReadFile("../spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document, err := spec.Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	document.Materialization.Strategy = ""
	document.Runtime = spec.Runtime{}
	plan, err := Compile(document, LocalDuckDB("data/pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := MarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := MarshalPlan(plan)
	if string(first) != string(second) {
		t.Fatal("plan serialization is not deterministic")
	}
	want, err := os.ReadFile("testdata/customers.plan.golden.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(want) {
		t.Fatalf("plan =\n%s\nwant:\n%s", first, want)
	}
}

func TestPolicyErrorNamesRule(t *testing.T) {
	document, _, err := spec.Read("../spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document.Destination.Type, document.Destination.Path = "", ""
	document.Destination.ConnectionRef = "production"
	_, err = Compile(document, LocalDuckDB("data/pompos.duckdb"))
	if err == nil || !strings.Contains(err.Error(), "policy.destination-connection") {
		t.Fatalf("error = %v", err)
	}
}

func TestPlanUsesInlineDestination(t *testing.T) {
	document, _, err := spec.Read("../spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document.Destination.Path = "data/warehouse.duckdb"
	plan, err := Compile(document, LocalDuckDB("data/default.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	if plan.DestinationURI != "duckdb://data/warehouse.duckdb" {
		t.Fatalf("destination URI = %q", plan.DestinationURI)
	}
}

func TestPlanIncludesLoadingKeys(t *testing.T) {
	document, _, err := spec.Read("../spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	document.Materialization = spec.Materialization{Strategy: "merge", PrimaryKey: []string{"account_id", "id"}, IncrementalKey: "updated_at"}
	plan, err := Compile(document, LocalDuckDB("data/default.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Strategy != "merge" || len(plan.PrimaryKey) != 2 || plan.PrimaryKey[0] != "account_id" || plan.IncrementalKey != "updated_at" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestPlanContainsOnlyCredentialReference(t *testing.T) {
	document := spec.Ingestion{APIVersion: spec.APIVersion, Kind: spec.Kind, Metadata: spec.Metadata{Name: "issues"},
		Source:      spec.Source{Type: "github", Owner: "openai", Repository: "codex", Table: "issues", CredentialRef: "github-prod"},
		Destination: spec.Destination{ConnectionRef: "local-duckdb", Object: "issues"}}
	plan, err := Compile(document, LocalDuckDB("data/pompos.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := MarshalPlan(plan)
	if !strings.Contains(string(data), "credentialRef: github-prod") {
		t.Fatalf("plan = %s", data)
	}
}
