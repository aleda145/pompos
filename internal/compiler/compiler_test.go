package compiler

import (
	"os"
	"pompos/internal/spec"
	"strings"
	"testing"
)

func TestPlanGoldenAndDefaults(t *testing.T) {
	doc, _, err := spec.Read("../spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc.Materialization.Strategy = ""
	plan, err := Compile(doc)
	if err != nil {
		t.Fatal(err)
	}
	first, err := MarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := MarshalPlan(plan)
	if string(first) != string(second) {
		t.Fatal("nondeterministic plan")
	}
	want, err := os.ReadFile("testdata/customers.plan.golden.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(want) {
		t.Fatalf("plan:\n%s\nwant:\n%s", first, want)
	}
}
func TestPlanUsesExplicitDestinationAndManagedSecretReferences(t *testing.T) {
	doc, _, err := spec.Read("../spec/testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc.Destination.Path = "data/warehouse.duckdb"
	doc.Destination.Schema = "raw"
	doc.Materialization = spec.Materialization{Strategy: "merge", PrimaryKey: []string{"account_id", "id"}}
	plan, err := Compile(doc)
	if err != nil {
		t.Fatal(err)
	}
	if plan.DestinationSchema != "raw" || plan.DestinationPath != doc.Destination.Path || plan.Strategy != "merge" || len(plan.PrimaryKey) != 2 || len(plan.SecretRefs) != 1 || plan.SecretRefs[0] != "source-key" {
		t.Fatalf("plan: %#v", plan)
	}
	doc.Destination.Path = ""
	if _, err := Compile(doc); err == nil || !strings.Contains(err.Error(), "destination.path") {
		t.Fatalf("missing destination accepted: %v", err)
	}
}
