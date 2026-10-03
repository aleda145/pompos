package spec

import (
	"os"
	"strings"
	"testing"
)

func TestCanonicalGolden(t *testing.T) {
	input, err := os.ReadFile("testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/customers.golden.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("canonical spec:\n%s\nwant:\n%s", got, want)
	}
}
func TestSpecRejectsRemovedFieldsAndUnsupportedRuntimes(t *testing.T) {
	input, err := os.ReadFile("testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range [][2]string{
		{"  engine: python", "  engine: external-cli"},
		{"  engine: python", "  implementation: python"},
		{"  orchestrator: direct", "  target: direct"},
		{"  path: data/pompos.duckdb", "  connectionRef: local-duckdb"},
		{"  type: python", "  type: csv"},
		{"  type: python", "  type: github"},
		{"  table: customers", "  format: csv"},
		{"  strategy: replace", "  strategy: replace\n  incrementalKey: id"},
	} {
		t.Run(change[1], func(t *testing.T) {
			if _, err := Parse([]byte(strings.Replace(string(input), change[0], change[1], 1))); err == nil {
				t.Fatal("removed field or runtime accepted")
			}
		})
	}
}
func TestMaterializationAndRuntimeRequirements(t *testing.T) {
	doc, _, err := Read("testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []Materialization{{Strategy: "merge"}, {Strategy: "delete+insert"}, {Strategy: "scd2"}, {Strategy: "merge", PrimaryKey: []string{"id", "id"}}} {
		doc.Materialization = m
		if err := doc.Validate(); err == nil {
			t.Fatalf("invalid materialization accepted: %#v", m)
		}
	}
	doc.Materialization = Materialization{Strategy: "merge", PrimaryKey: []string{"id"}}
	if err := doc.Validate(); err != nil {
		t.Fatal(err)
	}

}
func TestProjectionPreservesPythonSettings(t *testing.T) {
	doc, _, err := Read("testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc.Schedule = &Schedule{Cron: "0 6 * * *", Timezone: "UTC"}
	item := ToProjection(doc, "customers", "customers.yaml", "digest")
	roundtrip := FromIngestion(item)
	if roundtrip.Runtime.Script != doc.Runtime.Script || roundtrip.Runtime.SecretRefs[0] != "source-key" || roundtrip.Destination.Path != doc.Destination.Path || roundtrip.Schedule.Cron != doc.Schedule.Cron {
		t.Fatalf("settings lost: %#v", roundtrip)
	}
}
