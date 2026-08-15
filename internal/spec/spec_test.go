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
	document, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/customers.golden.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("canonical spec =\n%s\nwant:\n%s", got, want)
	}
}

func TestUnknownFieldFails(t *testing.T) {
	input, _ := os.ReadFile("testdata/customers.yaml")
	input = []byte(strings.Replace(string(input), "  format: csv", "  format: csv\n  surprise: true", 1))
	if _, err := Parse(input); err == nil || !strings.Contains(err.Error(), "field surprise not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestCredentialIsReferenceOnly(t *testing.T) {
	input := []byte(`apiVersion: pompos.dev/v1alpha1
kind: Ingestion
metadata: {name: issues}
source: {type: github, owner: openai, repository: codex, table: issues, credentialRef: github-prod}
destination: {connectionRef: local-duckdb, object: issues}
`)
	document, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "credentialRef: github-prod") {
		t.Fatalf("spec = %s", data)
	}
}
