package spec

import (
	"reflect"
	"testing"

	"pompos/internal/ingestion"
)

func TestPythonRequirementsRejectInstallerDirectives(t *testing.T) {
	for _, dep := range []string{"-r other.txt", "--index-url=https://other", "pkg\nother", "pkg @ https://other/pkg.whl", "../local", "", "pkg; python_version < '3.12'"} {
		if err := ValidatePythonRuntime("3.12", []string{dep}, nil); err == nil {
			t.Errorf("accepted %q", dep)
		}
	}
	for _, dep := range []string{"psycopg2", "psycopg2-binary==2.9.13", "psycopg[binary]>=3.2,<4", "requests~=2.32"} {
		if err := ValidatePythonRuntime("3.12", []string{dep}, nil); err != nil {
			t.Errorf("rejected %q: %v", dep, err)
		}
	}
	lock := &ingestion.DependencyLock{InputDigest: Digest([]byte("fixture")), Python: "3.12.1", Platform: "linux/amd64", Requirements: "--index-url=https://other\n"}
	if err := ValidatePythonRuntime("", nil, lock); err == nil {
		t.Fatal("lock allowed installer directives")
	}
}

func TestPythonDependencyLockSurvivesProjectionAndYAML(t *testing.T) {
	doc, _, err := Read("testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc.Runtime.Python = "3.12"
	doc.Runtime.Dependencies = []string{"psycopg2-binary"}
	doc.Runtime.DependencyLock = &ingestion.DependencyLock{InputDigest: Digest([]byte("fixture")), Python: "3.12.1", Platform: "linux/amd64", Requirements: "psycopg2-binary==2.9.13\n"}
	projected := FromIngestion(ToProjection(doc, "customers", "customers.yaml", "digest"))
	data, err := Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err := Parse(data)
	if err != nil || !reflect.DeepEqual(doc.Runtime, roundtrip.Runtime) {
		t.Fatalf("runtime changed: %#v %v", roundtrip.Runtime, err)
	}
}
