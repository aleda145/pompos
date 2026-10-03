package spec

import (
	"reflect"
	"testing"
)

func TestPythonRequirementsRejectInstallerDirectives(t *testing.T) {
	for _, dep := range []string{"-r other.txt", "--index-url=https://other", "pkg\nother", "pkg @ https://other/pkg.whl", "../local", "", "pkg; python_version < '3.12'"} {
		if err := ValidatePythonRuntime("3.12", []string{dep}, ""); err == nil {
			t.Errorf("accepted %q", dep)
		}
	}
	for _, dep := range []string{"psycopg2", "psycopg2-binary==2.9.13", "psycopg[binary]>=3.2,<4", "requests~=2.32"} {
		if err := ValidatePythonRuntime("3.12", []string{dep}, ""); err != nil {
			t.Errorf("rejected %q: %v", dep, err)
		}
	}
	if err := ValidatePythonRuntime("", nil, "not-a-digest"); err == nil {
		t.Fatal("invalid lock digest accepted")
	}
}

func TestPythonLockDigestSurvivesProjectionAndYAML(t *testing.T) {
	doc, _, err := Read("testdata/customers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc.Runtime.Python = "3.12"
	doc.Runtime.Dependencies = []string{"psycopg2-binary"}
	doc.Runtime.LockDigest = Digest([]byte("native uv lock"))
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
