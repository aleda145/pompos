package python

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/compiler"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestProbeRedactsSecretsAndRejectsChangedScript(t *testing.T) {
	binary, e := exec.LookPath("python3")
	if e != nil {
		t.Skip("python3 unavailable")
	}
	dir := t.TempDir()
	db, e := store.Open(context.Background(), filepath.Join(dir, "metadata.sqlite"), "")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.Secrets().Put(context.Background(), "token", []byte("private-token")); e != nil {
		t.Fatal(e)
	}
	data := []byte(Wrap("def fetch(secret, limit):\n    yield {'id': 1, 'value': secret('token')}\n"))
	path := filepath.Join(dir, "test.py")
	os.WriteFile(path, data, 0600)
	plan := compiler.ExecutionPlan{Script: path, ScriptDigest: spec.Digest(data), SecretRefs: []string{"token"}}
	r := Runner{Binary: binary, Secrets: db.Secrets()}
	out, e := r.Execute(context.Background(), plan, true)
	if e != nil || strings.Contains(out, "private-token") || !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("output %s, error %v", out, e)
	}
	os.WriteFile(path, append(data, '\n'), 0600)
	if _, e = r.Execute(context.Background(), plan, true); e == nil {
		t.Fatal("changed script accepted")
	}
}
func TestDLTLoadKeepsNestedRowsInOneTable(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for dlt integration test")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "load.py")
	data := []byte(Wrap("def fetch(secret, limit):\n    yield {'id': 1, 'nested': [{'value': 2}]}\n"))
	os.WriteFile(path, data, 0600)
	destination := filepath.Join(dir, "out.duckdb")
	plan := compiler.ExecutionPlan{Engine: "python", Script: path, ScriptDigest: spec.Digest(data), DestinationPath: destination, DestinationObject: "stars", Strategy: "replace"}
	r := Runner{Binary: binary}
	if _, e := r.Execute(context.Background(), plan, true); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(destination); !os.IsNotExist(e) {
		t.Fatal("probe wrote to destination")
	}
	for i := 0; i < 2; i++ {
		if _, e := r.Execute(context.Background(), plan, false); e != nil {
			t.Fatal(e)
		}
	}
	cmd := exec.Command(binary, "-c", `import duckdb,sys
c=duckdb.connect(sys.argv[1]); assert c.execute('select count(*) from main.stars').fetchone()[0]==1
assert c.execute("select count(*) from information_schema.tables where table_schema='main' and table_name not like '_dlt%'").fetchone()[0]==1
`, destination)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("%v: %s", e, out)
	}
}

func TestProbeRequiresActualSample(t *testing.T) {
	for _, code := range []string{"import sys\nsys.exit(0)\n", "def fetch(secret, limit):\n    return iter([])\n"} {
		dir := t.TempDir()
		path := filepath.Join(dir, "probe.py")
		data := []byte(Wrap(code))
		os.WriteFile(path, data, 0600)
		_, err := (Runner{Binary: "python3"}).Execute(context.Background(), compiler.ExecutionPlan{Script: path, ScriptDigest: spec.Digest(data)}, true)
		if err == nil {
			t.Fatal("probe accepted without sample")
		}
	}
}
