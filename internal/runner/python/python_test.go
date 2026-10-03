package python

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/compiler"
	"pompos/internal/store"
)

func TestProbeRedactsSecretsAndAcceptsManualEdits(t *testing.T) {
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
	plan := compiler.ExecutionPlan{Script: path, SecretRefs: []string{"token"}}
	r := Runner{Binary: binary, Secrets: db.Secrets()}
	out, e := r.Execute(context.Background(), plan, true)
	if e != nil || strings.Contains(out, "private-token") || !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("output %s, error %v", out, e)
	}
	os.WriteFile(path, append(data, '\n'), 0600)
	if _, e = r.Execute(context.Background(), plan, true); e != nil {
		t.Fatalf("manual edit rejected: %v", e)
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
	plan := compiler.ExecutionPlan{Engine: "python", Script: path, DestinationPath: destination, DestinationObject: "stars", Strategy: "replace"}
	r := Runner{Binary: binary}
	if _, e := r.Execute(context.Background(), plan, true); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(destination); !os.IsNotExist(e) {
		t.Fatal("probe wrote to destination")
	}
	for i := 0; i < 2; i++ {
		if e := r.Run(context.Background(), plan); e != nil {
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
		_, err := (Runner{Binary: "python3"}).Execute(context.Background(), compiler.ExecutionPlan{Script: path}, true)
		if err == nil {
			t.Fatal("probe accepted without sample")
		}
	}
}

func TestDLTLoadsAndPreviewsSeparateSchemas(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for dlt integration test")
	}
	ctx := context.Background()
	dir := t.TempDir()
	r := Runner{Binary: binary}
	for _, schema := range []string{"main", "raw"} {
		path := filepath.Join(dir, schema, "customers.py")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		data := []byte(Wrap("def fetch(secret, limit):\n    yield {'id': 1, 'schema': '" + schema + "'}\n"))
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		plan := compiler.ExecutionPlan{Engine: "python", Script: path, DestinationPath: filepath.Join(dir, "out.duckdb"), DestinationSchema: schema, DestinationObject: "customers", Strategy: "replace"}
		result, output, err := r.Validate(ctx, plan, 5)
		if err != nil || result.Preview == nil || result.Preview.Rows[0][1] != schema {
			t.Fatalf("validate %s: %#v, %v, %s", schema, result, err, output)
		}
		if err := r.Run(ctx, plan); err != nil {
			t.Fatal(err)
		}
	}
	for _, schema := range []string{"main", "raw"} {
		preview, err := r.Preview(ctx, compiler.ExecutionPlan{DestinationPath: filepath.Join(dir, "out.duckdb"), DestinationSchema: schema, DestinationObject: "customers"})
		if err != nil || preview.TotalRows != 1 || preview.Rows[0][1] != schema {
			t.Fatalf("preview %s: %#v, %v", schema, preview, err)
		}
	}
}

func TestProbeOnlyFetchesSample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.py")
	data := []byte(Wrap(`def fetch(secret, limit):
    assert limit == 5
    for i in range(10):
        yield {'id': i}

def estimate(secret):
    raise SystemExit('Probe must not call the legacy estimate hook')
`))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := (Runner{Binary: "python3"}).Execute(context.Background(), compiler.ExecutionPlan{Script: path}, true)
	if err != nil {
		t.Fatal(err)
	}
	var result ProbeResult
	if err := ReadResult(output, "POMPOS_PROBE_RESULT=", &result); err != nil {
		t.Fatal(err)
	}
	if result.Count != 5 || len(result.Rows) != 5 || strings.Contains(output, `"estimate"`) {
		t.Fatalf("expected only a bounded sample: %s", output)
	}
}

func TestDLTValidationLoadsBoundedSampleTwice(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for dlt integration test")
	}
	for _, strategy := range []string{"replace", "append", "merge"} {
		t.Run(strategy, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "load.py")
			// An unbounded generator ensures the runner applies its own consumed-row cap.
			code := "def fetch(secret, limit):\n    assert limit == 7\n    i = 0\n    while True:\n        yield {'id': i, 'nested': [{'value': i}]}\n        i += 1\n"
			data := []byte(Wrap(code))
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(dir, "production.duckdb")
			// The production destination must remain byte-for-byte unchanged.
			if err := os.WriteFile(destination, []byte("do not touch"), 0600); err != nil {
				t.Fatal(err)
			}
			plan := compiler.ExecutionPlan{Script: path, DestinationPath: destination, DestinationObject: "stars", Strategy: strategy}
			if strategy == "merge" {
				plan.PrimaryKey = []string{"id"}
			}
			result, output, err := (Runner{Binary: binary}).Validate(context.Background(), plan, 7)
			if err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
			expected := 7
			if strategy == "append" {
				expected = 14
			}
			if result.SampleCount != 7 || result.FirstLoadRows != 7 || result.SecondLoadRows != expected {
				t.Fatalf("bad counts: %#v", result)
			}
			if result.Preview == nil || len(result.Preview.Rows) != min(expected, 10) || result.Preview.HasMore != (expected > 10) {
				t.Fatalf("missing loaded-table preview: %#v (%s)", result.Preview, result.PreviewError)
			}
			if len(result.Preview.Columns) < 2 || result.Preview.Columns[0] != "id" || result.Preview.Columns[1] != "nested" {
				t.Fatalf("preview did not use the loaded schema: %#v", result.Preview)
			}
			saved, _ := os.ReadFile(destination)
			if string(saved) != "do not touch" {
				t.Fatal("validation modified production destination")
			}
			if _, err := os.Stat(filepath.Join(dir, ".dlt")); !os.IsNotExist(err) {
				t.Fatal("validation left pipeline state with the draft")
			}
		})
	}
}

func TestDLTValidationRejectsBadKeys(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for dlt integration test")
	}
	for _, rows := range []string{"[{'id': 1}, {'id': 1}]", "[{'id': None}]", "[{'other': 1}]"} {
		path := filepath.Join(t.TempDir(), "load.py")
		data := []byte(Wrap("def fetch(secret, limit):\n    yield from " + rows + "\n"))
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		_, _, err := (Runner{Binary: binary}).Validate(context.Background(), compiler.ExecutionPlan{Script: path, DestinationObject: "stars", Strategy: "merge", PrimaryKey: []string{"id"}}, 10)
		if err == nil {
			t.Fatalf("accepted bad keys: %s", rows)
		}
	}
}
