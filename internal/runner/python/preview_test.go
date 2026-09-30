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

func TestDuckDBPreviewIsBoundedReadOnlyAndUsesOnlyBaseTables(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for DuckDB integration test")
	}
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "out.duckdb")
	setup := exec.Command(binary, "-c", `import duckdb, sys
with duckdb.connect(sys.argv[1]) as db:
    for count in (0, 10, 11):
        db.execute(f'CREATE TABLE rows_{count} AS SELECT i AS id FROM range({count}) AS r(i)')
    db.execute('CREATE VIEW a_view AS SELECT * FROM rows_11')
    db.execute("CREATE TABLE typed AS SELECT NULL AS missing, true AS active, 1234567890123456789::BIGINT AS large_id, '{\"nested\":[1,2]}'::JSON AS nested, '2026-09-30'::DATE AS day, ? AS token, ? AS long_value", ['secret-value', 'x' * 995 + 'secret-value' + 'z' * 20])
`, path)
	if out, err := setup.CombinedOutput(); err != nil {
		t.Fatalf("setup: %v: %s", err, out)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Secrets().Put(ctx, "source-key", []byte("secret-value")); err != nil {
		t.Fatal(err)
	}
	r := Runner{Binary: binary, Secrets: db.Secrets()}
	for _, tc := range []struct {
		table string
		rows  int
		total int64
		more  bool
	}{{"rows_0", 0, 0, false}, {"rows_10", 10, 10, false}, {"rows_11", 10, 11, true}} {
		// No extractor exists: previewing must not execute one.
		preview, err := r.Preview(ctx, compiler.ExecutionPlan{DestinationPath: path, DestinationObject: tc.table, Script: "does-not-exist.py"})
		if err != nil || len(preview.Rows) != tc.rows || preview.TotalRows != tc.total || preview.HasMore != tc.more || len(preview.Columns) != 1 || preview.Columns[0] != "id" {
			t.Fatalf("%s: %#v, %v", tc.table, preview, err)
		}
	}
	preview, err := r.Preview(ctx, compiler.ExecutionPlan{DestinationPath: path, DestinationObject: "typed", SecretRefs: []string{"source-key"}})
	if err != nil {
		t.Fatal(err)
	}
	row := preview.Rows[0]
	if row[0] != "NULL" || row[1] != "true" || row[2] != "1234567890123456789" || row[3] != `{"nested":[1,2]}` || row[4] != "2026-09-30" || row[5] != "[REDACTED]" || !preview.Truncated || strings.Contains(row[6], "secret") {
		t.Fatalf("typed/redacted values: %#v", preview)
	}
	for _, table := range []string{"a_view", "missing", `rows_11"; DROP TABLE rows_11; --`, "read_csv('/etc/passwd')"} {
		if _, err := r.Preview(ctx, compiler.ExecutionPlan{DestinationPath: path, DestinationObject: table}); err == nil {
			t.Fatalf("accepted invalid preview target %q", table)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || spec.Digest(before) != spec.Digest(after) {
		t.Fatal("preview changed the destination database")
	}
	missing := filepath.Join(dir, "missing.duckdb")
	if _, err := r.Preview(ctx, compiler.ExecutionPlan{DestinationPath: missing, DestinationObject: "rows"}); err == nil {
		t.Fatal("preview accepted missing database")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("preview created a database")
	}
}
