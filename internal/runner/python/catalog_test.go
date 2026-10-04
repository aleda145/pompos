package python

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"pompos/internal/destination"
)

func TestDestinationCatalogReadOnly(t *testing.T) {
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for DuckDB integration test")
	}
	ctx := context.Background()
	dest := destination.NewDuckDB("elections", filepath.Join(t.TempDir(), "out.duckdb"))
	r := Runner{Binary: binary}
	missing, err := r.InspectDestination(ctx, dest)
	if err != nil || missing.Exists || len(missing.Schemas) != 0 {
		t.Fatalf("missing database: %#v, %v", missing, err)
	}
	if _, err := os.Stat(dest.Path); !os.IsNotExist(err) {
		t.Fatal("inspection created a database")
	}
	setup := exec.Command(binary, "-c", `import duckdb, sys
with duckdb.connect(sys.argv[1]) as db:
    db.execute('CREATE SCHEMA election2022')
    db.execute('CREATE SCHEMA election2026')
    db.execute('CREATE TABLE election2022.results AS SELECT 1 AS id')
    db.execute('CREATE TABLE election2022._dlt_loads(id INTEGER)')
    db.execute("CREATE VIEW election2022.turnout AS SELECT error('must not execute views')")
`, dest.Path)
	if out, err := setup.CombinedOutput(); err != nil {
		t.Fatalf("setup: %v: %s", err, out)
	}
	before, err := os.ReadFile(dest.Path)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := r.InspectDestination(ctx, dest)
	want := []DestinationSchema{{Name: "election2022", Tables: []string{"results", "turnout"}}, {Name: "election2026", Tables: []string{}}, {Name: "main", Tables: []string{}}}
	if err != nil || !catalog.Exists || catalog.Destination != dest.Name || !reflect.DeepEqual(catalog.Schemas, want) {
		t.Fatalf("catalog: %#v, %v", catalog, err)
	}
	after, err := os.ReadFile(dest.Path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inspection changed the destination database")
	}
	if err := os.WriteFile(dest.Path, []byte("not a database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.InspectDestination(ctx, dest); err == nil {
		t.Fatal("unreadable database reported as an empty catalog")
	}
}
