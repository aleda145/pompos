package python

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"pompos/internal/compiler"
)

func objectRunner(t *testing.T) Runner {
	t.Helper()
	binary := os.Getenv("POMPOS_TEST_PYTHON")
	if binary == "" {
		t.Skip("set POMPOS_TEST_PYTHON for object catalog integration tests")
	}
	return Runner{Binary: binary}
}

func objectRow(t *testing.T, r Runner, plan compiler.ExecutionPlan) map[string]string {
	t.Helper()
	preview, err := r.Preview(context.Background(), plan)
	if err != nil || preview.TotalRows != 1 {
		t.Fatalf("object preview: %#v, %v", preview, err)
	}
	row := map[string]string{}
	for i, name := range preview.Columns {
		row[name] = preview.Rows[0][i]
	}
	return row
}

func TestObjectsHTTPPublicationUpdatesAndRecovery(t *testing.T) {
	r := objectRunner(t)
	ctx := context.Background()
	var requests atomic.Int64
	var fail atomic.Bool
	var body atomic.Value
	body.Store("first PDF bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if fail.Load() {
			http.Error(w, "source unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/pdf")
		fmt.Fprint(w, body.Load().(string))
	}))
	defer server.Close()
	dir := t.TempDir()
	plan := compiler.ExecutionPlan{Engine: "python", Script: filepath.Join(dir, "reports.py"), DestinationType: "objects", DestinationPath: filepath.Join(dir, "destination"), DestinationSchema: "reports", DestinationObject: "objects", Strategy: "update"}
	write := func(version string) {
		t.Helper()
		code := fmt.Sprintf("def fetch(secret, limit):\n    yield {'object_id': 'report', 'source_uri': 'https://example.com/report', 'download_url': %q, 'filename': '2026/report.pdf', 'source_version': %q, 'metadata': {'year': 2026}}\n", server.URL, version)
		if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(code, "", nil)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("v1")
	probe, err := r.Execute(ctx, plan, true)
	if err != nil || requests.Load() != 0 || strings.Contains(probe, server.URL) {
		t.Fatalf("probe downloaded or disclosed a download URL: %s, %v", probe, err)
	}
	if _, err := os.Stat(plan.DestinationPath); !os.IsNotExist(err) {
		t.Fatal("probe created the production destination")
	}
	result, output, err := r.Validate(ctx, plan, 1)
	if err != nil || result.Data != "files" || result.FirstLoadRows != 1 || result.SecondLoadRows != 1 || result.Preview == nil || requests.Load() != 1 {
		t.Fatalf("validation: %#v, %v, %s", result, err, output)
	}
	if _, err := os.Stat(plan.DestinationPath); !os.IsNotExist(err) {
		t.Fatal("validation created the production destination")
	}
	if err := r.Run(ctx, plan); err != nil {
		t.Fatal(err)
	}
	first := objectRow(t, r, plan)
	u, err := url.Parse(first["uri"])
	if err != nil {
		t.Fatal(err)
	}
	firstPath := u.Path
	if first["size_bytes"] != "15" || first["content_type"] != "application/pdf" || !strings.Contains(first["metadata"], "2026") {
		t.Fatalf("catalog metadata: %#v", first)
	}
	if err := r.Run(ctx, plan); err != nil || requests.Load() != 2 {
		t.Fatalf("unchanged object redownloaded: %v, requests=%d", err, requests.Load())
	}
	body.Store("second PDF bytes")
	write("v2")
	fail.Store(true)
	if err := r.Run(ctx, plan); err == nil {
		t.Fatal("failed download succeeded")
	}
	failed := objectRow(t, r, plan)
	if failed["uri"] != first["uri"] || failed["source_version"] != "v1" {
		t.Fatal("failed update replaced the prior object")
	}
	fail.Store(false)
	if err := r.Run(ctx, plan); err != nil {
		t.Fatal(err)
	}
	second := objectRow(t, r, plan)
	if second["uri"] == first["uri"] || second["sha256"] == first["sha256"] || second["source_version"] != "v2" || second["first_seen_at"] != first["first_seen_at"] {
		t.Fatalf("update lost identity or version: %#v", second)
	}
	old, err := os.ReadFile(firstPath)
	if err != nil || string(old) != "first PDF bytes" {
		t.Fatalf("update changed the previous version: %s, %v", old, err)
	}
	// Corruption must force repair even when the source version is unchanged.
	u, _ = url.Parse(second["uri"])
	if err := os.WriteFile(u.Path, []byte("xxxxxxxxxxxxxxxx"), 0600); err != nil {
		t.Fatal(err)
	}
	before := requests.Load()
	if err := r.Run(ctx, plan); err != nil || requests.Load() != before+1 {
		t.Fatalf("corrupt object wasn't repaired: %v", err)
	}
	repaired, _ := os.ReadFile(u.Path)
	if string(repaired) != "second PDF bytes" {
		t.Fatal("repair didn't restore stored bytes")
	}
	// A new source version under skip retains the stored version and its metadata.
	write("v3")
	plan.Strategy = "skip"
	before = requests.Load()
	if err := r.Run(ctx, plan); err != nil || requests.Load() != before {
		t.Fatalf("skip redownloaded: %v", err)
	}
	if row := objectRow(t, r, plan); row["source_version"] != "v2" {
		t.Fatal("skip cataloged metadata for bytes that weren't downloaded")
	}
	// Unknown validators trigger a download on each update run without duplicating rows.
	write("")
	plan.Strategy = "update"
	for range 2 {
		if err := r.Run(ctx, plan); err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != before+2 {
		t.Fatal("unknown source version incorrectly skipped")
	}
	objectRow(t, r, plan)
	// An empty later listing retains previously downloaded objects.
	if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime("def fetch(secret, limit):\n    return iter([])\n", "", nil)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(ctx, plan); err != nil {
		t.Fatal(err)
	}
	objectRow(t, r, plan)
}

func TestObjectsCustomDownloadIsolationAndValidation(t *testing.T) {
	r := objectRunner(t)
	ctx := context.Background()
	dir := t.TempDir()
	plan := compiler.ExecutionPlan{Engine: "python", Script: filepath.Join(dir, "source.py"), DestinationType: "objects", DestinationPath: filepath.Join(dir, "objects"), DestinationSchema: "pictures", DestinationObject: "objects", Strategy: "update"}
	code := `def fetch(secret, limit):
    yield {'object_id': 'one', 'source_uri': 'bucket://images/one', 'filename': 'one.png', 'source_version': 'v1'}
def download(obj, target_path, secret):
    from pathlib import Path
    Path(target_path).write_bytes(b'image bytes')
`
	if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(code, "", nil)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Validate(ctx, plan, 11); err == nil {
		t.Fatal("unbounded file validation accepted")
	}
	for _, schema := range []string{"pictures", "reports", "objects"} {
		plan.DestinationSchema = schema
		if err := r.Run(ctx, plan); err != nil {
			t.Fatal(err)
		}
		row := objectRow(t, r, plan)
		if !strings.Contains(row["uri"], "/files/"+schema+"/") {
			t.Fatal("schema didn't own a separate file directory")
		}
	}
	// Verbose downloader progress must not hide the final result marker.
	noisy := strings.Replace(code, "b'image bytes')", "b'image bytes')\n    print('progress ' * 4000)", 1)
	if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(noisy, "", nil)), 0600); err != nil {
		t.Fatal(err)
	}
	plan.DestinationSchema = "noisy"
	if err := r.Run(ctx, plan); err != nil {
		t.Fatalf("downloader logs hid successful publication: %v", err)
	}
	for _, bad := range []struct{ name, code string }{
		{"traversal", strings.Replace(code, "one.png", "../../escape", 1)},
		{"duplicate identity", strings.Replace(code, "def download", "    yield {'object_id': 'one', 'source_uri': 'bucket://images/two', 'filename': 'two.png'}\ndef download", 1)},
		{"missing staged file", strings.Replace(code, "Path(target_path).write_bytes(b'image bytes')", "pass", 1)},
		{"partial download", strings.Replace(code, "b'image bytes')", "b'partial')\n    raise RuntimeError('interrupted transfer')", 1)},
		{"byte budget", strings.Replace(code, "Path(target_path).write_bytes(b'image bytes')", "with open(target_path, 'wb') as f:\n        for i in range(51):\n            f.write(b'x' * 1024 * 1024)", 1)},
	} {
		t.Run(bad.name, func(t *testing.T) {
			if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(bad.code, "", nil)), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := r.Validate(ctx, plan, 2); err == nil {
				t.Fatal("invalid object validation succeeded")
			}
			if row := objectRow(t, r, plan); row["source_version"] != "v1" {
				t.Fatal("failed validation modified production")
			}
		})
	}
	// Custom validation budgets must reach the Python loader and timeout.
	plan.ValidationMaxBytes = 4
	if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(code, "", nil)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Validate(ctx, plan, 1); err == nil {
		t.Fatal("custom byte budget was ignored")
	}
	plan.ValidationMaxBytes = 1024
	plan.ValidationTimeoutSeconds = 1
	slow := strings.Replace(code, "from pathlib import Path", "import time\n    time.sleep(5)\n    from pathlib import Path", 1)
	if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(slow, "", nil)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Validate(ctx, plan, 1); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("custom timeout was ignored: %v", err)
	}
}

func TestObjectsRejectEscapingStorageAndWrongLoader(t *testing.T) {
	r := objectRunner(t)
	dir := t.TempDir()
	root := filepath.Join(dir, "objects")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "files")); err != nil {
		t.Fatal(err)
	}
	plan := compiler.ExecutionPlan{Engine: "python", Script: filepath.Join(dir, "ingest.py"), DestinationType: "objects", DestinationPath: root, DestinationSchema: "reports", DestinationObject: "objects", Strategy: "update"}
	code := "def fetch(secret, limit):\n    yield {'object_id': 'one', 'source_uri': 'https://example.com/report', 'filename': 'report.pdf'}\n"
	if err := os.WriteFile(plan.Script, []byte(WrapObjectsWithRuntime(code, "", nil)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("escaping storage accepted: %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatal("wrote outside destination")
	}
	if err := os.WriteFile(plan.Script, []byte(Wrap("def fetch(secret, limit):\n    yield {'id': 1}\n")), 0600); err != nil {
		t.Fatal(err)
	}
	plan.DestinationPath = filepath.Join(dir, "must-not-be-a-db")
	if err := r.Run(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "Row ingestion") {
		t.Fatalf("wrong loader accepted: %v", err)
	}
	if _, err := os.Stat(plan.DestinationPath); !os.IsNotExist(err) {
		t.Fatal("row loader created a database at the objects directory path")
	}
}
