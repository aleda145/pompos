package python

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/spec"
	"pompos/internal/testutil"
)

func environmentFixture(t *testing.T) (Runner, compiler.ExecutionPlan) {
	t.Helper()
	dir := t.TempDir()
	r := Runner{Environments: &Environments{Dir: filepath.Join(dir, "environments"), UVBinary: testutil.FakeUV(t)}}
	code := []byte(WrapWithRuntime("def fetch(secret, limit):\n    import fixture_package, sys\n    yield {'version': fixture_package.VERSION, 'python': sys.executable}\n", "", []string{"fixture-package==1.0"}))
	script := filepath.Join(dir, "source.py")
	if err := os.WriteFile(script, code, 0600); err != nil {
		t.Fatal(err)
	}
	return r, compiler.ExecutionPlan{Engine: "python", Script: script, ScriptDigest: spec.Digest(code), DestinationPath: filepath.Join(dir, "data.duckdb"), DestinationObject: "customers", Dependencies: []string{"fixture-package==1.0"}}
}

func TestEnvironmentsIsolateReuseAndLockVersions(t *testing.T) {
	r, plan := environmentFixture(t)
	ctx := context.Background()
	t.Setenv("OPENAI_API_KEY", "must-not-reach-installer")
	t.Setenv("PYTHONPATH", "must-not-leak")
	t.Setenv("UV_INDEX_URL", "must-not-leak")
	prepared, err := r.Prepare(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	out, err := r.Execute(ctx, prepared, true)
	if err != nil || !strings.Contains(out, `"version": "1.0"`) || !strings.Contains(out, prepared.PythonBinary) {
		t.Fatalf("wrong environment: %s, %v", out, err)
	}
	log := filepath.Join(filepath.Dir(r.Environments.UVBinary), "calls.jsonl")
	before, _ := os.ReadFile(log)
	reused, err := r.Prepare(ctx, plan)
	if err != nil || reused.PythonBinary != prepared.PythonBinary {
		t.Fatalf("environment not reused: %#v %v", reused, err)
	}
	after, _ := os.ReadFile(log)
	if string(before) != string(after) {
		t.Fatal("cached environment invoked uv again")
	}
	// Publishing changes the script path but retains the destination identity.
	original := plan.Script
	plan.Script = filepath.Join(t.TempDir(), "published.py")
	for _, extension := range []string{"", ".lock"} {
		data, err := os.ReadFile(original + extension)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(plan.Script+extension, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	plan.LockDigest = prepared.LockDigest
	published, err := r.Prepare(ctx, plan)
	if err != nil || published.PythonBinary != prepared.PythonBinary {
		t.Fatalf("published ingestion lost its environment: %v", err)
	}
	plan.DestinationObject = "other_customers"
	other, err := r.Prepare(ctx, plan)
	if err != nil || other.PythonBinary == prepared.PythonBinary {
		t.Fatalf("ingestions shared an environment: %v", err)
	}
	plan.Dependencies = []string{"fixture-package==2.0"}
	if _, err := r.Prepare(ctx, plan); err == nil || !strings.Contains(err.Error(), "metadata") {
		t.Fatalf("changed dependencies reused the old lock: %v", err)
	}
	plan.LockDigest = ""
	updatedCode, err := os.ReadFile(plan.Script)
	if err != nil {
		t.Fatal(err)
	}
	updatedCode = []byte(strings.Replace(string(updatedCode), "fixture-package==1.0", "fixture-package==2.0", 1))
	if err := os.WriteFile(plan.Script, updatedCode, 0600); err != nil {
		t.Fatal(err)
	}
	plan.ScriptDigest = spec.Digest(updatedCode)
	updated, err := r.Prepare(ctx, plan)
	if err != nil || updated.PythonBinary == other.PythonBinary {
		t.Fatalf("dependency change mutated the old environment: %v", err)
	}
	check := exec.Command(updated.PythonBinary, "-I", "-c", "import fixture_package; assert fixture_package.VERSION == '2.0'")
	if out, err := check.CombinedOutput(); err != nil {
		t.Fatalf("new version missing: %s %v", out, err)
	}
	// Earlier prepared executions continue to use the original revision.
	if out, err := r.Execute(ctx, prepared, true); err != nil || !strings.Contains(out, `"version": "1.0"`) {
		t.Fatalf("old environment changed: %s %v", out, err)
	}
}

func TestEnvironmentConcurrentPreparationAndInstallRetry(t *testing.T) {
	r, plan := environmentFixture(t)
	ctx := context.Background()
	failure := filepath.Join(filepath.Dir(r.Environments.UVBinary), "fail-install")
	if err := os.WriteFile(failure, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Prepare(ctx, plan); err == nil || !strings.Contains(err.Error(), "fixture installation failed") {
		t.Fatalf("failed install accepted: %v", err)
	}
	if err := os.Remove(failure); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	paths := make(chan string, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready, err := r.Prepare(ctx, plan)
			if err != nil {
				t.Error(err)
				return
			}
			paths <- ready.PythonBinary
		}()
	}
	wg.Wait()
	close(paths)
	first := ""
	for path := range paths {
		if first != "" && first != path {
			t.Fatal("concurrent preparations produced different environments")
		}
		first = path
	}
	if first == "" {
		t.Fatal("no environment prepared")
	}
	log, _ := os.ReadFile(filepath.Join(filepath.Dir(r.Environments.UVBinary), "calls.jsonl"))
	if strings.Count(string(log), `"sync"`) != 2 {
		t.Fatalf("expected one failed and one successful installation: %s", log)
	}
}

func TestEnvironmentLockWaitIsCancellable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	guard, err := lockEnvironment(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := lockEnvironment(ctx, path); err != context.DeadlineExceeded {
		t.Fatalf("lock ignored cancellation: %v", err)
	}
}

func TestSavedEnvironmentRejectsMissingOrChangedLock(t *testing.T) {
	r, plan := environmentFixture(t)
	prepared, err := r.Prepare(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.LockDigest = prepared.LockDigest
	r.Environments.UVBinary = "/uv-must-not-relock-a-saved-ingestion"
	lock, err := ReadScriptLock(plan.Script, plan.LockDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plan.Script+".lock", append(lock, []byte("# changed\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Prepare(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("cached environment accepted changed lock: %v", err)
	}
	if err := os.Remove(plan.Script + ".lock"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Prepare(context.Background(), plan); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("saved execution regenerated missing lock: %v", err)
	}
}

// This exercises actual resolution, installation, probes, loading and previews.
func TestUVIntegration(t *testing.T) {
	uv := os.Getenv("POMPOS_TEST_UV")
	if uv == "" {
		t.Skip("set POMPOS_TEST_UV to run the real uv integration test")
	}
	dir := t.TempDir()
	r := Runner{Environments: &Environments{Dir: filepath.Join(dir, "environments"), UVBinary: uv}}
	code := []byte(WrapWithRuntime("def fetch(secret, limit):\n    import psycopg2\n    yield {'id': 1, 'driver': psycopg2.__version__}\n", "", []string{"psycopg2-binary"}))
	script := filepath.Join(dir, "postgres.py")
	if err := os.WriteFile(script, code, 0600); err != nil {
		t.Fatal(err)
	}
	plan := compiler.ExecutionPlan{Engine: "python", Script: script, ScriptDigest: spec.Digest(code), DestinationPath: filepath.Join(dir, "out.duckdb"), DestinationObject: "customers", Strategy: "replace", Dependencies: []string{"psycopg2-binary"}}
	ctx := context.Background()
	prepared, err := r.Prepare(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := ReadScriptLock(script, prepared.LockDigest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(lock), `name = "psycopg2-binary"`) || !strings.Contains(string(lock), "sha256:") {
		t.Fatal("source dependency missing from lock")
	}
	if _, err := r.Execute(ctx, prepared, true); err != nil {
		t.Fatal(err)
	}
	result, output, err := r.Validate(ctx, prepared, 5)
	if err != nil || result.Preview == nil || result.Preview.TotalRows != 1 {
		t.Fatalf("validation: %#v %s %v", result, output, err)
	}
	if err := r.Run(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if preview, err := r.Preview(ctx, prepared); err != nil || preview.TotalRows != 1 {
		t.Fatalf("preview: %#v %v", preview, err)
	}
	// A fresh runner and saved lock can recover the same environment.
	plan.LockDigest = prepared.LockDigest
	r.Environments.UVBinary = "/uv-must-not-be-needed-again"
	if _, err := r.Execute(ctx, plan, true); err != nil {
		t.Fatal(err)
	}
}
