package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"pompos/internal/compiler"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/testutil"
)

func TestDependencyProbeValidationAndPublication(t *testing.T) {
	s, runner, v := validationFixture(t)
	runner.Environments = &runnerpython.Environments{Dir: filepath.Join(t.TempDir(), "environments"), UVBinary: testutil.FakeUV(t)}
	ctx := context.Background()
	call := func(name string, input any) {
		t.Helper()
		args, _ := json.Marshal(input)
		c := Call{}
		c.Function.Name, c.Function.Arguments = name, string(args)
		if _, err := s.execute(ctx, &v, c); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	draft := *v.Draft
	draft.Dependencies = []string{"fixture-package==1.0"}
	draft.Code = "def fetch(secret, limit):\n    import fixture_package\n    yield {'id': 1, 'version': fixture_package.VERSION}\n"
	call("write_script", draft)
	call("test_script", nil)
	if v.Draft.LockDigest == "" {
		t.Fatal("successful probe did not save its environment lock")
	}
	lock, err := runnerpython.ReadScriptLock(s.scriptPath(v.ID), v.Draft.LockDigest)
	if err != nil {
		t.Fatal(err)
	}
	// Editing only source code preserves resolved package versions.
	draft.Code += "\n"
	call("write_script", draft)
	afterEdit, err := runnerpython.ReadScriptLock(s.scriptPath(v.ID), "")
	if err != nil || string(afterEdit) != string(lock) || v.Draft.LockDigest != "" || v.TestedDigest != "" || v.Validation != nil {
		t.Fatal("editing source lost the lock or kept stale validation")
	}
	call("test_script", nil)
	// Even an out-of-band dependency edit cannot reuse a successful probe.
	v.Draft.Dependencies = []string{"fixture-package==2.0"}
	if _, _, err := s.validationPlan(ctx, &v); err == nil {
		t.Fatal("validation accepted dependencies that were not probed")
	}
	draft.Dependencies = []string{"fixture-package==2.0"}
	// Restore the previous request so write_script sees an actual change.
	v.Draft.Dependencies = []string{"fixture-package==1.0"}
	call("write_script", draft)
	if v.Draft.LockDigest != "" || v.TestedDigest != "" || v.Validation != nil {
		t.Fatal("dependency edit retained a stale lock or test")
	}
	call("test_script", nil)
	if _, err := s.proposeValidation(ctx, &v, `{"limit":5}`); err != nil {
		t.Fatal(err)
	}
	if err := s.runValidation(ctx, &v, v.Pending.Validation, func(Event) {}); err != nil {
		t.Fatal(err)
	}
	call("finish", nil)
	if err := s.save(v); err != nil {
		t.Fatal(err)
	}
	artifactDir := t.TempDir()
	testedLock, err := runnerpython.ReadScriptLock(s.scriptPath(v.ID), v.Draft.LockDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.scriptPath(v.ID)+".lock", append(append([]byte(nil), testedLock...), []byte("# changed\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, v.ID, artifactDir, func(string, spec.Ingestion) error { t.Fatal("published changed lock"); return nil }); err == nil {
		t.Fatal("changed lock accepted")
	}
	if err := os.WriteFile(s.scriptPath(v.ID)+".lock", testedLock, 0600); err != nil {
		t.Fatal(err)
	}
	published := false
	_, err = s.Publish(ctx, v.ID, artifactDir, func(id string, doc spec.Ingestion) error {
		published = true
		if doc.Runtime.Script != spec.ArtifactPath(artifactDir, id, ".py") {
			t.Fatalf("wrong artifact path: %s", doc.Runtime.Script)
		}
		copied, err := runnerpython.ReadScriptLock(doc.Runtime.Script, doc.Runtime.LockDigest)
		if err != nil || string(copied) != string(testedLock) {
			t.Fatalf("publication changed the native lock: %v", err)
		}
		// Exercise the same YAML round trip and compiler used by scheduled runs.
		data, err := spec.Marshal(doc)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "dependencyLock:") {
			t.Fatal("YAML embeds the old lock")
		}
		if err := WriteFile(spec.ArtifactPath(artifactDir, id, ".yaml"), data); err != nil {
			return err
		}
		doc, err = spec.Parse(data)
		if err != nil {
			return err
		}
		plan, err := compiler.Compile(doc)
		if err != nil {
			return err
		}
		if plan.LockDigest != v.Draft.LockDigest || !reflect.DeepEqual(plan.Dependencies, draft.Dependencies) {
			t.Fatal("publication lost the tested environment")
		}
		_, err = runner.Execute(ctx, plan, true)
		return err
	})
	entries, readErr := os.ReadDir(filepath.Join(artifactDir, v.Draft.Destination, "main", v.Draft.Table))
	if readErr != nil || len(entries) != 3 {
		t.Fatalf("expected YAML, Python, and native lock: %v %v", entries, readErr)
	}
	if err != nil || !published {
		t.Fatalf("publication: %v", err)
	}
}
