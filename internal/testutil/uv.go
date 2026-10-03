// Package testutil provides local subprocess fixtures for runtime tests.
package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// FakeUV creates real, empty virtual environments and installs a small fixture
// module. It exercises subprocess isolation without downloading packages.
func FakeUV(t *testing.T) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	path := filepath.Join(t.TempDir(), "uv")
	code := `
import hashlib, json, os, pathlib, re, subprocess, sys
root = pathlib.Path(__file__).parent
args = sys.argv[1:]
assert not any(k in os.environ for k in ["POMPOS_SECRETS", "OPENAI_API_KEY", "PYTHONPATH", "UV_INDEX_URL"])
with (root / "calls.jsonl").open("a") as f:
    f.write(json.dumps(args) + "\n")
args = args[4:] # --no-config --no-progress --cache-dir PATH
if args[0] == "venv":
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", args[-1]], check=True)
elif args[0] == "lock":
    script = pathlib.Path(args[args.index("--script") + 1])
    source = script.read_text().split("# ///", 2)[1]
    assert "dlt[duckdb]" in source and "duckdb==" in source
    if "missing-package" in source:
        sys.exit("No solution found for missing-package")
    version = "2.0" if "fixture-package==2.0" in source else "1.0"
    digest = hashlib.sha256(source.encode()).hexdigest()
    pathlib.Path(str(script) + ".lock").write_text('version = 1\n# metadata = ' + digest + '\n[[package]]\nname = "fixture-package"\nversion = "' + version + '"\n')
elif args[0] == "sync":
    assert "--active" in args and "--locked" in args
    if (root / "fail-install").exists():
        sys.exit("fixture installation failed")
    script = pathlib.Path(args[args.index("--script") + 1])
    source = script.read_text().split("# ///", 2)[1]
    lock = pathlib.Path(str(script) + ".lock").read_text()
    assert hashlib.sha256(source.encode()).hexdigest() in lock, "lock is stale"
    python = args[args.index("--python") + 1]
    assert pathlib.Path(python).parent.parent == pathlib.Path(os.environ["VIRTUAL_ENV"])
    site = subprocess.check_output([python, "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True).strip()
    version = re.search(r'version = "(.*?)"', lock).group(1)
    pathlib.Path(site, "fixture_package.py").write_text("VERSION = " + repr(version))
else:
    sys.exit("Unexpected uv invocation: " + repr(args))
`
	if err := os.WriteFile(path, []byte("#!"+python+"\n"+code), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
