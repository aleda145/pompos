package python

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"pompos/internal/compiler"
	"pompos/internal/destination"
	"pompos/internal/spec"
)

//go:embed requirements.txt
var loaderRequirements string

const EnvironmentPreparationTimeout = 10 * time.Minute

// Environments keeps separate installed environments per destination table and
// native uv lock revision. Package downloads share uv's cache.
type Environments struct {
	Dir      string
	UVBinary string
}

// ScriptMetadata is generated from YAML declarations, never independently edited.
func ScriptMetadata(python string, dependencies []string) string {
	requiresPython := ">=3.10"
	if python != "" {
		requiresPython = "==" + python
		if strings.Count(python, ".") == 1 {
			requiresPython += ".*"
		}
	}
	requirements := append(strings.Fields(loaderRequirements), dependencies...)
	sort.Strings(requirements)
	var out strings.Builder
	out.WriteString("# /// script\n# requires-python = ")
	quoted, _ := json.Marshal(requiresPython)
	out.Write(quoted)
	out.WriteString("\n# dependencies = [\n")
	for _, requirement := range requirements {
		quoted, _ := json.Marshal(requirement)
		out.WriteString("#   ")
		out.Write(quoted)
		out.WriteString(",\n")
	}
	out.WriteString("# ]\n# ///\n\n")
	return out.String()
}

// ReadScriptLock binds validation and scheduled execution to the tested bytes.
// The contents remain an opaque uv-native lock; uv validates its semantics.
func ReadScriptLock(script, digest string) ([]byte, error) {
	data, err := os.ReadFile(script + ".lock")
	if err != nil {
		return nil, fmt.Errorf("read Python script lock: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("Python script lock is empty; probe again")
	}
	if digest != "" && spec.Digest(data) != digest {
		return nil, fmt.Errorf("Python script lock changed since it was tested; probe and validate again")
	}
	return data, nil
}

// Prepare installs from the native script lock before execution timeouts start.
// A nil manager supports callers supplying an already provisioned interpreter.
func (r Runner) Prepare(ctx context.Context, plan compiler.ExecutionPlan) (compiler.ExecutionPlan, error) {
	if err := spec.ValidatePythonRuntime(plan.Python, plan.Dependencies, plan.LockDigest); err != nil {
		return plan, err
	}
	if plan.PythonBinary != "" {
		return plan, nil
	}
	if r.Environments == nil {
		if plan.Python != "" || len(plan.Dependencies) > 0 || plan.LockDigest != "" {
			return plan, fmt.Errorf("Python dependencies require a uv environment manager")
		}
		plan.PythonBinary = r.Binary
		if plan.PythonBinary == "" {
			plan.PythonBinary = "python3"
		}
		return plan, nil
	}
	code, err := os.ReadFile(plan.Script)
	if err != nil {
		return plan, err
	}
	if spec.Digest(code) != plan.ScriptDigest {
		return plan, fmt.Errorf("Python script changed since it was tested; test and save it again")
	}
	metadata := ScriptMetadata(plan.Python, plan.Dependencies)
	if !strings.HasPrefix(string(code), metadata) {
		return plan, fmt.Errorf("Python script metadata does not match its runtime declarations; rewrite and probe the script")
	}
	ctx, cancel := context.WithTimeout(ctx, EnvironmentPreparationTimeout)
	defer cancel()
	manager := *r.Environments
	if manager.Dir == "" {
		return plan, fmt.Errorf("Python environment directory is required")
	}
	manager.Dir, err = filepath.Abs(manager.Dir)
	if err != nil {
		return plan, err
	}
	script, err := filepath.Abs(plan.Script)
	if err != nil {
		return plan, err
	}
	if plan.EnvironmentKey == "" {
		if plan.DestinationPath != "" && plan.DestinationObject != "" {
			path, err := filepath.Abs(plan.DestinationPath)
			if err != nil {
				return plan, err
			}
			plan.EnvironmentKey = path + "\x00" + destination.SchemaName(plan.DestinationSchema) + "\x00" + plan.DestinationObject
		} else {
			plan.EnvironmentKey = script
		}
	}
	directory := filepath.Join(manager.Dir, hashName([]byte(plan.EnvironmentKey)))
	if err := os.MkdirAll(directory, 0700); err != nil {
		return plan, err
	}
	guard, err := lockEnvironment(ctx, filepath.Join(directory, ".lock"))
	if err != nil {
		return plan, err
	}
	defer guard.Close()

	selector := plan.Python
	if selector == "" {
		selector = r.Binary
	}
	if selector == "" {
		selector = "python3"
	}
	// Include metadata and interpreter selection so a cached environment cannot
	// bypass uv's check when declarations change but a stale lock remains.
	environmentPath := func(lock []byte) string {
		key := metadata + "\x00" + selector + "\x00" + runtime.GOOS + "/" + runtime.GOARCH + "\x00" + string(lock)
		return filepath.Join(directory, hashName([]byte(key)))
	}
	lock, err := ReadScriptLock(script, plan.LockDigest)
	if err != nil && (plan.LockDigest != "" || !errors.Is(err, os.ErrNotExist)) {
		return plan, err
	}
	if len(lock) > 0 {
		environment := environmentPath(lock)
		if _, err := os.Stat(filepath.Join(environment, ".ready")); err == nil {
			plan.PythonBinary = filepath.Join(environment, "bin", "python")
			if _, err := os.Stat(plan.PythonBinary); err != nil {
				return plan, fmt.Errorf("incomplete Python environment: %w", err)
			}
			plan.LockDigest = spec.Digest(lock)
			return plan, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return plan, err
		}
	}
	if plan.LockDigest == "" {
		// Draft probes may update a lock. Saved executions always use --locked.
		if _, err := manager.run(ctx, "lock", "--script", script, "--python", selector); err != nil {
			return plan, err
		}
		lock, err = ReadScriptLock(script, "")
		if err != nil {
			return plan, err
		}
	}
	environment := environmentPath(lock)
	binary := filepath.Join(environment, "bin", "python")
	if _, err := os.Stat(filepath.Join(environment, ".ready")); errors.Is(err, os.ErrNotExist) {
		// Only incomplete revisions are removed. Ready environments are immutable.
		if err := os.RemoveAll(environment); err != nil {
			return plan, err
		}
		if _, err := manager.run(ctx, "venv", "--no-project", "--python", selector, environment); err != nil {
			return plan, err
		}
		if _, err := manager.runInEnvironment(ctx, environment, "sync", "--active", "--locked", "--script", script, "--python", binary); err != nil {
			return plan, err
		}
		if _, err := ReadScriptLock(script, spec.Digest(lock)); err != nil {
			return plan, err
		}
		if err := os.WriteFile(filepath.Join(environment, ".ready"), []byte("ready\n"), 0600); err != nil {
			return plan, err
		}
	} else if err != nil {
		return plan, err
	}
	if _, err := os.Stat(binary); err != nil {
		return plan, fmt.Errorf("incomplete Python environment: %w", err)
	}

	plan.LockDigest = spec.Digest(lock)
	plan.PythonBinary = binary
	return plan, nil
}

func (m Environments) run(ctx context.Context, args ...string) (string, error) {
	return m.runInEnvironment(ctx, "", args...)
}

func (m Environments) runInEnvironment(ctx context.Context, environment string, args ...string) (string, error) {
	binary := m.UVBinary
	if binary == "" {
		binary = "uv"
	}
	cmd := exec.CommandContext(ctx, binary, append([]string{"--no-config", "--no-progress", "--cache-dir", filepath.Join(m.Dir, ".uv-cache")}, args...)...)
	cmd.Env = append(environmentVariables(), "UV_PYTHON_INSTALL_DIR="+filepath.Join(m.Dir, ".python"))
	if environment != "" {
		cmd.Env = append(cmd.Env, "VIRTUAL_ENV="+environment)
	}
	cmd.WaitDelay = 2 * time.Second
	var output cappedOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("prepare Python environment: %w", ctx.Err())
		}
		return "", fmt.Errorf("uv %s failed (install uv and check the declared packages): %w\n%s", args[0], err, output.String())
	}
	return output.String(), nil
}

// Package installation never receives source secrets or provider credentials.
func environmentVariables() []string {
	env := []string{}
	for _, key := range []string{"PATH", "HOME", "LANG", "SYSTEMROOT", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return env
}

func hashName(data []byte) string { return strings.TrimPrefix(spec.Digest(data), "sha256:") }

func lockEnvironment(ctx context.Context, path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			file.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
