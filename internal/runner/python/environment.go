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
	"pompos/internal/ingestion"
	"pompos/internal/spec"
)

//go:embed requirements.txt
var loaderRequirements string

// EnvironmentPreparationTimeout is separate from source execution limits.
const EnvironmentPreparationTimeout = 10 * time.Minute

// Environments owns uv-managed environments. Each destination table gets its
// own directory, with immutable revisions for different dependency locks.
type Environments struct {
	Dir      string
	UVBinary string
}

// Prepare resolves and installs dependencies before execution timeouts start.
// A nil manager supports callers supplying an already provisioned interpreter.
func (r Runner) Prepare(ctx context.Context, plan compiler.ExecutionPlan) (compiler.ExecutionPlan, error) {
	if err := spec.ValidatePythonRuntime(plan.Python, plan.Dependencies, plan.DependencyLock); err != nil {
		return plan, err
	}
	if plan.PythonBinary != "" {
		return plan, nil // This execution already prepared its environment.
	}
	if r.Environments == nil {
		if plan.Python != "" || len(plan.Dependencies) > 0 || plan.DependencyLock != nil {
			return plan, fmt.Errorf("Python dependencies require a uv environment manager")
		}
		plan.PythonBinary = r.Binary
		if plan.PythonBinary == "" {
			plan.PythonBinary = "python3"
		}
		return plan, nil
	}
	ctx, cancel := context.WithTimeout(ctx, EnvironmentPreparationTimeout)
	defer cancel()
	manager := *r.Environments
	if manager.Dir == "" {
		return plan, fmt.Errorf("Python environment directory is required")
	}
	root, err := filepath.Abs(manager.Dir)
	if err != nil {
		return plan, err
	}
	manager.Dir = root
	if plan.EnvironmentKey == "" {
		// Identity survives copying a draft script or replacing its destination
		// with a disposable validation database.
		if plan.DestinationPath != "" && plan.DestinationObject != "" {
			path, err := filepath.Abs(plan.DestinationPath)
			if err != nil {
				return plan, err
			}
			plan.EnvironmentKey = path + "\x00" + destination.SchemaName(plan.DestinationSchema) + "\x00" + plan.DestinationObject
		} else {
			plan.EnvironmentKey, err = filepath.Abs(plan.Script)
			if err != nil {
				return plan, err
			}
		}
	}
	directory := filepath.Join(root, hashName([]byte(plan.EnvironmentKey)))
	if err := os.MkdirAll(directory, 0700); err != nil {
		return plan, err
	}
	// OS locks coordinate server and CLI processes and are released on crashes.
	guard, err := lockEnvironment(ctx, filepath.Join(directory, ".lock"))
	if err != nil {
		return plan, err
	}
	defer guard.Close()
	dependencies := append([]string(nil), plan.Dependencies...)
	sort.Strings(dependencies)
	input, _ := json.Marshal(struct {
		Python       string
		Dependencies []string
		Loader       string
	}{plan.Python, dependencies, loaderRequirements})
	digest := spec.Digest(input)
	lockPath := filepath.Join(directory, hashName(input)+".json")
	lock := plan.DependencyLock
	if lock == nil {
		data, readErr := os.ReadFile(lockPath)
		if readErr == nil {
			lock = &ingestion.DependencyLock{}
			if err := json.Unmarshal(data, lock); err != nil {
				return plan, fmt.Errorf("read Python dependency lock: %w", err)
			}
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return plan, readErr
		}
	}
	if lock == nil {
		selector := plan.Python
		if selector == "" {
			selector = r.Binary
		}
		if selector == "" {
			selector = "python3"
		}
		lock, err = manager.resolve(ctx, directory, selector, dependencies, digest)
		if err != nil {
			return plan, err
		}
	}
	if err := spec.ValidatePythonRuntime(plan.Python, plan.Dependencies, lock); err != nil {
		return plan, err
	}
	if lock.InputDigest != digest {
		return plan, fmt.Errorf("Python dependencies changed since they were tested; clear dependencyLock and probe and validate the ingestion again")
	}
	if lock.Platform != runtime.GOOS+"/"+runtime.GOARCH {
		return plan, fmt.Errorf("Python dependency lock targets %s; probe and validate on %s/%s", lock.Platform, runtime.GOOS, runtime.GOARCH)
	}
	data, _ := json.Marshal(lock)
	environment := filepath.Join(directory, hashName(data))
	binary := filepath.Join(environment, "bin", "python")
	ready := filepath.Join(environment, ".ready")
	if _, err := os.Stat(ready); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return plan, err
		}
		// No process uses an environment until the ready marker exists. A failed
		// installation is discarded on retry; ready revisions are never mutated.
		if err := os.RemoveAll(environment); err != nil {
			return plan, err
		}
		if _, err := manager.run(ctx, "venv", "--no-project", "--python", lock.Python, environment); err != nil {
			return plan, err
		}
		requirements := filepath.Join(environment, "requirements.txt")
		if err := os.WriteFile(requirements, []byte(lock.Requirements), 0600); err != nil {
			return plan, err
		}
		if _, err := manager.run(ctx, "pip", "sync", "--python", binary, requirements); err != nil {
			return plan, err
		}
		if err := os.WriteFile(ready, []byte("ready\n"), 0600); err != nil {
			return plan, err
		}
	}
	if _, err := os.Stat(binary); err != nil {
		return plan, fmt.Errorf("Python environment is incomplete: %w", err)
	}
	// Also retain the lock locally for hand-written YAML without a saved lock.
	if err := writeEnvironmentFile(lockPath, data); err != nil {
		return plan, err
	}
	plan.DependencyLock = lock
	plan.PythonBinary = binary
	return plan, nil
}

func (m Environments) resolve(ctx context.Context, directory, selector string, dependencies []string, digest string) (*ingestion.DependencyLock, error) {
	temporary, err := os.MkdirTemp(directory, ".resolve-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(temporary)
	venv := filepath.Join(temporary, "venv")
	if _, err := m.run(ctx, "venv", "--no-project", "--python", selector, venv); err != nil {
		return nil, err
	}
	binary := filepath.Join(venv, "bin", "python")
	cmd := exec.CommandContext(ctx, binary, "-I", "-c", "import platform; print(platform.python_version())")
	cmd.Env = environmentVariables()
	cmd.WaitDelay = time.Second
	version, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("read environment Python version: %w", err)
	}
	input := filepath.Join(temporary, "requirements.in")
	output := filepath.Join(temporary, "requirements.txt")
	if err := os.WriteFile(input, []byte(loaderRequirements+"\n"+strings.Join(dependencies, "\n")+"\n"), 0600); err != nil {
		return nil, err
	}
	if _, err := m.run(ctx, "pip", "compile", "--python", binary, "--no-header", "--no-annotate", "--output-file", output, input); err != nil {
		return nil, err
	}
	requirements, err := os.ReadFile(output)
	if err != nil {
		return nil, err
	}
	return &ingestion.DependencyLock{InputDigest: digest, Python: strings.TrimSpace(string(version)), Platform: runtime.GOOS + "/" + runtime.GOARCH, Requirements: string(requirements)}, nil
}

func (m Environments) run(ctx context.Context, args ...string) (string, error) {
	binary := m.UVBinary
	if binary == "" {
		binary = "uv"
	}
	cmd := exec.CommandContext(ctx, binary, append([]string{"--no-config", "--no-progress", "--cache-dir", filepath.Join(m.Dir, ".uv-cache")}, args...)...)
	cmd.Env = append(environmentVariables(), "UV_PYTHON_INSTALL_DIR="+filepath.Join(m.Dir, ".python"))
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

func writeEnvironmentFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".lock-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

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
