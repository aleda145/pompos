package python

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"pompos/internal/compiler"
)

// DLTSource runs setup only. Generated files are research material for the
// agent to adapt into the ingestion script, never a scheduled entrypoint.
func (r Runner) DLTSource(ctx context.Context, source string, eject bool) (string, error) {
	if source != "" && !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(source) {
		return "", fmt.Errorf("use a source name from list_dlt_sources")
	}
	ctx, cancel := context.WithTimeout(ctx, EnvironmentPreparationTimeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "pompos-dlt-source-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	script := filepath.Join(dir, "setup.py")
	if err := os.WriteFile(script, []byte("# dlt source setup environment\n"), 0600); err != nil {
		return "", err
	}
	plan, err := r.Prepare(ctx, compiler.ExecutionPlan{Script: script, EnvironmentKey: "pompos-dlt-source-setup"})
	if err != nil {
		return "", err
	}
	// A private working directory and home keep init away from live ingestions
	// and from any existing dlt credentials or configuration.
	workspace := filepath.Join(dir, "workspace")
	home := filepath.Join(dir, "home")
	for _, path := range []string{workspace, home} {
		if err := os.Mkdir(path, 0700); err != nil {
			return "", err
		}
	}
	args := []string{"--non-interactive", "init", "--list-sources"}
	if source != "" {
		args = []string{"--non-interactive", "init", source, "duckdb"}
		if eject {
			args = append(args, "--eject")
		}
	}
	// Use the CLI belonging to the same pinned dlt installation as the loader.
	cmd := commandContext(ctx, plan.PythonBinary, append([]string{"-c", "import sys; from importlib.metadata import distribution; sys.exit(next(e for e in distribution('dlt').entry_points if e.group == 'console_scripts' and e.name == 'dlt').load()())"}, args...)...)
	cmd.Dir = workspace
	for _, entry := range environmentVariables() {
		if !strings.HasPrefix(entry, "HOME=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "HOME="+home, "DLT_DATA_DIR="+home, "RUNTIME__DLTHUB_TELEMETRY=false")
	cmd.WaitDelay = 2 * time.Second
	var output cappedOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("dlt source setup failed: %w\n%s", err, output.String())
	}
	files := map[string]string{}
	total := 0
	err = filepath.WalkDir(workspace, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected symlink in dlt scaffold")
		}
		ext := filepath.Ext(path)
		if ext != ".py" && ext != ".toml" && ext != ".txt" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > int64(1000000-total) {
			return fmt.Errorf("dlt scaffold exceeds 1 MB of source material")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		total += len(data)
		name, err := filepath.Rel(workspace, path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(name)] = string(data)
		return nil
	})
	if err != nil {
		return "", err
	}
	result, err := json.Marshal(map[string]any{"output": output.String(), "files": files})
	return string(result), err
}
