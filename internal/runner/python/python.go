// Package python executes trusted, operator-owned ingestion code. It is not a sandbox.
package python

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/runner"
	"pompos/internal/secrets"
	"pompos/internal/spec"
)

type Runner struct {
	Binary  string
	Secrets secrets.Store
	Legacy  runner.Runner
}

func (r Runner) Run(ctx context.Context, id string, plan compiler.ExecutionPlan, credential string) error {
	if plan.Engine != "python" {
		if r.Legacy == nil {
			return fmt.Errorf("unsupported runtime engine %q", plan.Engine)
		}
		return r.Legacy.Run(ctx, id, plan, credential)
	}
	_, err := r.Execute(ctx, plan, false)
	return err
}
func (r Runner) Execute(ctx context.Context, plan compiler.ExecutionPlan, probe bool) (string, error) {
	data, err := os.ReadFile(plan.Script)
	if err != nil {
		return "", err
	}
	if spec.Digest(data) != plan.ScriptDigest {
		return "", fmt.Errorf("Python script changed since it was tested; test and save it again")
	}
	values := map[string]string{}
	for _, ref := range plan.SecretRefs {
		value, err := r.Secrets.Get(ctx, ref)
		if err != nil {
			return "", fmt.Errorf("load secret %q: %w", ref, err)
		}
		values[ref] = string(value)
	}
	payload, _ := json.Marshal(values)
	config, _ := json.Marshal(map[string]any{"destination": plan.DestinationPath, "table": plan.DestinationObject, "strategy": plan.Strategy, "primary_key": plan.PrimaryKey})
	timeout := 30 * time.Minute
	if probe {
		timeout = 45 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	binary := r.Binary
	if binary == "" {
		binary = "python3"
	}
	script, err := filepath.Abs(plan.Script)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, binary, "-u", script)
	// Do not inherit provider keys or unrelated service credentials.
	for _, key := range []string{"PATH", "HOME", "LANG", "SYSTEMROOT", "TMPDIR"} {
		if value, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	cmd.Env = append(cmd.Env, "POMPOS_SECRETS="+string(payload), "POMPOS_CONFIG="+string(config), "DLT_TELEMETRY=0")
	if probe {
		cmd.Env = append(cmd.Env, "POMPOS_PROBE=1")
	}
	cmd.WaitDelay = 2 * time.Second
	var output cappedOutput
	cmd.Stdout = &output
	cmd.Stderr = &output
	err = cmd.Run()
	result := output.String()
	probeValid := !probe
	if probe {
		const marker = "POMPOS_PROBE_RESULT="
		if index := strings.LastIndex(result, marker); index >= 0 {
			var sample struct {
				Rows  []map[string]any `json:"rows"`
				Count int              `json:"sample_count"`
			}
			probeValid = json.Unmarshal([]byte(strings.TrimSpace(result[index+len(marker):])), &sample) == nil && sample.Count > 0 && sample.Count <= 5 && len(sample.Rows) == sample.Count
		}
	}
	for _, value := range values {
		if value != "" {
			result = strings.ReplaceAll(result, value, "[REDACTED]")
			result = strings.ReplaceAll(result, url.QueryEscape(value), "[REDACTED]")
		}
	}
	if ctx.Err() != nil {
		return result, fmt.Errorf("Python execution timed out or was cancelled")
	}
	if err != nil {
		return result, fmt.Errorf("Python execution failed: %w\n%s", err, result)
	}
	if !probeValid {
		return result, fmt.Errorf("Python exited without a valid nonempty probe sample; ensure fetch yields dictionaries and does not exit early")
	}
	return result, nil
}

type cappedOutput struct {
	sync.Mutex
	data []byte
}

func (b *cappedOutput) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	n := len(p)
	remaining := 16384 - len(b.data)
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}
func (b *cappedOutput) String() string { b.Lock(); defer b.Unlock(); return string(b.data) }

// Wrap adds the same entrypoint for preview and scheduled execution. Nested source
// values remain JSON columns, rather than creating extra destination tables.
func Wrap(code string) string {
	return code + `

# Pompos entrypoint: fetch(secret, limit) yields dictionaries for ONE source table.
if __name__ == "__main__":
    import os, json, itertools
    from pathlib import Path
    _secrets = json.loads(os.environ.get("POMPOS_SECRETS", "{}"))
    def secret(name):
        if name not in _secrets:
            raise ValueError("Missing managed secret: " + name)
        return _secrets[name]
    _probe = os.environ.get("POMPOS_PROBE") == "1"
    def _rows():
        for row in fetch(secret, 5 if _probe else None):
            if not isinstance(row, dict):
                raise TypeError("fetch must yield dictionaries")
            yield row
    if _probe:
        _sample = list(itertools.islice(_rows(), 5))
        if not _sample:
            raise ValueError("Probe returned no rows; verify the source and filters before saving")
        print("POMPOS_PROBE_RESULT=" + json.dumps({"rows": _sample, "sample_count": len(_sample)}, default=str))
    else:
        import dlt
        _config = json.loads(os.environ["POMPOS_CONFIG"])
        _path = str(Path(_config["destination"]).resolve())
        Path(_path).parent.mkdir(parents=True, exist_ok=True)
        _pipeline = dlt.pipeline(pipeline_name="pompos_" + Path(__file__).stem,
            pipelines_dir=str(Path(__file__).parent / ".dlt"),
            destination=dlt.destinations.duckdb(credentials=_path), dataset_name="main")
        _resource = dlt.resource(_rows(), name=_config["table"], table_name=_config["table"],
            max_table_nesting=0, write_disposition=_config["strategy"],
            primary_key=_config.get("primary_key") or None)
        print(_pipeline.run(_resource))
`
}
