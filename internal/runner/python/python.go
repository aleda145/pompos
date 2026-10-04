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
	"pompos/internal/destination"
	"pompos/internal/secrets"
)

type Runner struct {
	Environments *Environments
	Binary       string
	Secrets      secrets.Store
}

func (r Runner) Run(ctx context.Context, plan compiler.ExecutionPlan) error {
	if plan.Engine != "python" {
		return fmt.Errorf("unsupported runtime engine %q", plan.Engine)
	}
	_, err := r.Execute(ctx, plan, false)
	return err
}
func (r Runner) Execute(ctx context.Context, plan compiler.ExecutionPlan, probe bool) (string, error) {
	return r.execute(ctx, plan, probe, 0, false)
}

// Validate loads the selected sample twice into a disposable database. Zero selects all items.
func (r Runner) Validate(ctx context.Context, plan compiler.ExecutionPlan, limit int) (ValidationResult, string, error) {
	var result ValidationResult
	if limit < 0 || plan.ValidationTimeoutSeconds < 0 {
		return result, "", fmt.Errorf("validation limit and timeout must be nonnegative")
	}
	if plan.DestinationType == "objects" {
		var err error
		plan.ValidationMaxBytes, plan.ValidationTimeoutSeconds, err = ObjectValidationBudget(plan.ValidationMaxBytes, plan.ValidationTimeoutSeconds)
		if err != nil {
			return result, "", err
		}
	}
	plan, err := r.Prepare(ctx, plan)
	if err != nil {
		return result, "", err
	}
	directory, err := os.MkdirTemp("", "pompos-validation-*")
	if err != nil {
		return result, "", err
	}
	defer os.RemoveAll(directory)
	data, err := os.ReadFile(plan.Script)
	if err != nil {
		return result, "", err
	}
	plan.Script = filepath.Join(directory, "validate.py")
	if err = os.WriteFile(plan.Script, data, 0600); err != nil {
		return result, "", err
	}
	plan.DestinationPath = filepath.Join(directory, "sample.duckdb")
	if plan.DestinationType == "objects" {
		plan.DestinationPath = filepath.Join(directory, "objects")
	}
	output, err := r.execute(ctx, plan, false, limit, true)
	if err != nil {
		return result, output, err
	}
	if err = ReadResult(output, "POMPOS_VALIDATION_RESULT=", &result); err != nil {
		return result, output, err
	}
	expected := result.SampleCount
	if plan.Strategy == "append" {
		expected *= 2
	}
	if result.SampleCount < 1 || (limit > 0 && result.SampleCount > limit) || result.FirstLoadRows != result.SampleCount || result.SecondLoadRows != expected {
		return result, output, fmt.Errorf("validation returned inconsistent load counts")
	}
	preview, previewErr := r.Preview(ctx, plan)
	if previewErr != nil {
		result.PreviewError = "Validation passed, but its table preview is unavailable."
	} else {
		result.Preview = &preview
	}
	return result, output, nil
}

type ValidationResult struct {
	Data            string        `json:"data,omitempty"`
	DownloadedBytes int64         `json:"downloaded_bytes,omitempty"`
	SampleCount     int           `json:"sample_count"`
	FirstLoadRows   int           `json:"first_load_rows"`
	SecondLoadRows  int           `json:"second_load_rows"`
	Preview         *TablePreview `json:"preview,omitempty"`
	PreviewError    string        `json:"preview_error,omitempty"`
}
type ProbeResult struct {
	Rows  []map[string]any `json:"rows"`
	Count int              `json:"sample_count"`
}

func ReadResult(output, marker string, result any) error {
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, marker) {
			return json.Unmarshal([]byte(strings.TrimPrefix(line, marker)), result)
		}
	}
	return fmt.Errorf("Python exited without a %s result", strings.TrimSuffix(marker, "="))
}

func (r Runner) execute(ctx context.Context, plan compiler.ExecutionPlan, probe bool, validationLimit int, validation bool) (string, error) {
	plan, err := r.Prepare(ctx, plan)
	if err != nil {
		return "", err
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
	config, _ := json.Marshal(map[string]any{"destination_type": plan.DestinationType, "destination": plan.DestinationPath, "schema": destination.SchemaName(plan.DestinationSchema), "table": plan.DestinationObject, "strategy": plan.Strategy, "primary_key": plan.PrimaryKey, "validation": validation, "validation_limit": validationLimit, "validation_max_bytes": plan.ValidationMaxBytes})
	timeout := 30 * time.Minute
	if validation {
		timeout = 0
		// Durations beyond time.Duration's range must not wrap into an immediate timeout.
		if int64(plan.ValidationTimeoutSeconds) <= (1<<63-1)/int64(time.Second) {
			timeout = time.Duration(plan.ValidationTimeoutSeconds) * time.Second
		}
	}
	if probe {
		timeout = 45 * time.Second
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	binary := plan.PythonBinary
	script, err := filepath.Abs(plan.Script)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, binary, "-I", "-u", script)
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
	output := cappedOutput{tail: plan.DestinationType == "objects"}
	cmd.Stdout = &output
	cmd.Stderr = &output
	err = cmd.Run()
	result := output.String()
	probeValid := !probe
	if probe {
		var sample ProbeResult
		probeValid = ReadResult(result, "POMPOS_PROBE_RESULT=", &sample) == nil && sample.Count > 0 && sample.Count <= 5 && len(sample.Rows) == sample.Count
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
	if plan.DestinationType == "objects" && !probe && !validation {
		var summary struct {
			Objects int `json:"objects"`
		}
		if err := ReadResult(result, "POMPOS_OBJECTS_RESULT=", &summary); err != nil {
			return result, err
		}
	}
	return result, nil
}

type cappedOutput struct {
	sync.Mutex
	data  []byte
	limit int
	tail  bool
}

func (b *cappedOutput) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	n := len(p)
	limit := b.limit
	if limit == 0 {
		limit = 16384
	}
	if b.tail {
		if len(p) >= limit {
			b.data = append(b.data[:0], p[len(p)-limit:]...)
		} else {
			if extra := len(b.data) + len(p) - limit; extra > 0 {
				b.data = b.data[extra:]
			}
			b.data = append(b.data, p...)
		}
		return n, nil
	}
	remaining := limit - len(b.data)
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
func Wrap(code string) string { return WrapWithRuntime(code, "", nil) }

func WrapWithRuntime(code, python string, dependencies []string) string {
	return ScriptMetadata(python, dependencies) + code + `

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
    _config = json.loads(os.environ.get("POMPOS_CONFIG", "{}"))
    if _config.get("destination_type") not in (None, "", "duckdb"):
        raise ValueError("Row ingestion requires a DuckDB destination")
    _validation_limit = _config.get("validation_limit", 0)
    _limit = 5 if _probe else (_validation_limit or None)
    def _rows():
        for row in fetch(secret, _limit):
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
        _path = str(Path(_config["destination"]).resolve())
        Path(_path).parent.mkdir(parents=True, exist_ok=True)
        _pipeline = dlt.pipeline(pipeline_name="pompos_" + Path(__file__).stem,
            pipelines_dir=str(Path(__file__).parent / ".dlt"),
            destination=dlt.destinations.duckdb(credentials=_path), dataset_name=_config.get("schema") or "main")
        def _load(rows):
            _resource = dlt.resource(rows, name=_config["table"], table_name=_config["table"],
                max_table_nesting=0, write_disposition=_config["strategy"],
                primary_key=_config.get("primary_key") or None)
            _info = _pipeline.run(_resource)
            _info.raise_on_failed_jobs()
            return _info
        if _config.get("validation", bool(_validation_limit)):
            _sample = list(itertools.islice(_rows(), _validation_limit or None))
            if not _sample:
                raise ValueError("Validation returned no rows")
            _keys = _config.get("primary_key") or []
            if _keys:
                _seen = set()
                for _row in _sample:
                    if any(_row.get(key) is None for key in _keys):
                        raise ValueError("A row key is missing or null in the validation sample")
                    _key = json.dumps([_row[key] for key in _keys], sort_keys=True, default=str)
                    if _key in _seen:
                        raise ValueError("Duplicate row keys in the validation sample")
                    _seen.add(_key)
            # Load the same source sample twice; fetch is called only once.
            import copy, duckdb
            def _count():
                with duckdb.connect(_path, read_only=True) as _db:
                    _table = _pipeline.default_schema.naming.normalize_table_identifier(_config["table"])
                    _schema = _pipeline.dataset_name
                    return _db.execute('SELECT count(*) FROM "' + _schema.replace('"', '""') + '"."' + _table.replace('"', '""') + '"').fetchone()[0]
            _load(copy.deepcopy(_sample))
            _first = _count()
            _load(copy.deepcopy(_sample))
            _second = _count()
            _expected = len(_sample) * (2 if _config["strategy"] == "append" else 1)
            if _first != len(_sample) or _second != _expected:
                raise ValueError("Unexpected row counts after loading the sample twice")
            print("POMPOS_VALIDATION_RESULT=" + json.dumps({"sample_count": len(_sample), "first_load_rows": _first, "second_load_rows": _second}))
        else:
            print(_load(_rows()))
`
}
