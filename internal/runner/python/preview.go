package python

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/destination"
)

// TablePreview contains display values from a fixed, bounded table read.
type TablePreview struct {
	Columns   []string   `json:"columns"`
	Rows      [][]string `json:"rows"`
	TotalRows int64      `json:"total_rows"`
	HasMore   bool       `json:"has_more"`
	Truncated bool       `json:"truncated"`
}

const PreviewUnavailable = "Preview unavailable. The table may not have been loaded yet, or the database may be busy. Try again after the ingestion finishes."

//go:embed preview.py
var previewScript string

var previewTableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Preview runs only Pompos's built-in reader, never the ingestion script or SQL
// supplied by a user/model. The destination comes from the saved ingestion.
func (r Runner) Preview(ctx context.Context, plan compiler.ExecutionPlan) (TablePreview, error) {
	var result TablePreview
	schema := destination.SchemaName(plan.DestinationSchema)
	if !previewTableName.MatchString(schema) {
		return result, fmt.Errorf("invalid preview schema name")
	}
	if !previewTableName.MatchString(plan.DestinationObject) {
		return result, fmt.Errorf("invalid preview table name")
	}
	if plan.DestinationType != "" && plan.DestinationType != "duckdb" {
		return result, fmt.Errorf("preview requires a DuckDB destination")
	}
	if _, err := os.Stat(plan.DestinationPath); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	binary := r.Binary
	if binary == "" {
		binary = "python3"
	}
	cmd := exec.CommandContext(ctx, binary, "-c", previewScript, plan.DestinationPath, schema, plan.DestinationObject)
	cmd.Env = []string{"DLT_TELEMETRY=0"}
	for _, key := range []string{"PATH", "LANG", "SYSTEMROOT", "TMPDIR"} {
		if value, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	cmd.WaitDelay = time.Second
	output := cappedOutput{limit: 1024 * 1024}
	var stderr cappedOutput
	cmd.Stdout, cmd.Stderr = &output, &stderr
	if err := cmd.Run(); err != nil {
		return result, fmt.Errorf("read table preview: %w", err)
	}
	if err := json.Unmarshal([]byte(output.String()), &result); err != nil {
		return TablePreview{}, fmt.Errorf("decode table preview: %w", err)
	}
	// Redact values before storing validation previews or returning destination rows.
	for _, ref := range plan.SecretRefs {
		if r.Secrets == nil {
			return TablePreview{}, fmt.Errorf("preview secret store unavailable")
		}
		value, err := r.Secrets.Get(ctx, ref)
		if err != nil {
			return TablePreview{}, fmt.Errorf("load preview secret %q: %w", ref, err)
		}
		if len(value) == 0 {
			continue
		}
		redact := func(cell string) string {
			cell = strings.ReplaceAll(cell, string(value), "[REDACTED]")
			return strings.ReplaceAll(cell, url.QueryEscape(string(value)), "[REDACTED]")
		}
		for i := range result.Columns {
			result.Columns[i] = redact(result.Columns[i])
		}
		for _, row := range result.Rows {
			for i := range row {
				row[i] = redact(row[i])
			}
		}
	}
	for _, row := range result.Rows {
		for i, cell := range row {
			if chars := []rune(cell); len(chars) > 1000 {
				row[i] = string(chars[:1000]) + "…"
				result.Truncated = true
			}
		}
	}
	return result, nil
}
