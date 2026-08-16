package ingestr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"pompos/internal/compiler"
)

const maxOutput = 8 * 1024

type Runner struct {
	Binary string
	Logger *log.Logger
}

type ExecutionError struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Err      error
}

func (e *ExecutionError) Error() string {
	detail := strings.TrimSpace(e.Stderr)
	if detail == "" {
		detail = strings.TrimSpace(e.Stdout)
	}
	if detail == "" {
		detail = e.Err.Error()
	}
	if e.ExitCode >= 0 {
		return fmt.Sprintf("ingestr exited with code %d: %s", e.ExitCode, detail)
	}
	return fmt.Sprintf("could not execute ingestr: %s", detail)
}

func (e *ExecutionError) Unwrap() error { return e.Err }

func BuildArgs(plan compiler.ExecutionPlan, credentialValue string) ([]string, error) {
	if plan.Engine != "ingestr" {
		return nil, fmt.Errorf("unsupported engine %q", plan.Engine)
	}
	if plan.SourceURI == "" || plan.SourceTable == "" || plan.DestinationURI == "" || plan.DestinationObject == "" {
		return nil, errors.New("compiled plan is incomplete")
	}
	sourceURI := plan.SourceURI
	if plan.CredentialRef != "" {
		if credentialValue == "" {
			return nil, errors.New("GitHub access token is required because ingestr reads GitHub through the authenticated GraphQL API")
		}
		parsed, err := url.Parse(sourceURI)
		if err != nil {
			return nil, fmt.Errorf("invalid compiled source URI: %w", err)
		}
		query := parsed.Query()
		query.Set("access_token", credentialValue)
		parsed.RawQuery = query.Encode()
		if parsed.Scheme == "github" && parsed.Host == "" {
			sourceURI = "github://?" + query.Encode()
		} else {
			sourceURI = parsed.String()
		}
	}
	args := []string{
		"ingest",
		"--source-uri", sourceURI,
		"--source-table", plan.SourceTable,
		"--dest-uri", plan.DestinationURI,
		"--dest-table", plan.DestinationObject,
		"--schema-naming", plan.SchemaNaming,
		"--incremental-strategy", plan.Strategy,
	}
	for _, key := range plan.PrimaryKey {
		args = append(args, "--primary-key", key)
	}
	if plan.IncrementalKey != "" {
		args = append(args, "--incremental-key", plan.IncrementalKey)
	}
	return args, nil
}

func (r Runner) Run(ctx context.Context, runID string, plan compiler.ExecutionPlan, credentialValue string) error {
	args, err := BuildArgs(plan, credentialValue)
	if err != nil {
		return err
	}

	started := time.Now()
	r.logf("ingestr starting ingestion_id=%s source=%s source_table=%s destination_table=%s binary=%s",
		runID, plan.SourceURI, plan.SourceTable, plan.DestinationObject, r.Binary)

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, r.Binary, args...)
	cmd.Stdout = r.outputWriter(&stdout, runID, plan, credentialValue, "stdout")
	cmd.Stderr = r.outputWriter(&stderr, runID, plan, credentialValue, "stderr")
	err = cmd.Run()
	if err == nil {
		r.logf("ingestr succeeded ingestion_id=%s destination_table=%s duration=%s",
			runID, plan.DestinationObject, time.Since(started).Round(time.Millisecond))
		return nil
	}

	exitCode := -1
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		exitCode = exitError.ExitCode()
	}
	r.logf("ingestr failed ingestion_id=%s destination_table=%s exit_code=%d duration=%s error=%q",
		runID, plan.DestinationObject, exitCode, time.Since(started).Round(time.Millisecond), err)
	return &ExecutionError{
		ExitCode: exitCode,
		Stdout:   truncate(redact(stdout.String(), credentialValue)),
		Stderr:   truncate(redact(stderr.String(), credentialValue)),
		Err:      err,
	}
}

func redact(value, secret string) string {
	if secret == "" {
		return value
	}
	value = strings.ReplaceAll(value, secret, "[REDACTED]")
	return strings.ReplaceAll(value, url.QueryEscape(secret), "[REDACTED]")
}

func (r Runner) outputWriter(buffer *bytes.Buffer, runID string, plan compiler.ExecutionPlan, credentialValue, stream string) io.Writer {
	if r.Logger == nil {
		return buffer
	}
	return io.MultiWriter(buffer, &logWriter{
		logger: r.Logger,
		prefix: fmt.Sprintf("ingestr %s ingestion_id=%s table=%s", stream, runID, plan.DestinationObject),
		secret: credentialValue,
	})
}

func (r Runner) logf(format string, values ...any) {
	if r.Logger != nil {
		r.Logger.Printf(format, values...)
	}
}

type logWriter struct {
	logger *log.Logger
	prefix string
	secret string
}

func (w *logWriter) Write(data []byte) (int, error) {
	message := strings.TrimSpace(string(data))
	if w.secret != "" {
		message = strings.ReplaceAll(message, w.secret, "[REDACTED]")
		message = strings.ReplaceAll(message, url.QueryEscape(w.secret), "[REDACTED]")
	}
	if message != "" {
		for _, line := range strings.Split(strings.ReplaceAll(message, "\r", "\n"), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				w.logger.Printf("%s %s", w.prefix, line)
			}
		}
	}
	return len(data), nil
}

func truncate(value string) string {
	if len(value) <= maxOutput {
		return value
	}
	return value[:maxOutput] + "\n[output truncated]"
}
