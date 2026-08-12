package ingestr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"pompos/internal/ingestion"
)

const maxOutput = 8 * 1024

type Runner struct {
	Binary string
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

func BuildArgs(item ingestion.Ingestion) ([]string, error) {
	if item.Source.Type != "csv" {
		return nil, fmt.Errorf("unsupported source type %q", item.Source.Type)
	}
	if item.Destination.Type != "duckdb" {
		return nil, fmt.Errorf("unsupported destination type %q", item.Destination.Type)
	}
	if item.Source.URL == "" || item.Destination.Path == "" || item.Destination.Table == "" {
		return nil, errors.New("source URL, destination path, and destination table are required")
	}

	return []string{
		"ingest",
		"--source-uri", item.Source.URL,
		"--source-table", "data#csv",
		"--dest-uri", duckDBURI(item.Destination.Path),
		"--dest-table", item.Destination.Table,
		"--schema-naming", "direct",
	}, nil
}

func (r Runner) Run(ctx context.Context, item ingestion.Ingestion) error {
	args, err := BuildArgs(item)
	if err != nil {
		return err
	}

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, r.Binary, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err == nil {
		return nil
	}

	exitCode := -1
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		exitCode = exitError.ExitCode()
	}
	return &ExecutionError{
		ExitCode: exitCode,
		Stdout:   truncate(stdout.String()),
		Stderr:   truncate(stderr.String()),
		Err:      err,
	}
}

func duckDBURI(path string) string {
	absolute := filepath.IsAbs(path)
	path = filepath.ToSlash(path)
	if absolute {
		return "duckdb://" + path
	}
	return "duckdb://" + strings.TrimPrefix(path, "./")
}

func truncate(value string) string {
	if len(value) <= maxOutput {
		return value
	}
	return value[:maxOutput] + "\n[output truncated]"
}
