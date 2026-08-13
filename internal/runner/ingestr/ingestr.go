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
	"path/filepath"
	"strings"
	"time"

	"pompos/internal/ingestion"
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

func BuildArgs(item ingestion.Ingestion) ([]string, error) {
	if item.Destination.Type != "duckdb" {
		return nil, fmt.Errorf("unsupported destination type %q", item.Destination.Type)
	}
	if item.Destination.Path == "" || item.Destination.Table == "" {
		return nil, errors.New("destination path and destination table are required")
	}

	var sourceURI, sourceTable string
	switch item.Source.Type {
	case "csv":
		if item.Source.URL == "" {
			return nil, errors.New("source URL is required")
		}
		sourceURI, sourceTable = item.Source.URL, "data#csv"
	case "github":
		if item.Source.Owner == "" || item.Source.Repository == "" || item.Source.Table == "" {
			return nil, errors.New("GitHub owner, repository, and source table are required")
		}
		if item.Source.AccessToken == "" {
			return nil, errors.New("GitHub access token is required because ingestr reads GitHub through the authenticated GraphQL API")
		}
		query := url.Values{"owner": {item.Source.Owner}, "repo": {item.Source.Repository}}
		query.Set("access_token", item.Source.AccessToken)
		sourceURI, sourceTable = "github://?"+query.Encode(), item.Source.Table
	default:
		return nil, fmt.Errorf("unsupported source type %q", item.Source.Type)
	}

	return []string{
		"ingest",
		"--source-uri", sourceURI,
		"--source-table", sourceTable,
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

	started := time.Now()
	r.logf("ingestr starting ingestion_id=%s source=%s source_table=%s destination_table=%s binary=%s",
		item.ID, sourceDescription(item.Source), sourceTable(item.Source), item.Destination.Table, r.Binary)

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, r.Binary, args...)
	cmd.Stdout = r.outputWriter(&stdout, item, "stdout")
	cmd.Stderr = r.outputWriter(&stderr, item, "stderr")
	err = cmd.Run()
	if err == nil {
		r.logf("ingestr succeeded ingestion_id=%s destination_table=%s duration=%s",
			item.ID, item.Destination.Table, time.Since(started).Round(time.Millisecond))
		return nil
	}

	exitCode := -1
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		exitCode = exitError.ExitCode()
	}
	r.logf("ingestr failed ingestion_id=%s destination_table=%s exit_code=%d duration=%s error=%q",
		item.ID, item.Destination.Table, exitCode, time.Since(started).Round(time.Millisecond), err)
	return &ExecutionError{
		ExitCode: exitCode,
		Stdout:   truncate(stdout.String()),
		Stderr:   truncate(stderr.String()),
		Err:      err,
	}
}

func (r Runner) outputWriter(buffer *bytes.Buffer, item ingestion.Ingestion, stream string) io.Writer {
	if r.Logger == nil {
		return buffer
	}
	return io.MultiWriter(buffer, &logWriter{
		logger: r.Logger,
		prefix: fmt.Sprintf("ingestr %s ingestion_id=%s table=%s", stream, item.ID, item.Destination.Table),
		secret: item.Source.AccessToken,
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

func sourceDescription(source ingestion.Source) string {
	if source.Type == "github" {
		return "github:" + source.Owner + "/" + source.Repository
	}
	return "csv:" + source.URL
}

func sourceTable(source ingestion.Source) string {
	if source.Type == "github" {
		return source.Table
	}
	return "data#csv"
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
