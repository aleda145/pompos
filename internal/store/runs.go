package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"pompos/internal/ingestion"
)

const runColumns = `id, ingestion_id, trigger, scheduled_for, attempts, spec_path, spec_digest, status, claimed_at, finished_at, last_error`

type RunQueue struct {
	Total int
	Runs  []ingestion.Run
}

type RunHistory struct {
	Runs  []ingestion.Run
	Total int
	Page  int
	Pages int
}

// RunHistory returns ten completed runs, newest first, from one snapshot.
func (s *SQLite) RunHistory(ctx context.Context, page int) (RunHistory, error) {
	var history RunHistory
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return history, err
	}
	defer tx.Rollback()
	const completed = `status IN ('succeeded', 'failed', 'cancelled')`
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM ingestion_runs WHERE `+completed).Scan(&history.Total); err != nil {
		return history, err
	}
	history.Pages = max(1, (history.Total+9)/10)
	history.Page = min(max(1, page), history.Pages)
	rows, err := tx.QueryContext(ctx, `SELECT `+runColumns+` FROM ingestion_runs WHERE `+completed+` ORDER BY id DESC LIMIT 10 OFFSET ?`, (history.Page-1)*10)
	if err != nil {
		return history, err
	}
	defer rows.Close()
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return history, err
		}
		history.Runs = append(history.Runs, run)
	}
	return history, rows.Err()
}

// RunQueue returns the next 100 pending runs and the full queue count from one snapshot.
func (s *SQLite) RunQueue(ctx context.Context) (RunQueue, error) {
	var queue RunQueue
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queue, err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM ingestion_runs WHERE status = 'pending'`).Scan(&queue.Total); err != nil {
		return queue, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+runColumns+` FROM ingestion_runs WHERE status = 'pending' ORDER BY scheduled_for, id LIMIT 100`)
	if err != nil {
		return queue, err
	}
	defer rows.Close()
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return queue, err
		}
		queue.Runs = append(queue.Runs, run)
	}
	return queue, rows.Err()
}

func (s *SQLite) ListRuns(ctx context.Context, id string, before int64, limit int) ([]ingestion.Run, error) {
	if limit < 1 || limit > 100 {
		limit = 25
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM ingestion_runs
WHERE ingestion_id = ? AND (? = 0 OR id < ?) ORDER BY id DESC LIMIT ?`, id, before, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []ingestion.Run{}
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *SQLite) GetRun(ctx context.Context, ingestionID string, id int64) (ingestion.Run, error) {
	run, err := scanRun(s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM ingestion_runs WHERE ingestion_id = ? AND id = ?`, ingestionID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return run, ErrNotFound
	}
	if err != nil {
		return run, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT output, truncated FROM ingestion_run_logs WHERE run_id = ?`, id).Scan(&run.Log, &run.LogTruncated)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return run, err
}

func (s *SQLite) HasActiveRuns(ctx context.Context, id string) (bool, error) {
	var active bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ingestion_runs WHERE ingestion_id = ? AND status IN ('pending', 'running'))`, id).Scan(&active)
	return active, err
}

// Bound each durable log while retaining the most recent output. Logs are
// already redacted by the runner before they reach storage.
func (s *SQLite) AppendRunLog(ctx context.Context, id int64, output string) error {
	output = strings.ToValidUTF8(strings.ReplaceAll(output, "\x00", ""), "�")
	_, err := s.db.ExecContext(ctx, `INSERT INTO ingestion_run_logs (run_id, output, truncated)
SELECT id, substr(?, -65536), length(?) > 65536 FROM ingestion_runs WHERE id = ?
ON CONFLICT(run_id) DO UPDATE SET
output = substr(ingestion_run_logs.output || excluded.output, -65536),
truncated = ingestion_run_logs.truncated OR excluded.truncated OR length(ingestion_run_logs.output || excluded.output) > 65536`, output, output, id)
	return err
}

func scanRun(row interface{ Scan(...any) error }) (ingestion.Run, error) {
	var run ingestion.Run
	var scheduled string
	var started, finished sql.NullString
	if err := row.Scan(&run.ID, &run.IngestionID, &run.Trigger, &scheduled, &run.Attempts, &run.SpecPath, &run.SpecDigest, &run.Status, &started, &finished, &run.LastError); err != nil {
		return run, err
	}
	var err error
	if run.ScheduledFor, err = time.Parse(time.RFC3339Nano, scheduled); err != nil {
		return run, fmt.Errorf("parse run time: %w", err)
	}
	for _, field := range []struct {
		value  sql.NullString
		target **time.Time
	}{{started, &run.StartedAt}, {finished, &run.FinishedAt}} {
		if field.value.Valid {
			value, err := time.Parse(time.RFC3339Nano, field.value.String)
			if err != nil {
				return run, fmt.Errorf("parse run time: %w", err)
			}
			*field.target = &value
		}
	}
	return run, nil
}
