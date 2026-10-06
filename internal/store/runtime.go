package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"pompos/internal/runner"
)

// WorkerCount returns zero until the operator saves a runtime setting.
func (s *SQLite) WorkerCount(ctx context.Context) (int, error) {
	var workers int
	err := s.db.QueryRowContext(ctx, `SELECT workers FROM runtime_settings WHERE id = 1`).Scan(&workers)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return workers, err
}

func (s *SQLite) RunTimeoutMinutes(ctx context.Context) (int, error) {
	var minutes int
	err := s.db.QueryRowContext(ctx, `SELECT minutes FROM runtime_run_timeout WHERE id = 1`).Scan(&minutes)
	if errors.Is(err, sql.ErrNoRows) {
		return runner.DefaultRunTimeoutMinutes, nil
	}
	return minutes, err
}

func (s *SQLite) SaveRuntimeSettings(ctx context.Context, workers, minutes int) error {
	if workers < 1 {
		return fmt.Errorf("invalid worker count")
	}
	if minutes < 1 || minutes > runner.MaxRunTimeoutMinutes {
		return fmt.Errorf("invalid run timeout")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_settings (id, workers) VALUES (1, ?)
ON CONFLICT(id) DO UPDATE SET workers = excluded.workers`, workers); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_run_timeout (id, minutes) VALUES (1, ?)
ON CONFLICT(id) DO UPDATE SET minutes = excluded.minutes`, minutes); err != nil {
		return err
	}
	return tx.Commit()
}
