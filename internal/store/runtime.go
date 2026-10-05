package store

import (
	"context"
	"database/sql"
	"errors"
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

func (s *SQLite) SaveWorkerCount(ctx context.Context, workers int) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO runtime_settings (id, workers) VALUES (1, ?)
ON CONFLICT(id) DO UPDATE SET workers = excluded.workers`, workers)
	return err
}
