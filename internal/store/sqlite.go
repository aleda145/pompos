package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"pompos/internal/ingestion"
	"pompos/internal/secrets"
)

var ErrNotFound = errors.New("ingestion not found")

const schemaVersion = 4

type SQLite struct {
	db              *sql.DB
	destinationPath string
}

func Open(ctx context.Context, path, destinationPath string) (*SQLite, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create metadata directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open metadata database: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &SQLite{db: db, destinationPath: destinationPath}
	if err := store.initialize(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLite) initialize(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read metadata schema version: %w", err)
	}
	if version != schemaVersion {
		if _, err := s.db.ExecContext(ctx, `DROP TABLE IF EXISTS ingestion_runs; DROP TABLE IF EXISTS scheduled_runs; DROP TABLE IF EXISTS ingestions; DROP TABLE IF EXISTS secrets;`); err != nil {
			return fmt.Errorf("reset incompatible metadata schema: %w", err)
		}
	}
	const schema = `
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS ingestions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    csv_url TEXT NOT NULL,
    destination_table TEXT NOT NULL,
    status TEXT NOT NULL,
    last_run_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    schedule TEXT NOT NULL DEFAULT '',
    next_run_at TEXT,
    source_type TEXT NOT NULL,
    source_owner TEXT NOT NULL DEFAULT '',
    source_repository TEXT NOT NULL DEFAULT '',
    source_secret_key TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS secrets (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingestion_id TEXT NOT NULL,
    trigger TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL,
    claimed_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (ingestion_id, trigger, scheduled_for)
);
CREATE INDEX IF NOT EXISTS ingestion_runs_claim
    ON ingestion_runs (status, scheduled_for);
PRAGMA user_version = 4;`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize metadata database: %w", err)
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) Create(ctx context.Context, item ingestion.Ingestion) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO ingestions (
    id, name, csv_url, destination_table, status, last_error, schedule,
    source_type, source_owner, source_repository, source_secret_key, source_table
)
VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.Source.URL, item.Destination.Table, item.Status, item.Schedule,
		item.Source.Type, item.Source.Owner, item.Source.Repository, item.Source.SecretKey, item.Source.Table)
	if err != nil {
		return fmt.Errorf("create ingestion metadata: %w", err)
	}
	return nil
}

func (s *SQLite) Get(ctx context.Context, id string) (ingestion.Ingestion, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, name, csv_url, destination_table, status, last_run_at, last_error, schedule, next_run_at,
       source_type, source_owner, source_repository, source_secret_key, source_table
FROM ingestions WHERE id = ?`, id)
	item, err := s.scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return ingestion.Ingestion{}, ErrNotFound
	}
	if err != nil {
		return ingestion.Ingestion{}, fmt.Errorf("get ingestion metadata: %w", err)
	}
	return item, nil
}

func (s *SQLite) List(ctx context.Context) ([]ingestion.Ingestion, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, csv_url, destination_table, status, last_run_at, last_error, schedule, next_run_at,
       source_type, source_owner, source_repository, source_secret_key, source_table
FROM ingestions ORDER BY name, id`)
	if err != nil {
		return nil, fmt.Errorf("list ingestion metadata: %w", err)
	}
	defer rows.Close()

	var items []ingestion.Ingestion
	for rows.Next() {
		item, err := s.scan(rows)
		if err != nil {
			return nil, fmt.Errorf("scan ingestion metadata: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ingestion metadata: %w", err)
	}
	return items, nil
}

func (s *SQLite) MarkRunning(ctx context.Context, id string, at time.Time) error {
	return s.update(ctx, `UPDATE ingestions SET status = ?, last_run_at = ?, last_error = '' WHERE id = ?`, ingestion.StatusRunning, at.UTC().Format(time.RFC3339Nano), id)
}

func (s *SQLite) Finish(ctx context.Context, id, status, lastError string) error {
	return s.update(ctx, `UPDATE ingestions SET status = ?, last_error = ? WHERE id = ?`, status, lastError, id)
}

func (s *SQLite) UpdateSchedule(ctx context.Context, id, schedule string, nextRun *time.Time) error {
	var value any
	if nextRun != nil {
		value = nextRun.UTC().Format(time.RFC3339Nano)
	}
	return s.update(ctx, `UPDATE ingestions SET schedule = ?, next_run_at = ? WHERE id = ?`, schedule, value, id)
}

// EnqueueRun durably records a manual run and marks the ingestion pending.
func (s *SQLite) EnqueueRun(ctx context.Context, ingestionID string, queuedAt time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin run enqueue: %w", err)
	}
	defer tx.Rollback()
	value := queuedAt.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO ingestion_runs (ingestion_id, trigger, scheduled_for, status, created_at)
VALUES (?, 'manual', ?, 'pending', ?)`, ingestionID, value, value); err != nil {
		return fmt.Errorf("enqueue ingestion run: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE ingestions SET status = ?, last_error = '' WHERE id = ?`, ingestion.StatusPending, ingestionID)
	if err != nil {
		return fmt.Errorf("mark queued ingestion pending: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read queued ingestion update count: %w", err)
	}
	if updated == 0 {
		return ErrNotFound
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit run enqueue: %w", err)
	}
	return nil
}

// EnqueueScheduledRun atomically advances an ingestion's clock and records the
// due run. A false result means another worker already advanced it.
func (s *SQLite) EnqueueScheduledRun(ctx context.Context, ingestionID string, scheduledFor, nextRun time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin schedule enqueue: %w", err)
	}
	defer tx.Rollback()
	scheduledValue := scheduledFor.UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO ingestion_runs (ingestion_id, trigger, scheduled_for, status, created_at)
VALUES (?, 'scheduled', ?, 'pending', ?)`, ingestionID, scheduledValue, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, fmt.Errorf("enqueue scheduled run: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read scheduled run insert count: %w", err)
	}
	if inserted == 0 {
		return false, nil
	}
	result, err = tx.ExecContext(ctx, `
UPDATE ingestions SET next_run_at = ?
WHERE id = ? AND schedule <> '' AND next_run_at = ?`, nextRun.UTC().Format(time.RFC3339Nano), ingestionID, scheduledValue)
	if err != nil {
		return false, fmt.Errorf("advance ingestion schedule: %w", err)
	}
	advanced, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read schedule advance count: %w", err)
	}
	if advanced == 0 {
		return false, nil
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit schedule enqueue: %w", err)
	}
	return true, nil
}

func (s *SQLite) ClaimRun(ctx context.Context, now, staleBefore time.Time) (ingestion.Run, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ingestion.Run{}, false, fmt.Errorf("begin run claim: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `
SELECT id, ingestion_id, trigger, scheduled_for, attempts
FROM ingestion_runs
WHERE status = 'pending' OR (status = 'running' AND claimed_at <= ?)
ORDER BY scheduled_for, id LIMIT 1`, staleBefore.UTC().Format(time.RFC3339Nano))
	var run ingestion.Run
	var scheduledFor string
	if err := row.Scan(&run.ID, &run.IngestionID, &run.Trigger, &scheduledFor, &run.Attempts); errors.Is(err, sql.ErrNoRows) {
		return ingestion.Run{}, false, nil
	} else if err != nil {
		return ingestion.Run{}, false, fmt.Errorf("select run: %w", err)
	}
	run.ScheduledFor, err = time.Parse(time.RFC3339Nano, scheduledFor)
	if err != nil {
		return ingestion.Run{}, false, fmt.Errorf("parse run time: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE ingestion_runs SET status = 'running', claimed_at = ?, attempts = attempts + 1
WHERE id = ? AND (status = 'pending' OR (status = 'running' AND claimed_at <= ?))`,
		now.UTC().Format(time.RFC3339Nano), run.ID, staleBefore.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return ingestion.Run{}, false, fmt.Errorf("claim run: %w", err)
	}
	claimed, err := result.RowsAffected()
	if err != nil {
		return ingestion.Run{}, false, fmt.Errorf("read run claim count: %w", err)
	}
	if claimed == 0 {
		return ingestion.Run{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return ingestion.Run{}, false, fmt.Errorf("commit run claim: %w", err)
	}
	run.Attempts++
	return run, true, nil
}

func (s *SQLite) FinishRun(ctx context.Context, id int64, runError string) error {
	status := "succeeded"
	if runError != "" {
		status = "failed"
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE ingestion_runs SET status = ?, last_error = ?, finished_at = ? WHERE id = ?`,
		status, runError, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return fmt.Errorf("finish run: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read finished run count: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) ReleaseRun(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE ingestion_runs SET status = 'pending', claimed_at = NULL WHERE id = ? AND status = 'running'`, id)
	if err != nil {
		return fmt.Errorf("release run: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read released run count: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) RecoverRuns(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE ingestion_runs SET status = 'pending', claimed_at = NULL WHERE status = 'running'`)
	if err != nil {
		return 0, fmt.Errorf("recover interrupted runs: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read recovered run count: %w", err)
	}
	return count, nil
}

func (s *SQLite) update(ctx context.Context, query string, args ...any) error {
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update ingestion metadata: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read updated row count: %w", err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

type scanner interface {
	Scan(...any) error
}

func (s *SQLite) scan(row scanner) (ingestion.Ingestion, error) {
	var item ingestion.Ingestion
	var lastRun sql.NullString
	var nextRun sql.NullString
	err := row.Scan(&item.ID, &item.Name, &item.Source.URL, &item.Destination.Table, &item.Status, &lastRun, &item.LastError, &item.Schedule, &nextRun,
		&item.Source.Type, &item.Source.Owner, &item.Source.Repository, &item.Source.SecretKey, &item.Source.Table)
	if err != nil {
		return ingestion.Ingestion{}, err
	}
	item.Destination.Type = "duckdb"
	item.Destination.Path = s.destinationPath
	if lastRun.Valid {
		parsed, err := time.Parse(time.RFC3339Nano, lastRun.String)
		if err != nil {
			return ingestion.Ingestion{}, fmt.Errorf("parse last run timestamp: %w", err)
		}
		item.LastRun = &parsed
	}
	if nextRun.Valid {
		parsed, err := time.Parse(time.RFC3339Nano, nextRun.String)
		if err != nil {
			return ingestion.Ingestion{}, fmt.Errorf("parse next run timestamp: %w", err)
		}
		item.NextRun = &parsed
	}
	return item, nil
}

func (s *SQLite) Secrets() secrets.Store { return sqliteSecrets{db: s.db} }

type sqliteSecrets struct{ db *sql.DB }

func (s sqliteSecrets) Put(ctx context.Context, key string, value []byte) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO secrets (key, value, created_at, updated_at) VALUES (?, ?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, key, value, now, now)
	if err != nil {
		return fmt.Errorf("store secret: %w", err)
	}
	return nil
}

func (s sqliteSecrets) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM secrets WHERE key = ?`, key).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, secrets.ErrNotFound
		}
		return nil, fmt.Errorf("get secret: %w", err)
	}
	return value, nil
}

func (s sqliteSecrets) List(ctx context.Context) ([]secrets.Entry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, created_at, updated_at FROM secrets ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	defer rows.Close()
	var entries []secrets.Entry
	for rows.Next() {
		var entry secrets.Entry
		if err := rows.Scan(&entry.Key, &entry.CreatedAt, &entry.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan secret metadata: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate secrets: %w", err)
	}
	return entries, nil
}

func (s sqliteSecrets) Delete(ctx context.Context, key string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM secrets WHERE key = ?`, key); err != nil {
		return fmt.Errorf("delete secret: %w", err)
	}
	return nil
}
