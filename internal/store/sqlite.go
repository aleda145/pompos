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
)

var ErrNotFound = errors.New("ingestion not found")

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
	const schema = `
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS ingestions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    csv_url TEXT NOT NULL,
    destination_table TEXT NOT NULL,
    status TEXT NOT NULL,
    last_run_at TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize metadata database: %w", err)
	}
	columns := []struct{ name, definition string }{
		{"source_type", "TEXT NOT NULL DEFAULT 'csv'"},
		{"source_owner", "TEXT NOT NULL DEFAULT ''"},
		{"source_repository", "TEXT NOT NULL DEFAULT ''"},
		{"source_token", "TEXT NOT NULL DEFAULT ''"},
		{"source_table", "TEXT NOT NULL DEFAULT ''"},
	}
	for _, column := range columns {
		if err := s.addColumn(ctx, column.name, column.definition); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLite) addColumn(ctx context.Context, name, definition string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(ingestions)`)
	if err != nil {
		return fmt.Errorf("inspect metadata schema: %w", err)
	}
	found := false
	for rows.Next() {
		var cid int
		var columnName, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return fmt.Errorf("scan metadata schema: %w", err)
		}
		found = found || columnName == name
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close metadata schema rows: %w", err)
	}
	if found {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE ingestions ADD COLUMN `+name+` `+definition); err != nil {
		return fmt.Errorf("add metadata column %s: %w", name, err)
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) Create(ctx context.Context, item ingestion.Ingestion) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO ingestions (
    id, name, csv_url, destination_table, status, last_error,
    source_type, source_owner, source_repository, source_token, source_table
)
VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.Source.URL, item.Destination.Table, item.Status,
		item.Source.Type, item.Source.Owner, item.Source.Repository, item.Source.AccessToken, item.Source.Table)
	if err != nil {
		return fmt.Errorf("create ingestion metadata: %w", err)
	}
	return nil
}

func (s *SQLite) Get(ctx context.Context, id string) (ingestion.Ingestion, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, name, csv_url, destination_table, status, last_run_at, last_error,
       source_type, source_owner, source_repository, source_token, source_table
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
SELECT id, name, csv_url, destination_table, status, last_run_at, last_error,
       source_type, source_owner, source_repository, source_token, source_table
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
	err := row.Scan(&item.ID, &item.Name, &item.Source.URL, &item.Destination.Table, &item.Status, &lastRun, &item.LastError,
		&item.Source.Type, &item.Source.Owner, &item.Source.Repository, &item.Source.AccessToken, &item.Source.Table)
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
	return item, nil
}
