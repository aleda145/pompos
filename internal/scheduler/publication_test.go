package scheduler

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/store"
)

func TestPublicationCannotOverlapRunningWork(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "metadata.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	item := ingestion.Ingestion{ID: "rows", Name: "Rows", Source: ingestion.Source{Type: "python", URL: "fixture"}, Destination: ingestion.Destination{Type: "duckdb", Path: filepath.Join(dir, "out.duckdb"), Table: "rows"}}
	persistSpec(t, dir, &item)
	if err := db.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	m, err := New(log.New(io.Discard, "", 0), db, func(context.Context, ingestion.Run) error { close(started); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(ctx)
	if err := m.Enqueue(ctx, item.ID); err != nil {
		t.Fatal(err)
	}
	go func() { m.poll(ctx); close(finished) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("run never started")
	}
	err = m.WithPublication(func() error { t.Error("publication overlapped running code"); return nil })
	close(release)
	<-finished
	if err == nil {
		t.Fatal("running work did not block publication")
	}
	if err := m.WithPublication(func() error { return nil }); err != nil {
		t.Fatal("idle publication blocked", err)
	}
}
