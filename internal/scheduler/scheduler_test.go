package scheduler

import (
	"context"
	"io"
	"log"
	"testing"

	"pompos/internal/ingestion"
)

func TestValidateAndUpsert(t *testing.T) {
	manager, err := New(log.New(io.Discard, "", 0), func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	if err := manager.Validate("not a cron"); err == nil {
		t.Fatal("invalid cron was accepted")
	}
	item := ingestion.Ingestion{ID: "abc", Schedule: "*/5 * * * *"}
	if err := manager.Upsert(item); err != nil {
		t.Fatal(err)
	}
	if len(manager.jobs) != 1 {
		t.Fatalf("jobs = %d", len(manager.jobs))
	}
	item.Schedule = ""
	if err := manager.Upsert(item); err != nil {
		t.Fatal(err)
	}
	if len(manager.jobs) != 0 {
		t.Fatalf("jobs after disable = %d", len(manager.jobs))
	}
}
