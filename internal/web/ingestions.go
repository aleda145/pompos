package web

import (
	"context"
	"errors"
	"os"
	"strings"

	"pompos/internal/agent"
	"pompos/internal/ingestion"
	"pompos/internal/scheduler"
	"pompos/internal/spec"
	"pompos/internal/store"
)

// Publication is shared by the browser and MCP, including scheduler registration
// and retrying a partially completed save with the same artifact ID.
func (a *App) publishSession(ctx context.Context, id string) (string, error) {
	return a.Agent.Publish(ctx, id, a.SpecDir, func(ingestionID string, doc spec.Ingestion) error {
		data, e := spec.Marshal(doc)
		if e != nil {
			return e
		}
		path := spec.ArtifactPath(a.SpecDir, ingestionID, ".yaml")
		item := spec.ToProjection(doc, ingestionID, path, spec.Digest(data))
		if err := a.Scheduler.Validate(item.Schedule); err != nil {
			return err
		}
		if existing, err := a.Store.Get(ctx, ingestionID); err == nil {
			return a.Scheduler.Upsert(existing)
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if e = agent.WriteFile(path, data); e != nil {
			return e
		}
		if e = a.Store.Create(ctx, item); e != nil {
			_ = os.Remove(path)
			return e
		}
		return a.Scheduler.Upsert(item)
	})
}

func (a *App) getIngestion(ctx context.Context, id string) (ingestion.Ingestion, error) {
	item, err := a.Store.Get(ctx, id)
	if err != nil {
		return item, err
	}
	item, _ = a.hydrate(item)
	return item, nil
}

func (a *App) ingestionList(ctx context.Context) ([]ingestion.Ingestion, error) {
	items, err := a.Store.List(ctx)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i], _ = a.hydrate(items[i])
	}
	return items, nil
}

func (a *App) queueIngestion(ctx context.Context, id string) error {
	item, err := a.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	document, data, err := spec.Read(item.SpecPath)
	if err != nil {
		return err
	}
	if document.Schedule != nil {
		if err := scheduler.ValidateCron(document.Schedule.Cron); err != nil {
			return err
		}
	}
	digest := spec.Digest(data)
	if digest != item.SpecDigest {
		if err := a.Store.UpdateSpecReference(ctx, item.ID, item.SpecPath, digest); err != nil {
			return err
		}
	}
	if err := a.Scheduler.Enqueue(ctx, item.ID); err != nil {
		return err
	}
	a.Logger.Printf("rerun enqueued ingestion_id=%s", item.ID)
	return nil
}

func (a *App) setIngestionSchedule(ctx context.Context, id, schedule string) error {
	schedule = strings.TrimSpace(schedule)
	if err := a.Scheduler.Validate(schedule); err != nil {
		return err
	}
	item, err := a.getIngestion(ctx, id)
	if err != nil {
		return err
	}
	if item.LoadError != "" {
		return errors.New(item.LoadError)
	}
	previous := item.Schedule
	item.Schedule = schedule
	if _, err := spec.Write(a.SpecDir, item); err != nil {
		return err
	}
	path := spec.ArtifactPath(a.SpecDir, item.ID, ".yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := a.Store.UpdateSpecReference(ctx, item.ID, path, spec.Digest(data)); err != nil {
		return err
	}
	if err := a.Scheduler.Upsert(item); err != nil {
		item.Schedule = previous
		_, _ = spec.Write(a.SpecDir, item)
		_ = a.Scheduler.Upsert(item)
		return err
	}
	a.Logger.Printf("schedule updated ingestion_id=%s schedule=%q timezone=UTC", item.ID, schedule)
	return nil
}
