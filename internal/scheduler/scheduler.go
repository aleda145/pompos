package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"

	"pompos/internal/ingestion"
)

type RunFunc func(context.Context, string) error

type Manager struct {
	scheduler gocron.Scheduler
	logger    *log.Logger
	run       RunFunc
	mu        sync.Mutex
	jobs      map[string]uuid.UUID
}

func New(logger *log.Logger, run RunFunc) (*Manager, error) {
	if logger == nil || run == nil {
		return nil, fmt.Errorf("scheduler logger and run function are required")
	}
	s, err := gocron.NewScheduler(
		gocron.WithLocation(time.UTC),
		gocron.WithLimitConcurrentJobs(1, gocron.LimitModeWait),
	)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}
	return &Manager{scheduler: s, logger: logger, run: run, jobs: make(map[string]uuid.UUID)}, nil
}

func (m *Manager) Validate(expression string) error {
	if expression == "" {
		return nil
	}
	if len(strings.Fields(expression)) != 5 {
		return fmt.Errorf("invalid cron schedule: enter five fields (minute hour day-of-month month day-of-week)")
	}
	temporary, err := gocron.NewScheduler(gocron.WithLocation(time.UTC))
	if err != nil {
		return fmt.Errorf("create cron validator: %w", err)
	}
	defer temporary.Shutdown()
	if _, err := temporary.NewJob(gocron.CronJob(expression, false), gocron.NewTask(func() {})); err != nil {
		return fmt.Errorf("invalid cron schedule: %w", err)
	}
	return nil
}

func (m *Manager) Load(items []ingestion.Ingestion) error {
	for _, item := range items {
		if err := m.Upsert(item); err != nil {
			return fmt.Errorf("load schedule for %s: %w", item.ID, err)
		}
	}
	return nil
}

func (m *Manager) Upsert(item ingestion.Ingestion) error {
	if err := m.Validate(item.Schedule); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	hadJob := false
	if jobID, ok := m.jobs[item.ID]; ok {
		hadJob = true
		if err := m.scheduler.RemoveJob(jobID); err != nil {
			return fmt.Errorf("remove previous schedule: %w", err)
		}
		delete(m.jobs, item.ID)
	}
	if item.Schedule == "" {
		if hadJob {
			m.logger.Printf("schedule disabled ingestion_id=%s", item.ID)
		}
		return nil
	}
	job, err := m.scheduler.NewJob(
		gocron.CronJob(item.Schedule, false),
		gocron.NewTask(func(ctx context.Context) {
			started := time.Now()
			m.logger.Printf("scheduled ingestion triggered ingestion_id=%s schedule=%q", item.ID, item.Schedule)
			if err := m.run(ctx, item.ID); err != nil {
				m.logger.Printf("scheduled ingestion failed ingestion_id=%s duration=%s error=%q", item.ID, time.Since(started).Round(time.Millisecond), err)
				return
			}
			m.logger.Printf("scheduled ingestion completed ingestion_id=%s duration=%s", item.ID, time.Since(started).Round(time.Millisecond))
		}),
		gocron.WithName("ingestion-"+item.ID),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return fmt.Errorf("register cron schedule: %w", err)
	}
	m.jobs[item.ID] = job.ID()
	m.logger.Printf("schedule registered ingestion_id=%s schedule=%q timezone=UTC", item.ID, item.Schedule)
	return nil
}

func (m *Manager) NextRun(ingestionID string) *time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	jobID, ok := m.jobs[ingestionID]
	if !ok {
		return nil
	}
	for _, job := range m.scheduler.Jobs() {
		if job.ID() == jobID {
			next, err := job.NextRun()
			if err == nil && !next.IsZero() {
				next = next.UTC()
				return &next
			}
		}
	}
	return nil
}

func (m *Manager) Start() { m.scheduler.Start() }

func (m *Manager) Shutdown(ctx context.Context) error {
	return m.scheduler.ShutdownWithContext(ctx)
}
