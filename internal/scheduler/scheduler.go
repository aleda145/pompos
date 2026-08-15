package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/jonboulle/clockwork"

	"pompos/internal/ingestion"
	"pompos/internal/spec"
)

const (
	pollInterval = 5 * time.Second
	claimTimeout = time.Hour
)

type RunFunc func(context.Context, ingestion.Run) error

type Store interface {
	Get(context.Context, string) (ingestion.Ingestion, error)
	List(context.Context) ([]ingestion.Ingestion, error)
	UpdateNextRun(context.Context, string, *time.Time) error
	UpdateSpecReference(context.Context, string, string, string) error
	EnqueueRun(context.Context, string, time.Time) error
	EnqueueScheduledRun(context.Context, string, time.Time, time.Time) (bool, error)
	ClaimRun(context.Context, time.Time, time.Time) (ingestion.Run, bool, error)
	FinishRun(context.Context, int64, string) error
	ReleaseRun(context.Context, int64) error
	RecoverRuns(context.Context) (int64, error)
}

// Manager uses gocron to drive a poller and calculate cron occurrences. Both
// manual and scheduled executable work live in SQLite.
type Manager struct {
	scheduler gocron.Scheduler
	store     Store
	logger    *log.Logger
	run       RunFunc
	now       func() time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	pollMu    sync.Mutex
	wake      chan struct{}
}

func New(logger *log.Logger, store Store, run RunFunc) (*Manager, error) {
	if logger == nil || store == nil || run == nil {
		return nil, fmt.Errorf("scheduler logger, store, and run function are required")
	}
	s, err := gocron.NewScheduler(
		gocron.WithLocation(time.UTC),
		gocron.WithLimitConcurrentJobs(1, gocron.LimitModeWait),
	)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{scheduler: s, store: store, logger: logger, run: run, now: time.Now, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1)}
	if _, err := s.NewJob(
		gocron.DurationJob(pollInterval),
		gocron.NewTask(func(ctx context.Context) { m.poll(ctx) }),
		gocron.WithName("durable-run-poller"),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	); err != nil {
		cancel()
		_ = s.Shutdown()
		return nil, fmt.Errorf("register schedule poller: %w", err)
	}
	return m, nil
}

func (m *Manager) Validate(expression string) error {
	if expression == "" {
		return nil
	}
	if len(strings.Fields(expression)) != 5 {
		return fmt.Errorf("invalid cron schedule: enter five fields (minute hour day-of-month month day-of-week)")
	}
	if _, err := nextRun(expression, m.now().UTC()); err != nil {
		return fmt.Errorf("invalid cron schedule: %w", err)
	}
	return nil
}

// Upsert persists the schedule clock. Request handlers do not own any jobs.
func (m *Manager) Upsert(item ingestion.Ingestion) error {
	if err := m.Validate(item.Schedule); err != nil {
		return err
	}
	var next *time.Time
	if item.Schedule != "" {
		value, err := nextRun(item.Schedule, m.now().UTC())
		if err != nil {
			return err
		}
		next = &value
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.store.UpdateNextRun(ctx, item.ID, next); err != nil {
		return fmt.Errorf("persist schedule: %w", err)
	}
	if next == nil {
		m.logger.Printf("schedule disabled ingestion_id=%s", item.ID)
	} else {
		m.logger.Printf("schedule persisted ingestion_id=%s schedule=%q next_run_at=%s timezone=UTC", item.ID, item.Schedule, next.Format(time.RFC3339))
	}
	return nil
}

func (m *Manager) NextRun(ingestionID string) *time.Time {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	item, err := m.store.Get(ctx, ingestionID)
	if err != nil {
		return nil
	}
	return item.NextRun
}

// Enqueue records the run before waking the worker. Once this returns, the run
// survives request cancellation and process restarts.
func (m *Manager) Enqueue(ctx context.Context, ingestionID string) error {
	if err := m.store.EnqueueRun(ctx, ingestionID, m.now().UTC()); err != nil {
		return fmt.Errorf("enqueue ingestion: %w", err)
	}
	m.logger.Printf("manual run enqueued ingestion_id=%s", ingestionID)
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}

func (m *Manager) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	recovered, err := m.store.RecoverRuns(ctx)
	if err != nil {
		return fmt.Errorf("recover interrupted ingestion runs: %w", err)
	}
	if recovered > 0 {
		m.logger.Printf("interrupted ingestion runs recovered count=%d", recovered)
	}
	m.scheduler.Start()
	go m.wakeLoop()
	select {
	case m.wake <- struct{}{}:
	default:
	}
	m.logger.Printf("durable run worker started poll_interval=%s", pollInterval)
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.cancel()
	return m.scheduler.ShutdownWithContext(ctx)
}

func (m *Manager) wakeLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
			m.poll(m.ctx)
		}
	}
}

func (m *Manager) poll(ctx context.Context) {
	if !m.pollMu.TryLock() {
		return
	}
	defer m.pollMu.Unlock()
	if err := m.enqueueDue(ctx); err != nil {
		m.logger.Printf("schedule poll failed error=%q", err)
		return
	}
	for {
		now := m.now().UTC()
		run, ok, err := m.store.ClaimRun(ctx, now, now.Add(-claimTimeout))
		if err != nil {
			m.logger.Printf("ingestion run claim failed error=%q", err)
			return
		}
		if !ok {
			return
		}
		m.executeClaim(ctx, run)
	}
}

func (m *Manager) enqueueDue(ctx context.Context) error {
	items, err := m.store.List(ctx)
	if err != nil {
		return fmt.Errorf("list schedules: %w", err)
	}
	now := m.now().UTC()
	for _, item := range items {
		document, data, err := spec.Read(item.SpecPath)
		if err != nil {
			m.logger.Printf("schedule spec load failed ingestion_id=%s spec_path=%s error=%q", item.ID, item.SpecPath, err)
			continue
		}
		digest := spec.Digest(data)
		if digest != item.SpecDigest {
			if err := m.store.UpdateSpecReference(ctx, item.ID, item.SpecPath, digest); err != nil {
				return fmt.Errorf("refresh spec reference %s: %w", item.ID, err)
			}
			item.NextRun = nil
		}
		item.Schedule = ""
		if document.Schedule != nil {
			item.Schedule = document.Schedule.Cron
		}
		if item.Schedule == "" {
			if item.NextRun != nil {
				if err := m.store.UpdateNextRun(ctx, item.ID, nil); err != nil {
					return fmt.Errorf("disable schedule %s: %w", item.ID, err)
				}
			}
			continue
		}
		if item.NextRun == nil {
			next, err := nextRun(item.Schedule, now)
			if err != nil {
				m.logger.Printf("schedule invalid during reconciliation ingestion_id=%s error=%q", item.ID, err)
				continue
			}
			if err := m.store.UpdateNextRun(ctx, item.ID, &next); err != nil {
				return fmt.Errorf("initialize schedule %s: %w", item.ID, err)
			}
			continue
		}
		if item.NextRun.After(now) {
			continue
		}
		next, err := nextRun(item.Schedule, now)
		if err != nil {
			m.logger.Printf("schedule invalid while due ingestion_id=%s error=%q", item.ID, err)
			continue
		}
		enqueued, err := m.store.EnqueueScheduledRun(ctx, item.ID, item.NextRun.UTC(), next)
		if err != nil {
			return fmt.Errorf("enqueue schedule %s: %w", item.ID, err)
		}
		if enqueued {
			m.logger.Printf("scheduled run enqueued ingestion_id=%s scheduled_for=%s next_run_at=%s", item.ID, item.NextRun.UTC().Format(time.RFC3339), next.Format(time.RFC3339))
		}
	}
	return nil
}

func (m *Manager) executeClaim(ctx context.Context, queued ingestion.Run) {
	started := m.now()
	m.logger.Printf("ingestion run claimed run_id=%d ingestion_id=%s trigger=%s queued_for=%s attempt=%d", queued.ID, queued.IngestionID, queued.Trigger, queued.ScheduledFor.UTC().Format(time.RFC3339), queued.Attempts)
	runError := m.run(ctx, queued)
	if runError != nil && ctx.Err() != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.store.ReleaseRun(releaseCtx, queued.ID); err != nil {
			m.logger.Printf("interrupted ingestion run release failed run_id=%d ingestion_id=%s error=%q", queued.ID, queued.IngestionID, err)
		} else {
			m.logger.Printf("ingestion run interrupted and released run_id=%d ingestion_id=%s", queued.ID, queued.IngestionID)
		}
		return
	}
	message := ""
	if runError != nil {
		message = runError.Error()
		m.logger.Printf("ingestion run failed run_id=%d ingestion_id=%s trigger=%s duration=%s error=%q", queued.ID, queued.IngestionID, queued.Trigger, m.now().Sub(started).Round(time.Millisecond), runError)
	} else {
		m.logger.Printf("ingestion run completed run_id=%d ingestion_id=%s trigger=%s duration=%s", queued.ID, queued.IngestionID, queued.Trigger, m.now().Sub(started).Round(time.Millisecond))
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.store.FinishRun(finishCtx, queued.ID, message); err != nil {
		m.logger.Printf("ingestion run completion persistence failed run_id=%d ingestion_id=%s error=%q", queued.ID, queued.IngestionID, err)
	}
}

func nextRun(expression string, after time.Time) (time.Time, error) {
	clock := clockwork.NewFakeClockAt(after.UTC())
	s, err := gocron.NewScheduler(gocron.WithLocation(time.UTC), gocron.WithClock(clock))
	if err != nil {
		return time.Time{}, fmt.Errorf("create cron calculator: %w", err)
	}
	defer s.Shutdown()
	job, err := s.NewJob(gocron.CronJob(expression, false), gocron.NewTask(func() {}))
	if err != nil {
		return time.Time{}, err
	}
	s.Start()
	next, err := job.NextRun()
	if err != nil {
		return time.Time{}, err
	}
	return next.UTC(), nil
}
