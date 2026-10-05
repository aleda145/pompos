package scheduler

import (
	"context"
	"fmt"
	"maps"
	"time"

	"pompos/internal/ingestion"
)

type Worker struct {
	ID       int
	Run      ingestion.Run
	Draining bool
}

type Runtime struct {
	Status         string
	StartedAt      time.Time
	LastPoll       time.Time
	PollInterval   time.Duration
	PollError      string
	ScheduleErrors map[string]string
	Workers        []Worker
	Active         int
	WorkerLimit    int
}

// SetWorkers persists the limit before applying it. Active runs always finish.
func (m *Manager) SetWorkers(ctx context.Context, workers int) error {
	if workers < 1 {
		return fmt.Errorf("worker count must be a positive whole number")
	}
	// Serialize setting changes with claiming work, not with running work.
	m.pollMu.Lock()
	if err := m.store.SaveWorkerCount(ctx, workers); err != nil {
		m.pollMu.Unlock()
		return fmt.Errorf("save worker setting: %w", err)
	}
	m.resizeWorkers(workers)
	m.pollMu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
	m.logger.Printf("worker limit changed workers=%d", workers)
	return nil
}

func (m *Manager) resizeWorkers(workers int) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	if workers > len(m.workerRuns) {
		m.workerRuns = append(m.workerRuns, make([]ingestion.Run, workers-len(m.workerRuns))...)
	}
	m.workerLimit = workers
}

// The dispatcher holds pollMu, so the chosen slot cannot be resized or claimed
// elsewhere before assignment. Retiring workers still count toward the limit.
func (m *Manager) availableWorker() int {
	m.statusMu.RLock()
	defer m.statusMu.RUnlock()
	active, available := 0, -1
	for index, run := range m.workerRuns {
		if run.ID != 0 {
			active++
		} else if index < m.workerLimit && available < 0 {
			available = index
		}
	}
	if active >= m.workerLimit {
		return -1
	}
	return available
}

// Runtime copies live worker assignments without holding up the dispatcher.
func (m *Manager) Runtime() Runtime {
	m.statusMu.RLock()
	defer m.statusMu.RUnlock()
	view := Runtime{
		Status: ingestion.StatusPending, StartedAt: m.startedAt,
		LastPoll: m.lastPoll, PollInterval: pollInterval, PollError: m.pollError,
		ScheduleErrors: maps.Clone(m.reconcileErrors),
		WorkerLimit:    m.workerLimit,
	}
	if !m.startedAt.IsZero() {
		view.Status = ingestion.StatusRunning
	}
	if m.ctx.Err() != nil {
		view.Status = "cancelled"
	} else if m.pollError != "" {
		view.Status = ingestion.StatusFailed
	}
	for index, run := range m.workerRuns {
		if index >= m.workerLimit && run.ID == 0 {
			continue
		}
		view.Workers = append(view.Workers, Worker{ID: index + 1, Run: run, Draining: index >= m.workerLimit})
		if run.ID != 0 {
			view.Active++
		}
	}
	return view
}
