package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
)

var ErrSessionBusy = errors.New("this conversation is already working")

type ChatStatus struct {
	Session  Session `json:"session"`
	Running  bool    `json:"running"`
	Activity Event   `json:"activity"`
	Error    string  `json:"error,omitempty"`
}

type chatJob struct {
	snapshot []byte
	cancel   context.CancelFunc
	error    string
}

// Mutations serialize per conversation and fail immediately when it is busy.
// Reads use atomic file snapshots and never wait for model or tool execution.
func (s *Service) beginSession(id string) (func(), error) {
	if _, err := s.path(id); err != nil {
		return nil, err
	}
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.closing {
		return nil, errors.New("agent is shutting down")
	}
	if s.busy[id] {
		return nil, ErrSessionBusy
	}
	if s.busy == nil {
		s.busy = make(map[string]bool)
	}
	s.busy[id] = true
	return func() {
		s.workMu.Lock()
		delete(s.busy, id)
		s.workMu.Unlock()
	}, nil
}

// StartTurn returns after reserving the conversation. Browser disconnection
// does not cancel the work; Shutdown owns cancellation of background turns.
func (s *Service) StartTurn(id string, input Input) (ChatStatus, error) {
	release, err := s.beginSession(id)
	if err != nil {
		return ChatStatus{}, err
	}
	started := false
	defer func() {
		if !started {
			release()
		}
	}()
	v, err := s.LoadWebChat(id)
	if err != nil {
		return ChatStatus{}, err
	}
	cfg, err := s.Settings()
	if err != nil {
		return ChatStatus{}, err
	}
	if err := cfg.ValidateAgent(); err != nil {
		return ChatStatus{}, err
	}
	if input.ActionID == "" && (len(input.Message) == 0 || len(input.Message) > 16000) {
		return ChatStatus{}, errors.New("message must contain 1–16000 characters")
	}
	status := ChatStatus{Session: v, Running: true, Activity: Event{Type: "thinking"}}
	data, err := json.Marshal(status)
	if err != nil {
		return ChatStatus{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	job := &chatJob{snapshot: data, cancel: cancel}
	s.workMu.Lock()
	if s.closing {
		s.workMu.Unlock()
		cancel()
		return ChatStatus{}, errors.New("agent is shutting down")
	}
	if s.chatJobs == nil {
		s.chatJobs = make(map[string]*chatJob)
	}
	s.chatJobs[id] = job
	s.chatWorkers.Add(1)
	s.workMu.Unlock()
	started = true
	go func() {
		var turnErr, snapshotErr error
		defer s.chatWorkers.Done()
		defer release()
		defer cancel()
		defer func() {
			if value := recover(); value != nil {
				log.Printf("agent turn panic chat_id=%s panic=%v stack=%s", id, value, debug.Stack())
				turnErr = errors.New("agent turn failed unexpectedly; continue from the last saved step")
				if saved, err := s.load(id); err == nil {
					v = saved
				}
			}
			turnErr = errors.Join(turnErr, snapshotErr, s.save(v))
			s.workMu.Lock()
			defer s.workMu.Unlock()
			// Completed histories live on disk, not in the background-job registry.
			job.snapshot, job.cancel = nil, nil
			if turnErr != nil {
				job.error = turnErr.Error()
			}
		}()
		v, turnErr = s.turnWithEvents(ctx, id, input, func(current Session, event Event) {
			data, err := json.Marshal(ChatStatus{Session: current, Running: true, Activity: event})
			if err != nil {
				snapshotErr = err
				cancel()
				return
			}
			s.workMu.Lock()
			job.snapshot = data
			s.workMu.Unlock()
		})
	}()
	return status, nil
}

func (s *Service) ChatStatus(id string) (ChatStatus, error) {
	if _, err := s.path(id); err != nil {
		return ChatStatus{}, err
	}
	s.workMu.Lock()
	var snapshot []byte
	var message string
	if job := s.chatJobs[id]; job != nil {
		snapshot, message = job.snapshot, job.error
	}
	s.workMu.Unlock()
	if snapshot != nil {
		var status ChatStatus
		if err := json.Unmarshal(snapshot, &status); err != nil {
			return status, fmt.Errorf("read chat progress: %w", err)
		}
		return status, nil
	}
	v, err := s.LoadWebChat(id)
	return ChatStatus{Session: v, Error: message}, err
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.workMu.Lock()
	s.closing = true
	for _, job := range s.chatJobs {
		if job.cancel != nil {
			job.cancel()
		}
	}
	s.workMu.Unlock()
	done := make(chan struct{})
	go func() {
		s.chatWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
