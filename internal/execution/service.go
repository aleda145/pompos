package execution

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/ingestion"
	"pompos/internal/runner"
	"pompos/internal/spec"
)

type Store interface {
	Get(context.Context, string) (ingestion.Ingestion, error)
	MarkRunning(context.Context, string, time.Time) error
	Finish(context.Context, string, string, string) error
	AppendRunLog(context.Context, int64, string) error
}

// Service executes ingestions independently of any transport such as HTTP.
type Service struct {
	Store  Store
	Runner runner.Runner
	Logger *log.Logger
	gate   chan struct{}
}

func New(service Service) (*Service, error) {
	if service.Store == nil || service.Runner == nil {
		return nil, errors.New("execution service dependencies must not be nil")
	}
	if service.Logger == nil {
		service.Logger = log.Default()
	}
	service.gate = make(chan struct{}, 1)
	return &service, nil
}

func (s *Service) Run(ctx context.Context, queued ingestion.Run) (runErr error) {
	ctx = runner.WithLog(ctx, func(output string) {
		if queued.ID == 0 {
			return
		}
		logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Store.AppendRunLog(logCtx, queued.ID, output); err != nil {
			s.Logger.Printf("persist run log run_id=%d error=%q", queued.ID, err)
		}
	})
	runner.Log(ctx, fmt.Sprintf("%s Starting attempt %d\n", time.Now().UTC().Format("15:04:05"), queued.Attempts))
	defer func() {
		if value := recover(); value != nil {
			runErr = fmt.Errorf("ingestion panicked: %v", value)
			s.Logger.Printf("ingestion panic ingestion_id=%s error=%q stack=%s", queued.IngestionID, runErr, debug.Stack())
			runErr = errors.Join(runErr, s.finish(queued.IngestionID, ingestion.StatusFailed, runErr.Error()))
		}
		message := "SUCCESS"
		if runErr != nil {
			message = "FAILED: " + runErr.Error()
		}
		if ctx.Err() != nil {
			message = "Interrupted; queued for retry"
		}
		runner.Log(ctx, fmt.Sprintf("\n%s %s\n", time.Now().UTC().Format("15:04:05"), message))
	}()
	item, err := s.Store.Get(ctx, queued.IngestionID)
	if err != nil {
		return err
	}
	item.SpecPath, item.SpecDigest = queued.SpecPath, queued.SpecDigest
	if err := s.execute(ctx, item); err != nil {
		return err
	}
	updated, err := s.Store.Get(ctx, queued.IngestionID)
	if err != nil {
		return err
	}
	if updated.Status == ingestion.StatusFailed {
		return fmt.Errorf("ingestion failed: %s", updated.LastError)
	}
	return nil
}

func (s *Service) execute(ctx context.Context, item ingestion.Ingestion) error {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	started := time.Now()
	runner.Log(ctx, time.Now().UTC().Format("15:04:05")+" Loading YAML\n")
	document, _, err := spec.Read(item.SpecPath)
	if err != nil {
		s.Logger.Printf("spec load failed ingestion_id=%s spec_path=%s error=%q", item.ID, item.SpecPath, err)
		return s.finish(item.ID, ingestion.StatusFailed, err.Error())
	}

	plan, err := compiler.Compile(document)
	if err != nil {
		return s.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	s.Logger.Printf("marking ingestion running ingestion_id=%s destination_table=%s", item.ID, plan.DestinationObject)
	runner.Log(ctx, fmt.Sprintf("%s Loading %s · %s\n", time.Now().UTC().Format("15:04:05"), plan.DestinationObject, plan.Strategy))
	if err := s.Store.MarkRunning(ctx, item.ID, time.Now()); err != nil {
		s.Logger.Printf("failed to mark ingestion running ingestion_id=%s error=%q", item.ID, err)
		return err
	}
	if err := s.Runner.Run(ctx, plan); err != nil {
		s.Logger.Printf("ingestion failed ingestion_id=%s duration=%s error=%q", item.ID, time.Since(started).Round(time.Millisecond), err)
		return s.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	s.Logger.Printf("ingestion succeeded ingestion_id=%s duration=%s", item.ID, time.Since(started).Round(time.Millisecond))
	return s.finish(item.ID, ingestion.StatusSucceeded, "")
}

func (s *Service) finish(id, status, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Store.Finish(ctx, id, status, message)
}
