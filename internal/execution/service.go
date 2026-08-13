package execution

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"pompos/internal/ingestion"
	"pompos/internal/policy"
	"pompos/internal/runner"
	"pompos/internal/secrets"
)

type Store interface {
	Get(context.Context, string) (ingestion.Ingestion, error)
	MarkRunning(context.Context, string, time.Time) error
	Finish(context.Context, string, string, string) error
}

// Service executes ingestions independently of any transport such as HTTP.
type Service struct {
	Store   Store
	Policy  policy.Engine
	Runner  runner.Runner
	Secrets secrets.Store
	Logger  *log.Logger
	gate    chan struct{}
}

func New(service Service) (*Service, error) {
	if service.Store == nil || service.Policy == nil || service.Runner == nil || service.Secrets == nil {
		return nil, errors.New("execution service dependencies must not be nil")
	}
	if service.Logger == nil {
		service.Logger = log.Default()
	}
	service.gate = make(chan struct{}, 1)
	return &service, nil
}

func (s *Service) Run(ctx context.Context, id string) error {
	item, err := s.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.Execute(ctx, item); err != nil {
		return err
	}
	updated, err := s.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if updated.Status == ingestion.StatusFailed {
		return fmt.Errorf("ingestion failed: %s", updated.LastError)
	}
	return nil
}

func (s *Service) Execute(ctx context.Context, item ingestion.Ingestion) error {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	started := time.Now()
	resolved, err := s.resolveSecrets(ctx, item)
	if err != nil {
		s.Logger.Printf("secret resolution failed ingestion_id=%s secret_key=%s error=%q", item.ID, item.Source.SecretKey, err)
		return s.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	item = resolved
	if err := s.Policy.Validate(item); err != nil {
		s.Logger.Printf("run policy failed ingestion_id=%s error=%q", item.ID, err)
		return s.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	s.Logger.Printf("marking ingestion running ingestion_id=%s destination_table=%s", item.ID, item.Destination.Table)
	if err := s.Store.MarkRunning(ctx, item.ID, time.Now()); err != nil {
		s.Logger.Printf("failed to mark ingestion running ingestion_id=%s error=%q", item.ID, err)
		return err
	}
	if err := s.Runner.Run(ctx, item); err != nil {
		s.Logger.Printf("ingestion failed ingestion_id=%s duration=%s error=%q", item.ID, time.Since(started).Round(time.Millisecond), err)
		return s.finish(item.ID, ingestion.StatusFailed, err.Error())
	}
	s.Logger.Printf("ingestion succeeded ingestion_id=%s duration=%s", item.ID, time.Since(started).Round(time.Millisecond))
	return s.finish(item.ID, ingestion.StatusSucceeded, "")
}

func (s *Service) resolveSecrets(ctx context.Context, item ingestion.Ingestion) (ingestion.Ingestion, error) {
	if item.Source.Type != "github" || item.Source.AccessToken != "" {
		return item, nil
	}
	if item.Source.SecretKey == "" {
		return item, errors.New("GitHub ingestion has no saved token reference")
	}
	value, err := s.Secrets.Get(ctx, item.Source.SecretKey)
	if errors.Is(err, secrets.ErrNotFound) {
		return item, errors.New("the saved GitHub token no longer exists")
	}
	if err != nil {
		return item, fmt.Errorf("load GitHub token: %w", err)
	}
	item.Source.AccessToken = string(value)
	return item, nil
}

func (s *Service) finish(id, status, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Store.Finish(ctx, id, status, message)
}
