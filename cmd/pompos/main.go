package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"pompos/internal/config"
	"pompos/internal/execution"
	"pompos/internal/ingestion"
	"pompos/internal/policy"
	runneringestr "pompos/internal/runner/ingestr"
	"pompos/internal/scheduler"
	"pompos/internal/store"
	"pompos/internal/validation"
	"pompos/internal/web"
)

func main() {
	logger := log.New(os.Stdout, "pompos: ", log.LstdFlags)
	cfg := config.Load()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		logger.Fatal(err)
	}
	defer listener.Close()

	metadata, err := store.Open(context.Background(), cfg.MetadataPath, cfg.Destination.Path)
	if err != nil {
		logger.Fatal(err)
	}
	defer metadata.Close()

	policyEngine := policy.DefaultEngine{DestinationPath: cfg.Destination.Path}
	ingestionRunner := runneringestr.Runner{Binary: cfg.Runner.Binary, Logger: logger}
	secretStore := metadata.Secrets()
	executor, err := execution.New(execution.Service{
		Store: metadata, Policy: policyEngine, Runner: ingestionRunner,
		Secrets: secretStore, Logger: logger,
	})
	if err != nil {
		logger.Fatal(err)
	}
	scheduleManager, err := scheduler.New(logger, metadata, executor.Run)
	if err != nil {
		logger.Fatal(err)
	}
	app, err := web.New(web.App{
		Store:     metadata,
		Policy:    policyEngine,
		Secrets:   secretStore,
		Scheduler: scheduleManager,
		Validator: validation.HTTPSourceValidator{Client: &http.Client{
			Timeout: cfg.RequestTimeout,
		}},
		Destination: ingestion.Destination{Type: cfg.Destination.Type, Path: cfg.Destination.Path},
		SpecDir:     filepath.Join(cfg.DataDir, "ingestions"),
		Logger:      logger,
	})
	if err != nil {
		logger.Fatal(err)
	}
	if err := scheduleManager.Start(); err != nil {
		logger.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := scheduleManager.Shutdown(ctx); err != nil {
			logger.Printf("scheduler shutdown: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           app.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownSignal.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Printf("shutdown: %v", err)
		}
	}()

	logger.Printf("listening on %s", cfg.Address)
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		logger.Fatal(err)
	}
}
