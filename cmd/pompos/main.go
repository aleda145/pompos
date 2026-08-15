package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/config"
	"pompos/internal/execution"
	"pompos/internal/ingestion"
	runneringestr "pompos/internal/runner/ingestr"
	"pompos/internal/scheduler"
	"pompos/internal/spec"
	"pompos/internal/store"
	"pompos/internal/validation"
	"pompos/internal/web"
)

func main() {
	if len(os.Args) > 1 {
		if err := runCommand(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "pompos:", err)
			os.Exit(1)
		}
		return
	}
	runServer()
}

func runServer() {
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
	if err := rebuildSpecProjections(context.Background(), metadata, filepath.Join(cfg.DataDir, "ingestions"), cfg.Destination.Path, logger); err != nil {
		logger.Fatal(err)
	}

	ingestionRunner := runneringestr.Runner{Binary: cfg.Runner.Binary, Logger: logger}
	secretStore := metadata.Secrets()
	blueprint := compiler.LocalDuckDB(cfg.Destination.Path)
	executor, err := execution.New(execution.Service{
		Store: metadata, Blueprint: blueprint, Runner: ingestionRunner,
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

func rebuildSpecProjections(ctx context.Context, metadata *store.SQLite, directory, destinationPath string, logger *log.Logger) error {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read ingestion specs: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		document, data, err := spec.Read(path)
		if err != nil {
			if bytes.Contains(data, []byte("apiVersion: pompos.dev/v1\n")) {
				logger.Printf("legacy ingestion spec ignored path=%s; recreate it as pompos.dev/v1alpha1", path)
				continue
			}
			return fmt.Errorf("load desired ingestion %s: %w", path, err)
		}
		id := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		if err := metadata.UpsertProjection(ctx, spec.ToProjection(document, id, path, spec.Digest(data), destinationPath)); err != nil {
			return err
		}
	}
	return nil
}

func runCommand(args []string) error {
	return runCommandIO(args, os.Stdout, os.Stderr)
}

func runCommandIO(args []string, stdout, stderr io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: pompos <validate|plan|run> ingestion.yaml")
	}
	command, path := args[0], args[1]
	if command != "validate" && command != "plan" && command != "run" {
		return fmt.Errorf("unknown command %q; use validate, plan, or run", command)
	}
	document, _, err := spec.Read(path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	cfg := config.Load()
	plan, err := compiler.Compile(document, compiler.LocalDuckDB(cfg.Destination.Path))
	if err != nil {
		return err
	}
	switch command {
	case "validate":
		fmt.Fprintln(stdout, "valid")
	case "plan":
		data, err := compiler.MarshalPlan(plan)
		if err != nil {
			return err
		}
		_, err = stdout.Write(data)
		return err
	case "run":
		logger := log.New(stderr, "pompos: ", log.LstdFlags)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		credentialValue := ""
		if plan.CredentialRef != "" {
			metadata, err := store.Open(ctx, cfg.MetadataPath, cfg.Destination.Path)
			if err != nil {
				return err
			}
			defer metadata.Close()
			value, err := metadata.Secrets().Get(ctx, plan.CredentialRef)
			if err != nil {
				return fmt.Errorf("load credential %q: %w", plan.CredentialRef, err)
			}
			credentialValue = string(value)
		}
		started := time.Now()
		fmt.Fprintf(stdout, "Running %s\n", document.Metadata.Name)
		if err := (runneringestr.Runner{Binary: cfg.Runner.Binary, Logger: logger}).Run(ctx, document.Metadata.Name, plan, credentialValue); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("run %q interrupted: %w", document.Metadata.Name, ctx.Err())
			}
			return fmt.Errorf("run %q failed: %w", document.Metadata.Name, err)
		}
		fmt.Fprintf(stdout, "Succeeded %s in %s\n", document.Metadata.Name, time.Since(started).Round(time.Millisecond))
	}
	return nil
}
