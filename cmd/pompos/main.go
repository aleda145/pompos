package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"pompos/internal/agent"
	"pompos/internal/compiler"
	"pompos/internal/config"
	"pompos/internal/execution"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/scheduler"
	"pompos/internal/spec"
	"pompos/internal/store"
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
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal(err)
	}
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
	if err := rebuildSpecProjections(context.Background(), metadata, filepath.Join(cfg.DataDir, "ingestions")); err != nil {
		logger.Fatal(err)
	}

	ingestionRunner := runnerpython.Runner{RunTimeoutMinutes: metadata.RunTimeoutMinutes, Binary: cfg.PythonBinary, Secrets: metadata.Secrets(), Environments: &runnerpython.Environments{Dir: filepath.Join(cfg.DataDir, "environments"), UVBinary: cfg.UVBinary}}
	secretStore := metadata.Secrets()
	executor, err := execution.New(execution.Service{
		Store: metadata, Runner: ingestionRunner,
		Logger: logger,
	})
	if err != nil {
		logger.Fatal(err)
	}
	scheduleManager, err := scheduler.New(logger, metadata, executor.Run, cfg.Workers)
	if err != nil {
		logger.Fatal(err)
	}
	app, err := web.New(web.App{
		Store:        metadata,
		Secrets:      secretStore,
		Scheduler:    scheduleManager,
		Destinations: metadata,
		SpecDir:      filepath.Join(cfg.DataDir, "ingestions"),
		Agent:        &agent.Service{Dir: filepath.Join(cfg.DataDir, "agent"), Secrets: secretStore, Destinations: metadata, Python: ingestionRunner},
		Logger:       logger,
		Previewer:    ingestionRunner,
	})
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := app.Agent.Shutdown(ctx); err != nil {
			logger.Printf("agent shutdown: %v", err)
		}
	}()
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

func rebuildSpecProjections(ctx context.Context, metadata *store.SQLite, directory string) error {
	_, err := os.Stat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read ingestion specs: %w", err)
	}
	return filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(entry.Name(), ".") && path != directory {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".yaml" {
			return nil
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if strings.TrimSuffix(entry.Name(), ".yaml") != filepath.Base(filepath.Dir(relative)) {
			return nil
		}
		id := filepath.ToSlash(filepath.Dir(relative))
		document, data, err := spec.Read(path)
		item := ingestion.Ingestion{ID: id, Status: ingestion.StatusPending, SpecPath: path, SpecDigest: spec.Digest(data)}
		if err != nil {
			// Retain an identity for broken files so the UI can show the error.
			log.Printf("ingestion spec unavailable ingestion_id=%s spec_path=%s error=%q", id, path, err)
		} else {
			item = spec.ToProjection(document, id, path, spec.Digest(data))
		}
		return metadata.UpsertProjection(ctx, item)
	})
}

func runCommand(args []string) error {
	return runCommandIO(args, os.Stdout)
}

func runCommandIO(args []string, stdout io.Writer) error {
	if len(args) == 1 && args[0] == "mcp-token" {
		return errors.New("MCP no longer requires a token; connect from Agent settings")
	}

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
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	plan, err := compiler.Compile(document)
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
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		metadata, err := store.Open(ctx, cfg.MetadataPath, cfg.Destination.Path)
		if err != nil {
			return err
		}
		defer metadata.Close()
		python := runnerpython.Runner{RunTimeoutMinutes: metadata.RunTimeoutMinutes, Binary: cfg.PythonBinary, Secrets: metadata.Secrets(), Environments: &runnerpython.Environments{Dir: filepath.Join(cfg.DataDir, "environments"), UVBinary: cfg.UVBinary}}
		started := time.Now()
		fmt.Fprintf(stdout, "Running %s\n", document.Metadata.Name)
		if err := python.Run(ctx, plan); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("run %q interrupted: %w", document.Metadata.Name, ctx.Err())
			}
			return fmt.Errorf("run %q failed: %w", document.Metadata.Name, err)
		}
		fmt.Fprintf(stdout, "Succeeded %s in %s\n", document.Metadata.Name, time.Since(started).Round(time.Millisecond))
	}
	return nil
}
