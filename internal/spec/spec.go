package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"pompos/internal/ingestion"
)

const (
	APIVersion = "pompos.dev/v1alpha1"
	Kind       = "Ingestion"
)

type Ingestion struct {
	APIVersion      string          `yaml:"apiVersion"`
	Kind            string          `yaml:"kind"`
	Metadata        Metadata        `yaml:"metadata"`
	Source          Source          `yaml:"source"`
	Destination     Destination     `yaml:"destination"`
	Materialization Materialization `yaml:"materialization,omitempty"`
	Runtime         Runtime         `yaml:"runtime,omitempty"`
	Schedule        *Schedule       `yaml:"schedule,omitempty"`
}

type Metadata struct {
	Name  string `yaml:"name"`
	Owner string `yaml:"owner,omitempty"`
}
type Source struct {
	Type          string `yaml:"type"`
	URL           string `yaml:"url,omitempty"`
	Format        string `yaml:"format,omitempty"`
	Owner         string `yaml:"owner,omitempty"`
	Repository    string `yaml:"repository,omitempty"`
	Table         string `yaml:"table,omitempty"`
	CredentialRef string `yaml:"credentialRef,omitempty"`
}
type Destination struct {
	Type          string `yaml:"type,omitempty"`
	Path          string `yaml:"path,omitempty"`
	Object        string `yaml:"object"`
	ConnectionRef string `yaml:"connectionRef,omitempty"` // accepted for older v1alpha1 specs
}
type Materialization struct {
	Strategy       string   `yaml:"strategy,omitempty"`
	PrimaryKey     []string `yaml:"primaryKey,omitempty"`
	IncrementalKey string   `yaml:"incrementalKey,omitempty"`
}
type Runtime struct {
	Engine         string `yaml:"engine,omitempty"`
	Orchestrator   string `yaml:"orchestrator,omitempty"`
	Implementation string `yaml:"implementation,omitempty"` // accepted as a v1alpha1 compatibility alias
	Target         string `yaml:"target,omitempty"`         // accepted as a v1alpha1 compatibility alias
}
type Schedule struct {
	Cron     string `yaml:"cron"`
	Timezone string `yaml:"timezone,omitempty"`
}

var objectName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func Parse(data []byte) (Ingestion, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var document Ingestion
	if err := decoder.Decode(&document); err != nil {
		return Ingestion{}, fmt.Errorf("parse ingestion YAML: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Ingestion{}, errors.New("parse ingestion YAML: exactly one document is required")
	}
	runtime, err := normalizeRuntime(document.Runtime)
	if err != nil {
		return Ingestion{}, err
	}
	document.Runtime = runtime
	if err := document.Validate(); err != nil {
		return Ingestion{}, err
	}
	return document, nil
}

func Read(path string) (Ingestion, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Ingestion{}, nil, fmt.Errorf("read ingestion spec: %w", err)
	}
	document, err := Parse(data)
	return document, data, err
}

func Marshal(document Ingestion) ([]byte, error) {
	runtime, err := normalizeRuntime(document.Runtime)
	if err != nil {
		return nil, err
	}
	document.Runtime = runtime
	if err := document.Validate(); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(document); err != nil {
		return nil, fmt.Errorf("serialize ingestion YAML: %w", err)
	}
	_ = encoder.Close()
	return output.Bytes(), nil
}

func (s Ingestion) Validate() error {
	runtime, err := normalizeRuntime(s.Runtime)
	if err != nil {
		return err
	}
	if s.APIVersion != APIVersion {
		return fmt.Errorf("spec.apiVersion: must be %q", APIVersion)
	}
	if s.Kind != Kind {
		return fmt.Errorf("spec.kind: must be %q", Kind)
	}
	if strings.TrimSpace(s.Metadata.Name) == "" {
		return errors.New("spec.metadata.name: is required")
	}
	switch s.Source.Type {
	case "http-file":
		parsed, err := url.ParseRequestURI(s.Source.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errors.New("spec.source.url: must be a valid HTTP or HTTPS URL")
		}
		if s.Source.Format != "csv" {
			return errors.New("spec.source.format: http-file currently requires csv")
		}
	case "github":
		if s.Source.Owner == "" || s.Source.Repository == "" || s.Source.Table == "" {
			return errors.New("spec.source: github requires owner, repository, and table")
		}
		if s.Source.CredentialRef == "" {
			return errors.New("spec.source.credentialRef: github requires a credential reference")
		}
	default:
		return fmt.Errorf("spec.source.type: unsupported source type %q", s.Source.Type)
	}
	if s.Destination.Type == "" && s.Destination.Path == "" {
		if s.Destination.ConnectionRef == "" {
			return errors.New("spec.destination: type and path are required")
		}
	} else {
		if s.Destination.Type != "duckdb" {
			return fmt.Errorf("spec.destination.type: unsupported type %q", s.Destination.Type)
		}
		if strings.TrimSpace(s.Destination.Path) == "" {
			return errors.New("spec.destination.path: is required")
		}
		if strings.ContainsRune(s.Destination.Path, '\x00') {
			return errors.New("spec.destination.path: contains an invalid null character")
		}
	}
	if !objectName.MatchString(s.Destination.Object) {
		return errors.New("spec.destination.object: must start with a letter or underscore and contain only letters, numbers, and underscores")
	}
	if err := s.Materialization.Validate(); err != nil {
		return err
	}
	if runtime.Engine != "" && runtime.Engine != "ingestr" {
		return fmt.Errorf("spec.runtime.engine: unsupported engine %q", runtime.Engine)
	}
	orchestrator := runtime.Orchestrator
	if orchestrator != "" && orchestrator != "direct" {
		return fmt.Errorf("spec.runtime.orchestrator: unsupported orchestrator %q", orchestrator)
	}
	return nil
}

func (m Materialization) Validate() error {
	strategy := m.Strategy
	if strategy == "" {
		strategy = "replace"
	}
	switch strategy {
	case "replace", "append", "merge", "delete+insert", "scd2":
	default:
		return fmt.Errorf("spec.materialization.strategy: unsupported strategy %q", m.Strategy)
	}
	seenPrimaryKeys := make(map[string]struct{}, len(m.PrimaryKey))
	for _, key := range m.PrimaryKey {
		if strings.TrimSpace(key) == "" {
			return errors.New("spec.materialization.primaryKey: values cannot be empty")
		}
		if _, exists := seenPrimaryKeys[key]; exists {
			return fmt.Errorf("spec.materialization.primaryKey: duplicate key %q", key)
		}
		seenPrimaryKeys[key] = struct{}{}
	}
	if (strategy == "merge" || strategy == "scd2") && len(m.PrimaryKey) == 0 {
		return fmt.Errorf("spec.materialization.primaryKey: strategy %q requires at least one primary key", strategy)
	}
	if strategy == "delete+insert" && strings.TrimSpace(m.IncrementalKey) == "" {
		return errors.New("spec.materialization.incrementalKey: strategy \"delete+insert\" requires an incremental key")
	}
	return nil
}

func normalizeRuntime(runtime Runtime) (Runtime, error) {
	if runtime.Implementation != "" && runtime.Engine != "" && runtime.Implementation != runtime.Engine {
		return Runtime{}, errors.New("spec.runtime: implementation and engine cannot disagree")
	}
	if runtime.Target != "" && runtime.Orchestrator != "" && runtime.Target != runtime.Orchestrator {
		return Runtime{}, errors.New("spec.runtime: target and orchestrator cannot disagree")
	}
	if runtime.Engine == "" {
		runtime.Engine = runtime.Implementation
	}
	if runtime.Orchestrator == "" {
		runtime.Orchestrator = runtime.Target
	}
	runtime.Implementation = ""
	runtime.Target = ""
	return runtime, nil
}

func (r Runtime) EffectiveEngine() string {
	if r.Engine != "" {
		return r.Engine
	}
	return r.Implementation
}

func (r Runtime) EffectiveOrchestrator() string {
	if r.Orchestrator != "" {
		return r.Orchestrator
	}
	return r.Target
}

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// FromLegacy is the UI compatibility edge; persisted YAML is canonical v1alpha1.
func FromLegacy(item ingestion.Ingestion) Ingestion {
	source := Source{Type: "http-file", URL: item.Source.URL, Format: "csv"}
	if item.Source.Type == "github" {
		source = Source{Type: "github", Owner: item.Source.Owner, Repository: item.Source.Repository, Table: item.Source.Table, CredentialRef: item.Source.SecretKey}
	}
	engine, orchestrator := item.Runtime.Engine, item.Runtime.Orchestrator
	if engine == "" {
		engine = "ingestr"
	}
	if orchestrator == "" {
		orchestrator = "direct"
	}
	strategy := item.Materialization.Strategy
	if strategy == "" {
		strategy = "replace"
	}
	document := Ingestion{APIVersion: APIVersion, Kind: Kind, Metadata: Metadata{Name: item.Name}, Source: source,
		Destination:     Destination{Type: item.Destination.Type, Path: item.Destination.Path, Object: item.Destination.Table},
		Materialization: Materialization{Strategy: strategy, PrimaryKey: item.Materialization.PrimaryKey, IncrementalKey: item.Materialization.IncrementalKey},
		Runtime:         Runtime{Engine: engine, Orchestrator: orchestrator}}
	if item.Schedule != "" {
		document.Schedule = &Schedule{Cron: item.Schedule, Timezone: "UTC"}
	}
	return document
}

func ToProjection(document Ingestion, id, path, digest, destinationPath string) ingestion.Ingestion {
	source := ingestion.Source{Type: "csv", URL: document.Source.URL}
	if document.Source.Type == "github" {
		source = ingestion.Source{Type: "github", Owner: document.Source.Owner, Repository: document.Source.Repository, Table: document.Source.Table, SecretKey: document.Source.CredentialRef}
	}
	engine, orchestrator := document.Runtime.EffectiveEngine(), document.Runtime.EffectiveOrchestrator()
	if engine == "" {
		engine = "ingestr"
	}
	if orchestrator == "" {
		orchestrator = "direct"
	}
	item := ingestion.Ingestion{ID: id, Name: document.Metadata.Name, Status: ingestion.StatusPending, Source: source,
		Destination:     ingestion.Destination{Ref: document.Destination.ConnectionRef, Type: document.Destination.Type, Path: document.Destination.Path, Table: document.Destination.Object},
		Materialization: ingestion.Materialization{Strategy: defaultStrategy(document.Materialization.Strategy), PrimaryKey: document.Materialization.PrimaryKey, IncrementalKey: document.Materialization.IncrementalKey},
		Runtime:         ingestion.Runtime{Engine: engine, Orchestrator: orchestrator}, SpecPath: path, SpecDigest: digest}
	if item.Destination.Type == "" {
		item.Destination.Type, item.Destination.Path = "duckdb", destinationPath
	}
	if document.Schedule != nil {
		item.Schedule = document.Schedule.Cron
	}
	return item
}

func defaultStrategy(strategy string) string {
	if strategy == "" {
		return "replace"
	}
	return strategy
}

func Generate(item ingestion.Ingestion) []byte { data, _ := Marshal(FromLegacy(item)); return data }

func Write(directory string, item ingestion.Ingestion) (string, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", fmt.Errorf("create ingestion spec directory: %w", err)
	}
	path := filepath.Join(directory, item.ID+".yaml")
	data, err := Marshal(FromLegacy(item))
	if err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(directory, ".pompos-spec-*.yaml")
	if err != nil {
		return "", fmt.Errorf("create temporary ingestion spec: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return "", fmt.Errorf("set ingestion spec permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return "", fmt.Errorf("write ingestion spec: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", fmt.Errorf("sync ingestion spec: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close ingestion spec: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return "", fmt.Errorf("publish ingestion spec: %w", err)
	}
	return path, nil
}
