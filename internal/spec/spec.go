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
	ConnectionRef string `yaml:"connectionRef"`
	Object        string `yaml:"object"`
}
type Materialization struct {
	Strategy string `yaml:"strategy,omitempty"`
}
type Runtime struct {
	Implementation string `yaml:"implementation,omitempty"`
	Orchestrator   string `yaml:"orchestrator,omitempty"`
	Target         string `yaml:"target,omitempty"` // accepted as a v1alpha1 compatibility alias
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
	if s.Destination.ConnectionRef == "" {
		return errors.New("spec.destination.connectionRef: is required")
	}
	if !objectName.MatchString(s.Destination.Object) {
		return errors.New("spec.destination.object: must start with a letter or underscore and contain only letters, numbers, and underscores")
	}
	if s.Materialization.Strategy != "" && s.Materialization.Strategy != "replace" {
		return fmt.Errorf("spec.materialization.strategy: unsupported strategy %q", s.Materialization.Strategy)
	}
	if runtime.Implementation != "" && runtime.Implementation != "ingestr" {
		return fmt.Errorf("spec.runtime.implementation: unsupported implementation %q", runtime.Implementation)
	}
	orchestrator := runtime.Orchestrator
	if orchestrator != "" && orchestrator != "direct" {
		return fmt.Errorf("spec.runtime.orchestrator: unsupported orchestrator %q", orchestrator)
	}
	return nil
}

func normalizeRuntime(runtime Runtime) (Runtime, error) {
	if runtime.Target != "" && runtime.Orchestrator != "" && runtime.Target != runtime.Orchestrator {
		return Runtime{}, errors.New("spec.runtime: target and orchestrator cannot disagree")
	}
	if runtime.Orchestrator == "" {
		runtime.Orchestrator = runtime.Target
	}
	runtime.Target = ""
	return runtime, nil
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
	implementation, orchestrator := item.Runtime.Implementation, item.Runtime.Orchestrator
	if implementation == "" {
		implementation = "ingestr"
	}
	if orchestrator == "" {
		orchestrator = "direct"
	}
	document := Ingestion{APIVersion: APIVersion, Kind: Kind, Metadata: Metadata{Name: item.Name}, Source: source,
		Destination: Destination{ConnectionRef: "local-duckdb", Object: item.Destination.Table}, Materialization: Materialization{Strategy: "replace"}, Runtime: Runtime{Implementation: implementation, Orchestrator: orchestrator}}
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
	implementation, orchestrator := document.Runtime.Implementation, document.Runtime.EffectiveOrchestrator()
	if implementation == "" {
		implementation = "ingestr"
	}
	if orchestrator == "" {
		orchestrator = "direct"
	}
	item := ingestion.Ingestion{ID: id, Name: document.Metadata.Name, Status: ingestion.StatusPending, Source: source,
		Destination: ingestion.Destination{Type: "duckdb", Path: destinationPath, Table: document.Destination.Object},
		Runtime:     ingestion.Runtime{Implementation: implementation, Orchestrator: orchestrator}, SpecPath: path, SpecDigest: digest}
	if document.Schedule != nil {
		item.Schedule = document.Schedule.Cron
	}
	return item
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
