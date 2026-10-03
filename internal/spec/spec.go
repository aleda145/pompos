package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"pompos/internal/destination"
	"pompos/internal/ingestion"
)

const (
	APIVersion = "pompos.dev/v1alpha1"
	Kind       = "Ingestion"
)

type Ingestion struct {
	APIVersion      string          `yaml:"apiVersion"`
	Kind            string          `yaml:"kind"`
	Data            string          `yaml:"data,omitempty"`
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
	Type       string `yaml:"type"`
	URL        string `yaml:"url"`
	Table      string `yaml:"table,omitempty"`
	Collection string `yaml:"collection,omitempty"`
}
type Destination struct {
	Schema string `yaml:"schema,omitempty"`
	Type   string `yaml:"type"`
	Path   string `yaml:"path"`
	Object string `yaml:"object"`
}
type Materialization struct {
	Strategy   string   `yaml:"strategy,omitempty"`
	PrimaryKey []string `yaml:"primaryKey,omitempty"`
}
type Runtime struct {
	Python       string   `yaml:"python,omitempty"`
	Dependencies []string `yaml:"dependencies,omitempty"`
	Script       string   `yaml:"script"`
	SecretRefs   []string `yaml:"secretRefs,omitempty"`
	Engine       string   `yaml:"engine"`
	Orchestrator string   `yaml:"orchestrator,omitempty"`
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
	if s.APIVersion != APIVersion {
		return fmt.Errorf("spec.apiVersion: must be %q", APIVersion)
	}
	if s.Kind != Kind {
		return fmt.Errorf("spec.kind: must be %q", Kind)
	}
	if strings.TrimSpace(s.Metadata.Name) == "" {
		return errors.New("spec.metadata.name: is required")
	}
	if s.Source.Type != "python" {
		return errors.New("spec.source.type: must be python")
	}
	if strings.TrimSpace(s.Source.URL) == "" {
		return errors.New("spec.source.url: is required")
	}
	if s.Destination.Type == "objects" {
		if s.Data != "files" || strings.TrimSpace(s.Source.Collection) == "" || s.Source.Table != "" {
			return errors.New("objects require data: files and one source.collection, without source.table")
		}
		schema := strings.ToLower(s.Destination.Schema)
		if schema == "" || schema == "main" || schema == "information_schema" || schema == "pg_catalog" || s.Destination.Object != "objects" {
			return errors.New("objects require a dedicated destination.schema and destination.object: objects")
		}
	} else if s.Data != "" && s.Data != "rows" {
		return errors.New("DuckDB table ingestions require data: rows")
	} else if strings.TrimSpace(s.Source.Table) == "" || s.Source.Collection != "" {
		return errors.New("spec.source: a source URL or identifier and one source table are required")
	}
	if s.Runtime.Engine != "python" {
		return errors.New("spec.runtime.engine: must be python")
	}
	if s.Runtime.Script == "" || !strings.HasSuffix(s.Runtime.Script, ".py") {
		return errors.New("spec.runtime.script: must name a Python file")
	}
	if err := ValidatePythonRuntime(s.Runtime.Python, s.Runtime.Dependencies); err != nil {
		return err
	}
	if s.Runtime.Orchestrator != "" && s.Runtime.Orchestrator != "direct" {
		return errors.New("spec.runtime.orchestrator: must be direct")
	}
	if s.Destination.Type != "duckdb" && s.Destination.Type != "objects" {
		return errors.New("spec.destination.type: must be duckdb or objects")
	}
	if strings.TrimSpace(s.Destination.Path) == "" || strings.ContainsRune(s.Destination.Path, '\x00') {
		return errors.New("spec.destination.path: a valid path is required")
	}
	if s.Schedule != nil && s.Schedule.Timezone != "" && s.Schedule.Timezone != "UTC" {
		return errors.New("spec.schedule.timezone: must be UTC")
	}
	if !objectName.MatchString(destination.SchemaName(s.Destination.Schema)) {
		return errors.New("spec.destination.schema: must start with a letter or underscore and contain only letters, numbers, and underscores")
	}
	if !objectName.MatchString(s.Destination.Object) {
		return errors.New("spec.destination.object: must start with a letter or underscore and contain only letters, numbers, and underscores")
	}
	if err := s.Materialization.ValidateFor(s.Destination.Type); err != nil {
		return err
	}
	return nil
}

func (m Materialization) ValidateFor(kind string) error {
	if kind == "objects" {
		if m.Strategy != "" && m.Strategy != "update" && m.Strategy != "skip" {
			return errors.New("objects strategy must be update or skip")
		}
		if len(m.PrimaryKey) != 0 {
			return errors.New("objects use object_id; primaryKey must be empty")
		}
		return nil
	}
	return m.Validate()
}

func (s Ingestion) Strategy() string {
	if s.Materialization.Strategy == "" && s.Destination.Type == "objects" {
		return "update"
	}
	return defaultStrategy(s.Materialization.Strategy)
}

func (m Materialization) Validate() error {
	strategy := m.Strategy
	if strategy == "" {
		strategy = "replace"
	}
	switch strategy {
	case "replace", "append", "merge":
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
	if strategy == "merge" && len(m.PrimaryKey) == 0 {
		return fmt.Errorf("spec.materialization.primaryKey: strategy %q requires at least one primary key", strategy)
	}
	return nil
}

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// FromIngestion serializes the file-derived ingestion fields, excluding run state.
func FromIngestion(item ingestion.Ingestion) Ingestion {
	document := Ingestion{APIVersion: APIVersion, Kind: Kind, Metadata: Metadata{Name: item.Name},
		Data:            item.Data,
		Source:          Source{Type: item.Source.Type, URL: item.Source.URL, Table: item.Source.Table, Collection: item.Source.Collection},
		Destination:     Destination{Schema: item.Destination.Schema, Type: item.Destination.Type, Path: item.Destination.Path, Object: item.Destination.Table},
		Materialization: Materialization{Strategy: item.Materialization.Strategy, PrimaryKey: item.Materialization.PrimaryKey},
		Runtime:         Runtime{Engine: item.Runtime.Engine, Orchestrator: item.Runtime.Orchestrator, Script: item.Runtime.Script, SecretRefs: item.Runtime.SecretRefs, Python: item.Runtime.Python, Dependencies: item.Runtime.Dependencies},
	}
	if item.Destination.Type == "objects" {
		document.Data = "files"
	}
	document.Materialization.Strategy = document.Strategy()
	if item.Schedule != "" {
		document.Schedule = &Schedule{Cron: item.Schedule, Timezone: "UTC"}
	}
	return document
}

func ToProjection(document Ingestion, id, path, digest string) ingestion.Ingestion {
	item := ingestion.Ingestion{ID: id, Name: document.Metadata.Name, Status: ingestion.StatusPending,
		Data:            document.Data,
		Source:          ingestion.Source{Type: document.Source.Type, URL: document.Source.URL, Table: document.Source.Table, Collection: document.Source.Collection},
		Destination:     ingestion.Destination{Schema: destination.SchemaName(document.Destination.Schema), Type: document.Destination.Type, Path: document.Destination.Path, Table: document.Destination.Object},
		Materialization: ingestion.Materialization{Strategy: document.Strategy(), PrimaryKey: document.Materialization.PrimaryKey},
		Runtime:         ingestion.Runtime{Engine: document.Runtime.Engine, Orchestrator: document.Runtime.Orchestrator, Script: document.Runtime.Script, SecretRefs: document.Runtime.SecretRefs, Python: document.Runtime.Python, Dependencies: document.Runtime.Dependencies}, SpecPath: path, SpecDigest: digest,
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

func Write(directory string, item ingestion.Ingestion) (string, error) {
	if !filepath.IsLocal(item.ID) {
		return "", errors.New("invalid ingestion ID")
	}
	path := ArtifactPath(directory, item.ID, ".yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create ingestion spec directory: %w", err)
	}
	data, err := Marshal(FromIngestion(item))
	if err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".pompos-spec-*.yaml")
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

// ArtifactPath keeps the YAML, extractor and uv lock together for one ingestion.
func ArtifactPath(directory, id, extension string) string {
	return filepath.Join(directory, id, filepath.Base(id)+extension)
}
