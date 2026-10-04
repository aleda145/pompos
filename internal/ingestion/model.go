package ingestion

import (
	"pompos/internal/destination"
	"time"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

type Ingestion struct {
	ID              string
	Name            string
	Data            string
	Source          Source
	Destination     Destination
	Materialization Materialization
	Runtime         Runtime
	Status          string
	Schedule        string
	NextRun         *time.Time
	LastRun         *time.Time
	LastError       string
	// LoadError describes the current YAML configuration, independently of run history.
	LoadError  string
	SpecPath   string
	SpecDigest string
}

type Run struct {
	ID           int64
	IngestionID  string
	Trigger      string
	ScheduledFor time.Time
	Attempts     int
	SpecPath     string
	SpecDigest   string
	Status       string
	StartedAt    *time.Time
	FinishedAt   *time.Time
	LastError    string
	Log          string
	LogTruncated bool
}

func (r Run) Duration() string {
	if r.StartedAt == nil {
		return "—"
	}
	end := time.Now()
	if r.FinishedAt != nil {
		end = *r.FinishedAt
	}
	d := max(end.Sub(*r.StartedAt), 0)
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

type Source struct {
	Type       string
	URL        string
	Table      string
	Collection string
}

func (s Source) DisplayLocation() string { return s.URL }

type Destination struct {
	Schema string
	Type   string
	Path   string
	Table  string
}

func (d Destination) CatalogPath() string { return destination.CatalogPath(d.Type, d.Path) }

type Runtime struct {
	Python       string
	Dependencies []string
	Script       string
	SecretRefs   []string
	Engine       string
	Orchestrator string
}

type Materialization struct {
	Strategy   string
	PrimaryKey []string
}
