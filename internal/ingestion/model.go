package ingestion

import "time"

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

type Ingestion struct {
	ID              string
	Name            string
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
}

type Source struct {
	Estimate *RowEstimate
	Type     string
	URL      string
	Table    string
}

func (s Source) DisplayLocation() string { return s.URL }

type Destination struct {
	Type  string
	Path  string
	Table string
}

type Runtime struct {
	Script       string
	ScriptDigest string
	SecretRefs   []string
	Engine       string
	Orchestrator string
}

type Materialization struct {
	Strategy   string
	PrimaryKey []string
}
