package ingestion

import "time"

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

type Ingestion struct {
	ID          string
	Name        string
	Source      Source
	Destination Destination
	Status      string
	LastRun     *time.Time
	LastError   string
	SpecPath    string
}

type Source struct {
	Type string
	URL  string
}

type Destination struct {
	Type  string
	Path  string
	Table string
}
