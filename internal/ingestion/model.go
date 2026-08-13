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
	Schedule    string
	LastRun     *time.Time
	LastError   string
	SpecPath    string
}

type Source struct {
	Type        string
	URL         string
	Owner       string
	Repository  string
	AccessToken string
	SecretKey   string
	Table       string
}

func (s Source) DisplayLocation() string {
	if s.Type == "github" {
		return "github.com/" + s.Owner + "/" + s.Repository
	}
	return s.URL
}

type Destination struct {
	Type  string
	Path  string
	Table string
}
