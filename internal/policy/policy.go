package policy

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"pompos/internal/ingestion"
)

type Engine interface {
	Validate(ingestion.Ingestion) error
}

type DefaultEngine struct {
	DestinationPath string
}

var safeIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (p DefaultEngine) Validate(item ingestion.Ingestion) error {
	if item.Source.Type != "csv" {
		return errors.New("source type must be csv")
	}
	parsed, err := url.ParseRequestURI(item.Source.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("CSV URL must be a valid HTTP or HTTPS URL")
	}
	if item.Destination.Type != "duckdb" {
		return errors.New("destination type must be duckdb")
	}
	if item.Destination.Path != p.DestinationPath {
		return errors.New("destination path must match platform configuration")
	}
	if item.Destination.Table == "" {
		return errors.New("table name cannot be empty")
	}
	if !safeIdentifier.MatchString(item.Destination.Table) {
		return fmt.Errorf("table name %q must start with a letter or underscore and contain only letters, numbers, and underscores", item.Destination.Table)
	}
	return nil
}
