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
	switch item.Source.Type {
	case "csv":
		parsed, err := url.ParseRequestURI(item.Source.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errors.New("CSV URL must be a valid HTTP or HTTPS URL")
		}
	case "github":
		if !safeGitHubPart.MatchString(item.Source.Owner) || !safeGitHubPart.MatchString(item.Source.Repository) {
			return errors.New("GitHub repository must use the owner/repository format")
		}
		if _, ok := githubTables[item.Source.Table]; !ok {
			return errors.New("unsupported GitHub table")
		}
	default:
		return errors.New("source type must be csv or github")
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

var safeGitHubPart = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

var githubTables = map[string]struct{}{
	"issues": {}, "pull_requests": {}, "repo_events": {}, "stargazers": {},
}
