package destination

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	ErrNotFound = errors.New("destination not found")
	validName   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)
)

type Config struct {
	Name      string
	Type      string
	Path      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewDuckDB(name, path string) Config {
	return Config{Name: name, Type: "duckdb", Path: path}
}

func (c Config) Validate() error {
	if !validName.MatchString(c.Name) {
		return errors.New("destination name must start with a letter or number and contain only letters, numbers, underscores, and hyphens")
	}
	if c.Type != "duckdb" {
		return fmt.Errorf("unsupported destination type %q", c.Type)
	}
	if strings.TrimSpace(c.Path) == "" {
		return errors.New("DuckDB path is required")
	}
	if strings.ContainsRune(c.Path, '\x00') {
		return errors.New("DuckDB path contains an invalid null character")
	}
	return nil
}
