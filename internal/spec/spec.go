package spec

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"pompos/internal/ingestion"
)

func Generate(item ingestion.Ingestion) []byte {
	var source string
	if item.Source.Type == "github" {
		source = fmt.Sprintf("  type: github\n  owner: %s\n  repository: %s\n  table: %s", yamlString(item.Source.Owner), yamlString(item.Source.Repository), yamlString(item.Source.Table))
	} else {
		source = fmt.Sprintf("  type: csv\n  url: %s", yamlString(item.Source.URL))
	}
	return []byte(fmt.Sprintf(`apiVersion: pompos.dev/v1
kind: Ingestion

metadata:
  name: %s

source:
%s

destination:
  type: duckdb
  table: %s
`, yamlString(item.Name), source, yamlString(item.Destination.Table)))
}

func Write(directory string, item ingestion.Ingestion) (string, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", fmt.Errorf("create ingestion spec directory: %w", err)
	}
	path := filepath.Join(directory, item.ID+".yaml")
	if err := os.WriteFile(path, Generate(item), 0o644); err != nil {
		return "", fmt.Errorf("write ingestion spec: %w", err)
	}
	return path, nil
}

func yamlString(value string) string {
	return strconv.Quote(value)
}
