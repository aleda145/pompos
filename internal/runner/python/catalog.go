package python

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"pompos/internal/compiler"
	"pompos/internal/destination"
)

//go:embed catalog.py
var catalogScript string

type DestinationSchema struct {
	Name   string   `json:"name"`
	Tables []string `json:"tables"`
}

type DestinationCatalog struct {
	Destination string              `json:"destination"`
	Exists      bool                `json:"exists"`
	Schemas     []DestinationSchema `json:"schemas"`
}

// InspectDestination only reads catalog metadata. It never runs ingestion code.
func (r Runner) InspectDestination(ctx context.Context, dest destination.Config) (DestinationCatalog, error) {
	result := DestinationCatalog{Destination: dest.Name, Schemas: []DestinationSchema{}}
	if err := dest.Validate(); err != nil {
		return result, err
	}
	path := destination.CatalogPath(dest.Type, dest.Path)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return result, nil
	} else if err != nil {
		return result, err
	}
	result.Exists = true
	plan := compiler.ExecutionPlan{}
	if r.Environments != nil {
		// A built-in reader has its own environment, independent of any draft.
		dir, err := os.MkdirTemp("", "pompos-catalog-*")
		if err != nil {
			return result, err
		}
		defer os.RemoveAll(dir)
		plan.Script = filepath.Join(dir, "catalog.py")
		plan.EnvironmentKey = "pompos-destination-catalog"
		if err := os.WriteFile(plan.Script, []byte(ScriptMetadata("", nil)+catalogScript), 0600); err != nil {
			return result, err
		}
	}
	plan, err := r.Prepare(ctx, plan)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, plan.PythonBinary, "-I", "-c", catalogScript, path)
	cmd.Env = environmentVariables()
	cmd.WaitDelay = time.Second
	output := cappedOutput{limit: 1024 * 1024}
	var stderr cappedOutput
	cmd.Stdout, cmd.Stderr = &output, &stderr
	if err := cmd.Run(); err != nil {
		return result, fmt.Errorf("destination inspection unavailable (database may be busy); use existing_ingestions from context: %w", err)
	}
	if err := json.Unmarshal([]byte(output.String()), &result.Schemas); err != nil {
		return result, fmt.Errorf("decode destination catalog: %w", err)
	}
	return result, nil
}
