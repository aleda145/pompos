package compiler

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"pompos/internal/spec"
)

type Blueprint struct{ DestinationRef, DestinationType, DestinationPath, Engine, EngineVersion, Orchestrator, SchemaNaming string }

func LocalDuckDB(path string) Blueprint {
	return DuckDB("local-duckdb", path)
}

func DuckDB(ref, path string) Blueprint {
	return Blueprint{DestinationRef: ref, DestinationType: "duckdb", DestinationPath: path, Engine: "ingestr", EngineVersion: "1.1.8", Orchestrator: "direct", SchemaNaming: "direct"}
}

type ExecutionPlan struct {
	Engine            string `yaml:"engine"`
	EngineVersion     string `yaml:"engineVersion"`
	Orchestrator      string `yaml:"orchestrator"`
	SourceURI         string `yaml:"sourceUri"`
	SourceTable       string `yaml:"sourceTable"`
	CredentialRef     string `yaml:"credentialRef,omitempty"`
	DestinationType   string `yaml:"destinationType"`
	DestinationRef    string `yaml:"destinationRef,omitempty"`
	DestinationURI    string `yaml:"destinationUri"`
	DestinationObject string `yaml:"destinationObject"`
	Strategy          string `yaml:"strategy"`
	SchemaNaming      string `yaml:"schemaNaming"`
}

func Compile(document spec.Ingestion, blueprint Blueprint) (ExecutionPlan, error) {
	if err := document.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	destinationType, destinationPath, destinationRef := document.Destination.Type, document.Destination.Path, ""
	if destinationType == "" {
		if document.Destination.ConnectionRef != blueprint.DestinationRef {
			return ExecutionPlan{}, fmt.Errorf("policy.destination-connection: legacy connectionRef %q cannot be resolved; make the destination explicit", document.Destination.ConnectionRef)
		}
		destinationType, destinationPath, destinationRef = blueprint.DestinationType, blueprint.DestinationPath, blueprint.DestinationRef
	}
	if orchestrator := document.Runtime.EffectiveOrchestrator(); orchestrator != "" && orchestrator != blueprint.Orchestrator {
		return ExecutionPlan{}, fmt.Errorf("policy.runtime-orchestrator: orchestrator %q is not enabled", orchestrator)
	}
	if engine := document.Runtime.EffectiveEngine(); engine != "" && engine != blueprint.Engine {
		return ExecutionPlan{}, fmt.Errorf("policy.runtime-engine: engine %q is not allowed", engine)
	}
	plan := ExecutionPlan{Engine: blueprint.Engine, EngineVersion: blueprint.EngineVersion, Orchestrator: blueprint.Orchestrator,
		DestinationType: destinationType, DestinationRef: destinationRef, DestinationURI: duckDBURI(destinationPath), DestinationObject: document.Destination.Object,
		Strategy: defaultValue(document.Materialization.Strategy, "replace"), SchemaNaming: blueprint.SchemaNaming}
	switch document.Source.Type {
	case "http-file":
		plan.SourceURI, plan.SourceTable = document.Source.URL, "data#csv"
	case "github":
		plan.SourceURI = fmt.Sprintf("github://?owner=%s&repo=%s", document.Source.Owner, document.Source.Repository)
		plan.SourceTable = document.Source.Table
		plan.CredentialRef = document.Source.CredentialRef
	default:
		return ExecutionPlan{}, errors.New("policy.source-type: source is not supported by the direct blueprint")
	}
	return plan, nil
}

func MarshalPlan(plan ExecutionPlan) ([]byte, error) {
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(plan); err != nil {
		return nil, err
	}
	_ = enc.Close()
	return out.Bytes(), nil
}

func defaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
func duckDBURI(path string) string {
	absolute := filepath.IsAbs(path)
	path = filepath.ToSlash(path)
	if absolute {
		return "duckdb://" + path
	}
	return "duckdb://" + strings.TrimPrefix(path, "./")
}
