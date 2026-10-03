package compiler

import (
	"bytes"
	"gopkg.in/yaml.v3"
	"pompos/internal/destination"
	"pompos/internal/spec"
)

type ExecutionPlan struct {
	EnvironmentKey    string   `yaml:"-"`
	PythonBinary      string   `yaml:"-"`
	Python            string   `yaml:"python,omitempty"`
	Dependencies      []string `yaml:"dependencies,omitempty"`
	DestinationSchema string   `yaml:"destinationSchema"`
	Engine            string   `yaml:"engine"`
	Script            string   `yaml:"script"`
	SecretRefs        []string `yaml:"secretRefs,omitempty"`
	SourceURI         string   `yaml:"sourceUri"`
	SourceTable       string   `yaml:"sourceTable"`
	DestinationType   string   `yaml:"destinationType"`
	DestinationPath   string   `yaml:"destinationPath"`
	DestinationObject string   `yaml:"destinationObject"`
	Strategy          string   `yaml:"strategy"`
	PrimaryKey        []string `yaml:"primaryKey,omitempty"`
}

func Compile(document spec.Ingestion) (ExecutionPlan, error) {
	if err := document.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	strategy := document.Materialization.Strategy
	if strategy == "" {
		strategy = "replace"
	}
	return ExecutionPlan{
		Engine: "python", Script: document.Runtime.Script, SecretRefs: document.Runtime.SecretRefs, Python: document.Runtime.Python, Dependencies: document.Runtime.Dependencies,
		SourceURI: document.Source.URL, SourceTable: document.Source.Table,
		DestinationSchema: destination.SchemaName(document.Destination.Schema), DestinationType: document.Destination.Type, DestinationPath: document.Destination.Path, DestinationObject: document.Destination.Object,
		Strategy: strategy, PrimaryKey: document.Materialization.PrimaryKey,
	}, nil
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
