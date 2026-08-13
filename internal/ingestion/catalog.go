package ingestion

type SourceDefinition struct {
	Type        string
	Name        string
	Description string
}

type SourceCatalog struct {
	definitions map[string]SourceDefinition
}

func NewSourceCatalog(definitions ...SourceDefinition) SourceCatalog {
	catalog := SourceCatalog{definitions: make(map[string]SourceDefinition, len(definitions))}
	for _, definition := range definitions {
		catalog.definitions[definition.Type] = definition
	}
	return catalog
}

func DefaultSourceCatalog() SourceCatalog {
	return NewSourceCatalog(
		SourceDefinition{Type: "csv", Name: "Remote CSV", Description: "Load any public CSV URL."},
		SourceDefinition{Type: "github", Name: "GitHub", Description: "Sync repository activity and metadata."},
	)
}

func (c SourceCatalog) Get(sourceType string) (SourceDefinition, bool) {
	definition, ok := c.definitions[sourceType]
	return definition, ok
}

func (c SourceCatalog) List() []SourceDefinition {
	definitions := make([]SourceDefinition, 0, len(c.definitions))
	for _, sourceType := range []string{"csv", "github"} {
		if definition, ok := c.definitions[sourceType]; ok {
			definitions = append(definitions, definition)
		}
	}
	return definitions
}
