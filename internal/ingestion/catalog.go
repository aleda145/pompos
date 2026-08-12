package ingestion

type SourceDefinition struct {
	Type string
	Name string
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
	return NewSourceCatalog(SourceDefinition{Type: "csv", Name: "Remote CSV"})
}

func (c SourceCatalog) Get(sourceType string) (SourceDefinition, bool) {
	definition, ok := c.definitions[sourceType]
	return definition, ok
}

func (c SourceCatalog) List() []SourceDefinition {
	definitions := make([]SourceDefinition, 0, len(c.definitions))
	if csv, ok := c.definitions["csv"]; ok {
		definitions = append(definitions, csv)
	}
	return definitions
}
