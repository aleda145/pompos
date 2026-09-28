package ingestion

import (
	"fmt"
	"strings"
)

// RowEstimate describes rows emitted by one full extraction, not new destination rows.
type RowEstimate struct {
	Rows       *int64 `json:"rows" yaml:"rows"`
	Kind       string `json:"kind" yaml:"kind"`
	Basis      string `json:"basis" yaml:"basis"`
	ObservedAt string `json:"observed_at" yaml:"observedAt,omitempty"`
}

func (e RowEstimate) Validate() error {
	if e.Kind != "exact" && e.Kind != "approximate" && e.Kind != "unknown" {
		return fmt.Errorf("row estimate kind must be exact, approximate, or unknown")
	}
	if (e.Kind == "unknown") != (e.Rows == nil) || (e.Rows != nil && *e.Rows < 0) {
		return fmt.Errorf("known row estimates require a nonnegative count; unknown estimates require null")
	}
	if strings.TrimSpace(e.Basis) == "" || len(e.Basis) > 1000 {
		return fmt.Errorf("row estimates require a short explanation of their source or uncertainty")
	}
	return nil
}

func (e RowEstimate) Description() string {
	if e.Rows == nil {
		return "Unknown"
	}
	if e.Kind == "approximate" {
		return fmt.Sprintf("About %d rows", *e.Rows)
	}
	return fmt.Sprintf("%d rows at observation time", *e.Rows)
}
