package runner

import (
	"context"

	"pompos/internal/ingestion"
)

type Runner interface {
	Run(context.Context, ingestion.Ingestion) error
}
