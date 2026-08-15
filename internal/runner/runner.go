package runner

import (
	"context"

	"pompos/internal/compiler"
)

type Runner interface {
	Run(context.Context, string, compiler.ExecutionPlan, string) error
}
