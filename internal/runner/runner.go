package runner

import (
	"context"

	"pompos/internal/compiler"
)

type Runner interface {
	Run(context.Context, compiler.ExecutionPlan) error
}
