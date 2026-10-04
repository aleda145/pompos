package runner

import (
	"context"

	"pompos/internal/compiler"
)

type Runner interface {
	Run(context.Context, compiler.ExecutionPlan) error
}

type logKey struct{}

func WithLog(ctx context.Context, write func(string)) context.Context {
	return context.WithValue(ctx, logKey{}, write)
}

func Log(ctx context.Context, output string) {
	if write, ok := ctx.Value(logKey{}).(func(string)); ok && output != "" {
		write(output)
	}
}
