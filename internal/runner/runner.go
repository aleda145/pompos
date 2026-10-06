package runner

import (
	"context"
	"time"

	"pompos/internal/compiler"
)

const DefaultRunTimeoutMinutes = 30
const MaxRunTimeoutMinutes = int((1<<63 - 1) / int64(time.Minute))

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
