package secrets

import "context"

type Store interface {
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type NoopStore struct{}

func (NoopStore) Put(context.Context, string, []byte) error { return nil }
func (NoopStore) Delete(context.Context, string) error      { return nil }
