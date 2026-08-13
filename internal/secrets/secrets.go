package secrets

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("secret not found")

type Entry struct {
	Key       string
	CreatedAt string
	UpdatedAt string
}

type Store interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context) ([]Entry, error)
	Delete(ctx context.Context, key string) error
}

type NoopStore struct{}

func (NoopStore) Put(context.Context, string, []byte) error { return nil }
func (NoopStore) Get(context.Context, string) ([]byte, error) {
	return nil, ErrNotFound
}
func (NoopStore) List(context.Context) ([]Entry, error) { return nil, nil }
func (NoopStore) Delete(context.Context, string) error  { return nil }
