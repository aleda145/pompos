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
