package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrObjectAlreadyExists = errors.New("object already exists")
	ErrObjectNotFound      = errors.New("object not found")
	ErrInvalidObjectKey    = errors.New("invalid object key")
)

type PutOptions struct {
	CreateOnly  bool
	ContentType string
}

type ObjectInfo struct {
	Key         string
	Size        int64
	ContentType string
	ModifiedAt  time.Time
}

type ListedObject struct {
	Key        string
	Size       int64
	ModifiedAt time.Time
}

type ListInput struct {
	Prefix     string
	StartAfter string
	Limit      int
}

type ListPage struct {
	Objects      []ListedObject
	HasMore      bool
	NextAfterKey string
}

type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte, options PutOptions) error
	Get(ctx context.Context, key string) ([]byte, error)
	Head(ctx context.Context, key string) (ObjectInfo, error)
	List(ctx context.Context, input ListInput) (ListPage, error)
	Delete(ctx context.Context, key string) error
}
