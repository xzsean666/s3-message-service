package localfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sean/s3-message-service/internal/storage"
)

type Store struct {
	root string
}

func New(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, storage.ErrInvalidObjectKey
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

func (store *Store) Put(ctx context.Context, key string, data []byte, options storage.PutOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := store.pathForKey(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	flags := os.O_WRONLY | os.O_CREATE
	if options.CreateOnly {
		flags |= os.O_EXCL
	} else {
		flags |= os.O_TRUNC
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return storage.ErrObjectAlreadyExists
		}
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func (store *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := store.pathForKey(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, err
	}
	return data, nil
}

func (store *Store) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.ObjectInfo{}, err
	}
	path, err := store.pathForKey(key)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storage.ObjectInfo{}, storage.ErrObjectNotFound
		}
		return storage.ObjectInfo{}, err
	}
	return storage.ObjectInfo{
		Key:        key,
		Size:       info.Size(),
		ModifiedAt: info.ModTime().UTC(),
	}, nil
}

func (store *Store) List(ctx context.Context, input storage.ListInput) (storage.ListPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.ListPage{}, err
	}
	if input.Limit <= 0 {
		input.Limit = 100
	}
	prefix := strings.TrimLeft(input.Prefix, "/")
	var objects []storage.ListedObject
	walkRoot := store.root
	if prefix != "" {
		prefixDirectory := prefix
		if !strings.HasSuffix(prefixDirectory, "/") {
			lastSlash := strings.LastIndex(prefixDirectory, "/")
			if lastSlash >= 0 {
				prefixDirectory = prefixDirectory[:lastSlash+1]
			} else {
				prefixDirectory = ""
			}
		}
		if prefixDirectory != "" {
			candidate, err := store.pathForKey(prefixDirectory)
			if err != nil {
				return storage.ListPage{}, err
			}
			if _, err := os.Stat(candidate); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return storage.ListPage{}, nil
				}
				return storage.ListPage{}, err
			}
			walkRoot = candidate
		}
	}

	err := filepath.WalkDir(walkRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(store.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(relative)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		if input.StartAfter != "" && key <= input.StartAfter {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		objects = append(objects, storage.ListedObject{
			Key:        key,
			Size:       info.Size(),
			ModifiedAt: info.ModTime().UTC(),
		})
		return nil
	})
	if err != nil {
		return storage.ListPage{}, err
	}

	sort.Slice(objects, func(left, right int) bool {
		return objects[left].Key < objects[right].Key
	})

	hasMore := false
	if len(objects) > input.Limit {
		hasMore = true
		objects = objects[:input.Limit]
	}
	nextAfterKey := ""
	if len(objects) > 0 {
		nextAfterKey = objects[len(objects)-1].Key
	}

	return storage.ListPage{
		Objects:      objects,
		HasMore:      hasMore,
		NextAfterKey: nextAfterKey,
	}, nil
}

func (store *Store) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := store.pathForKey(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storage.ErrObjectNotFound
		}
		return err
	}
	return nil
}

func (store *Store) pathForKey(key string) (string, error) {
	cleanKey := strings.TrimLeft(key, "/")
	if cleanKey == "" || strings.Contains(cleanKey, "\\") {
		return "", storage.ErrInvalidObjectKey
	}
	cleanPath := filepath.Clean(cleanKey)
	if cleanPath == "." || strings.HasPrefix(cleanPath, "..") || filepath.IsAbs(cleanPath) {
		return "", storage.ErrInvalidObjectKey
	}
	return filepath.Join(store.root, filepath.FromSlash(cleanKey)), nil
}
