package localfs

import (
	"context"
	"errors"
	"testing"

	"github.com/sean/s3-message-service/internal/storage"
)

func TestStorePutGetListAndCreateOnly(t *testing.T) {
	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Put(ctx, "a/b/one.json", []byte(`{"one":1}`), storage.PutOptions{CreateOnly: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "a/b/two.json", []byte(`{"two":2}`), storage.PutOptions{CreateOnly: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "a/b/one.json", []byte(`{}`), storage.PutOptions{CreateOnly: true}); !errors.Is(err, storage.ErrObjectAlreadyExists) {
		t.Fatalf("expected create-only conflict, got %v", err)
	}

	data, err := store.Get(ctx, "a/b/one.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"one":1}` {
		t.Fatalf("unexpected object body: %s", data)
	}

	page, err := store.List(ctx, storage.ListInput{Prefix: "a/b/", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || !page.HasMore || page.NextAfterKey == "" {
		t.Fatalf("unexpected first page: %+v", page)
	}

	nextPage, err := store.List(ctx, storage.ListInput{Prefix: "a/b/", StartAfter: page.NextAfterKey, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(nextPage.Objects) != 1 || nextPage.Objects[0].Key != "a/b/two.json" {
		t.Fatalf("unexpected second page: %+v", nextPage)
	}
}
