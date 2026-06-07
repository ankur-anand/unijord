package blobcatalog

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestMemoryBackendPutGetAndDefensiveCopy(t *testing.T) {
	t.Parallel()

	backend := NewMemoryBackend()
	body := []byte("one")
	created, err := backend.Put(context.Background(), "pages/a.json", body)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if created.Key != "pages/a.json" || string(created.Body) != "one" || created.Token == "" {
		t.Fatalf("Put() object = %+v", created)
	}

	body[0] = 'x'
	got, err := backend.Get(context.Background(), "pages/a.json")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(got.Body) != "one" {
		t.Fatalf("stored body after input mutation = %q, want one", got.Body)
	}

	got.Body[0] = 'y'
	again, err := backend.Get(context.Background(), "pages/a.json")
	if err != nil {
		t.Fatalf("Get(again) error = %v", err)
	}
	if string(again.Body) != "one" {
		t.Fatalf("stored body after returned mutation = %q, want one", again.Body)
	}
}

func TestMemoryBackendImmutablePut(t *testing.T) {
	t.Parallel()

	backend := NewMemoryBackend()
	first, err := backend.Put(context.Background(), "pages/a.json", []byte("same"))
	if err != nil {
		t.Fatalf("Put(first) error = %v", err)
	}
	replayed, err := backend.Put(context.Background(), "pages/a.json", []byte("same"))
	if err != nil {
		t.Fatalf("Put(replay) error = %v", err)
	}
	if replayed.Token != first.Token {
		t.Fatalf("replay token = %q, want %q", replayed.Token, first.Token)
	}
	if _, err := backend.Put(context.Background(), "pages/a.json", []byte("different")); !errors.Is(err, ErrImmutableConflict) {
		t.Fatalf("Put(conflict) error = %v, want %v", err, ErrImmutableConflict)
	}
}

func TestMemoryBackendCompareAndSwap(t *testing.T) {
	t.Parallel()

	backend := NewMemoryBackend()
	if _, swapped, err := backend.CompareAndSwap(context.Background(), "head.json", "wrong", []byte("one")); err != nil || swapped {
		t.Fatalf("CAS missing with token = swapped %v err %v, want false nil", swapped, err)
	}

	created, swapped, err := backend.CompareAndSwap(context.Background(), "head.json", "", []byte("one"))
	if err != nil || !swapped {
		t.Fatalf("CAS create = swapped %v err %v, want true nil", swapped, err)
	}
	if string(created.Body) != "one" || created.Token == "" {
		t.Fatalf("created object = %+v", created)
	}

	current, swapped, err := backend.CompareAndSwap(context.Background(), "head.json", "wrong", []byte("two"))
	if err != nil || swapped {
		t.Fatalf("CAS wrong token = swapped %v err %v, want false nil", swapped, err)
	}
	if !bytes.Equal(current.Body, []byte("one")) || current.Token != created.Token {
		t.Fatalf("current after wrong token = %+v, want original", current)
	}

	updated, swapped, err := backend.CompareAndSwap(context.Background(), "head.json", created.Token, []byte("two"))
	if err != nil || !swapped {
		t.Fatalf("CAS update = swapped %v err %v, want true nil", swapped, err)
	}
	if string(updated.Body) != "two" || updated.Token == created.Token {
		t.Fatalf("updated object = %+v", updated)
	}
}

func TestMemoryBackendListPagination(t *testing.T) {
	t.Parallel()

	backend := NewMemoryBackend()
	for _, key := range []string{
		"catalog/p00000001/pages/b.json",
		"catalog/p00000001/pages/a.json",
		"catalog/p00000001/pages/c.json",
		"catalog/p00000002/pages/z.json",
	} {
		if _, err := backend.Put(context.Background(), key, []byte(key)); err != nil {
			t.Fatalf("Put(%s) error = %v", key, err)
		}
	}

	first, err := backend.List(context.Background(), ListOptions{
		Prefix: "catalog/p00000001/pages/",
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("List(first) error = %v", err)
	}
	if len(first.Objects) != 2 || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first page = %+v, want 2 objects has_more", first)
	}
	if first.Objects[0].Key != "catalog/p00000001/pages/a.json" || first.Objects[1].Key != "catalog/p00000001/pages/b.json" {
		t.Fatalf("first page keys = %+v", first.Objects)
	}

	second, err := backend.List(context.Background(), ListOptions{
		Prefix: "catalog/p00000001/pages/",
		Cursor: first.NextCursor,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("List(second) error = %v", err)
	}
	if len(second.Objects) != 1 || second.HasMore || second.Objects[0].Key != "catalog/p00000001/pages/c.json" {
		t.Fatalf("second page = %+v, want c only", second)
	}
}

func TestMemoryBackendDeleteIsIdempotent(t *testing.T) {
	t.Parallel()

	backend := NewMemoryBackend()
	if _, err := backend.Put(context.Background(), "pages/a.json", []byte("one")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := backend.Delete(context.Background(), "pages/a.json"); err != nil {
		t.Fatalf("Delete(existing) error = %v", err)
	}
	if _, err := backend.Get(context.Background(), "pages/a.json"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("Get(deleted) error = %v, want %v", err, ErrObjectNotFound)
	}
	if err := backend.Delete(context.Background(), "pages/a.json"); err != nil {
		t.Fatalf("Delete(missing) error = %v", err)
	}
}
