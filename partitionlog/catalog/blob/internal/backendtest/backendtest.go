package backendtest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	blobcatalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

type Config struct {
	NewBackend func(t testing.TB) blobcatalog.Backend
	WrongToken func(valid string) string
}

func Run(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.NewBackend == nil {
		t.Fatal("backendtest: nil NewBackend")
	}
	t.Run("put_get_immutable_replay", func(t *testing.T) {
		runPutGetImmutableReplay(t, cfg.newBackend(t))
	})
	t.Run("compare_and_swap", func(t *testing.T) {
		runCompareAndSwap(t, cfg.newBackend(t), cfg.wrongToken)
	})
	t.Run("list_and_delete", func(t *testing.T) {
		runListAndDelete(t, cfg.newBackend(t))
	})
	t.Run("bad_inputs", func(t *testing.T) {
		runBadInputs(t, cfg.newBackend(t))
	})
}

func (c Config) newBackend(t testing.TB) blobcatalog.Backend {
	t.Helper()
	backend := c.NewBackend(t)
	if backend == nil {
		t.Fatal("backendtest: NewBackend returned nil")
	}
	return backend
}

func (c Config) wrongToken(valid string) string {
	if c.WrongToken != nil {
		return c.WrongToken(valid)
	}
	return valid + "-wrong"
}

func runPutGetImmutableReplay(t *testing.T, backend blobcatalog.Backend) {
	t.Helper()
	ctx := context.Background()
	const key = "catalog/p00000001/pages/l00/leaf.json"

	first, err := backend.Put(ctx, key, []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put(first) error = %v", err)
	}
	if first.Key != key || first.Token == "" || string(first.Body) != `{"one":1}` {
		t.Fatalf("first object = %+v", first)
	}

	replay, err := backend.Put(ctx, key, []byte(`{"one":1}`))
	if err != nil {
		t.Fatalf("Put(replay) error = %v", err)
	}
	if replay.Token != first.Token || !bytes.Equal(replay.Body, first.Body) {
		t.Fatalf("replay = %+v, want token/body from first %+v", replay, first)
	}

	if _, err := backend.Put(ctx, key, []byte(`{"two":2}`)); !errors.Is(err, blobcatalog.ErrImmutableConflict) {
		t.Fatalf("Put(conflict) error = %v, want %v", err, blobcatalog.ErrImmutableConflict)
	}

	got, err := backend.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Token != first.Token || !bytes.Equal(got.Body, first.Body) {
		t.Fatalf("Get() = %+v, want first %+v", got, first)
	}

	got.Body[0] = 'x'
	again, err := backend.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(again) error = %v", err)
	}
	if string(again.Body) != `{"one":1}` {
		t.Fatalf("Get returned mutable body; got %q", again.Body)
	}
}

func runCompareAndSwap(t *testing.T, backend blobcatalog.Backend, wrongToken func(string) string) {
	t.Helper()
	ctx := context.Background()
	const key = "catalog/p00000001/head.json"

	missing, swapped, err := backend.CompareAndSwap(ctx, key, wrongToken("1"), []byte(`{"generation":1}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(missing with token) error = %v", err)
	}
	if swapped || missing.Token != "" || len(missing.Body) != 0 {
		t.Fatalf("CompareAndSwap(missing with token) = swapped=%v object=%+v, want false empty", swapped, missing)
	}

	created, swapped, err := backend.CompareAndSwap(ctx, key, "", []byte(`{"generation":1}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(create) error = %v", err)
	}
	if !swapped || created.Token == "" || string(created.Body) != `{"generation":1}` {
		t.Fatalf("CompareAndSwap(create) swapped=%v object=%+v", swapped, created)
	}

	current, swapped, err := backend.CompareAndSwap(ctx, key, wrongToken(created.Token), []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(wrong token) error = %v", err)
	}
	if swapped {
		t.Fatal("CompareAndSwap(wrong token) swapped=true, want false")
	}
	if current.Token != created.Token || !bytes.Equal(current.Body, created.Body) {
		t.Fatalf("current after wrong token = %+v, want created %+v", current, created)
	}

	alsoCurrent, swapped, err := backend.CompareAndSwap(ctx, key, "", []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(create existing) error = %v", err)
	}
	if swapped || alsoCurrent.Token != created.Token || !bytes.Equal(alsoCurrent.Body, created.Body) {
		t.Fatalf("CompareAndSwap(create existing) swapped=%v object=%+v, want current %+v", swapped, alsoCurrent, created)
	}

	updated, swapped, err := backend.CompareAndSwap(ctx, key, created.Token, []byte(`{"generation":2}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(update) error = %v", err)
	}
	if !swapped || updated.Token == "" || updated.Token == created.Token || string(updated.Body) != `{"generation":2}` {
		t.Fatalf("CompareAndSwap(update) swapped=%v object=%+v previous=%+v", swapped, updated, created)
	}

	stale, swapped, err := backend.CompareAndSwap(ctx, key, created.Token, []byte(`{"generation":3}`))
	if err != nil {
		t.Fatalf("CompareAndSwap(stale token) error = %v", err)
	}
	if swapped || stale.Token != updated.Token || !bytes.Equal(stale.Body, updated.Body) {
		t.Fatalf("CompareAndSwap(stale token) swapped=%v object=%+v, want current %+v", swapped, stale, updated)
	}
}

func runListAndDelete(t *testing.T, backend blobcatalog.Backend) {
	t.Helper()
	ctx := context.Background()
	keys := []string{
		"catalog/p00000001/pages/l00/a.json",
		"catalog/p00000001/pages/l00/b.json",
		"catalog/p00000001/pages/l00/c.json",
		"catalog/p00000002/pages/l00/d.json",
	}
	for _, key := range keys {
		if _, err := backend.Put(ctx, key, []byte(key)); err != nil {
			t.Fatalf("Put(%s) error = %v", key, err)
		}
	}

	first, err := backend.List(ctx, blobcatalog.ListOptions{Prefix: "catalog/p00000001/", Limit: 2})
	if err != nil {
		t.Fatalf("List(first) error = %v", err)
	}
	if len(first.Objects) != 2 || !first.HasMore || first.NextCursor == "" {
		t.Fatalf("first page = %+v, want 2 objects with cursor", first)
	}
	if first.Objects[0].Key != keys[0] || first.Objects[1].Key != keys[1] {
		t.Fatalf("first page keys = %+v, want %q then %q", first.Objects, keys[0], keys[1])
	}
	for _, obj := range first.Objects {
		if obj.Token == "" || obj.SizeBytes <= 0 {
			t.Fatalf("first page object missing token/size: %+v", obj)
		}
	}

	second, err := backend.List(ctx, blobcatalog.ListOptions{Prefix: "catalog/p00000001/", Cursor: first.NextCursor, Limit: 2})
	if err != nil {
		t.Fatalf("List(second) error = %v", err)
	}
	if len(second.Objects) != 1 || second.HasMore || second.NextCursor != "" || second.Objects[0].Key != keys[2] {
		t.Fatalf("second page = %+v, want only %q", second, keys[2])
	}

	if err := backend.Delete(ctx, first.Objects[0].Key); err != nil {
		t.Fatalf("Delete(existing) error = %v", err)
	}
	if err := backend.Delete(ctx, first.Objects[0].Key); err != nil {
		t.Fatalf("Delete(missing) error = %v", err)
	}
	if _, err := backend.Get(ctx, first.Objects[0].Key); !errors.Is(err, blobcatalog.ErrObjectNotFound) {
		t.Fatalf("Get(deleted) error = %v, want %v", err, blobcatalog.ErrObjectNotFound)
	}

	empty, err := backend.List(ctx, blobcatalog.ListOptions{Prefix: "catalog/absent/", Limit: 2})
	if err != nil {
		t.Fatalf("List(empty prefix) error = %v", err)
	}
	if len(empty.Objects) != 0 || empty.HasMore || empty.NextCursor != "" {
		t.Fatalf("List(empty prefix) = %+v, want empty", empty)
	}
}

func runBadInputs(t *testing.T, backend blobcatalog.Backend) {
	t.Helper()
	ctx := context.Background()

	if _, err := backend.Get(ctx, ""); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("Get(empty key) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
	if _, err := backend.Put(ctx, "", []byte("x")); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("Put(empty key) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
	if _, _, err := backend.CompareAndSwap(ctx, "", "", []byte("x")); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("CompareAndSwap(empty key) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
	if err := backend.Delete(ctx, ""); !errors.Is(err, blobcatalog.ErrCorruptCatalog) {
		t.Fatalf("Delete(empty key) error = %v, want %v", err, blobcatalog.ErrCorruptCatalog)
	}
}
