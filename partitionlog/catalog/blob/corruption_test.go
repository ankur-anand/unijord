package blob

import (
	"context"
	"errors"
	"testing"
)

func TestV2CorruptHeadRetainsPublicErrorClassification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := NewMemoryBackend()
	cat, err := New(backend, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cat.InitializePartition(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	key := HeadPath(cat.opts.Prefix, cat.opts.StreamID, 1)
	object, err := backend.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, swapped, err := backend.CompareAndSwap(ctx, key, object.Token, []byte("bad head")); err != nil || !swapped {
		t.Fatalf("corrupt head CAS swapped=%v err=%v", swapped, err)
	}
	if _, err := cat.LoadPartition(ctx, 1); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("LoadPartition() error = %v, want ErrCorruptCatalog", err)
	}
}

func TestV2CorruptPageRetainsPublicErrorClassification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := NewMemoryBackend()
	cat, err := New(backend, Options{LeafSegmentLimit: 2, IndexRefLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	for base := uint64(0); base < 20; base += 10 {
		if _, err := writer.AppendSegment(ctx, testSegmentRef(1, base, base+9, writer.Epoch())); err != nil {
			t.Fatal(err)
		}
	}
	objects, err := backend.List(ctx, ListOptions{Prefix: PageLevelPrefix(cat.opts.Prefix, cat.opts.StreamID, 1, 0)})
	if err != nil || len(objects.Objects) != 1 {
		t.Fatalf("List(l00) objects=%+v err=%v", objects.Objects, err)
	}
	object, err := backend.Get(ctx, objects.Objects[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, swapped, err := backend.CompareAndSwap(ctx, object.Key, object.Token, []byte("bad page")); err != nil || !swapped {
		t.Fatalf("corrupt page CAS swapped=%v err=%v", swapped, err)
	}
	if _, _, err := cat.FindSegment(ctx, 1, 0); !errors.Is(err, ErrCorruptCatalog) {
		t.Fatalf("FindSegment() error = %v, want ErrCorruptCatalog", err)
	}
}
