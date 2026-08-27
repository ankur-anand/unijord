package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

func TestValidatePublishedSnapshotRejectsNonAppendChanges(t *testing.T) {
	t.Parallel()

	identity := WriterIdentity{Epoch: 1, Tag: [16]byte{1}}
	current := Snapshot{
		Head: pmeta.PartitionHead{
			Partition:   7,
			NextLSN:     10,
			OldestLSN:   10,
			WriterEpoch: identity.Epoch,
		},
		Identity: identity,
	}
	segment := testCatalogSegment(7, 10, 11, identity.Epoch)
	segment.WriterTag = identity.Tag
	valid := Snapshot{
		Head: pmeta.PartitionHead{
			Partition:             7,
			NextLSN:               12,
			OldestLSN:             10,
			WriterEpoch:           identity.Epoch,
			SegmentCount:          1,
			ReachableSegmentCount: 1,
			LastSegment:           segment,
			HasLastSegment:        true,
		},
		Identity: identity,
	}

	tests := []struct {
		name    string
		current Snapshot
		next    Snapshot
		segment pmeta.SegmentRef
	}{
		{
			name:    "segment starts before current head",
			current: current,
			next:    valid,
			segment: func() pmeta.SegmentRef {
				older := testCatalogSegment(7, 8, 11, identity.Epoch)
				older.WriterTag = identity.Tag
				return older
			}(),
		},
		{
			name:    "segment count skips",
			current: current,
			next: func() Snapshot {
				next := valid
				next.Head.SegmentCount = 2
				return next
			}(),
			segment: segment,
		},
		{
			name:    "reachable segment count does not advance",
			current: current,
			next: func() Snapshot {
				next := valid
				next.Head.ReachableSegmentCount = 0
				return next
			}(),
			segment: segment,
		},
		{
			name:    "oldest lsn changes",
			current: current,
			next: func() Snapshot {
				next := valid
				next.Head.OldestLSN = 11
				return next
			}(),
			segment: segment,
		},
		{
			name:    "retention state changes",
			current: current,
			next: func() Snapshot {
				next := valid
				next.Head.AppliedRetentionLSN = 1
				next.Head.AppliedRetentionVersion = 1
				return next
			}(),
			segment: segment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := validatePublishedSnapshot(tt.current, tt.next, tt.segment); !errors.Is(err, ErrInvalidPublishResult) {
				t.Fatalf("validatePublishedSnapshot() error = %v, want %v", err, ErrInvalidPublishResult)
			}
		})
	}
}

func TestWriterStopsWhenCatalogFenceMoves(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemoryCatalog()
	session := newCatalogSession(t, cat, 1, [16]byte{1})
	opts := testSessionOptions(session, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	newOwner, err := cat.OpenWriter(context.Background(), 1, [16]byte{2})
	if err != nil {
		t.Fatalf("OpenWriter(new owner) error = %v", err)
	}

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("a")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := w.Flush(context.Background()); !errors.Is(err, ErrStaleWriter) {
		t.Fatalf("Flush() error = %v, want %v", err, ErrStaleWriter)
	}
	if _, err := w.Append(context.Background(), Record{TimestampMS: 2, Value: []byte("b")}); !errors.Is(err, ErrAborted) {
		t.Fatalf("Append(after stale fence) error = %v, want %v", err, ErrAborted)
	}
	state, err := cat.LoadPartition(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadPartition() error = %v", err)
	}
	if state.WriterEpoch != newOwner.Epoch() || state.SegmentCount != 0 || state.NextLSN != 0 {
		t.Fatalf("catalog state after stale publish = %+v", state)
	}
}
