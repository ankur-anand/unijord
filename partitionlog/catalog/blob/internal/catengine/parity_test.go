package catengine

import (
	"context"
	"reflect"
	"testing"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestEngineMatchesMemoryCatalogSemantics(t *testing.T) {
	ctx := context.Background()
	backend := newFaultStore()
	engine, err := NewEngine(backend, EngineOptions{
		CatalogPrefix: "catalog-format",
		DataRoot:      "data-root",
		LeafLimit:     4,
		IndexLimit:    3,
		HashAlgo:      segformat.HashXXH64,
		CASAttempts:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	memory := csession.NewMemoryCatalog()
	if _, _, err := engine.Initialize(ctx, 7, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := memory.InitializePartition(ctx, 7, 0); err != nil {
		t.Fatal(err)
	}
	writerID := filled16(0x91)
	actualWriter, err := engine.OpenWriter(ctx, 7, writerID)
	if err != nil {
		t.Fatal(err)
	}
	wantWriter, err := memory.OpenWriter(ctx, 7, writerID)
	if err != nil {
		t.Fatal(err)
	}

	for lsn := uint64(0); lsn < 50; lsn++ {
		segment := testSegment(actualWriter.config, actualWriter.head, lsn)
		actual, err := actualWriter.AppendSegment(ctx, segment)
		if err != nil {
			t.Fatalf("catformat AppendSegment(%d): %v", lsn, err)
		}
		want, err := wantWriter.AppendSegment(ctx, segment)
		if err != nil {
			t.Fatalf("memory AppendSegment(%d): %v", lsn, err)
		}
		if !reflect.DeepEqual(actual, want) {
			t.Fatalf("head mismatch after LSN %d:\nactual=%#v\nwant=%#v", lsn, actual, want)
		}
	}

	for lsn := uint64(0); lsn < 50; lsn++ {
		actual, actualFound, err := engine.FindSegment(ctx, 7, lsn)
		if err != nil {
			t.Fatal(err)
		}
		want, wantFound, err := memory.FindSegment(ctx, 7, lsn)
		if err != nil {
			t.Fatal(err)
		}
		if actualFound != wantFound || actual != want {
			t.Fatalf("FindSegment(%d) actual=(%#v,%v), want=(%#v,%v)", lsn, actual, actualFound, want, wantFound)
		}
	}
	for from := uint64(0); from < 50; from += 7 {
		actual, err := engine.ListSegments(ctx, 7, from, 5)
		if err != nil {
			t.Fatal(err)
		}
		want, err := memory.ListSegments(ctx, csession.ListSegmentsRequest{Partition: 7, FromLSN: from, Limit: 5})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, want) {
			t.Fatalf("ListSegments(%d) actual=%#v want=%#v", from, actual, want)
		}
	}
	for _, timestamp := range []int64{0, 100, 127, 149, 150} {
		actual, actualFound, err := engine.LookupTimestamp(ctx, 7, timestamp)
		if err != nil {
			t.Fatal(err)
		}
		want, err := memory.LookupTimestamp(ctx, csession.TimestampLookupRequest{Partition: 7, TimestampMS: timestamp})
		if err != nil {
			t.Fatal(err)
		}
		if actualFound != want.Found || actual != want.Segment {
			t.Fatalf("LookupTimestamp(%d) actual=(%#v,%v), want=(%#v,%v)", timestamp, actual, actualFound, want.Segment, want.Found)
		}
	}

	request := csession.RetentionRequest{Version: csession.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 27, CreatedUnixMS: 1}
	if _, err := memory.RequestRetention(ctx, 7, request); err != nil {
		t.Fatal(err)
	}
	wantRetention, err := wantWriter.(csession.RetentionWriterSession).ApplyPendingRetention(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actualHead, applied, err := actualWriter.ApplyRetention(ctx, request.BeforeLSN, request.PolicyVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !applied || !wantRetention.Applied || !reflect.DeepEqual(actualHead, wantRetention.Head) {
		t.Fatalf("retention mismatch actual=(%#v,%v) want=(%#v,%v)", actualHead, applied, wantRetention.Head, wantRetention.Applied)
	}
	for lsn := uint64(0); lsn < 50; lsn++ {
		_, actualFound, err := engine.FindSegment(ctx, 7, lsn)
		if err != nil {
			t.Fatal(err)
		}
		_, wantFound, err := memory.FindSegment(ctx, 7, lsn)
		if err != nil {
			t.Fatal(err)
		}
		if actualFound != wantFound {
			t.Fatalf("post-retention FindSegment(%d) actual found=%v want=%v", lsn, actualFound, wantFound)
		}
	}
}
