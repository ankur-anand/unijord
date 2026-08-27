package catengine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

func TestAppendSealsWriteOnceTreeAndReaderTraversesIt(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, _, err := NewHead(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	takeover, err := Takeover(config, head, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	head = takeover.Head
	pages := make(map[string][]byte)
	pageWrites := 0
	for lsn := uint64(0); lsn < 8; lsn++ {
		mutation, err := Append(config, head, testSegment(config, head, lsn))
		if err != nil {
			t.Fatalf("Append(%d) error = %v", lsn, err)
		}
		for _, object := range mutation.Pages {
			if existing, ok := pages[object.Key]; ok {
				if !reflect.DeepEqual(existing, object.Body) {
					t.Fatalf("page %q was overwritten with different bytes", object.Key)
				}
				t.Fatalf("page key %q was emitted twice", object.Key)
			}
			pages[object.Key] = append([]byte(nil), object.Body...)
			pageWrites++
		}
		head = mutation.Head
		parsed, err := catformat.ParseHead(mutation.HeadBody)
		if err != nil {
			t.Fatalf("ParseHead after append %d: %v", lsn, err)
		}
		if !reflect.DeepEqual(parsed, head) {
			t.Fatalf("encoded head differs after append %d:\nparsed=%#v\nhead=%#v", lsn, parsed, head)
		}
	}

	if got, want := pageWrites, 7; got != want {
		t.Fatalf("immutable page writes = %d, want %d", got, want)
	}
	if len(head.Active) != 0 || len(head.Sections) != 3 || len(head.Sections[0].Entries) != 0 || len(head.Sections[1].Entries) != 0 || len(head.Sections[2].Entries) != 1 {
		t.Fatalf("unexpected carry state: %#v", head.Sections)
	}
	root := head.Sections[2].Entries[0]
	if root.Level != 2 || root.SeqLo != 0 || root.SeqHi != 7 || root.SegmentCount != 8 {
		t.Fatalf("top root = %#v, want level 2 covering 0..7 with 8 segments", root)
	}
	for key := range pages {
		if !strings.HasSuffix(key, ".plc") || strings.HasSuffix(key, ".json") {
			t.Fatalf("page key is not canonical: %q", key)
		}
	}

	reader, err := NewReader(config, mapPageSource(pages))
	if err != nil {
		t.Fatal(err)
	}
	for lsn := uint64(0); lsn < 8; lsn++ {
		segment, found, err := reader.FindSegment(context.Background(), head, lsn)
		if err != nil {
			t.Fatalf("FindSegment(%d) error = %v", lsn, err)
		}
		if !found || segment.BaseLSN != lsn || segment.LastLSN != lsn {
			t.Fatalf("FindSegment(%d) = (%#v,%v), want matching segment", lsn, segment, found)
		}
	}
	if _, found, err := reader.FindSegment(context.Background(), head, 8); err != nil || found {
		t.Fatalf("FindSegment(8) = found %v, error %v; want absent", found, err)
	}
	segment, found, err := reader.LookupTimestamp(context.Background(), head, 105)
	if err != nil || !found || segment.BaseLSN != 5 {
		t.Fatalf("LookupTimestamp(105) = (%#v,%v,%v), want segment 5", segment, found, err)
	}

	var listed []pmeta.SegmentRef
	from := uint64(0)
	for {
		page, err := reader.ListSegments(context.Background(), head, from, 3)
		if err != nil {
			t.Fatalf("ListSegments(%d) error = %v", from, err)
		}
		listed = append(listed, page.Segments...)
		if !page.HasMore {
			break
		}
		if page.NextLSN <= from {
			t.Fatalf("non-advancing list cursor %d from %d", page.NextLSN, from)
		}
		from = page.NextLSN
	}
	if len(listed) != 8 {
		t.Fatalf("listed %d segments, want 8", len(listed))
	}
	for i := range listed {
		if listed[i].BaseLSN != uint64(i) {
			t.Fatalf("listed[%d].BaseLSN = %d", i, listed[i].BaseLSN)
		}
	}
}

func TestAppendRejectsNonDerivedSegmentURI(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, _, err := NewHead(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	takeover, err := Takeover(config, head, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	segment := testSegment(config, takeover.Head, 0)
	segment.URI = "some/other/object.plseg"
	if _, err := Append(config, takeover.Head, segment); err == nil || !strings.Contains(err.Error(), "want derived URI") {
		t.Fatalf("Append() error = %v, want derived URI rejection", err)
	}
}

func TestAppendRejectsExhaustedGeneration(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, _, err := NewHead(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	takeover, err := Takeover(config, head, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	head = takeover.Head
	head.Header.Generation = ^uint64(0)

	if _, err := Append(config, head, testSegment(config, head, 0)); !errors.Is(err, csession.ErrGenerationExhausted) {
		t.Fatalf("Append() error = %v, want ErrGenerationExhausted", err)
	}
}

func TestTakeoverRejectsCounterExhaustion(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, _, err := NewHead(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	head.Header.Generation = ^uint64(0)
	if _, err := Takeover(config, head, filled16(1)); !errors.Is(err, csession.ErrGenerationExhausted) {
		t.Fatalf("Takeover() error = %v, want ErrGenerationExhausted", err)
	}
	head.Header.Generation = 0
	head.Header.WriterEpoch = ^uint64(0)
	head.Header.WriterID = filled16(2)
	if _, err := Takeover(config, head, filled16(1)); !errors.Is(err, csession.ErrFenceExhausted) {
		t.Fatalf("Takeover() error = %v, want ErrFenceExhausted", err)
	}
}

func TestTerminalOpenSectionCannotSealLevel64Page(t *testing.T) {
	config := testConfig(t, 2, 2)
	head := catformat.Head{Sections: make([]catformat.OpenIndexSection, catformat.MaxOpenIndexLevel)}
	for i := range head.Sections {
		head.Sections[i].Level = uint8(i + 1)
	}
	ref := catformat.IndexEntry{Level: catformat.MaxIndexLevel}
	head.Sections[catformat.MaxOpenIndexLevel-1].Entries = []catformat.IndexEntry{ref}
	var pages []PageObject
	if err := push(config, &head, catformat.MaxOpenIndexLevel, ref, &pages); !errors.Is(err, ErrIndexFull) {
		t.Fatalf("push() error = %v, want ErrIndexFull", err)
	}
	if len(pages) != 0 {
		t.Fatalf("terminal push wrote %d pages", len(pages))
	}
}

func TestReaderRejectsPageThatDoesNotMatchReference(t *testing.T) {
	config := testConfig(t, 2, 2)
	head, _, err := NewHead(config, 0)
	if err != nil {
		t.Fatal(err)
	}
	takeover, err := Takeover(config, head, filled16(0x91))
	if err != nil {
		t.Fatal(err)
	}
	head = takeover.Head
	pages := make(map[string][]byte)
	for lsn := uint64(0); lsn < 2; lsn++ {
		mutation, err := Append(config, head, testSegment(config, head, lsn))
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range mutation.Pages {
			pages[object.Key] = object.Body
		}
		head = mutation.Head
	}
	ref := head.Sections[0].Entries[0]
	key := config.PagePath(ref)
	otherConfig := config
	otherConfig.Partition++
	page, _, err := catformat.ParseLeafPage(pages[key])
	if err != nil {
		t.Fatal(err)
	}
	page.Header.Partition = otherConfig.Partition
	wrongBody, _, err := catformat.MarshalLeafPage(page)
	if err != nil {
		t.Fatal(err)
	}
	pages[key] = wrongBody
	reader, err := NewReader(config, mapPageSource(pages))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.FindSegment(context.Background(), head, 0); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("FindSegment() error = %v, want reference mismatch", err)
	}
}

func testConfig(t *testing.T, leafLimit, indexLimit uint32) Config {
	t.Helper()
	config, err := NewConfig("catalog-format", "data-root", "tenant/events", 7, leafLimit, indexLimit, segformat.HashXXH64)
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func testSegment(config Config, head catformat.Head, lsn uint64) pmeta.SegmentRef {
	uuid := filled16(byte(lsn + 1))
	segment := pmeta.SegmentRef{
		StreamID:         config.StreamID,
		Partition:        config.Partition,
		WriterEpoch:      head.Header.WriterEpoch,
		SegmentUUID:      uuid,
		WriterTag:        head.Header.WriterID,
		BaseLSN:          lsn,
		LastLSN:          lsn,
		MinTimestampMS:   int64(100 + lsn),
		MaxTimestampMS:   int64(100 + lsn),
		RecordCount:      1,
		BlockCount:       1,
		SizeBytes:        4096,
		BlockIndexOffset: 2048,
		BlockIndexLength: 128,
		Codec:            segformat.CodecZstd,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      1000 + lsn,
		TrailerHash:      2000 + lsn,
	}
	segment.URI = config.SegmentURI(catformat.LeafEntry{
		BaseLSN:     segment.BaseLSN,
		WriterEpoch: segment.WriterEpoch,
		SegmentUUID: segment.SegmentUUID,
	})
	return segment
}

func mapPageSource(pages map[string][]byte) PageSource {
	return PageSourceFunc(func(ctx context.Context, key string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		body, ok := pages[key]
		if !ok {
			return nil, fmt.Errorf("missing page %q", key)
		}
		return append([]byte(nil), body...), nil
	})
}

func filled16(value byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = value
	}
	return out
}
