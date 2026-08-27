package catengine

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

type PageObject struct {
	Key   string
	Level uint8
	Ref   catformat.IndexEntry
	Body  []byte
}

type Mutation struct {
	Head     catformat.Head
	HeadBody []byte
	Pages    []PageObject
}

func NewHead(config Config, nextLSN uint64) (catformat.Head, []byte, error) {
	if nextLSN == catformat.ReservedLSN {
		return catformat.Head{}, nil, fmt.Errorf("%w: next_lsn exhausted", csession.ErrInvalidRequest)
	}
	head := catformat.Head{Header: catformat.HeadHeader{
		Partition:            config.Partition,
		HashAlgo:             config.HashAlgo,
		SegmentLayoutVersion: catformat.SegmentLayoutV1,
		LeafLimit:            config.LeafLimit,
		IndexLimit:           config.IndexLimit,
		NextLSN:              nextLSN,
		OldestLSN:            nextLSN,
		StreamKey:            config.StreamKey,
		DataRootKey:          config.DataRootKey,
	}}
	body, err := catformat.MarshalHead(head)
	return head, body, err
}

func Takeover(config Config, head catformat.Head, writerID [16]byte) (Mutation, error) {
	if writerID == ([16]byte{}) {
		return Mutation{}, fmt.Errorf("%w: empty writer_id", csession.ErrInvalidRequest)
	}
	if err := validateConfigHead(config, head); err != nil {
		return Mutation{}, err
	}
	if head.Header.WriterEpoch == math.MaxUint64 {
		return Mutation{}, fmt.Errorf("%w: partition=%d", csession.ErrFenceExhausted, config.Partition)
	}
	if head.Header.Generation == math.MaxUint64 {
		return Mutation{}, fmt.Errorf("%w: partition=%d", csession.ErrGenerationExhausted, config.Partition)
	}
	next := cloneHead(head)
	next.Header.WriterEpoch++
	next.Header.Generation++
	next.Header.WriterID = writerID
	body, err := catformat.MarshalHead(next)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{Head: next, HeadBody: body}, nil
}

func Append(config Config, head catformat.Head, segment pmeta.SegmentRef) (Mutation, error) {
	if err := validateConfigHead(config, head); err != nil {
		return Mutation{}, err
	}
	if head.Header.Generation == math.MaxUint64 {
		return Mutation{}, fmt.Errorf("%w: partition=%d", csession.ErrGenerationExhausted, config.Partition)
	}
	entry, err := config.leafEntry(segment)
	if err != nil {
		return Mutation{}, err
	}
	if head.Header.WriterEpoch == 0 || entry.WriterEpoch != head.Header.WriterEpoch {
		return Mutation{}, fmt.Errorf("%w: segment fence does not match head", csession.ErrStaleWriter)
	}
	if entry.WriterTag != head.Header.WriterID {
		return Mutation{}, fmt.Errorf("%w: segment writer_tag does not match writer_id", csession.ErrInvalidRequest)
	}
	if entry.BaseLSN != head.Header.NextLSN {
		return Mutation{}, fmt.Errorf("%w: segment base_lsn=%d next_lsn=%d", csession.ErrConflict, entry.BaseLSN, head.Header.NextLSN)
	}
	if head.HasLastSegment() && entry.MinTimestampMS < head.LastSegment.MaxTimestampMS {
		return Mutation{}, fmt.Errorf("%w: segment min_ts=%d previous max_ts=%d", csession.ErrTimestampOrder, entry.MinTimestampMS, head.LastSegment.MaxTimestampMS)
	}

	next := cloneHead(head)
	next.Header.Generation++
	next.Header.NextLSN = entry.LastLSN + 1
	next.Header.SegmentCount++
	next.Header.ReachableSegmentCount++
	if !next.HasLastSegment() {
		next.Header.OldestLSN = entry.BaseLSN
		next.Header.Flags |= catformat.FlagHasLastSegment
	}
	next.LastSegment = entry
	next.Active = append(next.Active, entry)
	result := Mutation{Head: next}
	if uint32(len(next.Active)) == config.LeafLimit {
		ref, object, err := sealLeaf(config, next.Header.Generation, next.Header.WriterEpoch, next.Active)
		if err != nil {
			return Mutation{}, err
		}
		result.Pages = append(result.Pages, object)
		next.Active = nil
		next.Header.ActiveCount = 0
		if err := push(config, &next, 1, ref, &result.Pages); err != nil {
			return Mutation{}, err
		}
	} else {
		next.Header.ActiveCount = uint32(len(next.Active))
	}
	next.Header.LevelCount = uint32(len(next.Sections))
	result.Head = next
	result.HeadBody, err = catformat.MarshalHead(next)
	if err != nil {
		return Mutation{}, err
	}
	return result, nil
}

func push(config Config, head *catformat.Head, level uint8, ref catformat.IndexEntry, pages *[]PageObject) error {
	if level == 0 || level > catformat.MaxOpenIndexLevel {
		return fmt.Errorf("%w: index level=%d", ErrIndexFull, level)
	}
	for len(head.Sections) < int(level) {
		head.Sections = append(head.Sections, catformat.OpenIndexSection{Level: uint8(len(head.Sections) + 1)})
	}
	section := &head.Sections[level-1]
	if level == catformat.MaxOpenIndexLevel && len(section.Entries)+1 >= int(config.IndexLimit) {
		return fmt.Errorf("%w: terminal open index level=%d", ErrIndexFull, level)
	}
	section.Entries = append(section.Entries, ref)
	if uint32(len(section.Entries)) < config.IndexLimit {
		return nil
	}
	parent, object, err := sealIndex(config, head.Header.Generation, head.Header.WriterEpoch, level, section.Entries)
	if err != nil {
		return err
	}
	*pages = append(*pages, object)
	section.Entries = nil
	return push(config, head, level+1, parent, pages)
}

func sealLeaf(config Config, generation, writerEpoch uint64, entries []catformat.LeafEntry) (catformat.IndexEntry, PageObject, error) {
	first, last := entries[0], entries[len(entries)-1]
	page := catformat.LeafPage{
		Header: catformat.PageHeader{
			Partition:      config.Partition,
			HashAlgo:       config.HashAlgo,
			EntryCount:     uint32(len(entries)),
			EntrySize:      catformat.LeafEntrySize,
			SeqLo:          first.BaseLSN,
			SeqHi:          last.LastLSN,
			MinTimestampMS: first.MinTimestampMS,
			MaxTimestampMS: last.MaxTimestampMS,
			Generation:     generation,
			WriterEpoch:    writerEpoch,
			StreamKey:      config.StreamKey,
			SegmentCount:   uint64(len(entries)),
		},
		Entries: slices.Clone(entries),
	}
	body, id, err := catformat.MarshalLeafPage(page)
	if err != nil {
		return catformat.IndexEntry{}, PageObject{}, err
	}
	ref := catformat.IndexEntry{
		EntryCount:     uint32(len(entries)),
		SeqLo:          first.BaseLSN,
		SeqHi:          last.LastLSN,
		MinTimestampMS: first.MinTimestampMS,
		MaxTimestampMS: last.MaxTimestampMS,
		Generation:     generation,
		PageID:         id,
		SegmentCount:   uint64(len(entries)),
	}
	return ref, PageObject{Key: config.LeafPagePath(ref), Ref: ref, Body: body}, nil
}

func sealIndex(config Config, generation, writerEpoch uint64, level uint8, entries []catformat.IndexEntry) (catformat.IndexEntry, PageObject, error) {
	first, last := entries[0], entries[len(entries)-1]
	var segmentCount uint64
	for _, entry := range entries {
		if math.MaxUint64-segmentCount < entry.SegmentCount {
			return catformat.IndexEntry{}, PageObject{}, fmt.Errorf("catengine: segment count overflow")
		}
		segmentCount += entry.SegmentCount
	}
	page := catformat.IndexPage{
		Header: catformat.PageHeader{
			Partition:      config.Partition,
			Level:          level,
			HashAlgo:       config.HashAlgo,
			EntryCount:     uint32(len(entries)),
			EntrySize:      catformat.IndexEntrySize,
			SeqLo:          first.SeqLo,
			SeqHi:          last.SeqHi,
			MinTimestampMS: first.MinTimestampMS,
			MaxTimestampMS: last.MaxTimestampMS,
			Generation:     generation,
			WriterEpoch:    writerEpoch,
			StreamKey:      config.StreamKey,
			SegmentCount:   segmentCount,
		},
		Entries: slices.Clone(entries),
	}
	body, id, err := catformat.MarshalIndexPage(page)
	if err != nil {
		return catformat.IndexEntry{}, PageObject{}, err
	}
	ref := catformat.IndexEntry{
		Level:          level,
		EntryCount:     uint32(len(entries)),
		SeqLo:          first.SeqLo,
		SeqHi:          last.SeqHi,
		MinTimestampMS: first.MinTimestampMS,
		MaxTimestampMS: last.MaxTimestampMS,
		Generation:     generation,
		PageID:         id,
		SegmentCount:   segmentCount,
	}
	return ref, PageObject{Key: config.IndexPagePath(ref), Level: level, Ref: ref, Body: body}, nil
}

func validateConfigHead(config Config, head catformat.Head) error {
	if head.Header.Partition != config.Partition || head.Header.StreamKey != config.StreamKey || head.Header.DataRootKey != config.DataRootKey {
		return fmt.Errorf("catengine: head identity does not match configuration")
	}
	if head.Header.LeafLimit != config.LeafLimit || head.Header.IndexLimit != config.IndexLimit || head.Header.HashAlgo != config.HashAlgo {
		return fmt.Errorf("catengine: head format options do not match configuration")
	}
	if head.Header.SegmentLayoutVersion != catformat.SegmentLayoutV1 {
		return fmt.Errorf("catengine: unsupported segment layout=%d", head.Header.SegmentLayoutVersion)
	}
	return nil
}

func cloneHead(head catformat.Head) catformat.Head {
	next := head
	next.Active = slices.Clone(head.Active)
	if len(head.Sections) == 0 {
		next.Sections = nil
		return next
	}
	next.Sections = make([]catformat.OpenIndexSection, len(head.Sections))
	for i := range head.Sections {
		next.Sections[i] = head.Sections[i]
		next.Sections[i].Entries = slices.Clone(head.Sections[i].Entries)
	}
	return next
}

func SamePageObject(left, right PageObject) bool {
	return left.Key == right.Key && left.Level == right.Level && left.Ref == right.Ref && bytes.Equal(left.Body, right.Body)
}

var ErrIndexFull = errors.New("catengine: index full")
