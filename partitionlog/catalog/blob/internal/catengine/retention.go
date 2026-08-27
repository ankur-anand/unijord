package catengine

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
)

func ApplyRetention(ctx context.Context, config Config, pages PageSource, head catformat.Head, beforeLSN, policyVersion uint64) (Mutation, error) {
	if err := ctx.Err(); err != nil {
		return Mutation{}, err
	}
	if err := validateConfigHead(config, head); err != nil {
		return Mutation{}, err
	}
	if policyVersion == 0 {
		return Mutation{}, fmt.Errorf("%w: zero retention policy version", csession.ErrInvalidRequest)
	}
	switch {
	case policyVersion == head.Header.AppliedRetentionVersion && beforeLSN == head.Header.AppliedRetentionLSN:
		body, err := catformat.MarshalHead(head)
		return Mutation{Head: head, HeadBody: body}, err
	case policyVersion <= head.Header.AppliedRetentionVersion:
		return Mutation{}, fmt.Errorf("%w: policy_version=%d applied=%d", csession.ErrRetentionRegression, policyVersion, head.Header.AppliedRetentionVersion)
	case beforeLSN < head.Header.AppliedRetentionLSN:
		return Mutation{}, fmt.Errorf("%w: before_lsn=%d applied=%d", csession.ErrRetentionRegression, beforeLSN, head.Header.AppliedRetentionLSN)
	}
	if head.Header.Generation == math.MaxUint64 {
		return Mutation{}, fmt.Errorf("%w: partition=%d", csession.ErrGenerationExhausted, config.Partition)
	}
	if pages == nil {
		return Mutation{}, fmt.Errorf("catengine: nil page source")
	}

	target := beforeLSN
	if target > head.Header.NextLSN {
		target = head.Header.NextLSN
	}
	next := cloneHead(head)
	next.Header.Generation++
	next.Header.AppliedRetentionLSN = target
	next.Header.AppliedRetentionVersion = policyVersion
	result := Mutation{Head: next}

	switch {
	case target <= head.Header.OldestLSN:
		// The policy advanced inside or behind the already-retained boundary.
	case target >= head.Header.NextLSN:
		next.Header.OldestLSN = head.Header.NextLSN
		next.Header.ReachableSegmentCount = 0
		next.Header.ActiveCount = 0
		next.Header.LevelCount = 0
		next.Active = nil
		next.Sections = nil
	default:
		reader, err := NewReader(config, pages)
		if err != nil {
			return Mutation{}, err
		}
		segment, found, err := reader.FindSegment(ctx, head, target)
		if err != nil {
			return Mutation{}, err
		}
		if !found {
			return Mutation{}, fmt.Errorf("catengine: retention target=%d is not in committed history", target)
		}
		effective := segment.BaseLSN
		if effective > head.Header.OldestLSN {
			if err := trimHead(ctx, config, reader, &next, effective, &result.Pages); err != nil {
				return Mutation{}, err
			}
			next.Header.OldestLSN = effective
		}
	}

	next.Header.ActiveCount = uint32(len(next.Active))
	next.Header.LevelCount = uint32(len(next.Sections))
	next.Header.ReachableSegmentCount = reachableSegmentCount(next)
	result.Head = next
	var err error
	result.HeadBody, err = catformat.MarshalHead(next)
	if err != nil {
		return Mutation{}, err
	}
	return result, nil
}

func trimHead(ctx context.Context, config Config, reader *Reader, head *catformat.Head, effective uint64, pages *[]PageObject) error {
	for i := range head.Sections {
		entries := head.Sections[i].Entries
		start := sort.Search(len(entries), func(j int) bool { return entries[j].SeqHi >= effective })
		if start == len(entries) {
			head.Sections[i].Entries = nil
			continue
		}
		entries = slices.Clone(entries[start:])
		if entries[0].SeqLo < effective {
			trimmed, objects, err := trimPageRef(ctx, config, reader, entries[0], effective, head.Header.Generation, head.Header.WriterEpoch)
			if err != nil {
				return err
			}
			entries[0] = trimmed
			*pages = append(*pages, objects...)
		}
		head.Sections[i].Entries = entries
	}
	for len(head.Sections) > 0 && len(head.Sections[len(head.Sections)-1].Entries) == 0 {
		head.Sections = head.Sections[:len(head.Sections)-1]
	}

	start := sort.Search(len(head.Active), func(i int) bool { return head.Active[i].LastLSN >= effective })
	if start == len(head.Active) {
		head.Active = nil
	} else {
		head.Active = slices.Clone(head.Active[start:])
	}

	roots := headRoots(*head)
	if len(roots) == 0 || roots[0].seqLo() != effective {
		return fmt.Errorf("catengine: retained roots do not start at %d", effective)
	}
	return nil
}

func trimPageRef(ctx context.Context, config Config, reader *Reader, ref catformat.IndexEntry, effective, generation, writerEpoch uint64) (catformat.IndexEntry, []PageObject, error) {
	if effective <= ref.SeqLo {
		return ref, nil, nil
	}
	if effective > ref.SeqHi {
		return catformat.IndexEntry{}, nil, fmt.Errorf("catengine: retention boundary=%d beyond page [%d,%d]", effective, ref.SeqLo, ref.SeqHi)
	}
	if ref.Level == 0 {
		page, err := reader.loadLeaf(ctx, ref, writerEpoch)
		if err != nil {
			return catformat.IndexEntry{}, nil, err
		}
		start := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].LastLSN >= effective })
		if start == len(page.Entries) || page.Entries[start].BaseLSN != effective {
			return catformat.IndexEntry{}, nil, fmt.Errorf("catengine: leaf does not start retained history at %d", effective)
		}
		trimmed, object, err := sealLeaf(config, generation, writerEpoch, page.Entries[start:])
		if err != nil {
			return catformat.IndexEntry{}, nil, err
		}
		return trimmed, []PageObject{object}, nil
	}

	page, err := reader.loadIndex(ctx, ref, writerEpoch)
	if err != nil {
		return catformat.IndexEntry{}, nil, err
	}
	start := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].SeqHi >= effective })
	if start == len(page.Entries) {
		return catformat.IndexEntry{}, nil, fmt.Errorf("catengine: index does not contain retained LSN=%d", effective)
	}
	entries := slices.Clone(page.Entries[start:])
	var objects []PageObject
	if entries[0].SeqLo < effective {
		trimmed, childObjects, err := trimPageRef(ctx, config, reader, entries[0], effective, generation, writerEpoch)
		if err != nil {
			return catformat.IndexEntry{}, nil, err
		}
		entries[0] = trimmed
		objects = append(objects, childObjects...)
	}
	if entries[0].SeqLo != effective {
		return catformat.IndexEntry{}, nil, fmt.Errorf("catengine: index retained seq_lo=%d, want %d", entries[0].SeqLo, effective)
	}
	trimmed, object, err := sealIndex(config, generation, writerEpoch, ref.Level, entries)
	if err != nil {
		return catformat.IndexEntry{}, nil, err
	}
	objects = append(objects, object)
	return trimmed, objects, nil
}

func reachableSegmentCount(head catformat.Head) uint64 {
	var count uint64
	for _, section := range head.Sections {
		for _, ref := range section.Entries {
			if math.MaxUint64-count < ref.SegmentCount {
				panic("catengine: validated segment count overflow")
			}
			count += ref.SegmentCount
		}
	}
	if math.MaxUint64-count < uint64(len(head.Active)) {
		panic("catengine: validated active segment count overflow")
	}
	return count + uint64(len(head.Active))
}
