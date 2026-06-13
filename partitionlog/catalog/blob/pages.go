package blob

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

type nextPageSet struct {
	IndexFrontier  []pageRef
	LeafFrontier   *pageRef
	ActiveSegments []pmeta.SegmentRef
}

func (c *Catalog) buildNextPageSet(ctx context.Context, head headFile, segment pmeta.SegmentRef, generation uint64) (nextPageSet, error) {
	active := slices.Clone(head.ActiveSegments)
	if len(active) >= c.opts.LeafSegmentLimit {
		return nextPageSet{}, fmt.Errorf("%w: active segment count=%d limit=%d", ErrCorruptCatalog, len(active), c.opts.LeafSegmentLimit)
	}
	active = append(active, segment)

	next := nextPageSet{
		IndexFrontier: cloneRefs(head.IndexFrontier),
		LeafFrontier:  clonePageRefPtr(head.LeafFrontier),
	}
	if len(active) < c.opts.LeafSegmentLimit {
		next.ActiveSegments = active
		return next, nil
	}

	if head.LeafFrontier != nil {
		nextFrontier, err := c.carryPageRef(ctx, segment.Partition, next.IndexFrontier, *head.LeafFrontier, generation)
		if err != nil {
			return nextPageSet{}, err
		}
		next.IndexFrontier = nextFrontier
	}
	leaf, _, err := c.writeLeaf(ctx, leafPage{
		Partition:  segment.Partition,
		Generation: generation,
		Segments:   active,
	})
	if err != nil {
		return nextPageSet{}, err
	}
	next.LeafFrontier = leaf
	next.IndexFrontier = trimFrontier(next.IndexFrontier)
	return next, nil
}

func (c *Catalog) carryPageRef(ctx context.Context, partition uint32, frontier []pageRef, child pageRef, generation uint64) ([]pageRef, error) {
	level := child.Level + 1
	frontier = ensureFrontierLevel(frontier, level)
	slot := int(level - 1)
	existing := frontier[slot]
	if existing.Path == "" {
		next, err := c.writeIndex(ctx, indexPage{
			Version:    pageVersion,
			Type:       "index",
			Level:      level,
			Partition:  partition,
			Generation: generation,
			Refs:       []pageRef{child},
		})
		if err != nil {
			return nil, err
		}
		frontier[slot] = *next
		return trimFrontier(frontier), nil
	}

	page, err := c.loadIndex(ctx, existing)
	if err != nil {
		return nil, err
	}
	if len(page.Refs) < c.opts.IndexRefLimit {
		page.Generation = generation
		page.Refs = slices.Clone(page.Refs)
		page.Refs = append(page.Refs, child)
		next, err := c.writeIndex(ctx, page)
		if err != nil {
			return nil, err
		}
		frontier[slot] = *next
		return trimFrontier(frontier), nil
	}

	frontier[slot] = pageRef{}
	frontier, err = c.carryPageRef(ctx, partition, frontier, existing, generation)
	if err != nil {
		return nil, err
	}
	frontier = ensureFrontierLevel(frontier, level)
	next, err := c.writeIndex(ctx, indexPage{
		Version:    pageVersion,
		Type:       "index",
		Level:      level,
		Partition:  partition,
		Generation: generation,
		Refs:       []pageRef{child},
	})
	if err != nil {
		return nil, err
	}
	frontier[slot] = *next
	return trimFrontier(frontier), nil
}

func (c *Catalog) writeLeaf(ctx context.Context, page leafPage) (*pageRef, leafPage, error) {
	if len(page.Segments) == 0 {
		return nil, leafPage{}, fmt.Errorf("%w: empty leaf", ErrCorruptCatalog)
	}
	page.Version = pageVersion
	page.Type = "leaf"
	page.SeqLo = page.Segments[0].BaseLSN
	page.SeqHi = page.Segments[len(page.Segments)-1].LastLSN
	if err := validateLeafPage(page); err != nil {
		return nil, leafPage{}, err
	}
	page.PageID = ""
	canonical, err := json.Marshal(page)
	if err != nil {
		return nil, leafPage{}, err
	}
	page.PageID = shortPageID(canonical)
	body, err := json.Marshal(page)
	if err != nil {
		return nil, leafPage{}, err
	}
	ref := pageRef{
		SeqLo:      page.SeqLo,
		SeqHi:      page.SeqHi,
		Generation: page.Generation,
		PageID:     page.PageID,
		Path:       LeafPagePath(c.opts.Prefix, page.Partition, page.SeqLo, page.SeqHi, page.Generation, page.PageID),
		Count:      len(page.Segments),
	}
	if _, err := c.backend.Put(ctx, ref.Path, body); err != nil {
		return nil, leafPage{}, err
	}
	return &ref, page, nil
}

func (c *Catalog) writeIndex(ctx context.Context, page indexPage) (*pageRef, error) {
	if len(page.Refs) == 0 {
		return nil, fmt.Errorf("%w: empty index", ErrCorruptCatalog)
	}
	page.Version = pageVersion
	page.Type = "index"
	page.SeqLo = page.Refs[0].SeqLo
	page.SeqHi = page.Refs[len(page.Refs)-1].SeqHi
	if err := validateIndexPage(page); err != nil {
		return nil, err
	}
	page.PageID = ""
	canonical, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	page.PageID = shortPageID(canonical)
	body, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	ref := pageRef{
		Level:      page.Level,
		SeqLo:      page.SeqLo,
		SeqHi:      page.SeqHi,
		Generation: page.Generation,
		PageID:     page.PageID,
		Path:       IndexPagePath(c.opts.Prefix, page.Partition, page.Level, page.SeqLo, page.SeqHi, page.Generation, page.PageID),
		Count:      len(page.Refs),
	}
	if _, err := c.backend.Put(ctx, ref.Path, body); err != nil {
		return nil, err
	}
	return &ref, nil
}

func (c *Catalog) loadLeaf(ctx context.Context, ref pageRef) (leafPage, error) {
	obj, err := c.backend.Get(ctx, ref.Path)
	if err != nil {
		return leafPage{}, err
	}
	var page leafPage
	if err := json.Unmarshal(obj.Body, &page); err != nil {
		return leafPage{}, fmt.Errorf("%w: decode leaf %s: %v", ErrCorruptCatalog, ref.Path, err)
	}
	if err := validateLeafPage(page); err != nil {
		return leafPage{}, err
	}
	if err := verifyLeafRef(page, ref); err != nil {
		return leafPage{}, err
	}
	if err := verifyPageID(ref.PageID, page); err != nil {
		return leafPage{}, err
	}
	return page, nil
}

func (c *Catalog) loadIndex(ctx context.Context, ref pageRef) (indexPage, error) {
	obj, err := c.backend.Get(ctx, ref.Path)
	if err != nil {
		return indexPage{}, err
	}
	var page indexPage
	if err := json.Unmarshal(obj.Body, &page); err != nil {
		return indexPage{}, fmt.Errorf("%w: decode index %s: %v", ErrCorruptCatalog, ref.Path, err)
	}
	if err := validateIndexPage(page); err != nil {
		return indexPage{}, err
	}
	if err := verifyIndexRef(page, ref); err != nil {
		return indexPage{}, err
	}
	if err := verifyPageID(ref.PageID, page); err != nil {
		return indexPage{}, err
	}
	return page, nil
}

func cloneRefs(refs []pageRef) []pageRef {
	return slices.Clone(refs)
}

func clonePageRefPtr(ref *pageRef) *pageRef {
	if ref == nil {
		return nil
	}
	cloned := *ref
	return &cloned
}

func cloneLeafPage(page leafPage) leafPage {
	page.Segments = slices.Clone(page.Segments)
	return page
}

func ensureFrontierLevel(frontier []pageRef, level uint8) []pageRef {
	for len(frontier) < int(level) {
		frontier = append(frontier, pageRef{})
	}
	return frontier
}

func trimFrontier(frontier []pageRef) []pageRef {
	for len(frontier) > 0 && frontier[len(frontier)-1].Path == "" {
		frontier = frontier[:len(frontier)-1]
	}
	return frontier
}
