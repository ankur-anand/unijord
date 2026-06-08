package blobcatalog

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

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

func cloneRefs(refs []pageRef) []pageRef {
	return slices.Clone(refs)
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
