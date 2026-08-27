package catengine

import (
	"context"
	"fmt"
	"sort"

	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

type PageSource interface {
	GetPage(ctx context.Context, key string) ([]byte, error)
}

type PageSourceFunc func(context.Context, string) ([]byte, error)

func (f PageSourceFunc) GetPage(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

type Reader struct {
	config Config
	pages  PageSource
}

// PageTarget is the immutable identity needed to check whether a catalog page
// is reachable from one decoded head. Key is compared exactly; the remaining
// fields bound traversal to branches that could contain it.
type PageTarget struct {
	Key   string
	Level uint8
	SeqLo uint64
	SeqHi uint64
}

// PagePathPage is a bounded, seq_lo-ordered view of reachable immutable page
// objects at one level.
type PagePathPage struct {
	Paths     []string
	NextSeqLo uint64
	HasMore   bool
}

func NewReader(config Config, pages PageSource) (*Reader, error) {
	if pages == nil {
		return nil, fmt.Errorf("catengine: nil page source")
	}
	return &Reader{config: config, pages: pages}, nil
}

func (r *Reader) FindSegment(ctx context.Context, head catformat.Head, lsn uint64) (pmeta.SegmentRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	if err := validateConfigHead(r.config, head); err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	if !head.HasLastSegment() || lsn < head.Header.OldestLSN || lsn >= head.Header.NextLSN {
		return pmeta.SegmentRef{}, false, nil
	}
	roots := headRoots(head)
	i := sort.Search(len(roots), func(i int) bool { return roots[i].seqHi() >= lsn })
	if i == len(roots) || roots[i].seqLo() > lsn {
		return pmeta.SegmentRef{}, false, nil
	}
	if roots[i].leaf != nil {
		return r.config.segmentRef(*roots[i].leaf), true, nil
	}
	return r.findInPage(ctx, *roots[i].ref, lsn, head.Header.WriterEpoch)
}

func (r *Reader) LookupTimestamp(ctx context.Context, head catformat.Head, timestampMS int64) (pmeta.SegmentRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	if err := validateConfigHead(r.config, head); err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	if !head.HasLastSegment() || head.Header.OldestLSN == head.Header.NextLSN || timestampMS > head.LastSegment.MaxTimestampMS {
		return pmeta.SegmentRef{}, false, nil
	}
	roots := headRoots(head)
	i := sort.Search(len(roots), func(i int) bool { return roots[i].maxTimestamp() >= timestampMS })
	if i == len(roots) {
		return pmeta.SegmentRef{}, false, nil
	}
	if roots[i].leaf != nil {
		return r.config.segmentRef(*roots[i].leaf), true, nil
	}
	return r.findTimestampInPage(ctx, *roots[i].ref, timestampMS, head.Header.WriterEpoch)
}

func (r *Reader) ListSegments(ctx context.Context, head catformat.Head, fromLSN uint64, limit int) (pmeta.SegmentPage, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.SegmentPage{}, err
	}
	if limit <= 0 || limit > pmeta.MaxSegmentPageLimit {
		limit = pmeta.DefaultSegmentPageLimit
	}
	if err := validateConfigHead(r.config, head); err != nil {
		return pmeta.SegmentPage{}, err
	}
	if !head.HasLastSegment() || fromLSN >= head.Header.NextLSN {
		return pmeta.SegmentPage{}, nil
	}
	collector := pageCollector{config: r.config, fromLSN: fromLSN, limit: limit}
	for _, root := range headRoots(head) {
		if collector.done() {
			break
		}
		if root.seqHi() < fromLSN {
			continue
		}
		if root.leaf != nil {
			collector.add(*root.leaf)
			continue
		}
		if err := r.collectPage(ctx, *root.ref, head.Header.WriterEpoch, &collector); err != nil {
			return pmeta.SegmentPage{}, err
		}
	}
	return pmeta.SegmentPage{Segments: collector.segments, NextLSN: collector.nextLSN, HasMore: collector.hasMore}, nil
}

// ListPagePaths returns reachable immutable pages at level ordered by their
// logical seq_lo. It walks references from the supplied head; it never lists
// the object store, so the result and head generation cannot be mixed.
func (r *Reader) ListPagePaths(ctx context.Context, head catformat.Head, level uint8, fromSeqLo uint64, limit int) (PagePathPage, error) {
	if err := ctx.Err(); err != nil {
		return PagePathPage{}, err
	}
	if level > catformat.MaxIndexLevel {
		return PagePathPage{}, fmt.Errorf("catengine: page level=%d exceeds maximum=%d", level, catformat.MaxIndexLevel)
	}
	if limit <= 0 || limit > pmeta.MaxSegmentPageLimit {
		limit = pmeta.DefaultSegmentPageLimit
	}
	if err := validateConfigHead(r.config, head); err != nil {
		return PagePathPage{}, err
	}
	collector := pagePathCollector{config: r.config, fromSeqLo: fromSeqLo, limit: limit}
	for _, root := range headRoots(head) {
		if collector.done() {
			break
		}
		if root.ref == nil {
			continue
		}
		if err := r.collectPagePath(ctx, *root.ref, level, head.Header.WriterEpoch, &collector); err != nil {
			return PagePathPage{}, err
		}
	}
	return PagePathPage{Paths: collector.paths, NextSeqLo: collector.nextSeqLo, HasMore: collector.hasMore}, nil
}

// IsPageReachable follows only branches whose level and sequence bounds can
// contain target. The caller must parse and validate the physical key first.
func (r *Reader) IsPageReachable(ctx context.Context, head catformat.Head, target PageTarget) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if target.Key == "" || target.Level > catformat.MaxIndexLevel || target.SeqHi < target.SeqLo {
		return false, fmt.Errorf("catengine: invalid page reachability target")
	}
	if err := validateConfigHead(r.config, head); err != nil {
		return false, err
	}
	for _, root := range headRoots(head) {
		if root.ref == nil {
			continue
		}
		reachable, err := r.pageReachable(ctx, *root.ref, target, head.Header.WriterEpoch)
		if err != nil || reachable {
			return reachable, err
		}
	}
	return false, nil
}

// MaxPageLevel returns the highest immutable page level referenced by head.
// Zero is also the leaf-page level, so an empty catalog and a catalog with
// only l00 pages both report zero; callers already scan l00 in either case.
func MaxPageLevel(head catformat.Head) uint8 {
	var maximum uint8
	for _, section := range head.Sections {
		for _, ref := range section.Entries {
			if ref.Level > maximum {
				maximum = ref.Level
			}
		}
	}
	return maximum
}

func (r *Reader) findInPage(ctx context.Context, ref catformat.IndexEntry, lsn, maxWriterEpoch uint64) (pmeta.SegmentRef, bool, error) {
	if ref.Level == 0 {
		page, err := r.loadLeaf(ctx, ref, maxWriterEpoch)
		if err != nil {
			return pmeta.SegmentRef{}, false, err
		}
		i := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].LastLSN >= lsn })
		if i == len(page.Entries) || page.Entries[i].BaseLSN > lsn {
			return pmeta.SegmentRef{}, false, nil
		}
		return r.config.segmentRef(page.Entries[i]), true, nil
	}
	page, err := r.loadIndex(ctx, ref, maxWriterEpoch)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	i := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].SeqHi >= lsn })
	if i == len(page.Entries) || page.Entries[i].SeqLo > lsn {
		return pmeta.SegmentRef{}, false, nil
	}
	return r.findInPage(ctx, page.Entries[i], lsn, page.Header.WriterEpoch)
}

func (r *Reader) findTimestampInPage(ctx context.Context, ref catformat.IndexEntry, timestampMS int64, maxWriterEpoch uint64) (pmeta.SegmentRef, bool, error) {
	if ref.Level == 0 {
		page, err := r.loadLeaf(ctx, ref, maxWriterEpoch)
		if err != nil {
			return pmeta.SegmentRef{}, false, err
		}
		i := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].MaxTimestampMS >= timestampMS })
		if i == len(page.Entries) {
			return pmeta.SegmentRef{}, false, nil
		}
		return r.config.segmentRef(page.Entries[i]), true, nil
	}
	page, err := r.loadIndex(ctx, ref, maxWriterEpoch)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	i := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].MaxTimestampMS >= timestampMS })
	if i == len(page.Entries) {
		return pmeta.SegmentRef{}, false, nil
	}
	return r.findTimestampInPage(ctx, page.Entries[i], timestampMS, page.Header.WriterEpoch)
}

func (r *Reader) collectPage(ctx context.Context, ref catformat.IndexEntry, maxWriterEpoch uint64, collector *pageCollector) error {
	if collector.done() || ref.SeqHi < collector.fromLSN {
		return nil
	}
	if ref.Level == 0 {
		page, err := r.loadLeaf(ctx, ref, maxWriterEpoch)
		if err != nil {
			return err
		}
		start := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].LastLSN >= collector.fromLSN })
		for i := start; i < len(page.Entries) && !collector.done(); i++ {
			collector.add(page.Entries[i])
		}
		return nil
	}
	page, err := r.loadIndex(ctx, ref, maxWriterEpoch)
	if err != nil {
		return err
	}
	start := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].SeqHi >= collector.fromLSN })
	for i := start; i < len(page.Entries) && !collector.done(); i++ {
		if err := r.collectPage(ctx, page.Entries[i], page.Header.WriterEpoch, collector); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) collectPagePath(ctx context.Context, ref catformat.IndexEntry, level uint8, maxWriterEpoch uint64, collector *pagePathCollector) error {
	if collector.done() || ref.SeqHi < collector.fromSeqLo || ref.Level < level {
		return nil
	}
	if ref.Level == level {
		collector.add(ref)
		return nil
	}
	page, err := r.loadIndex(ctx, ref, maxWriterEpoch)
	if err != nil {
		return err
	}
	start := sort.Search(len(page.Entries), func(i int) bool { return page.Entries[i].SeqHi >= collector.fromSeqLo })
	for i := start; i < len(page.Entries) && !collector.done(); i++ {
		if err := r.collectPagePath(ctx, page.Entries[i], level, page.Header.WriterEpoch, collector); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) pageReachable(ctx context.Context, ref catformat.IndexEntry, target PageTarget, maxWriterEpoch uint64) (bool, error) {
	if r.config.PagePath(ref) == target.Key {
		return true, nil
	}
	if target.SeqHi < ref.SeqLo || target.SeqLo > ref.SeqHi || target.Level >= ref.Level || ref.Level == 0 {
		return false, nil
	}
	page, err := r.loadIndex(ctx, ref, maxWriterEpoch)
	if err != nil {
		return false, err
	}
	for _, child := range page.Entries {
		if target.SeqHi < child.SeqLo || target.SeqLo > child.SeqHi {
			continue
		}
		reachable, err := r.pageReachable(ctx, child, target, page.Header.WriterEpoch)
		if err != nil || reachable {
			return reachable, err
		}
	}
	return false, nil
}

func (r *Reader) loadLeaf(ctx context.Context, ref catformat.IndexEntry, maxWriterEpoch uint64) (catformat.LeafPage, error) {
	key := r.config.LeafPagePath(ref)
	body, err := r.pages.GetPage(ctx, key)
	if err != nil {
		return catformat.LeafPage{}, fmt.Errorf("catengine: get leaf %q: %w", key, err)
	}
	page, id, err := catformat.ParseLeafPage(body)
	if err != nil {
		return catformat.LeafPage{}, fmt.Errorf("%w: parse leaf %q: %w", ErrCorruptCatalog, key, err)
	}
	if id != ref.PageID || page.Header.WriterEpoch > maxWriterEpoch || !matchesPageRef(page.Header, ref, r.config) {
		return catformat.LeafPage{}, fmt.Errorf("%w: leaf %q does not match its reference", ErrCorruptCatalog, key)
	}
	return page, nil
}

func (r *Reader) loadIndex(ctx context.Context, ref catformat.IndexEntry, maxWriterEpoch uint64) (catformat.IndexPage, error) {
	key := r.config.IndexPagePath(ref)
	body, err := r.pages.GetPage(ctx, key)
	if err != nil {
		return catformat.IndexPage{}, fmt.Errorf("catengine: get index %q: %w", key, err)
	}
	page, id, err := catformat.ParseIndexPage(body)
	if err != nil {
		return catformat.IndexPage{}, fmt.Errorf("%w: parse index %q: %w", ErrCorruptCatalog, key, err)
	}
	if id != ref.PageID || page.Header.WriterEpoch > maxWriterEpoch || !matchesPageRef(page.Header, ref, r.config) {
		return catformat.IndexPage{}, fmt.Errorf("%w: index %q does not match its reference", ErrCorruptCatalog, key)
	}
	return page, nil
}

func matchesPageRef(header catformat.PageHeader, ref catformat.IndexEntry, config Config) bool {
	return header.Partition == config.Partition &&
		header.StreamKey == config.StreamKey &&
		header.Level == ref.Level &&
		header.EntryCount == ref.EntryCount &&
		header.SeqLo == ref.SeqLo &&
		header.SeqHi == ref.SeqHi &&
		header.MinTimestampMS == ref.MinTimestampMS &&
		header.MaxTimestampMS == ref.MaxTimestampMS &&
		header.Generation == ref.Generation &&
		header.SegmentCount == ref.SegmentCount
}

type headRoot struct {
	ref  *catformat.IndexEntry
	leaf *catformat.LeafEntry
}

func (r headRoot) seqLo() uint64 {
	if r.ref != nil {
		return r.ref.SeqLo
	}
	return r.leaf.BaseLSN
}

func (r headRoot) seqHi() uint64 {
	if r.ref != nil {
		return r.ref.SeqHi
	}
	return r.leaf.LastLSN
}

func (r headRoot) maxTimestamp() int64 {
	if r.ref != nil {
		return r.ref.MaxTimestampMS
	}
	return r.leaf.MaxTimestampMS
}

func headRoots(head catformat.Head) []headRoot {
	roots := make([]headRoot, 0, len(head.Active)+len(head.Sections)*int(head.Header.IndexLimit))
	for i := len(head.Sections) - 1; i >= 0; i-- {
		for j := range head.Sections[i].Entries {
			roots = append(roots, headRoot{ref: &head.Sections[i].Entries[j]})
		}
	}
	for i := range head.Active {
		roots = append(roots, headRoot{leaf: &head.Active[i]})
	}
	return roots
}

type pageCollector struct {
	config   Config
	fromLSN  uint64
	limit    int
	segments []pmeta.SegmentRef
	nextLSN  uint64
	hasMore  bool
}

func (c *pageCollector) add(entry catformat.LeafEntry) {
	if c.done() || entry.LastLSN < c.fromLSN {
		return
	}
	if len(c.segments) == c.limit {
		c.hasMore = true
		c.nextLSN = entry.BaseLSN
		return
	}
	c.segments = append(c.segments, c.config.segmentRef(entry))
}

func (c *pageCollector) done() bool { return c.hasMore }

type pagePathCollector struct {
	config    Config
	fromSeqLo uint64
	limit     int
	paths     []string
	nextSeqLo uint64
	hasMore   bool
}

func (c *pagePathCollector) add(ref catformat.IndexEntry) {
	if c.done() || ref.SeqHi < c.fromSeqLo {
		return
	}
	if len(c.paths) == c.limit {
		c.nextSeqLo = ref.SeqLo
		c.hasMore = true
		return
	}
	c.paths = append(c.paths, c.config.PagePath(ref))
}

func (c *pagePathCollector) done() bool { return c.hasMore }
