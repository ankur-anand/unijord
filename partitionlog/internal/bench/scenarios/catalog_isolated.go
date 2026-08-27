package scenarios

import (
	"context"
	"fmt"
	mrand "math/rand/v2"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/internal/bench"
)

func init() { bench.Register(catalogIsolated{}) }

// catalogIsolated drives the blob catalog alone with synthetic segment refs
// (no segment uploads), so catalog costs are measured without segment I/O
// and every GET and byte of a lookup can be counted exactly.
type catalogIsolated struct{}

const isolatedStream = "plbench/isolated"

func (catalogIsolated) Name() string { return "catalog_isolated" }
func (catalogIsolated) Description() string {
	return "blob catalog only, synthetic refs: publish cost, inventory, exact GETs/bytes per lookup"
}

func (catalogIsolated) Profiles() map[string]bench.Params {
	base := bench.Params{RecordsPerSegment: 4, LeafLimit: 128, IndexLimit: 128}
	smoke, ci, deep := base, base, base
	smoke.Segments, smoke.IndexLimit, smoke.Samples = 3_000, 8, 100
	ci.Segments, ci.Samples = 20_000, 200
	deep.Segments, deep.Samples = 100_000, 300
	return map[string]bench.Params{"smoke": smoke, "ci": ci, "deep": deep}
}

func (catalogIsolated) Run(ctx context.Context, run *bench.Run) error {
	p := run.Params
	store := bench.NewCountingStore(run.Provider.Backend())
	cat, err := catalogblob.New(store, catalogblob.Options{
		Prefix: run.Prefix + "/catalog", SegmentRootPrefix: run.Prefix, StreamID: isolatedStream,
		LeafSegmentLimit: p.LeafLimit, IndexRefLimit: p.IndexLimit,
	})
	if err != nil {
		return err
	}
	writerID := bench.NewID()
	session, err := cat.OpenWriter(ctx, 0, writerID)
	if err != nil {
		return err
	}
	head := session.Head()

	// ---- publish ---------------------------------------------------------------
	if int(head.SegmentCount) < p.Segments {
		run.Section("publish: %d -> %d synthetic segments (no segment uploads)", head.SegmentCount, p.Segments)
		pub := &bench.Sample{}
		next := head.NextLSN
		t0 := time.Now()
		progress := max(p.Segments/10, 1)
		for i := int(head.SegmentCount); i < p.Segments; i++ {
			seg := bench.SyntheticSegment(run.Prefix, isolatedStream, 0, next, p.RecordsPerSegment, session.Epoch(), writerID)
			var err error
			pub.Time(func() { _, err = session.AppendSegment(ctx, seg) })
			if err != nil {
				return fmt.Errorf("publish %d: %w", i, err)
			}
			next = seg.LastLSN + 1
			if done := i + 1; done%progress == 0 || done == p.Segments {
				run.Logf("%-8d %8.0fs", done, time.Since(t0).Seconds())
			}
		}
		run.Latency("publish", pub.Stats(), time.Since(t0), fmt.Sprintf("%d segments", p.Segments-int(head.SegmentCount)))
	}
	head, err = cat.LoadPartition(ctx, 0)
	if err != nil {
		return err
	}
	run.Section("history: %d reachable segments, LSN [%d, %d)", head.ReachableSegmentCount, head.OldestLSN, head.NextLSN)

	// ---- inventory + structural checks ------------------------------------------
	inv, _, err := bench.CatalogInventory(ctx, store.Inner(), run.Prefix)
	if err != nil {
		return err
	}
	for k, v := range inv {
		run.Result.Inventory[k] = v
	}
	leafBytes, indexBytes := bench.PageSizes(p.LeafLimit, p.IndexLimit)
	expLeaves, expIndex := bench.ExpectedSealedPages(int(head.ReachableSegmentCount), p.LeafLimit, p.IndexLimit)
	levels := len(expIndex) + 1
	run.Scalar("catalog.head.bytes", float64(inv["head"].Bytes), "bytes", "")
	run.Scalar("catalog.leaf.objects", float64(inv["leaf"].Objects), "", "")
	run.Check("catalog.leaf_objects_expected", inv["leaf"].Objects == expLeaves, "%d, expected %d", inv["leaf"].Objects, expLeaves)
	run.Check("catalog.leaf_size_exact", inv["leaf"].Objects == 0 || inv["leaf"].MaxBytes == leafBytes, "max %d B, want %d", inv["leaf"].MaxBytes, leafBytes)
	deadTotal := 0
	for level := 1; level <= max(len(expIndex), 1); level++ {
		class := fmt.Sprintf("index-l%02d", level)
		c := inv[class]
		exp := 0
		if level-1 < len(expIndex) {
			exp = expIndex[level-1]
		}
		deadTotal += c.Objects - exp
		run.Scalar("catalog."+class+".objects", float64(c.Objects), "", "")
		run.Check("catalog."+class+"_objects_expected", c.Objects == exp, "%d, expected %d sealed", c.Objects, exp)
		if c.Objects > 0 {
			run.Check("catalog."+class+"_size_exact", c.MaxBytes == indexBytes, "max %d B, want %d", c.MaxBytes, indexBytes)
		}
	}
	run.Scalar("catalog.dead_index_objects", float64(deadTotal), "", "must be 0 for a write-once tree")
	run.Check("catalog.head_within_bound", inv["head"].Bytes <= bench.HeadBound(p.LeafLimit, p.IndexLimit, levels), "%d B, bound %d", inv["head"].Bytes, bench.HeadBound(p.LeafLimit, p.IndexLimit, levels))

	// ---- lookups with exact request accounting ---------------------------------
	run.Section("lookups (fresh head load each; GETs and bytes counted)")
	rng := mrand.New(mrand.NewPCG(7, 11))
	randLSN := func() uint64 { return head.OldestLSN + rng.Uint64N(head.NextLSN-head.OldestLSN) }
	// A lookup reads the head, then one index page per sealed index level,
	// then one leaf: levels counts the leaf level, so levels-1 index pages.
	maxGETs := 1 + levels
	maxBytes := inv["head"].Bytes + (levels-1)*indexBytes + leafBytes
	wrong := 0
	measure := func(name string, fn func() error, note string) error {
		store.Reset()
		var maxGets, lastGets int64
		wrapped := func() error {
			before := store.Gets()
			err := fn()
			lastGets = store.Gets() - before
			maxGets = max(maxGets, lastGets)
			return err
		}
		if err := run.Measure(name, p.Samples, wrapped, note); err != nil {
			return err
		}
		gets := float64(store.Gets()) / float64(p.Samples)
		bytes := float64(store.Bytes()) / float64(p.Samples)
		run.Scalar(name+".gets_max", float64(maxGets), "", "max GETs in one lookup (exact)")
		run.Scalar(name+".gets_mean", gets, "gets", "mean GETs per lookup")
		run.Scalar(name+".bytes", bytes, "bytes", "mean bytes per lookup")
		run.Check(name+"_request_profile", int(maxGets) <= maxGETs && bytes <= float64(maxBytes), "max %d GETs (limit %d), mean %.0f B (limit %d)", maxGets, maxGETs, bytes, maxBytes)
		return nil
	}
	if err := measure("catalog.find_segment", func() error {
		lsn := randLSN()
		seg, ok, err := cat.FindSegment(ctx, 0, lsn)
		if err != nil {
			return err
		}
		if !ok || lsn < seg.BaseLSN || lsn > seg.LastLSN {
			wrong++
		}
		return nil
	}, "random LSN over the whole history"); err != nil {
		return err
	}
	if err := measure("catalog.lookup_timestamp", func() error {
		lsn := randLSN()
		ts := bench.TSBase + int64(lsn)
		res, err := cat.LookupTimestamp(ctx, catalog.TimestampLookupRequest{Partition: 0, TimestampMS: ts})
		if err != nil {
			return err
		}
		if !res.Found || res.Segment.MaxTimestampMS < ts || res.Segment.MinTimestampMS > ts {
			wrong++
		}
		return nil
	}, "random timestamp over the whole history"); err != nil {
		return err
	}
	if err := measure("catalog.find_segment_recent", func() error {
		lo := head.NextLSN - max((head.NextLSN-head.OldestLSN)/100, 1)
		lsn := lo + rng.Uint64N(head.NextLSN-lo)
		seg, ok, err := cat.FindSegment(ctx, 0, lsn)
		if err != nil {
			return err
		}
		if !ok || lsn < seg.BaseLSN || lsn > seg.LastLSN {
			wrong++
		}
		return nil
	}, "newest 1 % of history (inline roots)"); err != nil {
		return err
	}
	run.Check("catalog.lookups_correct", wrong == 0, "%d wrong results", wrong)
	return nil
}
