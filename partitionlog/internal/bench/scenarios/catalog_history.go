package scenarios

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	plifecycle "github.com/ankur-anand/unijord/partitionlog/blob/lifecycle"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/internal/bench"
	"github.com/ankur-anand/unijord/partitionlog/reader"
)

func init() { bench.Register(catalogHistory{}) }

// catalogHistory drives the real writer, reader, catalog, and lifecycle
// through one partition with a deep history: write N small segments, measure
// every catalog and reader path at that depth, fail over the writer, then
// apply retention, reclaim, and scrub.
//
// Resumable only during the append phase: rerunning with the same prefix
// continues toward the segment target as long as the partition has never had
// retention applied. Once retention has run, the tree holds trimmed pages and
// the sealed-page arithmetic no longer describes it, so a rerun is refused
// and a fresh prefix is required.
type catalogHistory struct{}

const (
	historyStream    = "plbench/history"
	historyPartition = uint32(0)
)

func (catalogHistory) Name() string { return "catalog_history" }
func (catalogHistory) Description() string {
	return "real writer/reader/lifecycle over a deep single-partition history: lookups, failover, retention, reclaim, scrub"
}

func (catalogHistory) Profiles() map[string]bench.Params {
	base := bench.Params{RecordsPerSegment: 4, RecordBytes: 256, LeafLimit: 128, Inflight: 8, WriterOpens: 3, Retention: true, Reclaim: true, Scrub: true}
	smoke, ci, deep := base, base, base
	// smoke: k=8 forces two index levels inside 2k segments so every
	// depth-dependent path executes.
	smoke.Segments, smoke.IndexLimit, smoke.Samples, smoke.ReplaySegments, smoke.WriterOpens = 2_000, 8, 60, 300, 2
	ci.Segments, ci.IndexLimit, ci.Samples, ci.ReplaySegments = 20_000, 128, 200, 2_000
	deep.Segments, deep.IndexLimit, deep.Samples, deep.ReplaySegments = 100_000, 128, 300, 5_000
	return map[string]bench.Params{"smoke": smoke, "ci": ci, "deep": deep}
}

type publishMetrics struct {
	publish, finalize *bench.Sample
}

func (m *publishMetrics) Observe(metric partitionlog.Metric) {
	if metric.Err != nil {
		return
	}
	switch metric.Name {
	case partitionlog.MetricWriterSegmentPublish:
		m.publish.Add(metric.Duration)
	case partitionlog.MetricWriterSegmentFinalize:
		m.finalize.Add(metric.Duration)
	}
}

func (s catalogHistory) Run(ctx context.Context, run *bench.Run) error {
	p := run.Params
	store, err := run.Provider.Store(ctx, run.Prefix, historyStream, p.LeafLimit, p.IndexLimit)
	if err != nil {
		return err
	}
	metrics := &publishMetrics{publish: &bench.Sample{}, finalize: &bench.Sample{}}
	log, err := partitionlog.Open(partitionlog.Options{Store: store, Metrics: metrics})
	if err != nil {
		return err
	}
	defer log.Close()
	backend := run.Provider.Backend()

	// ---- write -------------------------------------------------------------
	if err := s.write(ctx, run, log, metrics, p); err != nil {
		return err
	}

	cat := store.ReaderCatalog()
	head, err := cat.LoadPartition(ctx, historyPartition)
	if err != nil {
		return err
	}
	run.Section("history: %d reachable segments (%d lifetime), LSN [%d, %d), epoch %d", head.ReachableSegmentCount, head.SegmentCount, head.OldestLSN, head.NextLSN, head.WriterEpoch)
	if head.ReachableSegmentCount == 0 {
		return errors.New("history: nothing to measure")
	}

	// ---- inventory + structural checks -------------------------------------
	inv, _, err := bench.CatalogInventory(ctx, backend, run.Prefix)
	if err != nil {
		return err
	}
	for k, v := range inv {
		run.Result.Inventory[k] = v
	}
	leafBytes, indexBytes := bench.PageSizes(p.LeafLimit, p.IndexLimit)
	expLeaves, expIndex := bench.ExpectedSealedPages(int(head.ReachableSegmentCount), p.LeafLimit, p.IndexLimit)
	run.Scalar("catalog.head.bytes", float64(inv["head"].Bytes), "bytes", "head.plc")
	run.Scalar("catalog.leaf.objects", float64(inv["leaf"].Objects), "", "")
	run.Scalar("catalog.leaf.bytes", float64(inv["leaf"].Bytes), "bytes", "")
	run.Check("catalog.leaf_objects_expected", inv["leaf"].Objects == expLeaves, "%d objects, expected %d sealed", inv["leaf"].Objects, expLeaves)
	run.Check("catalog.leaf_size_exact", inv["leaf"].MaxBytes == leafBytes || inv["leaf"].Objects == 0, "max %d B, sealed leaf must be %d B", inv["leaf"].MaxBytes, leafBytes)
	totalIndex := 0
	for level := 1; level <= max(len(expIndex), 1); level++ {
		class := fmt.Sprintf("index-l%02d", level)
		c := inv[class]
		totalIndex += c.Objects
		exp := 0
		if level-1 < len(expIndex) {
			exp = expIndex[level-1]
		}
		run.Scalar("catalog."+class+".objects", float64(c.Objects), "", "")
		run.Check("catalog."+class+"_objects_expected", c.Objects == exp, "%d objects, expected %d sealed (write-once: no generations)", c.Objects, exp)
		if c.Objects > 0 {
			run.Check("catalog."+class+"_size_exact", c.MaxBytes == indexBytes, "max %d B, sealed index page must be %d B", c.MaxBytes, indexBytes)
		}
	}
	run.Check("catalog.head_within_bound", inv["head"].Bytes <= bench.HeadBound(p.LeafLimit, p.IndexLimit, len(expIndex)+1), "%d B, bound %d B", inv["head"].Bytes, bench.HeadBound(p.LeafLimit, p.IndexLimit, len(expIndex)+1))

	// ---- catalog lookups -----------------------------------------------------
	run.Section("catalog (every call is head + pages from the store)")
	rng := mrand.New(mrand.NewPCG(1, 2))
	randLSN := func() uint64 { return head.OldestLSN + rng.Uint64N(head.NextLSN-head.OldestLSN) }
	lookupErrors := 0
	if err := run.Measure("catalog.head_load", p.Samples, func() error {
		_, err := cat.LoadPartition(ctx, historyPartition)
		return err
	}, "GET head.plc"); err != nil {
		return err
	}
	if err := run.Measure("catalog.lookup_timestamp", p.Samples, func() error {
		lsn := randLSN()
		ts := bench.TSBase + int64(lsn)
		res, err := cat.LookupTimestamp(ctx, catalog.TimestampLookupRequest{Partition: historyPartition, TimestampMS: ts})
		if err != nil {
			return err
		}
		if !res.Found || res.Segment.MaxTimestampMS < ts || res.Segment.MinTimestampMS > ts {
			lookupErrors++
		}
		return nil
	}, "seek by time"); err != nil {
		return err
	}
	if err := run.Measure("catalog.find_segment", p.Samples, func() error {
		lsn := randLSN()
		seg, found, err := cat.FindSegment(ctx, historyPartition, lsn)
		if err != nil {
			return err
		}
		if !found || lsn < seg.BaseLSN || lsn > seg.LastLSN {
			lookupErrors++
		}
		return nil
	}, "point lookup by LSN"); err != nil {
		return err
	}
	run.Check("catalog.lookups_correct", lookupErrors == 0, "%d of %d lookups returned a segment not covering the key", lookupErrors, 2*p.Samples)
	if err := run.Measure("catalog.list_page", p.Samples, func() error {
		_, err := cat.ListSegments(ctx, catalog.ListSegmentsRequest{Partition: historyPartition, FromLSN: randLSN(), Limit: catalog.DefaultSegmentPageLimit})
		return err
	}, fmt.Sprintf("one page of %d refs", catalog.DefaultSegmentPageLimit)); err != nil {
		return err
	}
	{
		s := &bench.Sample{}
		from, refs := head.OldestLSN, 0
		t0 := time.Now()
		for {
			var page interface{ Len() int }
			_ = page
			var err error
			var next uint64
			var more bool
			s.Time(func() {
				pg, e := cat.ListSegments(ctx, catalog.ListSegmentsRequest{Partition: historyPartition, FromLSN: from, Limit: catalog.MaxSegmentPageLimit})
				err, next, more = e, pg.NextLSN, pg.HasMore
				refs += len(pg.Segments)
			})
			if err != nil {
				return err
			}
			if !more {
				break
			}
			from = next
		}
		el := time.Since(t0)
		run.Latency("catalog.list_full", s.Stats(), el, fmt.Sprintf("%d refs, %.0f refs/s", refs, float64(refs)/el.Seconds()))
		run.Check("catalog.list_full_count", uint64(refs) == head.ReachableSegmentCount, "listed %d refs, head says %d reachable", refs, head.ReachableSegmentCount)
	}

	// ---- reader ----------------------------------------------------------------
	run.Section("reader (cold: no range cache, no segment-reader cache)")
	cold, err := log.NewReader(partitionlog.ReaderOptions{MaxRecordsPerBatch: 1024})
	if err != nil {
		return err
	}
	defer cold.Close()
	part := cold.Partition(historyPartition)
	if err := run.Measure("reader.head_refresh_tail", p.Samples, func() error {
		_, err := part.Read(ctx, partitionlog.ReadRequest{StartLSN: head.NextLSN, Limit: 1, Freshness: partitionlog.FreshnessLatest})
		return err
	}, "tail poll: forced head reload, no data"); err != nil {
		return err
	}
	readErrors := 0
	if err := run.Measure("reader.read_random_lsn", p.Samples, func() error {
		res, err := part.Read(ctx, partitionlog.ReadRequest{StartLSN: randLSN(), Limit: 100, Freshness: partitionlog.FreshnessLatest})
		if err != nil {
			return err
		}
		if len(res.Records) == 0 {
			readErrors++
		}
		return nil
	}, "head + find segment + open segment + block read"); err != nil {
		return err
	}
	if err := run.Measure("reader.fetch_record", p.Samples, func() error {
		lsn := randLSN()
		res, err := cold.Fetch(ctx, reader.FetchRequest{Partition: historyPartition, LSN: lsn})
		if err != nil {
			return err
		}
		if !res.Found || res.Record.LSN != lsn {
			readErrors++
		}
		return nil
	}, "single record by LSN"); err != nil {
		return err
	}
	if err := run.Measure("reader.read_from_timestamp", p.Samples, func() error {
		res, err := cold.ConsumeFromTimestamp(ctx, reader.ConsumeFromTimestampRequest{Partition: historyPartition, TimestampMS: bench.TSBase + int64(randLSN()), Limit: 100})
		if err != nil {
			return err
		}
		if len(res.Records) == 0 {
			readErrors++
		}
		return nil
	}, "seek by time then read"); err != nil {
		return err
	}
	run.Check("reader.reads_correct", readErrors == 0, "%d reads returned wrong/empty results", readErrors)
	{
		warm, err := log.NewReader(partitionlog.ReaderOptions{MaxRecordsPerBatch: 1024, RangeCacheBytes: 256 << 20, OpenSegmentReaders: 4096})
		if err != nil {
			return err
		}
		defer warm.Close()
		replaySegs := min(p.ReplaySegments, int(head.ReachableSegmentCount))
		startLSN := max(head.NextLSN-uint64(replaySegs)*uint64(p.RecordsPerSegment), head.OldestLSN)
		cur, err := warm.Partition(historyPartition).Cursor(partitionlog.CursorOptions{StartLSN: startLSN, Limit: 1024})
		if err != nil {
			return err
		}
		s := &bench.Sample{}
		records, bytes := 0, 0
		t0 := time.Now()
		for cur.Position() < head.NextLSN {
			var res partitionlog.ReadResult
			var err error
			s.Time(func() { res, err = cur.Next(ctx) })
			if err != nil {
				return err
			}
			if len(res.Records) == 0 {
				break
			}
			records += len(res.Records)
			for _, r := range res.Records {
				bytes += len(r.Value)
			}
		}
		el := time.Since(t0)
		cur.Close()
		run.Latency("reader.replay_next", s.Stats(), el, fmt.Sprintf("%d segments, %d records", replaySegs, records))
		run.Scalar("reader.replay_records_per_s", float64(records)/el.Seconds(), "rec/s", fmt.Sprintf("%.1f MB/s", float64(bytes)/el.Seconds()/1e6))
		run.Check("reader.replay_complete", uint64(records) == head.NextLSN-startLSN, "replayed %d records, expected %d", records, head.NextLSN-startLSN)
	}

	// ---- failover ---------------------------------------------------------------
	run.Section("writer failover (new fenced writer over the existing history)")
	{
		open, first := &bench.Sample{}, &bench.Sample{}
		var previous *partitionlog.Writer
		fenced := true
		var fenceErr error
		for i := 0; i < p.WriterOpens; i++ {
			var w *partitionlog.Writer
			var err error
			open.Time(func() {
				w, err = log.OpenWriter(ctx, partitionlog.WriterOptions{Partition: historyPartition, WriterID: bench.NewID()})
			})
			if err != nil {
				return err
			}
			first.Time(func() {
				lsn := w.State().Snapshot.Head.NextLSN
				if _, err = w.Append(ctx, partitionlog.Record{TimestampMS: bench.TSBase + int64(lsn), Value: []byte("failover-probe")}); err == nil {
					_, err = w.Flush(ctx)
				}
			})
			if err != nil {
				return err
			}
			if previous != nil {
				// The displaced writer must be refused.
				lsn := previous.State().OptimisticNextLSN
				_, aerr := previous.Append(ctx, partitionlog.Record{TimestampMS: bench.TSBase + int64(lsn), Value: []byte("stale")})
				if aerr == nil {
					_, aerr = previous.Flush(ctx)
				}
				if aerr == nil {
					fenced = false
				} else if fenceErr == nil {
					fenceErr = aerr
				}
				_ = previous.Abort(context.Background())
			}
			previous = w
		}
		if previous != nil {
			if _, err := previous.Close(ctx); err != nil {
				return err
			}
		}
		run.Latency("writer.open", open.Stats(), 0, "acquire fence: load head, bump epoch, CAS")
		run.Latency("writer.first_publish", first.Stats(), 0, "append + flush one segment in the new epoch")
		if p.WriterOpens > 1 {
			run.Check("writer.old_writer_fenced", fenced, "displaced writer publish -> %v", fenceErr)
		}
		after, err := cat.LoadPartition(ctx, historyPartition)
		if err != nil {
			return err
		}
		run.Check("head.counts_after_publish", after.SegmentCount == head.SegmentCount+uint64(p.WriterOpens) && after.ReachableSegmentCount == head.ReachableSegmentCount+uint64(p.WriterOpens) && after.ReachableSegmentCount <= after.SegmentCount,
			"lifetime %d->%d reachable %d->%d for %d publishes", head.SegmentCount, after.SegmentCount, head.ReachableSegmentCount, after.ReachableSegmentCount, p.WriterOpens)
		head = after
	}

	// ---- retention / reclaim / scrub ------------------------------------------
	if p.Retention {
		run.Section("retention")
		mid := head.OldestLSN + (head.NextLSN-head.OldestLSN)/2
		w, err := log.OpenWriter(ctx, partitionlog.WriterOptions{Partition: historyPartition, WriterID: bench.NewID()})
		if err != nil {
			return err
		}
		req := &bench.Sample{}
		req.Time(func() {
			_, err = log.RequestRetention(ctx, partitionlog.RetentionRequest{Partition: historyPartition, PolicyVersion: uint64(time.Now().UnixMilli()), BeforeLSN: mid})
		})
		if err != nil {
			return err
		}
		run.Latency("retention.request", req.Stats(), 0, fmt.Sprintf("before_lsn=%d", mid))
		apply := &bench.Sample{}
		var res partitionlog.RetentionResult
		apply.Time(func() { res, err = w.ApplyRetention(ctx) })
		if err != nil {
			return err
		}
		if _, err := w.Close(ctx); err != nil {
			return err
		}
		run.Latency("retention.apply", apply.Stats(), 0, fmt.Sprintf("oldest %d -> %d, reachable %d -> %d", head.OldestLSN, res.Snapshot.Head.OldestLSN, head.ReachableSegmentCount, res.Snapshot.Head.ReachableSegmentCount))
		after, err := cat.LoadPartition(ctx, historyPartition)
		if err != nil {
			return err
		}
		seg, found, err := cat.FindSegment(ctx, historyPartition, after.OldestLSN)
		if err != nil {
			return err
		}
		var expired partitionlog.LSNExpiredError
		_, berr := part.Read(ctx, partitionlog.ReadRequest{StartLSN: after.OldestLSN - 1, Limit: 1, Freshness: partitionlog.FreshnessLatest})
		run.Check("retention.applied", res.Applied && after.OldestLSN <= mid && found && seg.BaseLSN == after.OldestLSN,
			"applied=%v oldest=%d requested=%d, oldest is a segment base=%v", res.Applied, after.OldestLSN, mid, found && seg.BaseLSN == after.OldestLSN)
		run.Check("retention.read_below_floor_expired", errors.As(berr, &expired), "read at %d -> %v", after.OldestLSN-1, berr)
		run.Check("retention.counts", after.SegmentCount == head.SegmentCount && after.ReachableSegmentCount < head.ReachableSegmentCount,
			"lifetime %d->%d (must not change), reachable %d->%d (must drop)", head.SegmentCount, after.SegmentCount, head.ReachableSegmentCount, after.ReachableSegmentCount)
		{
			refs := 0
			from := after.OldestLSN
			for {
				pg, err := cat.ListSegments(ctx, catalog.ListSegmentsRequest{Partition: historyPartition, FromLSN: from, Limit: catalog.MaxSegmentPageLimit})
				if err != nil {
					return err
				}
				refs += len(pg.Segments)
				if !pg.HasMore {
					break
				}
				from = pg.NextLSN
			}
			run.Check("retention.reachable_count_matches_tree", uint64(refs) == after.ReachableSegmentCount, "tree lists %d refs, head says %d", refs, after.ReachableSegmentCount)
		}
		if err := run.Measure("catalog.lookup_timestamp_after_retention", p.Samples, func() error {
			lsn := after.OldestLSN + rng.Uint64N(after.NextLSN-after.OldestLSN)
			_, err := cat.LookupTimestamp(ctx, catalog.TimestampLookupRequest{Partition: historyPartition, TimestampMS: bench.TSBase + int64(lsn)})
			return err
		}, "after retention"); err != nil {
			return err
		}
		head = after
	}

	if p.Reclaim {
		run.Section("physical reclaim")
		limiter, err := plifecycle.NewTokenBucketDeleteLimiter(50_000, 5_000)
		if err != nil {
			return err
		}
		rec, err := store.NewReclaimer(plifecycle.Options{
			DeleteDelay: time.Millisecond, MaxPassDuration: 20 * time.Second,
			MaxObjectsPerRun: 10_000, MaxDeletesPerRun: 4_000, MaxDeleteBytes: 64 << 30,
			DeleteBatchSize: 1000, DeleteConcurrency: 16, DeleteRateLimiter: limiter,
		})
		if err != nil {
			return err
		}
		pass := &bench.Sample{}
		deleted, scanned, passes, idle := 0, 0, 0, 0
		t0 := time.Now()
		for idle < 2 && passes < 1000 {
			time.Sleep(5 * time.Millisecond)
			var r plifecycle.Result
			pass.Time(func() { r, err = rec.RunPartition(ctx, historyPartition) })
			if err != nil {
				return err
			}
			passes++
			deleted += r.DeletedObjects
			scanned += r.ScannedObjects
			if r.DeletedObjects == 0 && !r.HasMore {
				idle++
			} else {
				idle = 0
			}
		}
		el := time.Since(t0)
		run.Latency("reclaim.pass", pass.Stats(), 0, fmt.Sprintf("%d passes, scanned %d, deleted %d", passes, scanned, deleted))
		run.Scalar("reclaim.deletes_per_s", float64(deleted)/el.Seconds(), "del/s", "")
		inv, objects, err := bench.CatalogInventory(ctx, backend, run.Prefix)
		if err != nil {
			return err
		}
		below := 0
		for _, o := range objects {
			parsed, err := catalogblob.ParsePagePath(run.Prefix+"/catalog", historyStream, historyPartition, o.Key)
			if err != nil {
				continue
			}
			if parsed.SeqHi < head.OldestLSN {
				below++
			}
		}
		run.Check("reclaim.no_pages_below_floor", below == 0, "%d catalog pages with seq_hi < %d remain", below, head.OldestLSN)
		run.Scalar("reclaim.leaf_objects_after", float64(inv["leaf"].Objects), "", "")

		if p.Scrub {
			run.Section("scrub (page reachability; only trim originals and crash orphans remain)")
			pass := &bench.Sample{}
			// A scrub sweep spans several passes and reports HasMore=false at the
			// end of each cycle. Quarantined pages are deleted only after the
			// delete delay, i.e. in a later cycle, so "dry" means one full cycle
			// that neither quarantined nor deleted anything and has nothing pending.
			cycles, passes, deleted, scanned := 0, 0, 0, 0
			cycleQ, cycleD, cyclePending := 0, 0, 0
			var perCycle []string
			dry := false
			t0 := time.Now()
			for !dry && cycles < 6 && passes < 1000 {
				time.Sleep(5 * time.Millisecond)
				var r plifecycle.Result
				pass.Time(func() { r, err = rec.ScrubPartition(ctx, historyPartition) })
				if err != nil {
					return err
				}
				passes++
				deleted += r.DeletedObjects
				scanned += r.ScannedObjects
				cycleQ += r.QuarantinedObjects
				cycleD += r.DeletedObjects
				cyclePending = r.PendingQuarantine
				if !r.HasMore {
					cycles++
					perCycle = append(perCycle, fmt.Sprintf("q%d/d%d/p%d", cycleQ, cycleD, cyclePending))
					dry = cycleQ == 0 && cycleD == 0 && cyclePending == 0
					cycleQ, cycleD = 0, 0
				}
			}
			el := time.Since(t0)
			run.Latency("scrub.pass", pass.Stats(), 0, fmt.Sprintf("%d passes in %d cycles, scanned %d, deleted %d", passes, cycles, scanned, deleted))
			run.Scalar("scrub.objects_per_s", float64(scanned)/el.Seconds(), "obj/s", "")
			run.Scalar("scrub.deleted_objects", float64(deleted), "", "trim originals + crash orphans")
			run.Scalar("scrub.cycles_to_dry", float64(cycles), "", "quarantine + delayed delete need two cycles; a third must be dry")
			run.Check("scrub.reaches_dry_cycle", dry && cycles <= 3, "cycles (quarantined/deleted/pending): %v", perCycle)
		}
	}
	return nil
}

func (catalogHistory) write(ctx context.Context, run *bench.Run, log *partitionlog.Log, metrics *publishMetrics, p bench.Params) error {
	w, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition: historyPartition, WriterID: bench.NewID(),
		Batch:        partitionlog.BatchPolicy{MaxRecords: uint32(p.RecordsPerSegment), MaxDelay: time.Hour},
		Backpressure: partitionlog.BackpressurePolicy{MaxPendingBatches: p.Inflight},
	})
	if err != nil {
		return err
	}
	defer w.Abort(context.Background())
	start := w.State().Snapshot.Head
	if start.AppliedRetentionVersion != 0 || start.ReachableSegmentCount != start.SegmentCount {
		_ = w.Abort(context.Background())
		return fmt.Errorf("catalog_history: prefix %s has post-append state (retention applied, %d of %d segments reachable); resumption is append-phase only, use a fresh prefix", run.Prefix, start.ReachableSegmentCount, start.SegmentCount)
	}
	have := int(start.SegmentCount)
	if have >= p.Segments {
		run.Section("write: history already has %d segments (target %d), skipping", have, p.Segments)
		_, err := w.Close(ctx)
		return err
	}
	run.Section("write: %d -> %d segments, %d inflight", have, p.Segments, p.Inflight)
	value := make([]byte, p.RecordBytes)
	_, _ = rand.Read(value)
	next := start.NextLSN
	appendS := &bench.Sample{}
	t0 := time.Now()
	progress := max(p.Segments/10, 1)
	for seg := have; seg < p.Segments; seg++ {
		for range p.RecordsPerSegment {
			var err error
			appendS.Time(func() {
				_, err = w.Append(ctx, partitionlog.Record{TimestampMS: bench.TSBase + int64(next), Value: value})
			})
			if err != nil {
				return err
			}
			next++
		}
		if done := seg + 1; done%progress == 0 || done == p.Segments {
			pub := bench.Summarize(metrics.publish.Drain())
			run.Logf("%-8d %8.0fs  publish p50=%s p99=%s", done, time.Since(t0).Seconds(), pub.P50.Round(100*time.Microsecond), pub.P99.Round(100*time.Microsecond))
		}
	}
	closeS := &bench.Sample{}
	closeS.Time(func() { _, err = w.Close(ctx) })
	if err != nil {
		return err
	}
	el := time.Since(t0)
	written := p.Segments - have
	run.Latency("write.append", appendS.Stats(), el, fmt.Sprintf("%d segments", written))
	run.Scalar("write.segments_per_s", float64(written)/el.Seconds(), "seg/s", fmt.Sprintf("%d inflight", p.Inflight))
	run.Latency("write.close", closeS.Stats(), 0, "drain + close")
	return nil
}
