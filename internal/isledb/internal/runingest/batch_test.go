package runingest

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

var batchTestTime = time.Unix(123, 0)
var errBatchInjected = errors.New("injected E05 failure")

func batchTestConfig() BatchConfig {
	return BatchConfig{TargetRunBytes: 4 << 20, MaxSlotChargedBytes: 64 << 20,
		MaxEventBytes: runcontract.MaxValueBytes, MaxRecords: 10000, MaxTimelines: 10000,
		MaxResidence: time.Second, SlabBytes: 1024, LargeThreshold: 1024,
		HeadroomBytes: 128, ExpectedLeaderEpoch: -1}
}

func testBatch(t testing.TB, c BatchConfig) *batchSlot {
	t.Helper()
	s, err := newBatchSlot(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func batchRecord(offset int64, timeline string) BorrowedRecord {
	return BorrowedRecord{Offset: offset, LeaderEpoch: -1, Timeline: []byte(timeline), Value: []byte("value")}
}

func appendBatch(t testing.TB, s *batchSlot, r BorrowedRecord) AppendResult {
	t.Helper()
	out, err := s.AppendBorrowed(r, batchTestTime)
	if err != nil || (out.Disposition != AppendMutable && out.Disposition != AppendSealed && out.Disposition != AppendSingleton) {
		t.Fatalf("append: %+v %v", out, err)
	}
	return out
}

func sealBatch(t testing.TB, s *batchSlot) *sealedBatch {
	t.Helper()
	b, err := s.Seal(SealControl)
	if err != nil || b == nil {
		t.Fatal(b, err)
	}
	return b
}

type batchSnapshot struct {
	Records                 []recordRef
	RecordCap               int
	Payload, TimelineArena  arenaStats
	Catalog                 timelineCatalogStats
	Accounting              BatchAccounting
	Interval                SourceInterval
	Started                 time.Time
	Reason                  SealReason
	PayloadBytes, Timelines [][]byte
}

func snapshotBatch(t testing.TB, s *batchSlot) batchSnapshot {
	t.Helper()
	x := batchSnapshot{Records: append([]recordRef(nil), s.records...), RecordCap: cap(s.records),
		Payload: s.payload.Stats(), TimelineArena: s.catalog.arena.Stats(), Catalog: s.catalog.Stats(),
		Accounting: s.Accounting(), Interval: s.Interval(), Started: s.started, Reason: s.reason}
	for _, r := range s.records {
		if err := s.payload.WithBytes(r.Value, func(b []byte) error {
			x.PayloadBytes = append(x.PayloadBytes, bytes.Clone(b))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range s.catalog.metadata {
		x.Timelines = append(x.Timelines, bytes.Clone(s.catalog.bytes(timelineID(i))))
	}
	return x
}

func unchangedBatch(t testing.TB, s *batchSlot, before batchSnapshot) {
	t.Helper()
	if after := snapshotBatch(t, s); !reflect.DeepEqual(before, after) {
		t.Fatalf("failed append changed state\nbefore: %+v\nafter: %+v", before, after)
	}
}

func checkBatchAccounting(t testing.TB, s *batchSlot) {
	t.Helper()
	checkArenaAccounting(t, s.payload)
	checkCatalogAccounting(t, s.catalog)
	a := s.Accounting()
	want := batchFixedBytes + s.config.HeadroomBytes + s.payload.stats.ChargedBytes +
		s.catalog.stats.ChargedBytes - timelineFixedBytes + uint64(cap(s.records))*recordRefBytes
	var canonical uint64
	for _, r := range s.records {
		canonical += r.Value.length
	}
	if a.ChargedBytes != want || a.HighWater < want || a.HighWater > s.config.MaxSlotChargedBytes ||
		a.CanonicalBytes != canonical || canonical != s.payload.stats.LogicalBytes ||
		a.Copies.TimelineBytes != s.catalog.stats.CopiedTimelineBytes ||
		a.Copies.TimelineCopies != uint64(len(s.catalog.metadata)) ||
		a.DescriptorCapacity != uint64(cap(s.records)) || a.TimelineCount != uint64(len(s.catalog.metadata)) ||
		a.Copies.ValueCopies != uint64(len(s.records)) || a.Copies.AnnotationCopies != uint64(len(s.records)) {
		t.Fatalf("accounting: %+v want charge=%d canonical=%d", a, want, canonical)
	}
}

func TestBatchBorrowedOwnershipAndCanonicalBytes(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	var observed CopyAccounting
	s.hooks.encode = func(dst []byte, e runcontract.Event) ([]byte, error) {
		// Independently instrument codec invocations and source field lengths.
		observed.ValueBytes += uint64(len(e.Payload))
		observed.ValueCopies++
		observed.AnnotationBytes += uint64(len(e.Annotations))
		observed.AnnotationCopies++
		checkNoAlias := func(src []byte) {
			for i := range src {
				if &src[i] == &dst[0] || &src[i] == &dst[len(dst)-1] {
					t.Fatal("borrowed/destination alias")
				}
			}
		}
		checkNoAlias(e.Payload)
		checkNoAlias(e.Annotations)
		for _, h := range e.Headers {
			observed.HeaderBytes += uint64(len(h.Key) + len(h.Value))
			observed.HeaderCopies += 2
			checkNoAlias(h.Key)
			checkNoAlias(h.Value)
		}
		if len(s.records) != int(observed.ValueCopies)-1 {
			t.Fatal("descriptor visible before encoding")
		}
		return runcontract.EncodeEvent(dst, e)
	}
	s.catalog.arena.hooks.copy = func(dst, src []byte) (int, error) {
		observed.TimelineCopies++
		observed.TimelineBytes += uint64(len(src))
		if &dst[0] == &src[0] {
			t.Fatal("timeline alias")
		}
		return copy(dst, src), nil
	}
	var want [][]byte
	for i, timestamp := range []int64{math.MinInt64, math.MaxInt64, -1, 0} {
		r := BorrowedRecord{Offset: int64(i), TimestampMS: timestamp, Flags: RecordTimestampPresent,
			LeaderEpoch: -1, Timeline: []byte{0, 255, 4}, Value: []byte{0, 2, 255},
			Headers:     []BorrowedHeader{{Key: []byte("dup"), Value: nil}, {Key: []byte("dup"), Value: []byte{}}, {Key: []byte{}, Value: []byte{7, 0, 255}}},
			Annotations: []byte{9, 0, 8}}
		if i == 2 {
			r.Value = nil
		} else if i == 3 {
			r.Value, r.Flags = []byte{}, r.Flags|RecordSeal
		}
		n, err := runcontract.EventSize(r.event())
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := runcontract.EncodeEvent(make([]byte, n), r.event())
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, encoded)
		appendBatch(t, s, r)
		for _, b := range [][]byte{r.Timeline, r.Value, r.Annotations} {
			clear(b)
		}
		for j := range r.Headers {
			clear(r.Headers[j].Key)
			clear(r.Headers[j].Value)
			r.Headers[j] = BorrowedHeader{}
		}
	}
	b := sealBatch(t, s)
	for i, expected := range want {
		r, err := b.Record(i)
		if err != nil || r.Timeline != 0 {
			t.Fatal(r, err)
		}
		out := make([]byte, len(expected))
		if n, err := b.CopyValue(i, out); err != nil || n != len(out) || !bytes.Equal(out, expected) {
			t.Fatal(n, err, out, expected)
		}
		e, err := runcontract.DecodeEvent(out)
		if err != nil || e.Timestamp != r.TimestampMS || e.Offset != uint64(r.Offset) ||
			e.Headers[0].Value != nil || e.Headers[1].Value == nil || len(e.Headers[1].Value) != 0 ||
			(e.Payload == nil) != (i == 2) {
			t.Fatal(e, err)
		}
		clear(out) // Inspection destination is not a mutable alias.
	}
	var timeline [512]byte
	if n, err := b.CopyTimeline(0, timeline[:]); err != nil || !bytes.Equal(timeline[:n], []byte{0, 255, 4}) {
		t.Fatal(n, err, timeline[:n])
	}
	if s.Accounting().Copies != observed || observed.TimelineCopies != 1 || observed.TimelineBytes != 3 {
		t.Fatal(s.Accounting().Copies, observed)
	}
	checkBatchAccounting(t, s)
}

func TestBatchTimelineCollisionsAndGrowth(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	s.catalog.hooks.hash = func([]byte) uint64 { return 7 }
	var first arenaRef
	for i := 0; i < 64; i++ {
		r := batchRecord(int64(i), string([]byte{0, byte(i / 2), 255}))
		appendBatch(t, s, r)
		if s.records[i].Timeline != timelineID(i/2) {
			t.Fatal("hash collision merged identities")
		}
		if i == 0 {
			first = s.records[0].Value
		} else if s.records[0].Value != first {
			t.Fatal("payload relocated")
		}
		checkBatchAccounting(t, s)
	}
	if a := s.Accounting(); a.Copies.TimelineBytes != 96 || a.TimelineCount != 32 {
		t.Fatal(a)
	}
}

func TestBatchInvalidFieldsAreAtomic(t *testing.T) {
	cases := []struct {
		name string
		edit func(*BorrowedRecord)
	}{
		{"empty-timeline", func(r *BorrowedRecord) { r.Timeline = nil }},
		{"long-timeline", func(r *BorrowedRecord) { r.Timeline = make([]byte, 513) }},
		{"flags", func(r *BorrowedRecord) { r.Flags = 128 }},
		{"absent-timestamp", func(r *BorrowedRecord) { r.TimestampMS = -1 }},
		{"epoch", func(r *BorrowedRecord) { r.LeaderEpoch = -2 }},
		{"negative-offset", func(r *BorrowedRecord) { r.Offset = -1 }},
		{"overflow-offset", func(r *BorrowedRecord) { r.Offset = math.MaxInt64 }},
		{"large-header-key", func(r *BorrowedRecord) { r.Headers = []BorrowedHeader{{Key: make([]byte, 65536)}} }},
		{"header-count", func(r *BorrowedRecord) { r.Headers = make([]BorrowedHeader, runcontract.MaxHeaders+1) }},
		{"large-value", func(r *BorrowedRecord) { r.Value = make([]byte, runcontract.MaxValueBytes) }},
		{"large-annotation", func(r *BorrowedRecord) { r.Annotations = make([]byte, runcontract.MaxValueBytes) }},
		{"unproved-gap", func(r *BorrowedRecord) { r.Offset++ }},
		{"false-gap-range", func(r *BorrowedRecord) { r.Offset++; r.Gap = SourceGap{0, 7, GapCompacted} }},
		{"invalid-gap-kind", func(r *BorrowedRecord) { r.Offset++; r.Gap = SourceGap{1, 2, 9} }},
		{"spurious-gap", func(r *BorrowedRecord) { r.Gap = SourceGap{1, 1, GapReadCommitted} }},
		{"unproved-epoch", func(r *BorrowedRecord) { r.LeaderEpoch = 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testBatch(t, batchTestConfig())
			appendBatch(t, s, batchRecord(0, "old"))
			before := snapshotBatch(t, s)
			r := batchRecord(1, "new")
			tc.edit(&r)
			out, err := s.AppendBorrowed(r, batchTestTime)
			if out.Disposition != AppendInvalid || err == nil {
				t.Fatal(out, err)
			}
			unchangedBatch(t, s, before)
		})
	}
}

func TestBatchMaximumFields(t *testing.T) {
	c := batchTestConfig()
	c.TargetRunBytes = 64 << 20
	s := testBatch(t, c)
	r := batchRecord(0, string(bytes.Repeat([]byte{255}, 512)))
	r.Flags, r.TimestampMS = RecordTimestampPresent, math.MaxInt64
	r.Headers = make([]BorrowedHeader, runcontract.MaxHeaders)
	r.Headers[0] = BorrowedHeader{Key: make([]byte, math.MaxUint16), Value: []byte{}}
	r.Value = make([]byte, runcontract.MaxValueBytes-runcontract.EventFixedBytes-6*runcontract.MaxHeaders-math.MaxUint16)
	appendBatch(t, s, r)
	if s.accounting.CanonicalBytes != runcontract.MaxValueBytes {
		t.Fatal(s.accounting)
	}
	checkBatchAccounting(t, s)
	b := sealBatch(t, s)
	got := make([]byte, runcontract.MaxValueBytes)
	if _, err := b.CopyValue(0, got); err != nil {
		t.Fatal(err)
	}
	e, err := runcontract.DecodeEvent(got)
	if err != nil || len(e.Headers) != runcontract.MaxHeaders || len(e.Headers[0].Key) != math.MaxUint16 || e.Timestamp != math.MaxInt64 {
		t.Fatal(err)
	}
}

func TestBatchSourceIntervalAndEpochs(t *testing.T) {
	c := batchTestConfig()
	c.ExpectedOffset, c.ExpectedLeaderEpoch = 100, 4
	s := testBatch(t, c)
	for _, offset := range []int64{105, 106, 110} {
		r := batchRecord(offset, "a")
		r.LeaderEpoch = 4
		if offset > s.interval.Next {
			r.Gap = SourceGap{s.interval.Next, offset, GapReadCommitted}
		}
		appendBatch(t, s, r)
	}
	want := SourceInterval{Expected: 100, FirstObserved: 105, Next: 111,
		ExpectedEpoch: 4, IntervalEpoch: 4, GapCount: 2, MissingOffsets: 8}
	if s.Interval() != want || len(s.records) != 3 {
		t.Fatal(s.Interval(), len(s.records))
	}
	before := snapshotBatch(t, s)
	for _, offset := range []int64{99, 100, 105, 109, 110, math.MaxInt64} {
		r := batchRecord(offset, "b")
		r.LeaderEpoch = 4
		if out, err := s.AppendBorrowed(r, batchTestTime); err == nil || out.Disposition != AppendInvalid {
			t.Fatal(offset, out, err)
		}
		unchangedBatch(t, s, before)
	}
	for _, epoch := range []int32{-1, 3, 5} {
		r := batchRecord(111, "b")
		r.LeaderEpoch = epoch
		if out, err := s.AppendBorrowed(r, batchTestTime); err == nil || out.Disposition != AppendInvalid {
			t.Fatal(epoch, out, err)
		}
		unchangedBatch(t, s, before)
	}
	r := batchRecord(111, "b")
	r.LeaderEpoch, r.EpochPrefixConfirmed = 5, true
	if out, err := s.AppendBorrowed(r, batchTestTime); err != nil || out.Disposition != AppendSealFirst || out.Reason != SealLeaderEpoch {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
	c.ExpectedOffset = 111
	next := testBatch(t, c)
	appendBatch(t, next, r)
	if next.Interval().IntervalEpoch != 5 {
		t.Fatal(next.Interval())
	}
	c.ExpectedOffset, c.ExpectedLeaderEpoch = math.MaxInt64-1, -1
	last := testBatch(t, c)
	appendBatch(t, last, batchRecord(math.MaxInt64-1, "last"))
	if last.Interval().Next != math.MaxInt64 {
		t.Fatal(last.Interval())
	}
	// Unknown may become known, with evidence, even for a gapped first record.
	c.ExpectedOffset = 0
	unknown := testBatch(t, c)
	r = batchRecord(3, "a")
	r.LeaderEpoch, r.EpochPrefixConfirmed, r.Gap = 0, true, SourceGap{0, 3, GapCompacted}
	appendBatch(t, unknown, r)
}

func TestBatchTargetBoundariesAndSingleton(t *testing.T) {
	r := batchRecord(0, "a")
	size, _ := runcontract.EventSize(r.event())
	first := estimateRecord(size, r.Timeline, true, true)
	second := estimateRecord(size, r.Timeline, false, false)
	for _, tc := range []struct {
		target uint64
		want   AppendDisposition
	}{
		{first + 1, AppendMutable}, {first, AppendSealed}, {first - 1, AppendSingleton},
	} {
		c := batchTestConfig()
		c.TargetRunBytes = tc.target
		s := testBatch(t, c)
		if out := appendBatch(t, s, r); out.Disposition != tc.want || (tc.want != AppendMutable && out.Reason != SealTargetBytes) {
			t.Fatal(tc, out)
		}
		checkBatchAccounting(t, s)
	}
	for _, delta := range []uint64{0, 1} {
		c := batchTestConfig()
		c.TargetRunBytes = first + second - delta
		s := testBatch(t, c)
		appendBatch(t, s, r)
		r2 := r
		r2.Offset = 1
		before := snapshotBatch(t, s)
		out, err := s.AppendBorrowed(r2, batchTestTime)
		if err != nil || out.Reason != SealTargetBytes {
			t.Fatal(out, err)
		}
		if delta == 0 {
			if out.Disposition != AppendSealed || len(s.records) != 2 {
				t.Fatal(out)
			}
		} else {
			if out.Disposition != AppendSealFirst {
				t.Fatal(out)
			}
			unchangedBatch(t, s, before)
			b, err := s.Seal(out.Reason)
			if err != nil || b == nil || s.interval.Next != 1 {
				t.Fatal(err)
			}
		}
	}
	c := batchTestConfig()
	c.MaxEventBytes, c.TargetRunBytes = 1024, 512
	s := testBatch(t, c)
	r.Value = make([]byte, 1024-runcontract.EventFixedBytes)
	if out := appendBatch(t, s, r); out.Disposition != AppendSingleton {
		t.Fatal(out)
	}
	s = testBatch(t, c)
	before := snapshotBatch(t, s)
	r.Value = append(r.Value, 0)
	if out, err := s.AppendBorrowed(r, batchTestTime); out.Disposition != AppendInvalid || !errors.Is(err, ErrBatchLimit) {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
}

func TestBatchCountAndAllSealTriggers(t *testing.T) {
	for _, reason := range []SealReason{SealRecordCount, SealTimelineCount} {
		c := batchTestConfig()
		if reason == SealRecordCount {
			c.MaxRecords = 2
		} else {
			c.MaxTimelines = 2
		}
		s := testBatch(t, c)
		if out := appendBatch(t, s, batchRecord(0, "a")); out.Disposition != AppendMutable {
			t.Fatal(out)
		}
		if out := appendBatch(t, s, batchRecord(1, "b")); out.Disposition != AppendSealed || out.Reason != reason {
			t.Fatal(out, reason)
		}
	}
	for reason := SealControl; reason <= SealL0Pressure; reason++ {
		s := testBatch(t, batchTestConfig())
		appendBatch(t, s, batchRecord(0, "a"))
		b, err := s.PollSeal(batchTestTime, reason.Signal())
		if err != nil || b == nil || s.reason != reason {
			t.Fatal(b, err, reason, s.reason)
		}
	}
	s := testBatch(t, batchTestConfig())
	appendBatch(t, s, batchRecord(0, "a"))
	if b, err := s.PollSeal(batchTestTime, externalSealSignals); err != nil || b == nil || s.reason != SealControl {
		t.Fatal(b, err, s.reason)
	}
	// Simultaneous target/record/timeline boundaries choose target first.
	c := batchTestConfig()
	c.TargetRunBytes, c.MaxRecords, c.MaxTimelines = 1, 1, 1
	s = testBatch(t, c)
	if out := appendBatch(t, s, batchRecord(0, "a")); out.Reason != SealTargetBytes {
		t.Fatal(out)
	}
}

func TestBatchResidenceAndEmptySeals(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	before := snapshotBatch(t, s)
	for reason := SealTargetBytes; reason <= SealLeaderEpoch; reason++ {
		if b, err := s.Seal(reason); err != nil || b != nil {
			t.Fatal(reason, b, err)
		}
		unchangedBatch(t, s, before)
	}
	if b, err := s.PollSeal(batchTestTime.Add(10*time.Hour), externalSealSignals); err != nil || b != nil {
		t.Fatal(b, err)
	}
	unchangedBatch(t, s, before)
	appendBatch(t, s, batchRecord(0, "a"))
	if b, err := s.PollSeal(batchTestTime.Add(time.Second-time.Nanosecond), 0); err != nil || b != nil {
		t.Fatal(b, err)
	}
	before = snapshotBatch(t, s)
	if out, err := s.AppendBorrowed(batchRecord(1, "b"), batchTestTime.Add(time.Second)); err != nil || out.Disposition != AppendSealFirst || out.Reason != SealResidence {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
	if _, err := s.PollSeal(batchTestTime.Add(-time.Nanosecond), 0); !errors.Is(err, ErrBatchClock) {
		t.Fatal(err)
	}
	if _, err := s.AppendBorrowed(batchRecord(1, "b"), batchTestTime.Add(-time.Nanosecond)); !errors.Is(err, ErrBatchClock) {
		t.Fatal(err)
	}
	unchangedBatch(t, s, before)
	if b, err := s.PollSeal(batchTestTime.Add(time.Second), externalSealSignals); err != nil || b == nil || s.reason != SealResidence {
		t.Fatal(b, err, s.reason)
	}
}

type batchCredits struct {
	current, highWater uint64
	reserves, releases int
	fail               bool
}

func (c *batchCredits) Reserve(n uint64) error {
	c.reserves++
	if c.fail {
		return errBatchInjected
	}
	c.current += n
	c.highWater = max(c.highWater, c.current)
	return nil
}

func (c *batchCredits) Release(n uint64) {
	if n > c.current {
		panic("credit released twice")
	}
	c.releases++
	c.current -= n
}

func TestBatchHardChargeAndCreditAdmission(t *testing.T) {
	c := batchTestConfig()
	probe := testBatch(t, c)
	r := batchRecord(0, "a")
	size, _ := runcontract.EventSize(r.event())
	p, err := probe.plan(r, size)
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []uint64{0, 1} {
		c.MaxSlotChargedBytes = p.peak - delta
		credits := &batchCredits{}
		c.Credits = credits
		s := testBatch(t, c)
		before := snapshotBatch(t, s)
		out, err := s.AppendBorrowed(r, batchTestTime)
		if delta == 0 {
			if err != nil || out.Disposition != AppendSealed || out.Reason != SealChargedBytes || credits.current != p.peak || credits.highWater > c.MaxSlotChargedBytes {
				t.Fatal(out, err, credits, p.peak)
			}
			checkBatchAccounting(t, s)
		} else {
			if !errors.Is(err, ErrBatchLimit) || out.Disposition != AppendInvalid || credits.reserves != 1 {
				t.Fatal(out, err, credits)
			}
			unchangedBatch(t, s, before)
		}
		s.Close()
		if credits.current != 0 {
			t.Fatal(credits)
		}
	}
	// Composite replacement overlap is admitted before any allocation, including
	// headroom and the old descriptor/table capacities that remain reachable.
	c = batchTestConfig()
	credits := &batchCredits{}
	c.Credits = credits
	s := testBatch(t, c)
	appendBatch(t, s, r)
	r = batchRecord(1, "new")
	size, _ = runcontract.EventSize(r.event())
	p, err = s.plan(r, size)
	if err != nil {
		t.Fatal(err)
	}
	s.config.MaxSlotChargedBytes = p.peak - 1
	before := snapshotBatch(t, s)
	reserves := credits.reserves
	s.hooks.records = func(int) ([]recordRef, error) { t.Fatal("allocated before admission"); return nil, nil }
	if out, err := s.AppendBorrowed(r, batchTestTime); err != nil || out.Disposition != AppendSealFirst || out.Reason != SealChargedBytes {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
	if credits.reserves != reserves {
		t.Fatal("reserved over hard bound")
	}
	s.config.MaxSlotChargedBytes = c.MaxSlotChargedBytes
	s.hooks.records = nil
	credits.fail = true
	if out, err := s.AppendBorrowed(r, batchTestTime); err == nil || out.Disposition != AppendRetry || out.RolledBack {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
	credits.fail = false
	appendBatch(t, s, r)
	if credits.current != s.accounting.ChargedBytes || credits.highWater != s.accounting.HighWater {
		t.Fatal(credits, s.accounting)
	}
	s.Close()
	releases := credits.releases
	s.Close()
	if credits.current != 0 || releases != credits.releases {
		t.Fatal(credits)
	}
}

func TestBatchAllFailureStagesRollback(t *testing.T) {
	stages := []struct {
		name string
		set  func(*batchSlot)
	}{
		{"record-allocation", func(s *batchSlot) { s.hooks.records = func(int) ([]recordRef, error) { return nil, errBatchInjected } }},
		{"bad-record-allocation", func(s *batchSlot) { s.hooks.records = func(int) ([]recordRef, error) { return nil, nil } }},
		{"payload-block-descriptors", func(s *batchSlot) {
			s.payload.hooks.blocks = func(int) ([]arenaBlock, error) { return nil, errBatchInjected }
		}},
		{"bad-payload-block-descriptors", func(s *batchSlot) { s.payload.hooks.blocks = func(int) ([]arenaBlock, error) { return nil, nil } }},
		{"payload-allocation", func(s *batchSlot) { s.payload.hooks.bytes = func(int) ([]byte, error) { return nil, errBatchInjected } }},
		{"bad-payload-allocation", func(s *batchSlot) { s.payload.hooks.bytes = func(int) ([]byte, error) { return nil, nil } }},
		{"partial-encode", func(s *batchSlot) {
			s.hooks.encode = func(dst []byte, _ runcontract.Event) ([]byte, error) { dst[0] = 255; return nil, errBatchInjected }
		}},
		{"short-encode", func(s *batchSlot) {
			s.hooks.encode = func(dst []byte, _ runcontract.Event) ([]byte, error) { return dst[:len(dst)-1], nil }
		}},
		{"foreign-encode", func(s *batchSlot) {
			s.hooks.encode = func(dst []byte, _ runcontract.Event) ([]byte, error) { return make([]byte, len(dst)), nil }
		}},
		{"catalog-slots", func(s *batchSlot) {
			s.catalog.hooks.slots = func(int) ([]timelineHashSlot, error) { return nil, errBatchInjected }
		}},
		{"catalog-metadata", func(s *batchSlot) {
			s.catalog.hooks.metadata = func(int) ([]timelineMetadata, error) { return nil, errBatchInjected }
		}},
		{"catalog-arena-blocks", func(s *batchSlot) {
			s.catalog.arena.hooks.blocks = func(int) ([]arenaBlock, error) { return nil, errBatchInjected }
		}},
		{"catalog-arena-bytes", func(s *batchSlot) {
			s.catalog.arena.hooks.bytes = func(int) ([]byte, error) { return nil, errBatchInjected }
		}},
		{"catalog-partial-copy", func(s *batchSlot) {
			s.catalog.arena.hooks.copy = func(dst, _ []byte) (int, error) { dst[0] = 255; return 1, errBatchInjected }
		}},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			c := batchTestConfig()
			credits := &batchCredits{}
			c.Credits = credits
			s := testBatch(t, c)
			before := snapshotBatch(t, s)
			stage.set(s)
			if out, err := s.AppendBorrowed(batchRecord(0, "new"), batchTestTime); err == nil || out.Disposition != AppendRetry || !out.RolledBack {
				t.Fatal(out, err)
			}
			unchangedBatch(t, s, before)
			if credits.reserves != 2 || credits.releases != 1 || credits.current != before.Accounting.ChargedBytes {
				t.Fatal(credits)
			}
			s.hooks, s.payload.hooks, s.catalog.hooks, s.catalog.arena.hooks = batchHooks{}, arenaHooks{}, timelineCatalogHooks{}, arenaHooks{}
			appendBatch(t, s, batchRecord(0, "new"))
			if s.records[0].Timeline != 0 || len(s.catalog.metadata) != 1 {
				t.Fatal("unused catalog ID after retry")
			}
			checkBatchAccounting(t, s)
		})
	}
	for _, repeated := range []bool{false, true} {
		s := testBatch(t, batchTestConfig())
		appendBatch(t, s, batchRecord(0, "old"))
		before := snapshotBatch(t, s)
		r := batchRecord(1, "new")
		if repeated {
			r.Timeline = []byte("old")
		}
		// A failed encoder may dirty an existing slab's unused tail. A later
		// successful append must overwrite all of it, preserving the prior span.
		s.hooks.encode = func(dst []byte, e runcontract.Event) ([]byte, error) {
			for i := range dst {
				dst[i] = 255
			}
			return nil, errBatchInjected
		}
		if _, err := s.AppendBorrowed(r, batchTestTime); err == nil {
			t.Fatal("fault ignored")
		}
		unchangedBatch(t, s, before)
		s.hooks.encode = nil
		appendBatch(t, s, r)
		checkArenaValue(t, s.payload, s.records[0].Value, before.PayloadBytes[0])
		checkBatchAccounting(t, s)
	}
}

func TestBatchGrowthRollbackAndPublicationBarrier(t *testing.T) {
	for _, stage := range []string{"record", "catalog-metadata", "catalog-slots", "catalog-copy", "payload-large"} {
		t.Run(stage, func(t *testing.T) {
			c := batchTestConfig()
			credits := &batchCredits{}
			c.Credits = credits
			s := testBatch(t, c)
			count := 1
			if stage == "catalog-slots" {
				count = 6
			}
			for i := range count {
				appendBatch(t, s, batchRecord(int64(i), string([]byte{byte(i + 1)})))
			}
			before := snapshotBatch(t, s)
			r := batchRecord(int64(count), "new")
			switch stage {
			case "record":
				s.hooks.records = func(int) ([]recordRef, error) { return nil, errBatchInjected }
			case "catalog-metadata":
				s.catalog.hooks.metadata = func(int) ([]timelineMetadata, error) { return nil, errBatchInjected }
			case "catalog-slots":
				s.catalog.hooks.slots = func(int) ([]timelineHashSlot, error) { return nil, errBatchInjected }
			case "catalog-copy":
				s.catalog.arena.hooks.copy = func(dst, src []byte) (int, error) {
					if len(s.records) != count || s.payload.stats != before.Payload || s.interval != before.Interval || s.accounting != before.Accounting {
						t.Fatal("published before final catalog copy")
					}
					copy(dst, src)
					return 0, errBatchInjected
				}
			case "payload-large":
				r.Value = make([]byte, 2048)
				s.payload.hooks.bytes = func(int) ([]byte, error) { return nil, errBatchInjected }
			}
			n, _ := runcontract.EventSize(r.event())
			plan, err := s.plan(r, n)
			if err != nil {
				t.Fatal(err)
			}
			oldReserves, oldReleases := credits.reserves, credits.releases
			if out, err := s.AppendBorrowed(r, batchTestTime); err == nil || !out.RolledBack {
				t.Fatal(out, err)
			}
			unchangedBatch(t, s, before)
			if credits.reserves != oldReserves+1 || credits.releases != oldReleases+1 || credits.current != before.Accounting.ChargedBytes || credits.highWater != max(before.Accounting.HighWater, plan.peak) {
				t.Fatal(credits, plan)
			}
			s.hooks, s.payload.hooks, s.catalog.hooks, s.catalog.arena.hooks = batchHooks{}, arenaHooks{}, timelineCatalogHooks{}, arenaHooks{}
			appendBatch(t, s, r)
			checkBatchAccounting(t, s)
			if credits.current != s.accounting.ChargedBytes {
				t.Fatal("credit imbalance")
			}
		})
	}
}

func TestBatchPermanentHardLimitAndSimultaneousBoundaries(t *testing.T) {
	c := batchTestConfig()
	c.MaxSlotChargedBytes = batchFixedBytes + c.HeadroomBytes + 8192
	s := testBatch(t, c)
	appendBatch(t, s, batchRecord(0, "a"))
	before := snapshotBatch(t, s)
	r := batchRecord(1, "b")
	r.Value = make([]byte, 16384)
	for _, target := range []uint64{c.TargetRunBytes, s.accounting.EstimatedRunBytes + 1} {
		s.config.TargetRunBytes = target
		if out, err := s.AppendBorrowed(r, batchTestTime); out.Disposition != AppendInvalid || !errors.Is(err, ErrBatchLimit) {
			t.Fatal(out, err)
		}
		unchangedBatch(t, s, before)
	}
	// Both the target and the charged overlap block the second record, which
	// does fit a fresh slot. The deterministic reason is the target.
	c = batchTestConfig()
	s = testBatch(t, c)
	appendBatch(t, s, batchRecord(0, "a"))
	r = batchRecord(1, "b")
	n, _ := runcontract.EventSize(r.event())
	p, err := s.plan(r, n)
	if err != nil {
		t.Fatal(err)
	}
	s.config.TargetRunBytes, s.config.MaxSlotChargedBytes = p.estimate-1, p.peak-1
	before = snapshotBatch(t, s)
	if out, err := s.AppendBorrowed(r, batchTestTime); err != nil || out.Disposition != AppendSealFirst || out.Reason != SealTargetBytes {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
	// The largest proved gap is a scalar interval; it does not allocate records
	// for missing offsets or overflow the missing-offset counter.
	s = testBatch(t, c)
	r = batchRecord(math.MaxInt64-1, "last")
	r.Gap = SourceGap{0, r.Offset, GapCompacted}
	appendBatch(t, s, r)
	if s.interval.Next != math.MaxInt64 || s.interval.MissingOffsets != math.MaxInt64-1 || len(s.records) != 1 {
		t.Fatal(s.interval)
	}
}

func TestBatchSealIdempotenceAndReadLifecycle(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	appendBatch(t, s, batchRecord(0, "a"))
	b := sealBatch(t, s)
	before := snapshotBatch(t, s)
	for reason := SealNone; reason <= SealLeaderEpoch; reason++ {
		if again, err := s.Seal(reason); err != nil || b != again {
			t.Fatal(again, err)
		}
	}
	if out, err := s.AppendBorrowed(batchRecord(1, "b"), batchTestTime); !errors.Is(err, ErrBatchSealed) || out.Disposition != AppendUnavailable {
		t.Fatal(out, err)
	}
	unchangedBatch(t, s, before)
	if _, err := b.Record(-1); !errors.Is(err, ErrBatchIndex) {
		t.Fatal(err)
	}
	if _, err := b.CopyValue(1, nil); !errors.Is(err, ErrBatchIndex) {
		t.Fatal(err)
	}
	if _, err := b.CopyValue(0, nil); !errors.Is(err, ErrBatchLimit) {
		t.Fatal(err)
	}
	if _, err := b.CopyTimeline(99, nil); err == nil {
		t.Fatal("invalid ID")
	}
	if _, err := b.CopyTimeline(0, nil); err == nil {
		t.Fatal("short destination")
	}
	r, _ := b.Record(0)
	r.Offset = 99
	if r2, _ := b.Record(0); r2.Offset != 0 {
		t.Fatal("descriptor alias")
	}
	s.Close()
	if _, err := b.Len(); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	if _, _, _, err := b.Summary(); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	if _, err := b.CopyValue(0, make([]byte, 64)); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	if _, err := s.Seal(SealControl); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	if _, err := s.PollSeal(batchTestTime, 0); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	if out, err := s.AppendBorrowed(batchRecord(1, "b"), batchTestTime); !errors.Is(err, ErrBatchClosed) || out.Disposition != AppendUnavailable {
		t.Fatal(out, err)
	}
}

func TestBatchSingleOwnerSealReadRace(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	ready := make(chan *sealedBatch)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			appendBatch(t, s, batchRecord(int64(i), "hot"))
		}
		ready <- sealBatch(t, s)
	}()
	b := <-ready // deterministic ownership transfer, no borrowed slices
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var dst [128]byte
			for i := range 100 {
				if _, err := b.CopyValue(i, dst[:]); err != nil {
					t.Error(err)
				}
				if _, err := b.CopyTimeline(0, dst[:]); err != nil {
					t.Error(err)
				}
				if _, _, _, err := b.Summary(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	// A read lease deterministically excludes terminal disposal.
	entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_ = b.inspect(func(*batchSlot) error { close(entered); <-release; return nil })
	}()
	<-entered
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close passed a read lease")
	default:
	}
	close(release)
	<-closed
	if _, err := b.Record(0); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
}

func TestBatchConfigurationAndEmptyState(t *testing.T) {
	for _, edit := range []func(*BatchConfig){
		func(c *BatchConfig) { c.TargetRunBytes = 0 },
		func(c *BatchConfig) { c.MaxSlotChargedBytes = batchFixedBytes },
		func(c *BatchConfig) { c.HeadroomBytes = math.MaxUint64 },
		func(c *BatchConfig) { c.MaxEventBytes = 37 },
		func(c *BatchConfig) { c.MaxEventBytes = runcontract.MaxValueBytes + 1 },
		func(c *BatchConfig) { c.MaxRecords = 0 },
		func(c *BatchConfig) { c.MaxTimelines = 0 },
		func(c *BatchConfig) { c.MaxResidence = 0 },
		func(c *BatchConfig) { c.ExpectedOffset = -1 },
		func(c *BatchConfig) { c.ExpectedLeaderEpoch = -2 },
		func(c *BatchConfig) { c.SlabBytes = 0 },
		func(c *BatchConfig) { c.SlabBytes = runcontract.MaxValueBytes + 1 },
		func(c *BatchConfig) { c.LargeThreshold = 0 },
		func(c *BatchConfig) { c.LargeThreshold = c.SlabBytes + 1 },
	} {
		c := batchTestConfig()
		credits := &batchCredits{}
		c.Credits = credits
		edit(&c)
		if s, err := newBatchSlot(c); !errors.Is(err, ErrBatchConfig) || s != nil || credits.reserves != 0 {
			t.Fatal(s, err, credits)
		}
	}
	c := batchTestConfig()
	c.Credits = &batchCredits{fail: true}
	if s, err := newBatchSlot(c); err == nil || s != nil {
		t.Fatal(s, err)
	}
	s := testBatch(t, batchTestConfig())
	if s.payload.normal != nil || s.catalog.slots != nil || s.records != nil {
		t.Fatal("eager backing allocation")
	}
	before := snapshotBatch(t, s)
	if _, err := s.Seal(SealNone); !errors.Is(err, ErrBatchConfig) {
		t.Fatal(err)
	}
	if _, err := s.PollSeal(batchTestTime, SealTargetBytes.Signal()); !errors.Is(err, ErrBatchConfig) {
		t.Fatal(err)
	}
	unchangedBatch(t, s, before)
	var zero batchSlot
	if _, err := zero.AppendBorrowed(batchRecord(0, "a"), batchTestTime); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	zero.Close()
	var b *sealedBatch
	if _, err := b.Len(); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
}
