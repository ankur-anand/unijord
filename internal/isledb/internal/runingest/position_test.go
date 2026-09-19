package runingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"os"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/internal/runcontract"
	"github.com/ankur-anand/isledb/internal/runfile"
)

func positionTestOptions() positionOptions {
	var ns [32]byte
	for i := range ns {
		ns[i] = byte(i)
	}
	return positionOptions{Namespace: ns, Shard: 7, NextSequence: 50}
}

func missingHeads(s *batchSlot) []ResolvedHead {
	v := catalogIdentity{Owner: s.catalog.arena.identity, Generation: s.catalog.generation}
	h := make([]ResolvedHead, len(s.catalog.metadata))
	for i := range h {
		h[i] = ResolvedHead{Catalog: v, ID: timelineID(i)}
	}
	return h
}

func testPosition(t testing.TB, b *sealedBatch, h []ResolvedHead, opts positionOptions) *positionedBatch {
	t.Helper()
	p, err := positionBatch(b, h, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func testPositionInput(t testing.TB, p *positionedBatch) *positionedInput {
	t.Helper()
	in, err := p.Input()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = in.Close() })
	return in
}

func ownedEntries(t testing.TB, it runfile.EntryIterator) []runfile.Entry {
	t.Helper()
	var out []runfile.Entry
	for it.Next() {
		e := it.Entry()
		e.Key, e.Value, e.Timeline = bytes.Clone(e.Key), bytes.Clone(e.Value), bytes.Clone(e.Timeline)
		out = append(out, e)
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPositionDenseOrderTimestampAndSeal(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	records := []BorrowedRecord{batchRecord(0, "b"), batchRecord(2, "a"), batchRecord(3, "b"), batchRecord(9, "a"), batchRecord(10, "b")}
	records[1].Gap = SourceGap{From: 1, To: 2, Kind: GapReadCommitted}
	records[3].Gap = SourceGap{From: 4, To: 9, Kind: GapCompacted}
	for i := range records {
		records[i].Flags = RecordTimestampPresent
		records[i].TimestampMS = int64(i) - 10
	}
	records[3].Flags, records[3].TimestampMS = 0, 0
	records[4].Flags |= RecordSeal
	for _, r := range records {
		appendBatch(t, s, r)
	}
	b := sealBatch(t, s)
	h := missingHeads(s)
	h[0].Present, h[0].Head = true, runcontract.Head{NextLSN: 12, TimestampPresent: true, Timestamp: 100}
	before := snapshotBatch(t, s)
	p := testPosition(t, b, h, positionTestOptions())
	if !slices.Equal(p.lsns, []uint64{12, 1, 13, 2, 14}) || p.seqHi != 54 || p.nextSequence != 55 {
		t.Fatal(p.lsns, p.seqHi, p.nextSequence)
	}
	if p.heads[0].head != (runcontract.Head{Sealed: true, TimestampPresent: true, Timestamp: -6, NextLSN: 15, LastOffset: 10}) ||
		p.heads[1].head != (runcontract.Head{NextLSN: 3, LastOffset: 9}) {
		t.Fatal(p.heads)
	}
	// The caller's resolved heads are no longer consulted after positioning.
	h[0] = ResolvedHead{}
	in := testPositionInput(t, p)
	events, heads := ownedEntries(t, &in.events), ownedEntries(t, &in.heads)
	if !slices.Equal(p.eventOrder, []uint32{1, 3, 0, 2, 4}) || !slices.Equal(p.headOrder, []uint32{1, 0}) {
		t.Fatal(p.eventOrder, p.headOrder)
	}
	if events[0].Seq != 51 || events[2].Seq != 50 || heads[0].Seq != 53 || heads[1].Seq != 54 {
		t.Fatal(events, heads)
	}
	last, err := runcontract.DecodeEvent(events[4].Value)
	if err != nil || last.Kind != runcontract.Seal {
		t.Fatal(last, err)
	}
	_ = in.Close()
	_ = p.Close()
	unchangedBatch(t, s, before)
}

func TestPositionTimestampRelations(t *testing.T) {
	for _, ts := range []int64{math.MinInt64, 0, 9, 10, 11, math.MaxInt64} {
		t.Run(strconv.FormatInt(ts, 10), func(t *testing.T) {
			s := testBatch(t, batchTestConfig())
			r := batchRecord(0, "a")
			r.Flags, r.TimestampMS = RecordTimestampPresent, ts
			appendBatch(t, s, r)
			b := sealBatch(t, s)
			h := missingHeads(s)
			h[0].Present, h[0].Head = true, runcontract.Head{NextLSN: 2, TimestampPresent: true, Timestamp: 10}
			p := testPosition(t, b, h, positionTestOptions())
			if p.heads[0].head.Timestamp != ts || !p.heads[0].head.TimestampPresent {
				t.Fatal(p.heads)
			}
		})
	}
}

func TestPositionValidationAndBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func([]ResolvedHead) []ResolvedHead
	}{
		{"missing", func(h []ResolvedHead) []ResolvedHead { return h[:1] }},
		{"extra", func(h []ResolvedHead) []ResolvedHead { return append(h, h[0]) }},
		{"duplicate", func(h []ResolvedHead) []ResolvedHead { h[1].ID = 0; return h }},
		{"out-of-range", func(h []ResolvedHead) []ResolvedHead { h[0].ID = math.MaxUint32; return h }},
		{"misaligned", func(h []ResolvedHead) []ResolvedHead { h[0], h[1] = h[1], h[0]; return h }},
		{"stale", func(h []ResolvedHead) []ResolvedHead { h[0].Catalog.Generation++; return h }},
		{"wrong-catalog", func(h []ResolvedHead) []ResolvedHead { h[0].Catalog.Owner++; return h }},
		{"missing-sealed", func(h []ResolvedHead) []ResolvedHead { h[0].Head.Sealed = true; return h }},
		{"missing-next", func(h []ResolvedHead) []ResolvedHead { h[0].Head.NextLSN = 1; return h }},
		{"present-zero", func(h []ResolvedHead) []ResolvedHead { h[0].Present = true; return h }},
		{"present-one", func(h []ResolvedHead) []ResolvedHead { h[0].Present = true; h[0].Head.NextLSN = 1; return h }},
		{"absent-timestamp", func(h []ResolvedHead) []ResolvedHead {
			h[0].Present = true
			h[0].Head = runcontract.Head{NextLSN: 2, Timestamp: 1}
			return h
		}},
		{"offset", func(h []ResolvedHead) []ResolvedHead {
			h[0].Present = true
			h[0].Head = runcontract.Head{NextLSN: 2, LastOffset: math.MaxInt64}
			return h
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testBatch(t, batchTestConfig())
			appendBatch(t, s, batchRecord(0, "a"))
			appendBatch(t, s, batchRecord(1, "b"))
			b := sealBatch(t, s)
			h := tc.change(missingHeads(s))
			before := snapshotBatch(t, s)
			orig := slices.Clone(h)
			p, err := positionBatchWithHooks(b, h, positionTestOptions(), positionHooks{phase: func(string) error { t.Fatal("allocated before complete validation"); return nil }})
			if p != nil || !errors.Is(err, ErrResolvedHead) {
				t.Fatal(p, err)
			}
			unchangedBatch(t, s, before)
			if !reflect.DeepEqual(h, orig) {
				t.Fatal("resolved heads mutated")
			}
		})
	}
	for _, tc := range []struct {
		name                    string
		count                   int
		lsn, seq                uint64
		sealed, sealFirst, fail bool
	}{
		{"maximum", 1, math.MaxUint64 - 1, runcontract.MaxSequence, false, true, false},
		{"last-two-sequences", 2, 2, runcontract.MaxSequence - 1, false, false, false},
		{"lsn-exhausted", 1, math.MaxUint64, 1, false, false, true},
		{"lsn-range-exhausted", 2, math.MaxUint64 - 1, 1, false, false, true},
		{"sequence-exhausted", 1, 2, runcontract.MaxSequence + 1, false, false, true},
		{"sequence-range-exhausted", 2, 2, runcontract.MaxSequence, false, false, true},
		{"sequence-zero", 1, 2, 0, false, false, true},
		{"sequence-overflow", 1, 2, math.MaxUint64, false, false, true},
		{"committed-seal", 1, 2, 1, true, false, true},
		{"batch-seal", 2, 2, 1, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testBatch(t, batchTestConfig())
			for i := 0; i < tc.count; i++ {
				r := batchRecord(int64(i), "a")
				if i == 0 && tc.sealFirst {
					r.Flags = RecordSeal
				}
				appendBatch(t, s, r)
			}
			b := sealBatch(t, s)
			h := missingHeads(s)
			h[0].Present = true
			h[0].Head = runcontract.Head{NextLSN: tc.lsn, Sealed: tc.sealed}
			before := snapshotBatch(t, s)
			opts := positionTestOptions()
			opts.NextSequence = tc.seq
			p, err := positionBatch(b, h, opts)
			if tc.fail {
				if err == nil || p != nil {
					if p != nil {
						_ = p.Close()
					}
					t.Fatal("expected failure", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if p.nextSequence != tc.seq+uint64(tc.count) {
					t.Fatal(p.nextSequence)
				}
				_ = p.Close()
			}
			unchangedBatch(t, s, before)
		})
	}
	s := testBatch(t, batchTestConfig())
	if p, err := positionBatch(nil, nil, positionTestOptions()); p != nil || err == nil {
		t.Fatal(p, err)
	}
	if p, err := positionBatch(&s.sealed, nil, positionTestOptions()); p != nil || err == nil {
		t.Fatal(p, err)
	}
	// Synthetic empty sealed handle exercises arithmetic rejection, without allocating.
	s.reason = SealControl
	if p, err := positionBatch(&s.sealed, nil, positionTestOptions()); p != nil || err == nil {
		t.Fatal(p, err)
	}
	for _, counts := range [][2]uint64{{0, 0}, {math.MaxUint64, 1}, {math.MaxUint32 + 1, 1}, {1, 2}} {
		if _, err := positionCharge(counts[0], counts[1]); err == nil {
			t.Fatal(counts)
		}
	}
}

func TestPositionBinaryOrderingLifetimeAndNoMovement(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	timelines := [][]byte{{255}, {0}, {1}, {0, 255}, {0, 0}, {1, 0}, bytes.Repeat([]byte{0}, 512), bytes.Repeat([]byte{255}, 512)}
	for i := 0; i < 32; i++ {
		r := batchRecord(int64(i), "")
		r.Timeline = timelines[(31-i)%len(timelines)]
		appendBatch(t, s, r)
	}
	b := sealBatch(t, s)
	h := missingHeads(s)
	before := snapshotBatch(t, s)
	valueAddresses := make([]*byte, len(s.records))
	timelineAddresses := make([]*byte, len(h))
	for i, r := range s.records {
		_ = s.payload.WithBytes(r.Value, func(v []byte) error { valueAddresses[i] = &v[0]; return nil })
	}
	for i := range h {
		timelineAddresses[i] = &s.catalog.bytes(timelineID(i))[0]
	}
	var firstEvents, firstHeads []runfile.Entry
	for repeat := 0; repeat < 2; repeat++ {
		p := testPosition(t, b, h, positionTestOptions())
		in := testPositionInput(t, p)
		if _, err := positionBatch(b, h, positionTestOptions()); !errors.Is(err, ErrPositionBusy) {
			t.Fatal(err)
		}
		var prev []byte
		var keyPtr *byte
		masks := make([]uint8, len(h))
		for in.events.Next() {
			e := in.events.Entry()
			idx := int(e.Seq - 50)
			if &e.Value[0] != valueAddresses[idx] || &e.Timeline[0] != timelineAddresses[e.TimelineID] || !bytes.Equal(e.Value, before.PayloadBytes[idx]) {
				t.Fatal("arena moved")
			}
			if keyPtr != nil && keyPtr != &e.Key[0] {
				t.Fatal("scratch not reused")
			}
			keyPtr = &e.Key[0]
			if prev != nil && bytes.Compare(prev, e.Key) >= 0 {
				t.Fatal("event order")
			}
			prev = append(prev[:0], e.Key...)
			masks[e.TimelineID] |= 1
		}
		if in.events.Next() || in.events.Err() != nil || in.events.Entry().Key != nil {
			t.Fatal("exhaustion")
		}
		prev = nil
		var valuePtr *byte
		for in.heads.Next() {
			e := in.heads.Entry()
			if len(e.Value) != 32 || &e.Timeline[0] != timelineAddresses[e.TimelineID] {
				t.Fatal("head storage")
			}
			if valuePtr != nil && valuePtr != &e.Value[0] {
				t.Fatal("head scratch")
			}
			valuePtr = &e.Value[0]
			if prev != nil && bytes.Compare(prev, e.Key) >= 0 {
				t.Fatal("head order")
			}
			prev = append(prev[:0], e.Key...)
			masks[e.TimelineID] |= 2
		}
		for i := 0; i < in.Len(); i++ {
			if &in.Timeline(runfile.TimelineID(i))[0] != timelineAddresses[i] {
				t.Fatal("catalog copy")
			}
			masks[i] |= 4
		}
		for _, mask := range masks {
			if mask != 7 {
				t.Fatal(masks)
			}
		}
		// Force an equal user key to check the comparator's descending sequence
		// branch, independently of the foreground uniqueness proof.
		a, c := p.eventOrder[0], p.eventOrder[1]
		old := p.lsns[c]
		p.lsns[c] = p.lsns[a]
		if p.compareEvents(a, c) != -p.compareEvents(c, a) || (a < c && p.compareEvents(a, c) != 1) {
			t.Fatal("tie sequence order")
		}
		p.lsns[c] = old
		_ = in.Close()
		_ = p.Close()
		unchangedBatch(t, s, before)
		// Fresh positioned ownership, not rewinding the public iterator contract.
		p = testPosition(t, b, h, positionTestOptions())
		in = testPositionInput(t, p)
		e, hd := ownedEntries(t, &in.events), ownedEntries(t, &in.heads)
		if repeat == 0 {
			firstEvents, firstHeads = e, hd
		} else if !reflect.DeepEqual(e, firstEvents) || !reflect.DeepEqual(hd, firstHeads) {
			t.Fatal("nondeterministic")
		}
		_ = in.events.Close()
		_ = in.events.Close()
		_ = in.heads.Close()
		_ = in.heads.Close()
		if in.events.Next() || !errors.Is(in.events.Err(), ErrPositionClosed) || in.heads.Next() || !errors.Is(in.heads.Err(), ErrPositionClosed) {
			t.Fatal("use after close")
		}
		if in.Len() != len(h) {
			t.Fatal("iterator Close invalidated catalog")
		}
		_ = p.Close()
		if p.disposed {
			t.Fatal("open input not pinned")
		}
		_ = in.Close()
		_ = in.Close()
		_ = p.Close()
		if in.Len() != 0 || in.Timeline(0) != nil {
			t.Fatal("closed input")
		}
	}
	unchangedBatch(t, s, before)
}

func positionBuildOptions(p *positionedBatch, dir string) runfile.BuildOptions {
	return runfile.BuildOptions{NamespaceHash: p.options.Namespace, Shard: p.options.Shard, SeqLo: p.options.NextSequence, SeqHi: p.seqHi,
		RunID: [16]byte{1}, PublicationHash: [32]byte{2}, CreatorRole: runfile.CreatorRoleWriterFlush, CreatorEpoch: 1,
		MaxTimelines: uint32(len(p.heads)), ScratchDir: dir}
}

func TestPositionPrepareRepeatableAndDisposal(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	for i := 0; i < 100; i++ {
		r := batchRecord(int64(i), "")
		r.Timeline = []byte{byte(i % 7), 0, 255}
		appendBatch(t, s, r)
	}
	b := sealBatch(t, s)
	heads := missingHeads(s)
	dir := t.TempDir()
	var want [32]byte
	for attempt := 0; attempt < 2; attempt++ {
		p := testPosition(t, b, heads, positionTestOptions())
		in := testPositionInput(t, p)
		opts := positionBuildOptions(p, dir)
		// The owner can close during preparation; input is the longer catalog lease.
		_ = p.Close()
		prepared, err := runfile.Prepare(context.Background(), opts, in.BuildInput())
		if err != nil {
			t.Fatal(err)
		}
		if !in.events.closed || !in.heads.closed || in.Len() != 7 {
			t.Fatal("Prepare lifetime")
		}
		_ = in.Close()
		for i := 0; i < 2; i++ {
			h := sha256.New()
			if err := prepared.WriteTo(context.Background(), h); err != nil {
				t.Fatal(err)
			}
			var got [32]byte
			copy(got[:], h.Sum(nil))
			if attempt == 0 && i == 0 {
				want = got
			} else if got != want {
				t.Fatal("different prepared bytes")
			}
		}
		ref := prepared.Ref()
		if ref.Events.EntryCount != 100 || ref.Heads.EntryCount != 7 {
			t.Fatal(ref)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 {
			t.Fatal(files, err)
		}
	}
	// Invalid build options still close both iterators; the catalog is pinned
	// until the caller ends the input lease, including failure paths.
	p := testPosition(t, b, heads, positionTestOptions())
	in := testPositionInput(t, p)
	if pr, err := runfile.Prepare(context.Background(), runfile.BuildOptions{}, in.BuildInput()); pr != nil || err == nil {
		t.Fatal(pr, err)
	}
	if !in.events.closed || !in.heads.closed || in.Len() != 7 {
		t.Fatal("failed Prepare cleanup")
	}
	t.Logf("deterministic run SHA256 %x", want)
}

func TestPositionFaultsAndCredits(t *testing.T) {
	for _, headroom := range []uint64{0, 128, 1 << 20} {
		for _, phase := range []string{"overlay", "events-projection", "heads-projection", "events-sort", "heads-sort", "iterators", "scan-sealed", "scan-lsn", "success"} {
			gate := &batchCredits{}
			c := batchTestConfig()
			c.Credits = gate
			c.HeadroomBytes = headroom
			s := testBatch(t, c)
			appendBatch(t, s, batchRecord(0, "b"))
			appendBatch(t, s, batchRecord(1, "a"))
			b := sealBatch(t, s)
			h := missingHeads(s)
			wantErr := errBatchInjected
			if phase == "scan-sealed" {
				h[1].Present = true
				h[1].Head = runcontract.Head{NextLSN: 2, Sealed: true}
				wantErr = ErrTimelineSealed
			}
			if phase == "scan-lsn" {
				h[1].Present = true
				h[1].Head.NextLSN = math.MaxUint64
				wantErr = runcontract.ErrLimit
			}
			base := gate.current
			before := snapshotBatch(t, s)
			original := slices.Clone(h)
			p, err := positionBatchWithHooks(b, h, positionTestOptions(), positionHooks{phase: func(got string) error {
				if got == phase {
					return errBatchInjected
				}
				return nil
			}})
			if phase == "success" {
				if err != nil {
					t.Fatal(err)
				}
				if gate.current != base+p.accounting.NewCredits || p.accounting.HeadroomUsed != min(headroom, p.accounting.ChargedBytes) || p.accounting.SlotHighWater > c.MaxSlotChargedBytes {
					t.Fatal(p.accounting, gate)
				}
				_ = p.Close()
				_ = p.Close()
			} else if p != nil || !errors.Is(err, wantErr) {
				t.Fatal(phase, p, err)
			}
			if gate.current != base || !reflect.DeepEqual(h, original) {
				t.Fatal("rollback", gate)
			}
			unchangedBatch(t, s, before)
			h = missingHeads(s)
			p = testPosition(t, b, h, positionTestOptions())
			_ = p.Close()
			s.Close()
			s.Close()
			if gate.current != 0 {
				t.Fatal(gate)
			}
		}
	}
	gate := &batchCredits{}
	c := batchTestConfig()
	c.Credits = gate
	s := testBatch(t, c)
	appendBatch(t, s, batchRecord(0, "a"))
	b := sealBatch(t, s)
	h := missingHeads(s)
	base := gate.current
	gate.fail = true
	if p, err := positionBatch(b, h, positionTestOptions()); p != nil || err == nil || gate.current != base {
		t.Fatal(p, err, gate)
	}
	gate.fail = false
	s.config.MaxSlotChargedBytes = base // fault-only hard boundary
	if p, err := positionBatch(b, h, positionTestOptions()); p != nil || !errors.Is(err, ErrBatchLimit) {
		t.Fatal(p, err)
	}
}

func TestPositionPreparedOutlivesSlot(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	appendBatch(t, s, batchRecord(0, "a"))
	p := testPosition(t, sealBatch(t, s), missingHeads(s), positionTestOptions())
	in := testPositionInput(t, p)
	prepared, err := runfile.Prepare(context.Background(), positionBuildOptions(p, t.TempDir()), in.BuildInput())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	_ = in.Close()
	_ = p.Close()
	s.Close()
	var first, second bytes.Buffer
	if err := prepared.WriteTo(context.Background(), &first); err != nil {
		t.Fatal(err)
	}
	if err := prepared.WriteTo(context.Background(), &second); err != nil {
		t.Fatal(err)
	}
	if first.Len() == 0 || !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("prepared run depends on disposed source")
	}
}

func TestPositionConcurrentReadersAndDisposal(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	for i := 0; i < 200; i++ {
		appendBatch(t, s, batchRecord(int64(i), "a"))
	}
	b := sealBatch(t, s)
	p := testPosition(t, b, missingHeads(s), positionTestOptions())
	in := testPositionInput(t, p)
	started, done := make(chan struct{}), make(chan struct{})
	go func() { close(started); s.Close(); close(done) }()
	<-started
	// Deterministic lock-state proof, no scheduling sleeps: the slot remains
	// pinned even when its disposal caller has not yet been scheduled.
	if s.mu.TryLock() {
		s.mu.Unlock()
		t.Fatal("slot not pinned")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if in.Len() != 1 || !bytes.Equal(in.Timeline(0), []byte("a")) {
					t.Error("catalog lost")
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for in.events.Next() {
			if len(in.events.Entry().Value) == 0 {
				t.Error("payload lost")
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for in.heads.Next() {
			if len(in.heads.Entry().Value) != 32 {
				t.Error("head lost")
			}
		}
	}()
	wg.Add(1)
	go func() { defer wg.Done(); _ = p.Close(); _ = p.Close() }()
	wg.Wait()
	if in.events.Err() != nil || in.heads.Err() != nil {
		t.Fatal(in.events.Err(), in.heads.Err())
	}
	_ = in.Close()
	<-done
	if _, err := b.Len(); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
}

func TestPositionLanguageNeutralVectors(t *testing.T) {
	raw, err := os.ReadFile("../runfile/testdata/compat/v1/logical-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Vectors []struct {
			Name string
			Hex  string `json:"encoded_hex"`
		}
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	lookup := func(name string) []byte {
		for _, v := range corpus.Vectors {
			if v.Name == name {
				b, e := hex.DecodeString(v.Hex)
				if e != nil {
					t.Fatal(e)
				}
				return b
			}
		}
		t.Fatal(name)
		return nil
	}
	for _, name := range []string{"event-null", "event-empty", "event-binary", "seal-max"} {
		t.Run(name, func(t *testing.T) {
			want := lookup(name)
			e, err := runcontract.DecodeEvent(want)
			if err != nil {
				t.Fatal(err)
			}
			c := batchTestConfig()
			c.ExpectedOffset = int64(e.Offset)
			c.ExpectedLeaderEpoch = e.LeaderEpoch
			s := testBatch(t, c)
			r := BorrowedRecord{Offset: int64(e.Offset), LeaderEpoch: e.LeaderEpoch, TimestampMS: e.Timestamp, Timeline: []byte{0, 255, 128}, Value: e.Payload, Headers: e.Headers, Annotations: e.Annotations}
			if e.TimestampPresent {
				r.Flags |= RecordTimestampPresent
			}
			if e.Kind == runcontract.Seal {
				r.Flags |= RecordSeal
			}
			appendBatch(t, s, r)
			b := sealBatch(t, s)
			h := missingHeads(s)
			opts := positionTestOptions()
			if name == "seal-max" {
				h[0].Present = true
				h[0].Head.NextLSN = math.MaxUint64 - 1
			}
			p := testPosition(t, b, h, opts)
			in := testPositionInput(t, p)
			if !in.events.Next() || !bytes.Equal(in.events.Entry().Value, want) {
				t.Fatal("event vector")
			}
			if name != "seal-max" && !bytes.Equal(in.events.Entry().Key, lookup("event-key-binary")) {
				t.Fatal("key vector")
			}
			if !in.heads.Next() {
				t.Fatal("head")
			}
			if name == "seal-max" && !bytes.Equal(in.heads.Entry().Value, lookup("head-max")) {
				t.Fatal("head max vector")
			}
			if (name == "event-null" || name == "event-empty") && !bytes.Equal(in.heads.Entry().Value, lookup("head-min")) {
				t.Fatal("head min vector")
			}
		})
	}
}

func TestPositionReferenceModel(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		positionModel(t, seed, 1+int(seed%197))
	}
}

func TestPositionAdmissionAndInputEdges(t *testing.T) {
	s := testBatch(t, batchTestConfig())
	r := batchRecord(0, "a")
	r.Value = bytes.Repeat([]byte{231}, 4096) // dedicated E03 block, not a normal slab
	appendBatch(t, s, r)
	b := sealBatch(t, s)
	h := missingHeads(s)
	noAllocation := positionHooks{phase: func(string) error { t.Fatal("allocation before preflight rejection"); return nil }}
	identity, err := b.CatalogIdentity()
	if err != nil || identity != h[0].Catalog {
		t.Fatal(identity, err)
	}
	opts := positionTestOptions()
	opts.NextSequence = math.MaxUint64
	if p, err := positionBatchWithHooks(b, h, opts, noAllocation); p != nil || err == nil {
		t.Fatal(p, err)
	}
	opts = positionTestOptions()
	opts.Namespace = [32]byte{}
	if p, err := positionBatchWithHooks(b, h, opts, noAllocation); p != nil || err == nil {
		t.Fatal(p, err)
	}
	// A busy slot must fail without queuing behind terminal disposal.
	s.mu.Lock()
	blocked, err := positionBatch(b, h, positionTestOptions())
	s.mu.Unlock()
	if blocked != nil || !errors.Is(err, ErrPositionBusy) {
		t.Fatal(blocked, err)
	}
	p := testPosition(t, b, h, positionTestOptions())
	in := testPositionInput(t, p)
	if _, err := p.Input(); !errors.Is(err, ErrPositionBusy) {
		t.Fatal(err)
	}
	if in.Timeline(math.MaxUint32) != nil {
		t.Fatal("out of range catalog ID")
	}
	if !in.events.Next() {
		t.Fatal(in.events.Err())
	}
	e := in.events.Entry()
	if &e.Value[0] != &s.payload.large[s.records[0].Value.index].data[0] {
		t.Fatal("large value copied")
	}
	_ = in.Close()
	_ = p.Close()
	if _, err := p.Input(); !errors.Is(err, ErrPositionClosed) {
		t.Fatal(err)
	}
	// Closing before opening Input also returns the lease and credits once.
	p = testPosition(t, b, h, positionTestOptions())
	_ = p.Close()
	_ = p.Close()
	s.Close()
	if _, err := b.CatalogIdentity(); !errors.Is(err, ErrBatchClosed) {
		t.Fatal(err)
	}
	if p, err := positionBatch(b, h, positionTestOptions()); p != nil || !errors.Is(err, ErrBatchClosed) {
		t.Fatal(p, err)
	}
}

func TestPositionResolvedHeadsContainOnlyScalars(t *testing.T) {
	// Head inputs can outlive disposal without retaining catalog/payload storage.
	var check func(reflect.Type)
	check = func(typ reflect.Type) {
		switch typ.Kind() {
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				check(typ.Field(i).Type)
			}
		case reflect.Bool, reflect.Uint32, reflect.Uint64, reflect.Int64:
		default:
			t.Fatalf("resolved head retains nonscalar field %v", typ)
		}
	}
	check(reflect.TypeFor[ResolvedHead]())
}

func TestPositionMaximumEncodedKeyAndHeadPrefix(t *testing.T) {
	raw, err := os.ReadFile("../runfile/testdata/compat/v1/logical-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Vectors []struct {
			Name string
			Hex  string `json:"encoded_hex"`
		}
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, v := range corpus.Vectors {
		if v.Name != "event-key-max" && v.Name != "head-key-prefix" {
			continue
		}
		want, err := hex.DecodeString(v.Hex)
		if err != nil {
			t.Fatal(err)
		}
		key, err := runcontract.DecodeKey(want)
		if err != nil {
			t.Fatal(err)
		}
		s := testBatch(t, batchTestConfig())
		r := batchRecord(0, "")
		r.Timeline = key.Timeline
		appendBatch(t, s, r)
		b := sealBatch(t, s)
		h := missingHeads(s)
		if key.Kind == runcontract.Events {
			h[0].Present = true
			h[0].Head.NextLSN = key.LSN
		}
		opts := positionTestOptions()
		opts.Namespace = key.Namespace
		opts.Shard = key.Shard
		p := testPosition(t, b, h, opts)
		in := testPositionInput(t, p)
		var it runfile.EntryIterator = &in.events
		if key.Kind == runcontract.Heads {
			it = &in.heads
		}
		if !it.Next() || !bytes.Equal(it.Entry().Key, want) {
			t.Fatal(v.Name, "vector mismatch")
		}
	}
}

func positionModel(t testing.TB, seed int64, n int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	s := testBatch(t, batchTestConfig())
	var timelines [][]byte
	var modelIDs []int
	var records []BorrowedRecord
	offset := int64(0)
	for i := 0; i < n; i++ {
		x := make([]byte, 1+rng.Intn(32))
		rng.Read(x)
		if len(timelines) > 0 && rng.Intn(3) != 0 {
			x = timelines[rng.Intn(len(timelines))]
		}
		id := -1
		for j, v := range timelines {
			if bytes.Equal(x, v) {
				id = j
				break
			}
		}
		if id < 0 {
			id = len(timelines)
			timelines = append(timelines, x)
		}
		r := batchRecord(offset, "")
		r.Timeline = x
		if rng.Intn(2) == 0 {
			r.Flags = RecordTimestampPresent
			r.TimestampMS = rng.Int63()
			if rng.Intn(2) == 0 {
				r.TimestampMS = -r.TimestampMS
			}
		}
		if gap := int64(rng.Intn(5)); gap != 0 {
			r.Offset += gap
			r.Gap = SourceGap{From: offset, To: r.Offset, Kind: GapCompacted}
		}
		offset = r.Offset + 1
		records = append(records, r)
		modelIDs = append(modelIDs, id)
		appendBatch(t, s, r)
	}
	b := sealBatch(t, s)
	h := missingHeads(s)
	next := make([]uint64, len(h))
	final := make([]runcontract.Head, len(h))
	last := make([]int, len(h))
	wantLSN := make([]uint64, n)
	for i := range h {
		next[i] = 1
		if rng.Intn(2) == 0 {
			h[i].Present = true
			h[i].Head.NextLSN = 2 + uint64(rng.Intn(10000))
			next[i] = h[i].Head.NextLSN
		}
	}
	for i, r := range records {
		id := modelIDs[i]
		wantLSN[i] = next[id]
		next[id]++
		last[id] = i
		final[id] = runcontract.Head{NextLSN: next[id], LastOffset: uint64(r.Offset), Timestamp: r.TimestampMS, TimestampPresent: r.Flags&RecordTimestampPresent != 0}
	}
	opts := positionTestOptions()
	opts.NextSequence = 1 + uint64(rng.Intn(10000))
	p := testPosition(t, b, h, opts)
	if !slices.Equal(p.lsns, wantLSN) {
		t.Fatal(seed, "lsn model")
	}
	for i, v := range p.heads {
		if v.head != final[i] || int(v.lastRecord) != last[i] {
			t.Fatal(seed, "head model")
		}
	}
	in := testPositionInput(t, p)
	events := ownedEntries(t, &in.events)
	heads := ownedEntries(t, &in.heads)
	for i, e := range events {
		key, err := runcontract.DecodeKey(e.Key)
		if err != nil {
			t.Fatal(err)
		}
		ridx := int(e.Seq - opts.NextSequence)
		if key.LSN != wantLSN[ridx] || !bytes.Equal(key.Timeline, timelines[modelIDs[ridx]]) || (i > 0 && bytes.Compare(events[i-1].Key, e.Key) >= 0) {
			t.Fatal(seed, "event model")
		}
	}
	for i, e := range heads {
		head, err := runcontract.DecodeHead(e.Value)
		if err != nil || head != final[e.TimelineID] || e.Seq != opts.NextSequence+uint64(last[e.TimelineID]) || (i > 0 && bytes.Compare(heads[i-1].Key, e.Key) >= 0) {
			t.Fatal(seed, "head order model", err)
		}
	}
	_ = in.Close()
	_ = p.Close()
}

func FuzzPositionModel(f *testing.F) {
	f.Add(int64(7), uint8(32))
	f.Add(int64(0), uint8(0))
	f.Add(int64(-1), uint8(255))
	f.Fuzz(func(t *testing.T, seed int64, count uint8) { positionModel(t, seed, int(count)+1) })
}
