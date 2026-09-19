package manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"unsafe"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

// Scale fixtures retain every exact encoded receipt and index page in the fake
// backend. Construction and sorting are excluded from measured work; timed
// reads only copy stored bytes. Fixture storage is not actor memory or provider
// latency. No receipt generation/encoding is hidden inside a measured read.
type e08SyntheticStorage struct {
	*e08Memory
	indexBytes uint64
}

func syntheticReceipt(i uint64) (*KafkaRunReceipt, error) {
	id := e08Identity()
	r := testRun(i, 0)
	r.ObjectKey = fmt.Sprintf("runs/synthetic/%016x", i)
	r.Source = &id
	p := runcontract.Publication{Namespace: id.Namespace, BindingID: id.BindingID, Cluster: id.Cluster, TopicID: id.TopicID, TopicName: id.TopicName, Partition: id.Partition, Shard: id.Shard, MappingGeneration: 1, WriterEpoch: 1, OwnerID: [16]byte{3}, ExpectedRevision: i + 1, ExpectedOffset: 99 + i, NextOffset: 100 + i, ExpectedLeaderEpoch: -1, LeaderEpoch: -1, NextSequence: i, ResultingSequence: i + 1, RecordCount: 1, RunID: r.ID, MutationDigest: [32]byte{9}}
	binary.BigEndian.PutUint64(p.AttemptID[8:], i)
	_, r.PublicationHash, _ = runcontract.PublicationHashes(p)
	q, err := NewPublishKafkaRunRequest(p, r)
	if err != nil {
		return nil, err
	}
	return &KafkaRunReceipt{i, q.PlanPreimage, q.PublicationID, q.PublicationHash, q.AttemptID, q.Run, committedResult(&q, i+2)}, nil
}
func e08SyntheticFixture(t testing.TB, n int) (*RunAuthority, *e08SyntheticStorage, PublishKafkaRunRequest) {
	t.Helper()
	s := &e08SyntheticStorage{e08Memory: newE08Memory(t)}
	items := make([]receiptIndexItem, n*receiptIdentityCount)
	var latest ReceiptPageRef
	var first PublishKafkaRunRequest
	for i := 1; i <= n; i++ {
		r, err := syntheticReceipt(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		b, err := EncodeKafkaRunReceipt(r)
		if err != nil {
			t.Fatal(err)
		}
		q, err := r.request()
		if err != nil {
			t.Fatal(err)
		}
		keys := requestReceiptKeys(&q)
		ref := ReceiptPageRef{Hash: sha256.Sum256(b), EncodedBytes: uint32(len(b)), Count: 1, Min: keys[0], Max: keys[0]}
		s.pages[ref.Hash] = b
		for j, k := range keys {
			items[(i-1)*receiptIdentityCount+j] = receiptIndexItem{k, ref}
		}
		if i == 1 {
			first = q
		}
		latest = ref
	}
	sort.Slice(items, func(i, j int) bool { return bytes.Compare(items[i].Key[:], items[j].Key[:]) < 0 })
	refs := make([]ReceiptPageRef, 0, (len(items)+MaxReceiptFanout-1)/MaxReceiptFanout)
	for start := 0; start < len(items); start += MaxReceiptFanout {
		end := start + MaxReceiptFanout
		if end > len(items) {
			end = len(items)
		}
		ref, err := writeReceiptIndex(context.Background(), s, &receiptIndexPage{Level: 1, Items: items[start:end]})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	for len(refs) > 1 {
		groups := (len(refs) + MaxReceiptFanout - 1) / MaxReceiptFanout
		next := make([]ReceiptPageRef, 0, groups)
		for group := 0; group < groups; group++ {
			lo := len(refs) * group / groups
			hi := len(refs) * (group + 1) / groups
			ref, err := writeReceiptIndex(context.Background(), s, &receiptIndexPage{Level: refs[lo].Level + 1, Children: refs[lo:hi]})
			if err != nil {
				t.Fatal(err)
			}
			next = append(next, ref)
		}
		refs = next
	}
	last, err := syntheticReceipt(uint64(n))
	if err != nil {
		t.Fatal(err)
	}
	m := testRunManifest(last.Run)
	m.Revision = uint64(n) + 2
	m.NextSequence = uint64(n) + 1
	m.WriterFence = &RunWriterFence{1, [16]byte{3}}
	m.Source = &KafkaSourceState{e08Identity(), [16]byte{3}, 1, uint64(n) + 100, -1}
	m.Receipts = ReceiptFrontier{Count: uint64(n), Root: refs[0], Latest: latest}
	s.state, err = EncodeRunCheckpoint(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range s.pages {
		if b[6] == runReceiptIndexKind {
			s.indexBytes += uint64(len(b))
		}
	}
	return NewRunAuthority(s), s, first
}

func TestE08MillionReceiptLookup(t *testing.T) {
	if os.Getenv("E08_SCALE") != "1" {
		t.Skip("run with E08_SCALE=1; setup builds the complete 5M-entry index")
	}
	a, s, q := e08SyntheticFixture(t, 1000000)
	snap := e08Snapshot(t, a)
	result, stats, err := snap.LookupPublication(context.Background(), q)
	if err != nil || result == nil || result.Revision != 3 || stats.PageReads > 5*uint64(snap.ReceiptFrontier().Root.Level+1) {
		t.Fatal(result, stats, err)
	}
	t.Logf("receipts=1000000 index_entries=5000000 depth=%d reads=%d bytes=%d frontier_wire_bytes=%d actor_frontier_bytes=%d backend_index_bytes=%d checkpoint_bytes=%d", stats.Depth, stats.PageReads, stats.PageBytes, frontierWireSize(snap.ReceiptFrontier()), unsafe.Sizeof(ReceiptFrontier{}), s.indexBytes, len(s.state))
}

func reportE08IO(b *testing.B, s *e08Memory, reads, readBytes, attempts, conflicts uint64) {
	b.ReportMetric(float64(s.reads-reads)/float64(b.N), "page_reads/op")
	b.ReportMetric(float64(s.readBytes-readBytes)/float64(b.N), "page_B/op")
	b.ReportMetric(float64(s.casAttempts-attempts)/float64(b.N), "CAS/op")
	b.ReportMetric(float64(s.conflicts-conflicts)/float64(b.N), "conflicts/op")
}
func BenchmarkE08Activation(b *testing.B) {
	s := newE08Memory(b)
	a := NewRunAuthority(s)
	original := bytes.Clone(s.state)
	q := ActivateKafkaSourceRequest{Identity: e08Identity(), ExpectedManifestRevision: 1, OwnerID: [16]byte{3}, InitialOffset: 100, InitialLeaderEpoch: -1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s.state = original
		b.StartTimer()
		if _, err := a.ActivateKafkaSource(context.Background(), q); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportE08IO(b, s, 0, 0, 0, 0)
}
func BenchmarkE08NewPublication(b *testing.B) {
	a, s, snap := e08Active(b)
	original := bytes.Clone(s.state)
	q := e08Request(b, snap, 1)
	attempts := s.casAttempts
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s.state = original
		b.StartTimer()
		if _, err := a.PublishKafkaRun(context.Background(), q); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportE08IO(b, s, 0, 0, attempts, 0)
}
func BenchmarkE08ImmediateReplay(b *testing.B) {
	a, s, snap := e08Active(b)
	q := e08Request(b, snap, 1)
	e08Publish(b, a, q)
	reads, readBytes, attempts := s.reads, s.readBytes, s.casAttempts
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.PublishKafkaRun(context.Background(), q); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportE08IO(b, s, reads, readBytes, attempts, 0)
}
func BenchmarkE08History(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			a, s, q := e08SyntheticFixture(b, n)
			snap := e08Snapshot(b, a)
			for _, phase := range []string{"old-exact-replay", "conflicting-lookup", "checkpoint-encode", "checkpoint-decode", "snapshot", "snapshot-clone"} {
				b.Run(phase, func(b *testing.B) {
					bad := q
					bad.Run = q.Run.Clone()
					bad.Run.PayloadHash[0]++
					reads, readBytes, attempts := s.reads, s.readBytes, s.casAttempts
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch phase {
						case "old-exact-replay":
							if _, err := a.PublishKafkaRun(context.Background(), q); err != nil {
								b.Fatal(err)
							}
						case "conflicting-lookup":
							if _, _, err := snap.LookupPublication(context.Background(), bad); err != ErrRunIdentityConflict {
								b.Fatal(err)
							}
						case "checkpoint-encode":
							if _, err := EncodeRunCheckpoint(snap.state); err != nil {
								b.Fatal(err)
							}
						case "checkpoint-decode":
							if _, err := DecodeRunCheckpoint(s.state); err != nil {
								b.Fatal(err)
							}
						case "snapshot":
							if _, err := a.Snapshot(context.Background()); err != nil {
								b.Fatal(err)
							}
						case "snapshot-clone":
							if _, err := snap.Manifest(); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					reportE08IO(b, s.e08Memory, reads, readBytes, attempts, 0)
					b.ReportMetric(float64(snap.ReceiptFrontier().Root.Level), "tree_depth")
					b.ReportMetric(float64(len(s.state)), "checkpoint_B")
					b.ReportMetric(float64(frontierWireSize(snap.ReceiptFrontier())), "frontier_B")
					b.ReportMetric(float64(unsafe.Sizeof(ReceiptFrontier{})), "actor_receipt_index_B")
					b.ReportMetric(float64(s.indexBytes), "fixture_backend_index_B")
				})
			}
		})
	}
}
func BenchmarkE08ReceiptCodec(b *testing.B) {
	a, s, snap := e08Active(b)
	q := e08Request(b, snap, 1)
	e08Publish(b, a, q)
	snap = e08Snapshot(b, a)
	leafBytes := s.pages[snap.ReceiptFrontier().Latest.Hash]
	leaf, _ := DecodeKafkaRunReceipt(leafBytes)
	indexBytes := s.pages[snap.ReceiptFrontier().Root.Hash]
	index, _ := decodeReceiptIndex(indexBytes)
	for _, phase := range []string{"leaf-encode", "leaf-decode", "index-encode", "index-decode"} {
		b.Run(phase, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				switch phase {
				case "leaf-encode":
					_, err = EncodeKafkaRunReceipt(leaf)
				case "leaf-decode":
					_, err = DecodeKafkaRunReceipt(leafBytes)
				case "index-encode":
					_, err = encodeReceiptIndex(index)
				case "index-decode":
					_, err = decodeReceiptIndex(indexBytes)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			if phase == "leaf-encode" || phase == "leaf-decode" {
				b.ReportMetric(float64(len(leafBytes)), "encoded_B")
			} else {
				b.ReportMetric(float64(len(indexBytes)), "encoded_B")
			}
		})
	}
}
func BenchmarkE08CASContention(b *testing.B) {
	a, s, snap := e08Active(b)
	original := bytes.Clone(s.state)
	q := e08Request(b, snap, 1)
	attempts := s.casAttempts
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s.state = original
		arrived := make(chan struct{}, 8)
		release := make(chan struct{})
		s.hook = func() { arrived <- struct{}{}; <-release }
		b.StartTimer()
		var wg sync.WaitGroup
		wg.Add(8)
		for j := 0; j < 8; j++ {
			go func() {
				defer wg.Done()
				if _, err := a.PublishKafkaRun(context.Background(), q); err != nil {
					b.Error(err)
				}
			}()
		}
		for j := 0; j < 8; j++ {
			<-arrived
		}
		close(release)
		wg.Wait()
	}
	b.StopTimer()
	reportE08IO(b, s, 0, 0, attempts, 0)
	b.ReportMetric(8, "publishers/round")
}

func BenchmarkE08LiveState(b *testing.B) {
	for _, n := range []int{1, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			a, s, _ := e08SyntheticFixture(b, n)
			snap := e08Snapshot(b, a)
			m, _ := snap.Manifest()
			m.L0Runs = make([]RunMeta, n)
			for i := range m.L0Runs {
				r, err := syntheticReceipt(uint64(i + 1))
				if err != nil {
					b.Fatal(err)
				}
				m.L0Runs[i] = r.Run
			}
			var err error
			s.state, err = EncodeRunCheckpoint(m)
			if err != nil {
				b.Fatal(err)
			}
			original := bytes.Clone(s.state)
			snap = e08Snapshot(b, a)
			q := e08Request(b, snap, uint64(n)+1)
			for _, phase := range []string{"new-publication", "snapshot", "snapshot-clone"} {
				b.Run(phase, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch phase {
						case "new-publication":
							b.StopTimer()
							s.state = original
							b.StartTimer()
							if _, err := a.PublishKafkaRun(context.Background(), q); err != nil {
								b.Fatal(err)
							}
						case "snapshot":
							if _, err := a.Snapshot(context.Background()); err != nil {
								b.Fatal(err)
							}
						case "snapshot-clone":
							if _, err := snap.Manifest(); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(len(original)), "checkpoint_B")
					b.ReportMetric(float64(snap.state.IndexBytes()), "live_run_index_B")
				})
			}
		})
	}
}
