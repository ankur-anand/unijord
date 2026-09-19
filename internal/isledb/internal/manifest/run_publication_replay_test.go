package manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

// A test persistence boundary with durable-in-the-test immutable page bytes,
// linearizable loads/CAS and deterministic failure/barrier injection. This is
// not a provider implementation and makes no provider durability claim.
type e08Memory struct {
	mu                                       sync.Mutex
	state                                    []byte
	pages                                    map[[32]byte][]byte
	mode                                     string
	readErr                                  error
	hook                                     func()
	loadHook                                 func()
	casAttempts, conflicts, reads, readBytes uint64
}

func newE08Memory(t testing.TB) *e08Memory {
	t.Helper()
	m := testRunManifest()
	m.WriterFence = nil
	m.NextSequence = 1
	b, err := EncodeRunCheckpoint(m)
	if err != nil {
		t.Fatal(err)
	}
	return &e08Memory{state: b, pages: make(map[[32]byte][]byte)}
}
func (s *e08Memory) LoadRunManifest(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	b, hook := bytes.Clone(s.state), s.loadHook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return b, nil
}
func (s *e08Memory) CompareAndSwapRunManifest(ctx context.Context, revision uint64, b []byte) (RunCASOutcome, error) {
	if err := ctx.Err(); err != nil {
		return RunCASUnknown, err
	}
	s.mu.Lock()
	hook := s.hook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.casAttempts++
	mode := s.mode
	s.mode = ""
	if mode == "lost-before" {
		return RunCASUnknown, errors.New("lost before apply")
	}
	if mode == "conflict" {
		s.conflicts++
		return RunCASConflict, nil
	}
	m, err := DecodeRunCheckpoint(s.state)
	if err != nil {
		return RunCASUnknown, err
	}
	if m.Revision != revision {
		s.conflicts++
		return RunCASConflict, nil
	}
	n, err := DecodeRunCheckpoint(b)
	if err != nil {
		return RunCASUnknown, err
	}
	if revision == math.MaxUint64 || n.Revision != revision+1 {
		return RunCASUnknown, errors.New("invalid CAS revision")
	}
	s.state = bytes.Clone(b)
	if mode == "lost-after" {
		return RunCASUnknown, errors.New("lost after apply")
	}
	return RunCASApplied, nil
}
func (s *e08Memory) ReadReceiptPage(ctx context.Context, ref ReceiptPageRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.readErr != nil {
		return nil, s.readErr
	}
	b, ok := s.pages[ref.Hash]
	if !ok {
		return nil, ErrMissingReceiptPage
	}
	s.readBytes += uint64(len(b))
	return bytes.Clone(b), nil
}
func (s *e08Memory) WriteReceiptPage(ctx context.Context, ref ReceiptPageRef, b []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if sha256.Sum256(b) != ref.Hash || len(b) != int(ref.EncodedBytes) {
		return errors.New("bad page write")
	}
	if prior, ok := s.pages[ref.Hash]; ok && !bytes.Equal(prior, b) {
		return ErrRunIdentityConflict
	}
	s.pages[ref.Hash] = bytes.Clone(b)
	return nil
}
func e08Identity() KafkaSourceIdentity {
	return KafkaSourceIdentity{BindingID: [16]byte{1}, Cluster: []byte{0, 255, 'c'}, TopicID: [16]byte{2}, TopicName: []byte("events"), Partition: 4, Namespace: [32]byte{1}, Shard: 7, MappingGeneration: 1}
}
func e08Active(t testing.TB) (*RunAuthority, *e08Memory, *RunSnapshot) {
	t.Helper()
	s := newE08Memory(t)
	a := NewRunAuthority(s)
	snap, err := a.ActivateKafkaSource(context.Background(), ActivateKafkaSourceRequest{Identity: e08Identity(), ExpectedManifestRevision: 1, OwnerID: [16]byte{3}, InitialOffset: 100, InitialLeaderEpoch: -1})
	if err != nil {
		t.Fatal(err)
	}
	return a, s, snap
}
func e08Request(t testing.TB, s *RunSnapshot, nonce uint64) PublishKafkaRunRequest {
	t.Helper()
	src := s.Source()
	r := testRun(src.NextOffset, 0)
	r.ObjectKey = fmt.Sprintf("runs/e08-%016x", nonce)
	binary.BigEndian.PutUint64(r.ID[8:], nonce)
	p := runcontract.Publication{Namespace: src.Identity.Namespace, BindingID: src.Identity.BindingID, Cluster: src.Identity.Cluster, TopicID: src.Identity.TopicID, TopicName: src.Identity.TopicName, Partition: src.Identity.Partition, Shard: src.Identity.Shard, MappingGeneration: src.Identity.MappingGeneration, WriterEpoch: src.Epoch, OwnerID: src.OwnerID, ExpectedRevision: s.Revision(), ExpectedOffset: src.NextOffset, NextOffset: src.NextOffset + 1, ExpectedLeaderEpoch: src.LeaderEpoch, LeaderEpoch: src.LeaderEpoch, NextSequence: s.NextSequence(), ResultingSequence: s.NextSequence() + 1, RecordCount: 1, RunID: r.ID, MutationDigest: [32]byte{9}}
	binary.BigEndian.PutUint64(p.AttemptID[8:], nonce)
	_, r.PublicationHash, _ = runcontract.PublicationHashes(p)
	r.CreatorEpoch = src.Epoch
	r.SeqLo = p.NextSequence
	r.SeqHi = p.NextSequence
	r.Events.SeqLo = p.NextSequence
	r.Events.SeqHi = p.NextSequence
	r.Heads.SeqLo = p.NextSequence
	r.Heads.SeqHi = p.NextSequence
	identity := src.Identity.Clone()
	r.Source = &identity
	q, err := NewPublishKafkaRunRequest(p, r)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
func e08Snapshot(t testing.TB, a *RunAuthority) *RunSnapshot {
	t.Helper()
	s, err := a.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func e08Publish(t testing.TB, a *RunAuthority, q PublishKafkaRunRequest) KafkaRunCommitted {
	t.Helper()
	r, err := a.PublishKafkaRun(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func e08Replan(t testing.TB, q PublishKafkaRunRequest, change func(*runcontract.Publication)) PublishKafkaRunRequest {
	t.Helper()
	p, err := runcontract.UnmarshalPublication(q.PlanPreimage)
	if err != nil {
		t.Fatal(err)
	}
	change(&p)
	_, q.Run.PublicationHash, _ = runcontract.PublicationHashes(p)
	src := sourceFromPlan(p)
	q.Run.Source = &src
	q.Run.CreatorEpoch = p.WriterEpoch
	q.Run.ID = p.RunID
	n, err := NewPublishKafkaRunRequest(p, q.Run)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestE08Activation(t *testing.T) {
	a, s, snap := e08Active(t)
	if snap.Revision() != 2 || snap.Source().Epoch != 1 || snap.Source().NextOffset != 100 || snap.NextSequence() != 1 {
		t.Fatal("first activation")
	}
	q := e08Request(t, snap, 1)
	original := e08Publish(t, a, q)
	snap = e08Snapshot(t, a)
	before := bytes.Clone(s.state)
	request := ActivateKafkaSourceRequest{Identity: e08Identity(), ExpectedManifestRevision: snap.Revision(), ExpectedWriterFence: &RunWriterFence{1, [16]byte{3}}, OwnerID: [16]byte{4}, InitialOffset: 101, InitialLeaderEpoch: -1}
	for name, mutate := range map[string]func(*ActivateKafkaSourceRequest){
		"mapping": func(q *ActivateKafkaSourceRequest) { q.Identity.MappingGeneration++ }, "namespace": func(q *ActivateKafkaSourceRequest) { q.Identity.Namespace[0]++ }, "topic": func(q *ActivateKafkaSourceRequest) { q.Identity.TopicID[0]++ }, "cluster": func(q *ActivateKafkaSourceRequest) { q.Identity.Cluster = []byte("other") }, "shard": func(q *ActivateKafkaSourceRequest) { q.Identity.Shard++ }, "revision": func(q *ActivateKafkaSourceRequest) { q.ExpectedManifestRevision-- }, "owner-zero": func(q *ActivateKafkaSourceRequest) { q.OwnerID = [16]byte{} }, "expected-owner": func(q *ActivateKafkaSourceRequest) { q.ExpectedWriterFence = &RunWriterFence{1, [16]byte{9}} }, "epoch": func(q *ActivateKafkaSourceRequest) { q.Epoch = 1 }, "cursor": func(q *ActivateKafkaSourceRequest) { q.InitialOffset++ }, "leader": func(q *ActivateKafkaSourceRequest) { q.InitialLeaderEpoch = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := request
			bad.Identity = request.Identity.Clone()
			mutate(&bad)
			if _, err := a.ActivateKafkaSource(context.Background(), bad); err == nil {
				t.Fatal("accepted")
			}
			if !bytes.Equal(before, s.state) {
				t.Fatal("mutated state")
			}
		})
	}
	after, err := a.ActivateKafkaSource(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if after.Source().Epoch != 2 || after.Revision() != snap.Revision()+1 || after.ReceiptFrontier() != snap.ReceiptFrontier() || after.Source().NextOffset != snap.Source().NextOffset || after.NextSequence() != snap.NextSequence() {
		t.Fatal("takeover changed committed history")
	}
	if replay := e08Publish(t, a, q); replay != original {
		t.Fatal("takeover replay")
	}
	request.Identity.Cluster[0] ^= 1
	src := after.Source()
	src.Identity.Cluster[0] ^= 1
	if !after.Source().Identity.Equal(e08Identity()) {
		t.Fatal("activation ownership")
	}
}

func TestE08PublicationAtomicReplayAndOwnership(t *testing.T) {
	a, s, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	before := bytes.Clone(s.state)
	result := e08Publish(t, a, q)
	after := e08Snapshot(t, a)
	m, _ := after.Manifest()
	if m.Revision != snap.Revision()+1 || len(m.L0Runs) != 1 || m.Source.NextOffset != q.Source.NextOffset || m.NextSequence != q.ResultingNextSequence || m.Receipts.Count != 1 {
		t.Fatal("partial publication", m)
	}
	if bytes.Equal(before, s.state) {
		t.Fatal("not applied")
	}
	attempts := s.casAttempts
	if replay := e08Publish(t, a, q); replay != result || attempts != s.casAttempts {
		t.Fatal("immediate replay reapplied")
	}
	if got, _, err := snap.LookupPublication(context.Background(), q); err != nil || got != nil || snap.Source().NextOffset != 100 {
		t.Fatal("snapshot mixed revisions")
	}
	q.Run.MinTimeline[0]++
	q.Run.Source.Cluster[0]++
	q.Source.Identity.TopicName[0]++
	q.PlanPreimage[0]++
	m.L0Runs[0].Heads.MinKey[0]++
	m.Source.Identity.TopicName[0]++
	current, _ := after.Manifest()
	if current.L0Runs[0].MinTimeline[0] != 0 || current.L0Runs[0].Heads.MinKey[0] != 1 || !current.Source.Identity.Equal(e08Identity()) {
		t.Fatal("caller aliases committed state")
	}
	if _, err := ApplyRunLogEntry(current, &RunLogEntry{Op: RunLogAdd, Revision: current.Revision + 1, NextSequence: 3, AddRuns: []RunMeta{testRun(2, 0)}}); err == nil {
		t.Fatal("cursor/receipt-free source add accepted")
	}
}

func TestE08ValidationIsAtomic(t *testing.T) {
	a, s, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	original := bytes.Clone(s.state)
	mutations := map[string]func(*PublishKafkaRunRequest){
		"owner": func(q *PublishKafkaRunRequest) { q.Source.OwnerID[0]++ }, "source-epoch": func(q *PublishKafkaRunRequest) { q.Source.Epoch++ }, "fence": func(q *PublishKafkaRunRequest) { q.ExpectedWriterFence.Epoch++ }, "fence-owner": func(q *PublishKafkaRunRequest) { q.ExpectedWriterFence.OwnerID[0]++ }, "revision": func(q *PublishKafkaRunRequest) { q.ExpectedManifestRevision-- }, "mapping": func(q *PublishKafkaRunRequest) { q.Source.Identity.MappingGeneration++ }, "expected-offset": func(q *PublishKafkaRunRequest) { q.Source.ExpectedOffset++ }, "result-offset": func(q *PublishKafkaRunRequest) { q.Source.NextOffset++ }, "offset-overflow": func(q *PublishKafkaRunRequest) { q.Source.NextOffset = math.MaxUint64 }, "cursor-only": func(q *PublishKafkaRunRequest) { q.Source.NextOffset = q.Source.ExpectedOffset }, "expected-leader": func(q *PublishKafkaRunRequest) { q.Source.ExpectedLeaderEpoch = 0 }, "invalid-leader": func(q *PublishKafkaRunRequest) { q.Source.LeaderEpoch = -2 }, "leader-result": func(q *PublishKafkaRunRequest) { q.Source.LeaderEpoch++ }, "expected-sequence": func(q *PublishKafkaRunRequest) { q.ExpectedNextSequence++ }, "result-sequence": func(q *PublishKafkaRunRequest) { q.ResultingNextSequence++ }, "sequence-overflow": func(q *PublishKafkaRunRequest) { q.ResultingNextSequence = math.MaxUint64 }, "run-sequence": func(q *PublishKafkaRunRequest) { q.Run.SeqHi++ }, "run-epoch": func(q *PublishKafkaRunRequest) { q.Run.CreatorEpoch++ }, "run-role": func(q *PublishKafkaRunRequest) { q.Run.CreatorRole = 2 }, "run-source": func(q *PublishKafkaRunRequest) { q.Run.Source = nil }, "run-hash": func(q *PublishKafkaRunRequest) { q.Run.PublicationHash[0]++ }, "hash": func(q *PublishKafkaRunRequest) { q.PublicationHash[0]++ }, "id": func(q *PublishKafkaRunRequest) { q.PublicationID[0]++ }, "attempt": func(q *PublishKafkaRunRequest) { q.AttemptID[0]++ }, "run-id": func(q *PublishKafkaRunRequest) { q.Run.ID[0]++ }, "run-ns": func(q *PublishKafkaRunRequest) { q.Run.NamespaceHash[0]++ }, "run-shard": func(q *PublishKafkaRunRequest) { q.Run.Shard++ }, "run-region": func(q *PublishKafkaRunRequest) { q.Run.Heads.Offset = 128 }, "run-filter": func(q *PublishKafkaRunRequest) { q.Run.TimelineFilter = nil }, "plan": func(q *PublishKafkaRunRequest) { q.PlanPreimage = append(bytes.Clone(q.PlanPreimage), 0) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			bad := q
			bad.Run = q.Run.Clone()
			bad.Source.Identity = q.Source.Identity.Clone()
			mutate(&bad)
			if _, err := a.PublishKafkaRun(context.Background(), bad); err == nil {
				t.Fatal("accepted invalid publication")
			}
			if !bytes.Equal(original, s.state) {
				t.Fatal("partial mutation")
			}
		})
	}
	// Known epochs may neither regress nor become unknown, even with recomputed
	// hashes. The existing canonical codec is the single authority for this rule.
	p, _ := runcontract.UnmarshalPublication(q.PlanPreimage)
	p.ExpectedLeaderEpoch = 3
	for _, epoch := range []int32{-1, 2} {
		p.LeaderEpoch = epoch
		if _, err := runcontract.MarshalPublication(p); err == nil {
			t.Fatal("epoch regression")
		}
	}
	p.ExpectedLeaderEpoch = -1
	p.LeaderEpoch = 0
	if _, err := runcontract.MarshalPublication(p); err != nil {
		t.Fatal("unknown to known", err)
	}
	// Exhaustion is never hidden by integer wrapping or a new ownership epoch.
	m, _ := DecodeRunCheckpoint(original)
	m.Revision = math.MaxUint64
	s.state, _ = EncodeRunCheckpoint(m)
	bad := q
	bad.ExpectedManifestRevision = math.MaxUint64
	if _, err := a.PublishKafkaRun(context.Background(), bad); !errors.Is(err, ErrRunCoordinateExhausted) {
		t.Fatal(err)
	}
	m.Revision = 2
	m.Source.Epoch = math.MaxUint64
	m.WriterFence.Epoch = math.MaxUint64
	s.state, _ = EncodeRunCheckpoint(m)
	if _, err := a.ActivateKafkaSource(context.Background(), ActivateKafkaSourceRequest{Identity: e08Identity(), ExpectedManifestRevision: 2, ExpectedWriterFence: m.WriterFence, OwnerID: [16]byte{4}, InitialOffset: 100, InitialLeaderEpoch: -1}); !errors.Is(err, ErrRunCoordinateExhausted) {
		t.Fatal(err)
	}
	p.NextSequence = MaxRunSequence + 1
	p.ResultingSequence = MaxRunSequence + 2
	if _, err := runcontract.MarshalPublication(p); err == nil {
		t.Fatal("56-bit exhaustion")
	}
}

func TestE08IdentityConflictsAndStaleReplay(t *testing.T) {
	a, _, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	original := e08Publish(t, a, q)
	for name, mutate := range map[string]func(*PublishKafkaRunRequest){
		"publication": func(q *PublishKafkaRunRequest) { q.Run.PayloadHash[0]++ }, "attempt": func(q *PublishKafkaRunRequest) { q.PublicationID[0]++ }, "run": func(q *PublishKafkaRunRequest) { q.PublicationID[0]++; q.AttemptID[0]++ }, "interval": func(q *PublishKafkaRunRequest) {
			q.PublicationID[0]++
			q.AttemptID[0]++
			q.Run.ID[0]++
			q.Run.ObjectKey += "other"
		}, "object": func(q *PublishKafkaRunRequest) {
			q.PublicationID[0]++
			q.AttemptID[0]++
			q.Run.ID[0]++
			q.Source.NextOffset++
		}, "stale-invalid-fence": func(q *PublishKafkaRunRequest) { q.ExpectedWriterFence.Epoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := q
			bad.Run = q.Run.Clone()
			mutate(&bad)
			if _, err := a.PublishKafkaRun(context.Background(), bad); !errors.Is(err, ErrRunIdentityConflict) {
				t.Fatal(err)
			}
		})
	}
	if replay := e08Publish(t, a, q); replay != original {
		t.Fatal("stale revision exact replay")
	}
	newQ := e08Request(t, snap, 99)
	newQ.Source.NextOffset++ // distinct interval avoids the intentional identity conflict.
	if _, err := a.PublishKafkaRun(context.Background(), newQ); !errors.Is(err, ErrRunCASConflict) {
		t.Fatal("stale revision without receipt", err)
	}
}

func TestE08LostCASAndDefiniteConflict(t *testing.T) {
	for _, mode := range []string{"lost-before", "lost-after", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			a, s, snap := e08Active(t)
			q := e08Request(t, snap, 1)
			before := bytes.Clone(s.state)
			s.mode = mode
			result, err := a.PublishKafkaRun(context.Background(), q)
			switch mode {
			case "lost-before":
				if !errors.Is(err, ErrRunIndeterminate) || !bytes.Equal(before, s.state) {
					t.Fatal(err)
				}
			case "lost-after":
				if err != nil || result.Revision != 3 {
					t.Fatal(err)
				}
			case "conflict":
				if !errors.Is(err, ErrRunCASConflict) || !bytes.Equal(before, s.state) {
					t.Fatal(err)
				}
			}
			result = e08Publish(t, a, q)
			if result.Revision != 3 || e08Snapshot(t, a).ReceiptFrontier().Count != 1 {
				t.Fatal("retry rebased or duplicated")
			}
		})
	}
}

func TestE08ConcurrentPublishers(t *testing.T) {
	for _, identical := range []bool{false, true} {
		t.Run(fmt.Sprint(identical), func(t *testing.T) {
			a, s, snap := e08Active(t)
			q1, q2 := e08Request(t, snap, 1), e08Request(t, snap, 2)
			if identical {
				q2 = q1
			}
			arrived := make(chan struct{}, 2)
			release := make(chan struct{})
			s.hook = func() { arrived <- struct{}{}; <-release }
			errs := make(chan error, 2)
			for _, q := range []PublishKafkaRunRequest{q1, q2} {
				go func(q PublishKafkaRunRequest) { _, err := a.PublishKafkaRun(context.Background(), q); errs <- err }(q)
			}
			<-arrived
			<-arrived
			close(release)
			e1, e2 := <-errs, <-errs
			if identical {
				if e1 != nil || e2 != nil {
					t.Fatal(e1, e2)
				}
			} else if (e1 == nil) == (e2 == nil) {
				t.Fatal("different plans both committed", e1, e2)
			}
			m, _ := e08Snapshot(t, a).Manifest()
			if m.Revision != 3 || m.Source.NextOffset != 101 || m.NextSequence != 2 || len(m.L0Runs) != 1 || m.Receipts.Count != 1 {
				t.Fatal("non-atomic concurrent state")
			}
		})
	}
}

func TestE08ActivationRacesPublication(t *testing.T) {
	for _, publicationFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(publicationFirst), func(t *testing.T) {
			a, s, snap := e08Active(t)
			q := e08Request(t, snap, 1)
			arrived := make(chan struct{})
			release := make(chan struct{})
			s.hook = func() { close(arrived); <-release }
			errCh := make(chan error, 1)
			activation := ActivateKafkaSourceRequest{Identity: e08Identity(), ExpectedManifestRevision: 2, ExpectedWriterFence: &RunWriterFence{1, [16]byte{3}}, OwnerID: [16]byte{4}, InitialOffset: 100, InitialLeaderEpoch: -1}
			if publicationFirst {
				go func() { _, err := a.ActivateKafkaSource(context.Background(), activation); errCh <- err }()
			} else {
				go func() { _, err := a.PublishKafkaRun(context.Background(), q); errCh <- err }()
			}
			<-arrived
			s.mu.Lock()
			s.hook = nil
			s.mu.Unlock()
			if publicationFirst {
				e08Publish(t, a, q)
			} else if _, err := a.ActivateKafkaSource(context.Background(), activation); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-errCh; !errors.Is(err, ErrRunCASConflict) {
				t.Fatal("loser silently rebased", err)
			}
			m, _ := e08Snapshot(t, a).Manifest()
			if m.Revision != 3 || m.Source.Epoch != m.WriterFence.Epoch || m.Source.OwnerID != m.WriterFence.OwnerID {
				t.Fatal("divergent authority")
			}
			if !publicationFirst {
				before := bytes.Clone(s.state)
				if _, err := a.PublishKafkaRun(context.Background(), q); !errors.Is(err, ErrRunFence) {
					t.Fatal(err)
				}
				if !bytes.Equal(before, s.state) {
					t.Fatal("stale owner mutated state")
				}
			}
		})
	}
}

func TestE08ReplayAfterHistoryCheckpointAndDeletion(t *testing.T) {
	a, s, snap := e08Active(t)
	first := e08Request(t, snap, 1)
	original := e08Publish(t, a, first)
	for i := 2; i <= 1000; i++ {
		snap = e08Snapshot(t, a)
		q := e08Request(t, snap, uint64(i))
		e08Publish(t, a, q)
		// Isolated E07 maintenance replay removes whole runs to keep live state
		// small. No compactor/lifecycle execution is introduced by this fixture.
		m, _ := e08Snapshot(t, a).Manifest()
		if len(m.L0Runs) >= 8 {
			ids := make([][16]byte, len(m.L0Runs))
			for j := range ids {
				ids[j] = m.L0Runs[j].ID
			}
			e := RunLogEntry{Op: RunLogRemove, Revision: m.Revision + 1, NextSequence: m.NextSequence, RemoveRunIDs: ids}
			next, err := ApplyRunLogEntry(m, &e)
			if err != nil {
				t.Fatal(err)
			}
			s.state, _ = EncodeRunCheckpoint(next)
		}
	}
	m, _ := e08Snapshot(t, a).Manifest()
	before := m.Receipts
	// A trivial complete-object compaction move and then deletion preserve the
	// receipt even when its original run is no longer live anywhere.
	if len(m.L0Runs) > 0 {
		r := m.L0Runs[0].Clone()
		r.Level = 1
		next, err := ApplyRunLogEntry(m, &RunLogEntry{Op: RunLogCompaction, Revision: m.Revision + 1, NextSequence: m.NextSequence, DestinationLevel: 1, RemoveRunIDs: [][16]byte{r.ID}, AddRuns: []RunMeta{r}})
		if err != nil {
			t.Fatal(err)
		}
		m = next
		next, err = ApplyRunLogEntry(m, &RunLogEntry{Op: RunLogRemove, Revision: m.Revision + 1, NextSequence: m.NextSequence, RemoveRunIDs: [][16]byte{r.ID}})
		if err != nil {
			t.Fatal(err)
		}
		m = next
	}
	checkpoint, err := EncodeRunCheckpoint(m)
	if err != nil {
		t.Fatal(err)
	}
	s.state = checkpoint
	a = NewRunAuthority(s)
	if replay := e08Publish(t, a, first); replay != original {
		t.Fatal("old receipt result changed")
	}
	if e08Snapshot(t, a).ReceiptFrontier() != before {
		t.Fatal("maintenance pruned receipts")
	}
	result, stats, err := e08Snapshot(t, a).LookupPublication(context.Background(), first)
	if err != nil || *result != original || stats.PageReads > 5*uint64(before.Root.Level+1) {
		t.Fatal("unbounded/missing replay", stats, err)
	}
}

func TestE08ReceiptFailuresAndCorruption(t *testing.T) {
	a, s, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	e08Publish(t, a, q)
	snap = e08Snapshot(t, a)
	s.readErr = errors.New("transport")
	if _, _, err := snap.LookupPublication(context.Background(), q); !errors.Is(err, ErrRunIndeterminate) {
		t.Fatal("transport became miss", err)
	}
	s.readErr = nil
	root := snap.ReceiptFrontier().Root
	saved := bytes.Clone(s.pages[root.Hash])
	delete(s.pages, root.Hash)
	if _, _, err := snap.LookupPublication(context.Background(), q); !errors.Is(err, ErrInvalidRunManifest) {
		t.Fatal("missing became miss", err)
	}
	s.pages[root.Hash] = bytes.Clone(saved)
	s.pages[root.Hash][len(saved)-1] ^= 1
	if _, _, err := snap.LookupPublication(context.Background(), q); !errors.Is(err, ErrInvalidRunManifest) {
		t.Fatal("corrupt page", err)
	}
	s.pages[root.Hash] = bytes.Clone(saved)
	f := snap.ReceiptFrontier()
	f.Count++
	if f.validate() == nil {
		t.Fatal("frontier count omission")
	}
	f = snap.ReceiptFrontier()
	f.Root = ReceiptPageRef{}
	if f.validate() == nil {
		t.Fatal("omitted frontier")
	}
	p, err := decodeReceiptIndex(saved)
	if err != nil {
		t.Fatal(err)
	}
	p.Items = append(p.Items, p.Items[0])
	if _, err := encodeReceiptIndex(p); err == nil {
		t.Fatal("duplicate receipt key")
	}
	cycle := &receiptIndexPage{Level: 2, Children: []ReceiptPageRef{root, root}}
	if _, err := encodeReceiptIndex(cycle); err == nil {
		t.Fatal("duplicate/cyclic child")
	}
	cycle.Children[1].Level = 2
	if _, err := encodeReceiptIndex(cycle); err == nil {
		t.Fatal("non-decreasing depth")
	}
	for _, change := range []func(*RunManifest){func(m *RunManifest) { m.Source.NextOffset++ }, func(m *RunManifest) { m.NextSequence++ }, func(m *RunManifest) { m.WriterFence.OwnerID[0]++ }, func(m *RunManifest) { m.Receipts = ReceiptFrontier{} }} {
		m, _ := snap.Manifest()
		change(m)
		encoded, err := EncodeRunCheckpoint(m)
		if err != nil {
			continue
		}
		old := s.state
		s.state = encoded
		if _, err := a.Snapshot(context.Background()); err == nil {
			t.Fatal("partial checkpoint accepted")
		}
		s.state = old
	}
}

func TestE08SnapshotLinearizationAndReplayRace(t *testing.T) {
	a, s, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	original := e08Publish(t, a, q)
	arrived := make(chan struct{})
	release := make(chan struct{})
	s.loadHook = func() { close(arrived); <-release }
	views := make(chan *RunSnapshot, 1)
	errs := make(chan error, 1)
	go func() { v, err := a.Snapshot(context.Background()); views <- v; errs <- err }()
	<-arrived
	s.mu.Lock()
	s.loadHook = nil
	s.mu.Unlock()
	later := e08Request(t, e08Snapshot(t, a), 2)
	e08Publish(t, a, later)
	close(release)
	old := <-views
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if old.Revision() != 3 || old.Source().NextOffset != 101 || old.NextSequence() != 2 || old.ReceiptFrontier().Count != 1 {
		t.Fatal("snapshot spliced two revisions")
	}
	if r, _, err := old.LookupPublication(context.Background(), later); err != nil || r != nil {
		t.Fatal("lookup used a newer frontier", err)
	}
	// Concurrent replay and activation share no mutable receipt or snapshot data.
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				got, err := a.PublishKafkaRun(context.Background(), q)
				if err != nil || got != original {
					t.Error("concurrent replay", err)
				}
				view, err := a.Snapshot(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				m, err := view.Manifest()
				if err != nil || m.Source.Epoch != m.WriterFence.Epoch {
					t.Error("snapshot fence", err)
				}
			}
		}()
	}
	close(start)
	current := e08Snapshot(t, a)
	_, err := a.ActivateKafkaSource(context.Background(), ActivateKafkaSourceRequest{Identity: e08Identity(), ExpectedManifestRevision: current.Revision(), ExpectedWriterFence: &RunWriterFence{1, [16]byte{3}}, OwnerID: [16]byte{4}, InitialOffset: 102, InitialLeaderEpoch: -1})
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestE08PublicationVectorsAndPrototypeRejection(t *testing.T) {
	b, err := os.ReadFile("../runfile/testdata/compat/v1/logical-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Publication struct {
			Preimage string `json:"preimage_hex"`
			ID       string `json:"publication_id_hex"`
			Hash     string `json:"publication_hash"`
		}
	}
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	pre, err := hex.DecodeString(v.Publication.Preimage)
	if err != nil {
		t.Fatal(err)
	}
	p, err := runcontract.UnmarshalPublication(pre)
	if err != nil {
		t.Fatal(err)
	}
	id, hash, err := runcontract.PublicationHashes(p)
	if err != nil || hex.EncodeToString(id[:]) != v.Publication.ID || hex.EncodeToString(hash[:]) != v.Publication.Hash {
		t.Fatal("E00 vectors", err)
	}
	r := testRun(1, 0)
	r.ID = p.RunID
	r.NamespaceHash = p.Namespace
	r.CreatorEpoch = p.WriterEpoch
	r.PublicationHash = hash
	r.SeqLo = p.NextSequence
	r.SeqHi = p.ResultingSequence - 1
	r.Events.SeqLo = r.SeqLo
	r.Events.SeqHi = r.SeqHi
	r.Events.EntryCount = 3
	r.Heads.SeqLo = 51
	r.Heads.SeqHi = 52
	r.Heads.EntryCount = 2
	r.TimelineFilter.KeyCount = 2
	r.TimelineFilter.Region.EntryCount = 2
	src := sourceFromPlan(p)
	r.Source = &src
	q, err := NewPublishKafkaRunRequest(p, r)
	if err != nil || !bytes.Equal(q.PlanPreimage, pre) || q.PublicationID != id || q.PublicationHash != hash {
		t.Fatal("publication API vector", err)
	}
	for _, kind := range []byte{runCheckpointKind, runPageKind, runLogKind, runReceiptKind, runReceiptIndexKind} {
		prototype := runEnvelope([]byte{1}, kind)
		binary.BigEndian.PutUint16(prototype[4:6], 1)
		if _, err := openRunEnvelope(prototype, kind, MaxRunCheckpointBytes); !errors.Is(err, ErrUnsupportedRunFormat) {
			t.Fatal("E07 prototype accepted", err)
		}
	}
}

func TestE08FrozenVectors(t *testing.T) {
	a, s, snap := e08Active(t)
	activated := bytes.Clone(s.state)
	q := e08Request(t, snap, 1)
	e08Publish(t, a, q)
	snap = e08Snapshot(t, a)
	vectors := map[string][]byte{"activated": activated, "published": s.state, "receipt": s.pages[snap.ReceiptFrontier().Latest.Hash], "receipt-index": s.pages[snap.ReceiptFrontier().Root.Hash]}
	want := map[string]string{"activated": "a69ac1968d673b11649e8a97d979d69e86b3556b18ad8ca5d2c31e3a11fb48c9", "published": "63d2a904e092bd25950fc571cb5d1afd6a8a19ef8b53f04080e5508a13a9f3d8", "receipt": "d4bc3c6e86207d5b5a392550b0c7a0327219e25002f2a873647ca385f5e8f68c", "receipt-index": "a4febddfffbd043b418c29d051d594e5c7a7335708650af8ec906337379bea90"}
	for name, b := range vectors {
		hash := sha256.Sum256(b)
		if hex.EncodeToString(hash[:]) != want[name] {
			t.Errorf("%s bytes=%d sha256=%x", name, len(b), hash)
		}
	}
}

func TestE08LedgerFailureAfterDefiniteConflict(t *testing.T) {
	a, s, snap := e08Active(t)
	e08Publish(t, a, e08Request(t, snap, 1))
	q := e08Request(t, e08Snapshot(t, a), 2)
	before := bytes.Clone(s.state)
	s.mode = "conflict"
	s.hook = func() { s.mu.Lock(); s.readErr = errors.New("ledger transport"); s.mu.Unlock() }
	if _, err := a.PublishKafkaRun(context.Background(), q); !errors.Is(err, ErrRunIndeterminate) {
		t.Fatal("unreadable receipt reported definitive absence", err)
	}
	if !bytes.Equal(before, s.state) {
		t.Fatal("conflict mutated state")
	}
}

func TestE08CheckpointShapesRetainEveryReceipt(t *testing.T) {
	a, s, snap := e08Active(t)
	requests := make([]PublishKafkaRunRequest, 0, 220)
	results := make([]KafkaRunCommitted, 0, 220)
	for i := 1; i <= 220; i++ {
		q := e08Request(t, snap, uint64(i))
		requests = append(requests, q)
		results = append(results, e08Publish(t, a, q))
		snap = e08Snapshot(t, a)
		if i == 1 || i == 7 || i == 33 || i == 220 {
			m, _ := snap.Manifest()
			cp, err := EncodeRunCheckpoint(m)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeRunCheckpoint(cp)
			if err != nil || decoded.Receipts != m.Receipts {
				t.Fatal("checkpoint frontier", err)
			}
			s.state = cp
			a = NewRunAuthority(s)
			view := e08Snapshot(t, a)
			for j, prior := range requests {
				got, stats, err := view.LookupPublication(context.Background(), prior)
				if err != nil || got == nil || *got != results[j] || stats.PageReads > 5*uint64(view.ReceiptFrontier().Root.Level+1) {
					t.Fatal("omitted historical receipt", i, j, err)
				}
			}
		}
	}
	// A newer latest receipt with an older root cannot silently drop history.
	m, _ := snap.Manifest()
	// Even a self-consistent replacement tree containing only the latest exact
	// receipt must not authorize shortening the durable history. The immutable
	// latest receipt binds its ordinal, independently of mutable frontier fields.
	last := requests[len(requests)-1]
	items := make([]receiptIndexItem, receiptIdentityCount)
	for i, k := range requestReceiptKeys(&last) {
		items[i] = receiptIndexItem{k, m.Receipts.Latest}
	}
	pruned, err := writeReceiptIndex(context.Background(), s, &receiptIndexPage{Level: 1, Items: items})
	if err != nil {
		t.Fatal(err)
	}
	forged, _ := snap.Manifest()
	forged.Receipts.Root = pruned
	forged.Receipts.Count = 1
	encoded, err := EncodeRunCheckpoint(forged)
	if err != nil {
		t.Fatal(err)
	}
	original := s.state
	s.state = encoded
	if _, err := a.Snapshot(context.Background()); !errors.Is(err, ErrInvalidRunManifest) {
		t.Fatal("self-consistent shortened frontier accepted", err)
	}
	s.state = original
	data := s.pages[m.Receipts.Root.Hash]
	root, err := decodeReceiptIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	if root.Level <= 1 {
		t.Fatal("fixture needs branch")
	}
	root.Children = root.Children[1:]
	if len(root.Children) >= 2 {
		b, err := encodeReceiptIndex(root)
		if err != nil {
			t.Fatal(err)
		}
		ref := root.reference(b)
		s.pages[ref.Hash] = b
		m.Receipts.Root = ref
		if _, err := EncodeRunCheckpoint(m); err == nil {
			t.Fatal("frontier omitted older receipts")
		}
	}
}

func TestE08LeaderAdvanceCanonicalConflictsAndCancellation(t *testing.T) {
	a, s, snap := e08Active(t)
	q := e08Replan(t, e08Request(t, snap, 1), func(p *runcontract.Publication) { p.LeaderEpoch = 3 })
	result := e08Publish(t, a, q)
	if e08Snapshot(t, a).Source().LeaderEpoch != 3 {
		t.Fatal("known leader epoch not installed")
	}
	next := e08Request(t, e08Snapshot(t, a), 2)
	for _, change := range []func(*runcontract.Publication){func(p *runcontract.Publication) { p.AttemptID = q.AttemptID }, func(p *runcontract.Publication) { p.RunID = q.Run.ID }} {
		bad := e08Replan(t, next, change)
		if _, err := a.PublishKafkaRun(context.Background(), bad); !errors.Is(err, ErrRunIdentityConflict) {
			t.Fatal("canonical identity reuse", err)
		}
	}
	bad := next
	bad.Run = next.Run.Clone()
	bad.Run.ObjectKey = q.Run.ObjectKey
	if _, err := a.PublishKafkaRun(context.Background(), bad); !errors.Is(err, ErrRunIdentityConflict) {
		t.Fatal("canonical object-key reuse", err)
	}
	before := bytes.Clone(s.state)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.PublishKafkaRun(ctx, next); !errors.Is(err, ErrRunIndeterminate) || !bytes.Equal(before, s.state) {
		t.Fatal("cancellation before apply", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	s.mode = "lost-after"
	s.hook = cancel
	if _, err := a.PublishKafkaRun(ctx, next); !errors.Is(err, ErrRunIndeterminate) {
		t.Fatal("in-flight cancellation incorrectly definitive", err)
	}
	s.hook = nil
	replay := e08Publish(t, a, next)
	if replay.Revision != result.Revision+1 || e08Snapshot(t, a).ReceiptFrontier().Count != 2 {
		t.Fatal("cancelled lost response replay")
	}
}

func TestE08AdmissionBeforeReceiptWrites(t *testing.T) {
	for _, m := range []*RunManifest{testRunManifest(testRun(1, 0)), testRunManifest()} {
		s := newE08Memory(t)
		s.state, _ = EncodeRunCheckpoint(m)
		if _, err := NewRunAuthority(s).Snapshot(context.Background()); !errors.Is(err, ErrInvalidRunManifest) {
			t.Fatal("authority exposed receipt-free generic run history", err)
		}
	}
	_, _, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	m, _ := snap.Manifest()
	// Share test-only backing storage to represent the complete live count
	// without allocating hundreds of MiB merely to test pre-admission.
	runs := make([]RunMeta, MaxManifestRuns/MaxRunLevels)
	m.Levels = make([]RunLevel, MaxRunLevels)
	for i := range m.Levels {
		m.Levels[i] = RunLevel{Number: uint32(i + 1), Runs: runs}
	}
	if err := admitKafkaRun(m, &q.Run); !errors.Is(err, ErrRunManifestLimit) {
		t.Fatal("total live count admitted", err)
	}
	m.Levels = nil
	large := q.Run.Clone()
	large.ID[0]++
	large.ObjectKey += "large"
	key := bytes.Repeat([]byte{1}, 65527)
	large.Events.MinKey = key
	large.Events.MaxKey = key
	large.Heads.MinKey = key
	large.Heads.MaxKey = key
	// Direct helper boundary: every existing entry has the maximum admitted key
	// lengths, so its computed wire size must reject before metadata is cloned.
	n := int(MaxRunCheckpointBytes/runWireSize(&large)) + 1
	m.L0Runs = make([]RunMeta, n)
	for i := range m.L0Runs {
		m.L0Runs[i] = large
	}
	if err := admitKafkaRun(m, &q.Run); !errors.Is(err, ErrRunManifestLimit) {
		t.Fatal("aggregate checkpoint budget admitted", err)
	}
}

func TestE08ExhaustedSequenceAuthority(t *testing.T) {
	a, s, snap := e08Active(t)
	q := e08Request(t, snap, 1)
	p, _ := runcontract.UnmarshalPublication(q.PlanPreimage)
	// A compact synthetic persisted fixture at the 56-bit cursor boundary;
	// constructing the preceding 2^56 mutations is intentionally unnecessary.
	p.NextSequence = MaxRunSequence
	p.ResultingSequence = MaxRunSequence + 1
	r := q.Run.Clone()
	r.SeqLo = MaxRunSequence
	r.SeqHi = MaxRunSequence
	r.Events.SeqLo = MaxRunSequence
	r.Events.SeqHi = MaxRunSequence
	r.Heads.SeqLo = MaxRunSequence
	r.Heads.SeqHi = MaxRunSequence
	_, r.PublicationHash, _ = runcontract.PublicationHashes(p)
	q, err := NewPublishKafkaRunRequest(p, r)
	if err != nil {
		t.Fatal(err)
	}
	result := committedResult(&q, 3)
	receipt := &KafkaRunReceipt{1, q.PlanPreimage, q.PublicationID, q.PublicationHash, q.AttemptID, q.Run, result}
	frontier, err := appendReceipt(context.Background(), s, ReceiptFrontier{}, receipt)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := snap.Manifest()
	m.Receipts = frontier
	m.L0Runs = []RunMeta{r}
	m.Source.NextOffset = 101
	m.NextSequence = MaxRunSequence + 1
	m.Revision = 3
	s.state, err = EncodeRunCheckpoint(m)
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(s.state)
	exhausted := q
	exhausted.PublicationID[0]++
	exhausted.AttemptID[0]++
	exhausted.Run.ID[0]++
	exhausted.Run.ObjectKey += "next"
	exhausted.ExpectedManifestRevision = 3
	exhausted.Source.ExpectedOffset = 101
	exhausted.Source.NextOffset = 102
	exhausted.ExpectedNextSequence = MaxRunSequence + 1
	if _, err := a.PublishKafkaRun(context.Background(), exhausted); !errors.Is(err, ErrRunCoordinateExhausted) || !bytes.Equal(before, s.state) {
		t.Fatal("exhausted sequence advanced", err)
	}
	if replay := e08Publish(t, a, q); replay != result {
		t.Fatal("exhaustion blocked exact replay")
	}
}
