package manifest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

var (
	ErrSourceMismatch         = errors.New("Kafka source identity/cursor mismatch")
	ErrRunFence               = errors.New("run source-owner/writer fence lost")
	ErrRunCASConflict         = errors.New("definite run manifest CAS revision conflict")
	ErrRunIndeterminate       = errors.New("indeterminate run manifest outcome")
	ErrRunIdentityConflict    = errors.New("corrupt publication identity conflict")
	ErrRunCoordinateExhausted = errors.New("run manifest coordinate exhausted")
	ErrMissingReceiptPage     = errors.New("committed receipt page missing")
)

type KafkaSourceExpectation struct {
	Identity                         KafkaSourceIdentity
	OwnerID                          [16]byte
	Epoch                            uint64
	ExpectedOffset, NextOffset       uint64
	ExpectedLeaderEpoch, LeaderEpoch int32
}
type PublishKafkaRunRequest struct {
	ExpectedManifestRevision                    uint64
	ExpectedWriterFence                         RunWriterFence
	Source                                      KafkaSourceExpectation
	ExpectedNextSequence, ResultingNextSequence uint64
	Run                                         RunMeta
	PlanPreimage                                []byte
	PublicationID, PublicationHash              [32]byte
	AttemptID                                   [16]byte
}
type KafkaRunCommitted struct {
	Revision      uint64
	NextOffset    uint64
	LeaderEpoch   int32
	NextSequence  uint64
	RunID         [16]byte
	PublicationID [32]byte
	AttemptID     [16]byte
}

// NewPublishKafkaRunRequest freezes the canonical E00 plan and owns every byte
// slice. RunMeta source identity is manifest-only; run-object bytes are unchanged.
func NewPublishKafkaRunRequest(p runcontract.Publication, r RunMeta) (PublishKafkaRunRequest, error) {
	pre, err := runcontract.MarshalPublication(p)
	if err != nil {
		return PublishKafkaRunRequest{}, err
	}
	id, hash, err := runcontract.PublicationHashes(p)
	if err != nil {
		return PublishKafkaRunRequest{}, err
	}
	q := PublishKafkaRunRequest{
		ExpectedManifestRevision: p.ExpectedRevision, ExpectedWriterFence: RunWriterFence{p.WriterEpoch, p.OwnerID},
		Source:               KafkaSourceExpectation{sourceFromPlan(p).Clone(), p.OwnerID, p.WriterEpoch, p.ExpectedOffset, p.NextOffset, p.ExpectedLeaderEpoch, p.LeaderEpoch},
		ExpectedNextSequence: p.NextSequence, ResultingNextSequence: p.ResultingSequence,
		Run: r, PlanPreimage: pre, PublicationID: id, PublicationHash: hash, AttemptID: p.AttemptID,
	}
	if err = q.validateRun(p); err != nil {
		return q, err
	}
	q.Run = r.Clone()
	return q, nil
}
func (q *PublishKafkaRunRequest) validateRun(p runcontract.Publication) error {
	r := &q.Run
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Source == nil || !r.Source.Equal(sourceFromPlan(p)) || r.ID != p.RunID || r.PublicationHash != q.PublicationHash || r.NamespaceHash != p.Namespace || r.Shard != p.Shard || r.CreatorRole != 1 || r.CreatorEpoch != p.WriterEpoch || r.Level != 0 || r.SeqLo != p.NextSequence || r.SeqHi+1 != p.ResultingSequence || r.Events.EntryCount != uint64(p.RecordCount) || r.TimelineFilter == nil || r.TimelineFilter.BitsPerKey != 10 {
		return runInvalid("publication RunMeta projection")
	}
	return nil
}
func (q *PublishKafkaRunRequest) equal(b *PublishKafkaRunRequest) bool {
	return q.ExpectedManifestRevision == b.ExpectedManifestRevision && q.ExpectedWriterFence == b.ExpectedWriterFence && q.Source.Identity.Equal(b.Source.Identity) && q.Source.OwnerID == b.Source.OwnerID && q.Source.Epoch == b.Source.Epoch && q.Source.ExpectedOffset == b.Source.ExpectedOffset && q.Source.NextOffset == b.Source.NextOffset && q.Source.ExpectedLeaderEpoch == b.Source.ExpectedLeaderEpoch && q.Source.LeaderEpoch == b.Source.LeaderEpoch && q.ExpectedNextSequence == b.ExpectedNextSequence && q.ResultingNextSequence == b.ResultingNextSequence && q.Run.Equal(b.Run) && bytes.Equal(q.PlanPreimage, b.PlanPreimage) && q.PublicationID == b.PublicationID && q.PublicationHash == b.PublicationHash && q.AttemptID == b.AttemptID
}
func (q *PublishKafkaRunRequest) validateCanonical() error {
	p, err := runcontract.UnmarshalPublication(q.PlanPreimage)
	if err != nil {
		return fmt.Errorf("%w: canonical publication: %v", ErrInvalidRunManifest, err)
	}
	c, err := NewPublishKafkaRunRequest(p, q.Run)
	if err != nil {
		return err
	}
	if !q.equal(&c) {
		return runInvalid("canonical publication fields/hash mismatch")
	}
	return nil
}
func (q *PublishKafkaRunRequest) admitLookup() error {
	if len(q.PlanPreimage) > runcontract.MaxPublicationBytes || len(q.Source.Identity.Cluster) > 256 || len(q.Source.Identity.TopicName) > 249 || len(q.Run.ObjectKey) > MaxRunObjectKeyBytes {
		return ErrRunManifestLimit
	}
	return nil
}
func committedResult(q *PublishKafkaRunRequest, revision uint64) KafkaRunCommitted {
	return KafkaRunCommitted{revision, q.Source.NextOffset, q.Source.LeaderEpoch, q.ResultingNextSequence, q.Run.ID, q.PublicationID, q.AttemptID}
}
func (w *runEncoder) committed(c KafkaRunCommitted) {
	w.u64(c.Revision)
	w.u64(c.NextOffset)
	w.u32(uint32(c.LeaderEpoch))
	w.u64(c.NextSequence)
	w.data = append(w.data, c.RunID[:]...)
	w.data = append(w.data, c.PublicationID[:]...)
	w.data = append(w.data, c.AttemptID[:]...)
}
func (r *runDecoder) committed() KafkaRunCommitted {
	c := KafkaRunCommitted{Revision: r.u64(), NextOffset: r.u64(), LeaderEpoch: int32(r.u32()), NextSequence: r.u64()}
	copy(c.RunID[:], r.take(16))
	copy(c.PublicationID[:], r.take(32))
	copy(c.AttemptID[:], r.take(16))
	return c
}

type RunCASOutcome uint8

const (
	RunCASUnknown RunCASOutcome = iota
	RunCASApplied
	RunCASConflict
)

// RunManifestPersistence is independent of the legacy ISLM/SST Store. Load is
// linearizable; CAS atomically replaces one complete UJRM checkpoint iff its
// revision is expected. Definite conflict guarantees no apply. Any error or
// unknown response may have applied. Successful page writes are durable before
// CAS; all committed frontiers and their pages must be retained indefinitely.
// Implementations must enforce checkpoint/page byte limits before buffering.
type RunManifestPersistence interface {
	ReceiptPageStorage
	LoadRunManifest(context.Context) ([]byte, error)
	CompareAndSwapRunManifest(context.Context, uint64, []byte) (RunCASOutcome, error)
}

type RunAuthority struct{ storage RunManifestPersistence }

func NewRunAuthority(s RunManifestPersistence) *RunAuthority { return &RunAuthority{storage: s} }

// RunSnapshot has no mutable exported state. Accessors return detached values.
// It and receipt lookups use the same immutable frontier from the Load that
// linearized this view; later ownership/publication cannot change this view.
type RunSnapshot struct {
	state *RunManifest
	pages ReceiptPageStorage
}

func (s *RunSnapshot) Manifest() (*RunManifest, error)  { return s.state.Clone() }
func (s *RunSnapshot) Revision() uint64                 { return s.state.Revision }
func (s *RunSnapshot) Source() *KafkaSourceState        { return s.state.Source.Clone() }
func (s *RunSnapshot) NextSequence() uint64             { return s.state.NextSequence }
func (s *RunSnapshot) ReceiptFrontier() ReceiptFrontier { return s.state.Receipts }
func (s *RunSnapshot) LookupPublication(ctx context.Context, q PublishKafkaRunRequest) (*KafkaRunCommitted, ReceiptLookupStats, error) {
	var stats ReceiptLookupStats
	if err := q.admitLookup(); err != nil {
		return nil, stats, err
	}
	var found *KafkaRunCommitted
	var ordinal uint64
	matches := 0
	for _, key := range requestReceiptKeys(&q) {
		r, err := lookupReceipt(ctx, s.pages, s.state.Receipts.Root, key, &stats)
		if err != nil {
			return nil, stats, err
		}
		if r == nil {
			continue
		}
		prior, err := r.request()
		if err != nil {
			return nil, stats, err
		}
		if !q.equal(&prior) {
			return nil, stats, ErrRunIdentityConflict
		}
		if r.Ordinal > s.state.Receipts.Count || (ordinal != 0 && ordinal != r.Ordinal) {
			return nil, stats, runInvalid("receipt ordinal/index disagreement")
		}
		ordinal = r.Ordinal
		v := r.Result
		found = &v
		matches++
	}
	if matches != 0 && matches != receiptIdentityCount {
		return nil, stats, runInvalid("incomplete receipt identity frontier")
	}
	return found, stats, nil
}
func (a *RunAuthority) Snapshot(ctx context.Context) (*RunSnapshot, error) {
	b, err := a.storage.LoadRunManifest(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: load: %w", ErrRunIndeterminate, err)
	}
	m, err := DecodeRunCheckpoint(b)
	if err != nil {
		return nil, err
	}
	// The general run codecs also support isolated metadata/replay fixtures.
	// This authority may open only a pristine bootstrap or source-backed state;
	// it cannot expose a generic add_run history as Kafka publication evidence.
	if m.Source == nil && (m.Revision != 1 || m.NextSequence != 1 || m.WriterFence != nil || len(m.L0Runs) != 0 || len(m.Levels) != 0) {
		return nil, runInvalid("Kafka authority requires pristine bootstrap or source state")
	}
	if m.Receipts.Count != 0 {
		var stats ReceiptLookupStats
		if _, e := loadReceiptIndex(ctx, a.storage, m.Receipts.Root, &stats); e != nil {
			return nil, e
		}
		data, e := readReceiptPage(ctx, a.storage, m.Receipts.Latest, &stats)
		if e != nil {
			return nil, e
		}
		r, e := DecodeKafkaRunReceipt(data)
		if e != nil {
			return nil, e
		}
		q, e := r.request()
		if e != nil {
			return nil, e
		}
		if r.Ordinal != m.Receipts.Count || r.Result.Revision > m.Revision || r.Result.NextOffset != m.Source.NextOffset || r.Result.LeaderEpoch != m.Source.LeaderEpoch || r.Result.NextSequence != m.NextSequence || !q.Source.Identity.Equal(m.Source.Identity) || q.Source.Epoch > m.Source.Epoch || m.Receipts.Latest.Min != requestReceiptKeys(&q)[0] {
			return nil, runInvalid("checkpoint source/sequence/receipt frontier")
		}
	}
	return &RunSnapshot{m, a.storage}, nil
}
func (a *RunAuthority) ActivateKafkaSource(ctx context.Context, q ActivateKafkaSourceRequest) (*RunSnapshot, error) {
	s, err := a.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	c, err := activateKafkaSource(s.state, q)
	if err != nil {
		return nil, err
	}
	b, err := EncodeRunCheckpoint(c)
	if err != nil {
		return nil, err
	}
	outcome, err := a.storage.CompareAndSwapRunManifest(ctx, s.state.Revision, b)
	if err != nil || outcome == RunCASUnknown {
		return nil, fmt.Errorf("%w: activation CAS: %v", ErrRunIndeterminate, err)
	}
	if outcome == RunCASConflict {
		return nil, ErrRunCASConflict
	}
	if outcome != RunCASApplied {
		return nil, ErrRunIndeterminate
	}
	return &RunSnapshot{c, a.storage}, nil
}

// PublishKafkaRun attempts exactly the supplied plan. It never rebases or
// rebuilds a request, including after definite conflict or an ambiguous CAS.
func (a *RunAuthority) PublishKafkaRun(ctx context.Context, q PublishKafkaRunRequest) (KafkaRunCommitted, error) {
	var zero KafkaRunCommitted
	s, err := a.Snapshot(ctx)
	if err != nil {
		return zero, err
	}
	prior, _, err := s.LookupPublication(ctx, q)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return *prior, nil
	}
	m := s.state
	// Authority and coordinates are checked only after exact receipt lookup.
	if m.Source == nil || !m.Source.Identity.Equal(q.Source.Identity) {
		return zero, ErrSourceMismatch
	}
	if m.Source.OwnerID != q.Source.OwnerID || m.Source.Epoch != q.Source.Epoch || m.WriterFence == nil || *m.WriterFence != q.ExpectedWriterFence || q.Source.OwnerID != q.ExpectedWriterFence.OwnerID || q.Source.Epoch != q.ExpectedWriterFence.Epoch {
		return zero, ErrRunFence
	}
	if m.Revision != q.ExpectedManifestRevision {
		return zero, ErrRunCASConflict
	}
	if m.Revision == math.MaxUint64 {
		return zero, ErrRunCoordinateExhausted
	}
	if m.Source.NextOffset != q.Source.ExpectedOffset || m.Source.LeaderEpoch != q.Source.ExpectedLeaderEpoch {
		return zero, ErrSourceMismatch
	}
	if m.NextSequence != q.ExpectedNextSequence {
		return zero, runInvalid("expected sequence")
	}
	if m.NextSequence > MaxRunSequence {
		return zero, ErrRunCoordinateExhausted
	}
	if err = q.validateCanonical(); err != nil {
		return zero, err
	}
	if err := admitKafkaRun(m, &q.Run); err != nil {
		return zero, err
	}
	c, err := m.Clone()
	if err != nil {
		return zero, err
	}
	c.L0Runs = append([]RunMeta{q.Run.Clone()}, c.L0Runs...)
	c.Source.NextOffset = q.Source.NextOffset
	c.Source.LeaderEpoch = q.Source.LeaderEpoch
	c.NextSequence = q.ResultingNextSequence
	c.Revision++
	result := committedResult(&q, c.Revision)
	receipt := &KafkaRunReceipt{m.Receipts.Count + 1, bytes.Clone(q.PlanPreimage), q.PublicationID, q.PublicationHash, q.AttemptID, q.Run.Clone(), result}
	c.Receipts, err = appendReceipt(ctx, a.storage, m.Receipts, receipt)
	if err != nil {
		return zero, err
	}
	if err = c.BuildIndexes(); err != nil {
		return zero, err
	}
	b, err := EncodeRunCheckpoint(c)
	if err != nil {
		return zero, err
	}
	outcome, casErr := a.storage.CompareAndSwapRunManifest(ctx, m.Revision, b)
	if casErr == nil && outcome == RunCASApplied {
		return result, nil
	}
	// Reload once and reconcile against the newly linearized frontier. Failure
	// to read the ledger is indeterminate even after a definite CAS conflict.
	current, err := a.Snapshot(ctx)
	if err != nil {
		return zero, err
	}
	prior, _, err = current.LookupPublication(ctx, q)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return *prior, nil
	}
	if casErr == nil && outcome == RunCASConflict {
		return zero, ErrRunCASConflict
	}
	return zero, fmt.Errorf("%w: publication CAS: %v", ErrRunIndeterminate, casErr)
}

// Admit the complete resulting live count and encoded checkpoint budget before
// cloning, allocating an added run, or writing any receipt pages. Source/fence
// are already present and canonical here; the resulting frontier is nonempty.
func admitKafkaRun(m *RunManifest, run *RunMeta) error {
	count := len(m.L0Runs)
	size := uint64(62+24+124+len(m.Source.Identity.Cluster)+len(m.Source.Identity.TopicName)+8*len(m.Levels)) + 230 + runWireSize(run)
	check := func(runs []RunMeta) error {
		for i := range runs {
			if runs[i].ID == run.ID || runs[i].ObjectKey == run.ObjectKey {
				return ErrRunIdentityConflict
			}
			n := runWireSize(&runs[i])
			if n > MaxRunCheckpointBytes-size {
				return ErrRunManifestLimit
			}
			size += n
		}
		return nil
	}
	for _, level := range m.Levels {
		count += len(level.Runs)
	}
	if count >= MaxManifestRuns {
		return ErrRunManifestLimit
	}
	if err := check(m.L0Runs); err != nil {
		return err
	}
	for _, level := range m.Levels {
		if err := check(level.Runs); err != nil {
			return err
		}
	}
	return nil
}
