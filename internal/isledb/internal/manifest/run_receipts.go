package manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

const (
	runReceiptKind       = 4
	runReceiptIndexKind  = 5
	MaxReceiptPageBytes  = 1 << 20
	MaxReceiptFanout     = 32
	MaxReceiptDepth      = 16
	receiptIdentityCount = 5
)

// ReceiptKey is an index accelerator only. A hit always loads and compares the
// complete receipt. Even a digest collision cannot produce false success.
type ReceiptKey [33]byte

type ReceiptPageRef struct {
	Hash         [32]byte
	EncodedBytes uint32
	Level        uint8  // 0: exact receipt; 1: index leaf; >1: index branch.
	Count        uint64 // index identities, or 1 for an exact receipt.
	Min, Max     ReceiptKey
}

// ReceiptFrontier is fixed-size regardless of historical receipt count.
// There is deliberately no receipt deletion, expiry, or frontier replacement API.
type ReceiptFrontier struct {
	Count  uint64
	Root   ReceiptPageRef
	Latest ReceiptPageRef
}

type KafkaRunReceipt struct {
	Ordinal         uint64 // append-only receipt position, bound by this immutable page.
	PlanPreimage    []byte
	PublicationID   [32]byte
	PublicationHash [32]byte
	AttemptID       [16]byte
	Run             RunMeta
	Result          KafkaRunCommitted
}

type receiptIndexItem struct {
	Key     ReceiptKey
	Receipt ReceiptPageRef
}
type receiptIndexPage struct {
	Level    uint8
	Items    []receiptIndexItem
	Children []ReceiptPageRef
}

type ReceiptLookupStats struct {
	PageReads, PageBytes uint64
	Depth                uint8
}

// ReceiptPageStorage must make writes durable before success and never replace
// or delete a content-addressed page. Read must enforce the supplied exact size
// before buffering a response. Missing committed pages are corruption, not misses.
type ReceiptPageStorage interface {
	ReadReceiptPage(context.Context, ReceiptPageRef) ([]byte, error)
	WriteReceiptPage(context.Context, ReceiptPageRef, []byte) error
}

func (r ReceiptPageRef) validate() error {
	if r.Hash == [32]byte{} || r.EncodedBytes <= runEnvelopeBytes || r.EncodedBytes > MaxReceiptPageBytes+runEnvelopeBytes || r.Level > MaxReceiptDepth || r.Count == 0 || bytes.Compare(r.Min[:], r.Max[:]) > 0 || r.Min[0] < 1 || r.Max[0] > receiptIdentityCount {
		return runInvalid("receipt page reference")
	}
	if r.Level == 0 && (r.Count != 1 || r.Min != r.Max || r.Min[0] != 1) {
		return runInvalid("receipt leaf reference")
	}
	return nil
}
func (f ReceiptFrontier) validate() error {
	if f.Count == 0 {
		if f.Root != (ReceiptPageRef{}) || f.Latest != (ReceiptPageRef{}) {
			return runInvalid("empty receipt frontier")
		}
		return nil
	}
	if f.Count > MaxRunSequence || f.Root.Level == 0 || f.Root.Count != f.Count*receiptIdentityCount || f.Latest.Level != 0 {
		return runInvalid("receipt frontier count")
	}
	if err := f.Root.validate(); err != nil {
		return err
	}
	return f.Latest.validate()
}
func (w *runEncoder) receiptRef(r ReceiptPageRef) {
	w.data = append(w.data, r.Hash[:]...)
	w.u32(r.EncodedBytes)
	w.u8(r.Level)
	w.u64(r.Count)
	w.data = append(w.data, r.Min[:]...)
	w.data = append(w.data, r.Max[:]...)
}
func (r *runDecoder) receiptRef() ReceiptPageRef {
	var p ReceiptPageRef
	copy(p.Hash[:], r.take(32))
	p.EncodedBytes = r.u32()
	p.Level = r.u8()
	p.Count = r.u64()
	copy(p.Min[:], r.take(33))
	copy(p.Max[:], r.take(33))
	return p
}
func (w *runEncoder) frontier(f ReceiptFrontier) {
	w.u64(f.Count)
	if f.Count != 0 {
		w.receiptRef(f.Root)
		w.receiptRef(f.Latest)
	}
}
func (r *runDecoder) frontier() ReceiptFrontier {
	f := ReceiptFrontier{Count: r.u64()}
	if f.Count != 0 {
		f.Root = r.receiptRef()
		f.Latest = r.receiptRef()
	}
	return f
}
func frontierWireSize(f ReceiptFrontier) uint64 {
	if f.Count == 0 {
		return 8
	}
	return 230
}

func receiptKey(tag byte, b []byte) ReceiptKey {
	var k ReceiptKey
	k[0] = tag
	h := sha256.Sum256(b)
	copy(k[1:], h[:])
	return k
}
func requestReceiptKeys(q *PublishKafkaRunRequest) [receiptIdentityCount]ReceiptKey {
	w := runEncoder{}
	w.sourceIdentity(q.Source.Identity)
	w.u64(q.Source.ExpectedOffset)
	w.u64(q.Source.NextOffset)
	return [receiptIdentityCount]ReceiptKey{receiptKey(1, q.PublicationID[:]), receiptKey(2, q.AttemptID[:]), receiptKey(3, q.Run.ID[:]), receiptKey(4, w.data), receiptKey(5, []byte(q.Run.ObjectKey))}
}

func (r *KafkaRunReceipt) request() (PublishKafkaRunRequest, error) {
	p, err := runcontract.UnmarshalPublication(r.PlanPreimage)
	if err != nil {
		return PublishKafkaRunRequest{}, runInvalid("receipt plan")
	}
	if r.Ordinal == 0 || r.Ordinal > p.NextSequence {
		return PublishKafkaRunRequest{}, runInvalid("receipt ordinal")
	}
	q, err := NewPublishKafkaRunRequest(p, r.Run)
	if err != nil {
		return q, err
	}
	if q.PublicationID != r.PublicationID || q.PublicationHash != r.PublicationHash || q.AttemptID != r.AttemptID {
		return q, runInvalid("receipt identities")
	}
	if r.Result != committedResult(&q, p.ExpectedRevision+1) {
		return q, runInvalid("receipt committed result")
	}
	return q, nil
}

func EncodeKafkaRunReceipt(r *KafkaRunReceipt) ([]byte, error) {
	if r == nil {
		return nil, runInvalid("nil receipt")
	}
	if _, err := r.request(); err != nil {
		return nil, err
	}
	w := runEncoder{}
	w.blob(r.PlanPreimage)
	w.data = append(w.data, r.PublicationID[:]...)
	w.data = append(w.data, r.PublicationHash[:]...)
	w.data = append(w.data, r.AttemptID[:]...)
	w.run(&r.Run)
	w.committed(r.Result)
	w.u64(r.Ordinal)
	if len(w.data) > MaxReceiptPageBytes {
		return nil, ErrRunManifestLimit
	}
	return runEnvelope(w.data, runReceiptKind), nil
}
func DecodeKafkaRunReceipt(data []byte) (*KafkaRunReceipt, error) {
	b, err := openRunEnvelope(data, runReceiptKind, MaxReceiptPageBytes)
	if err != nil {
		return nil, err
	}
	r := runDecoder{data: b}
	c := &KafkaRunReceipt{PlanPreimage: r.blob(runcontract.MaxPublicationBytes)}
	copy(c.PublicationID[:], r.take(32))
	copy(c.PublicationHash[:], r.take(32))
	copy(c.AttemptID[:], r.take(16))
	c.Run = r.run()
	c.Result = r.committed()
	c.Ordinal = r.u64()
	if err = r.done(); err != nil {
		return nil, err
	}
	if _, err = c.request(); err != nil {
		return nil, err
	}
	return c, nil
}

func (p *receiptIndexPage) reference(data []byte) ReceiptPageRef {
	r := ReceiptPageRef{Hash: sha256.Sum256(data), EncodedBytes: uint32(len(data)), Level: p.Level}
	if p.Level == 1 {
		r.Count = uint64(len(p.Items))
		r.Min = p.Items[0].Key
		r.Max = p.Items[len(p.Items)-1].Key
	} else {
		r.Min = p.Children[0].Min
		r.Max = p.Children[len(p.Children)-1].Max
		for _, c := range p.Children {
			r.Count += c.Count
		}
	}
	return r
}
func (p *receiptIndexPage) validate() error {
	if p == nil || p.Level < 1 || p.Level > MaxReceiptDepth {
		return runInvalid("receipt index depth")
	}
	if p.Level == 1 {
		if len(p.Items) == 0 || len(p.Items) > MaxReceiptFanout || len(p.Children) != 0 {
			return runInvalid("receipt index leaf shape")
		}
		for i, v := range p.Items {
			if v.Key[0] < 1 || v.Key[0] > receiptIdentityCount || (i > 0 && bytes.Compare(p.Items[i-1].Key[:], v.Key[:]) >= 0) || v.Receipt.Level != 0 {
				return runInvalid("duplicate/unordered receipt key")
			}
			if err := v.Receipt.validate(); err != nil {
				return err
			}
		}
	} else {
		if len(p.Children) < 2 || len(p.Children) > MaxReceiptFanout || len(p.Items) != 0 {
			return runInvalid("receipt index branch shape")
		}
		var count uint64
		for i, c := range p.Children {
			if err := c.validate(); err != nil {
				return err
			}
			if c.Level != p.Level-1 || c.Count > MaxRunSequence*receiptIdentityCount-count || (i > 0 && bytes.Compare(p.Children[i-1].Max[:], c.Min[:]) >= 0) {
				return runInvalid("receipt child level/range/count")
			}
			count += c.Count
		}
	}
	return nil
}
func encodeReceiptIndex(p *receiptIndexPage) ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	w := runEncoder{}
	w.u8(p.Level)
	if p.Level == 1 {
		w.u32(uint32(len(p.Items)))
		for _, v := range p.Items {
			w.data = append(w.data, v.Key[:]...)
			w.receiptRef(v.Receipt)
		}
	} else {
		w.u32(uint32(len(p.Children)))
		for _, c := range p.Children {
			w.receiptRef(c)
		}
	}
	return runEnvelope(w.data, runReceiptIndexKind), nil
}
func decodeReceiptIndex(data []byte) (*receiptIndexPage, error) {
	b, err := openRunEnvelope(data, runReceiptIndexKind, MaxReceiptPageBytes)
	if err != nil {
		return nil, err
	}
	r := runDecoder{data: b}
	p := &receiptIndexPage{Level: r.u8()}
	if p.Level == 1 {
		n := r.count(MaxReceiptFanout, 144)
		if r.err != nil {
			return nil, r.err
		}
		p.Items = make([]receiptIndexItem, n)
		for i := range p.Items {
			copy(p.Items[i].Key[:], r.take(33))
			p.Items[i].Receipt = r.receiptRef()
		}
	} else {
		n := r.count(MaxReceiptFanout, 111)
		if r.err != nil {
			return nil, r.err
		}
		p.Children = make([]ReceiptPageRef, n)
		for i := range p.Children {
			p.Children[i] = r.receiptRef()
		}
	}
	if err = r.done(); err != nil {
		return nil, err
	}
	if err = p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func readReceiptPage(ctx context.Context, s ReceiptPageStorage, ref ReceiptPageRef, stats *ReceiptLookupStats) ([]byte, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}
	b, err := s.ReadReceiptPage(ctx, ref)
	stats.PageReads++
	stats.PageBytes += uint64(len(b))
	if ref.Level > stats.Depth {
		stats.Depth = ref.Level
	}
	if errors.Is(err, ErrMissingReceiptPage) {
		return nil, runInvalid("committed receipt page missing")
	}
	if err != nil {
		return nil, fmt.Errorf("%w: receipt read: %w", ErrRunIndeterminate, err)
	}
	if len(b) != int(ref.EncodedBytes) || sha256.Sum256(b) != ref.Hash {
		return nil, runInvalid("receipt page integrity")
	}
	return b, nil
}
func loadReceiptIndex(ctx context.Context, s ReceiptPageStorage, ref ReceiptPageRef, stats *ReceiptLookupStats) (*receiptIndexPage, error) {
	b, err := readReceiptPage(ctx, s, ref, stats)
	if err != nil {
		return nil, err
	}
	p, err := decodeReceiptIndex(b)
	if err != nil {
		return nil, err
	}
	if p.reference(b) != ref {
		return nil, runInvalid("receipt index reference projection")
	}
	return p, nil
}
func lookupReceipt(ctx context.Context, s ReceiptPageStorage, root ReceiptPageRef, key ReceiptKey, stats *ReceiptLookupStats) (*KafkaRunReceipt, error) {
	if root == (ReceiptPageRef{}) {
		return nil, nil
	}
	for {
		p, err := loadReceiptIndex(ctx, s, root, stats)
		if err != nil {
			return nil, err
		}
		if p.Level == 1 {
			i := sort.Search(len(p.Items), func(i int) bool { return bytes.Compare(p.Items[i].Key[:], key[:]) >= 0 })
			if i == len(p.Items) || p.Items[i].Key != key {
				return nil, nil
			}
			ref := p.Items[i].Receipt
			b, err := readReceiptPage(ctx, s, ref, stats)
			if err != nil {
				return nil, err
			}
			receipt, err := DecodeKafkaRunReceipt(b)
			if err != nil {
				return nil, err
			}
			q, err := receipt.request()
			if err != nil {
				return nil, err
			}
			keys := requestReceiptKeys(&q)
			if ref.Min != keys[0] || keys[key[0]-1] != key {
				return nil, runInvalid("receipt index identity projection")
			}
			return receipt, nil
		}
		i := sort.Search(len(p.Children), func(i int) bool { return bytes.Compare(p.Children[i].Max[:], key[:]) >= 0 })
		if i == len(p.Children) || bytes.Compare(key[:], p.Children[i].Min[:]) < 0 {
			return nil, nil
		}
		root = p.Children[i]
	}
}

func writeReceiptIndex(ctx context.Context, s ReceiptPageStorage, p *receiptIndexPage) (ReceiptPageRef, error) {
	b, err := encodeReceiptIndex(p)
	if err != nil {
		return ReceiptPageRef{}, err
	}
	ref := p.reference(b)
	if err = s.WriteReceiptPage(ctx, ref, b); err != nil {
		return ReceiptPageRef{}, fmt.Errorf("%w: receipt write: %w", ErrRunIndeterminate, err)
	}
	return ref, nil
}

// insertReceiptIndex copies only a bounded root-to-leaf path. Split propagation
// returns one or two references. No existing page is mutated or pruned.
func insertReceiptIndex(ctx context.Context, s ReceiptPageStorage, root ReceiptPageRef, item receiptIndexItem) ([]ReceiptPageRef, error) {
	var p *receiptIndexPage
	if root == (ReceiptPageRef{}) {
		p = &receiptIndexPage{Level: 1}
	} else {
		var stats ReceiptLookupStats
		var err error
		p, err = loadReceiptIndex(ctx, s, root, &stats)
		if err != nil {
			return nil, err
		}
	}
	if p.Level == 1 {
		i := sort.Search(len(p.Items), func(i int) bool { return bytes.Compare(p.Items[i].Key[:], item.Key[:]) >= 0 })
		if i < len(p.Items) && p.Items[i].Key == item.Key {
			return nil, ErrRunIdentityConflict
		}
		p.Items = append(p.Items, receiptIndexItem{})
		copy(p.Items[i+1:], p.Items[i:])
		p.Items[i] = item
	} else {
		i := sort.Search(len(p.Children), func(i int) bool { return bytes.Compare(p.Children[i].Max[:], item.Key[:]) >= 0 })
		if i == len(p.Children) {
			i--
		}
		refs, err := insertReceiptIndex(ctx, s, p.Children[i], item)
		if err != nil {
			return nil, err
		}
		if len(refs) == 2 {
			p.Children = append(p.Children, ReceiptPageRef{})
			copy(p.Children[i+2:], p.Children[i+1:])
			p.Children[i+1] = refs[1]
		}
		p.Children[i] = refs[0]
	}
	n := len(p.Items)
	if p.Level > 1 {
		n = len(p.Children)
	}
	pages := []*receiptIndexPage{p}
	if n > MaxReceiptFanout {
		q := &receiptIndexPage{Level: p.Level}
		mid := n / 2
		if p.Level == 1 {
			q.Items = p.Items[mid:]
			p.Items = p.Items[:mid]
		} else {
			q.Children = p.Children[mid:]
			p.Children = p.Children[:mid]
		}
		pages = append(pages, q)
	}
	refs := make([]ReceiptPageRef, len(pages))
	for i, page := range pages {
		var err error
		refs[i], err = writeReceiptIndex(ctx, s, page)
		if err != nil {
			return nil, err
		}
	}
	return refs, nil
}
func appendReceipt(ctx context.Context, s ReceiptPageStorage, f ReceiptFrontier, c *KafkaRunReceipt) (ReceiptFrontier, error) {
	if err := f.validate(); err != nil {
		return f, err
	}
	if f.Count == MaxRunSequence {
		return f, ErrRunCoordinateExhausted
	}
	if c.Ordinal != f.Count+1 {
		return f, runInvalid("receipt append ordinal")
	}
	b, err := EncodeKafkaRunReceipt(c)
	if err != nil {
		return f, err
	}
	q, err := c.request()
	if err != nil {
		return f, err
	}
	keys := requestReceiptKeys(&q)
	leaf := ReceiptPageRef{Hash: sha256.Sum256(b), EncodedBytes: uint32(len(b)), Count: 1, Min: keys[0], Max: keys[0]}
	if err = s.WriteReceiptPage(ctx, leaf, b); err != nil {
		return f, fmt.Errorf("%w: receipt write: %w", ErrRunIndeterminate, err)
	}
	root := f.Root
	for _, key := range keys {
		refs, err := insertReceiptIndex(ctx, s, root, receiptIndexItem{key, leaf})
		if err != nil {
			return f, err
		}
		if len(refs) == 1 {
			root = refs[0]
		} else {
			if refs[0].Level == MaxReceiptDepth {
				return f, ErrRunManifestLimit
			}
			root, err = writeReceiptIndex(ctx, s, &receiptIndexPage{Level: refs[0].Level + 1, Children: refs})
			if err != nil {
				return f, err
			}
		}
	}
	return ReceiptFrontier{Count: f.Count + 1, Root: root, Latest: leaf}, nil
}
