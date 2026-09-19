package manifest

import (
	"bytes"
	"context"
	"testing"

	"github.com/ankur-anand/isledb/internal/runcontract"
)

func FuzzE08SourceState(f *testing.F) {
	w := runEncoder{}
	w.source(&KafkaSourceState{e08Identity(), [16]byte{3}, 1, 100, -1})
	f.Add(w.data)
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1024 {
			return
		}
		r := runDecoder{data: b}
		s := r.source()
		if r.done() != nil || s.validate() != nil {
			return
		}
		w := runEncoder{}
		w.source(s)
		if !bytes.Equal(w.data, b) {
			t.Fatal("noncanonical source state")
		}
		if !s.Equal(s.Clone()) {
			t.Fatal("source clone")
		}
	})
}

func FuzzE08ReceiptDecode(f *testing.F) {
	a, s, snap := e08Active(f)
	q := e08Request(f, snap, 1)
	e08Publish(f, a, q)
	snap = e08Snapshot(f, a)
	for _, ref := range []ReceiptPageRef{snap.ReceiptFrontier().Latest, snap.ReceiptFrontier().Root} {
		data := s.pages[ref.Hash]
		f.Add(data[6], data[48:])
	}
	f.Fuzz(func(t *testing.T, kind byte, b []byte) {
		if len(b) > 64<<10 {
			return
		}
		data := runEnvelope(b, kind)
		var encoded []byte
		var err error
		switch kind {
		case runReceiptKind:
			r, e := DecodeKafkaRunReceipt(data)
			if e != nil {
				return
			}
			encoded, err = EncodeKafkaRunReceipt(r)
		case runReceiptIndexKind:
			p, e := decodeReceiptIndex(data)
			if e != nil {
				return
			}
			encoded, err = encodeReceiptIndex(p)
		default:
			return
		}
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatal("noncanonical receipt/page", err)
		}
	})
}

func FuzzE08PublicationReplay(f *testing.F) {
	_, _, snap := e08Active(f)
	q := e08Request(f, snap, 1)
	f.Add(q.PlanPreimage)
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > runcontract.MaxPublicationBytes {
			return
		}
		p, err := runcontract.UnmarshalPublication(b)
		if err != nil {
			return
		}
		a, s, snap := e08Active(t)
		base := e08Request(t, snap, 1)
		_, base.Run.PublicationHash, _ = runcontract.PublicationHashes(p)
		q, err := NewPublishKafkaRunRequest(p, base.Run)
		if err != nil {
			return
		}
		before := bytes.Clone(s.state)
		result, err := a.PublishKafkaRun(context.Background(), q)
		if err != nil {
			if !bytes.Equal(before, s.state) {
				t.Fatal("failed publication mutated state")
			}
			return
		}
		committed := bytes.Clone(s.state)
		again, err := a.PublishKafkaRun(context.Background(), q)
		if err != nil || result != again || !bytes.Equal(committed, s.state) {
			t.Fatal("non-idempotent replay", err)
		}
	})
}
