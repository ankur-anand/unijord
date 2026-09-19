package runingest

import (
	"github.com/ankur-anand/isledb/internal/runcontract"
	"github.com/ankur-anand/isledb/internal/runfile"
)

type eventIterator struct {
	input  *positionedInput
	index  int
	closed bool
	err    error
	entry  runfile.Entry
	key    [runcontract.MaxKeyBytes]byte
}

var _ runfile.EntryIterator = (*eventIterator)(nil)

func (it *eventIterator) Next() bool {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	it.entry = runfile.Entry{}
	if it.closed || it.input.closed || p.disposed {
		it.err = ErrPositionClosed
		return false
	}
	if it.err != nil || it.index == len(p.eventOrder) {
		return false
	}
	index := p.eventOrder[it.index]
	r := p.slot.records[index]
	timeline := p.slot.catalog.bytes(r.Timeline)
	key, err := runcontract.EncodeKey(it.key[:], runcontract.Key{Kind: runcontract.Events,
		Namespace: p.options.Namespace, Shard: p.options.Shard, Timeline: timeline, LSN: p.lsns[index]})
	if err != nil {
		it.err = err
		return false
	}
	// The slot lease pins every payload block. E05 validated these references
	// before publication; no callback lease is escaped and no bytes are copied.
	ref := r.Value
	blocks := p.slot.payload.normal
	if ref.kind == arenaLarge {
		blocks = p.slot.payload.large
	}
	end := ref.offset + ref.length
	value := blocks[ref.index].data[ref.offset:end:end]
	it.entry = runfile.Entry{TimelineID: runfile.TimelineID(r.Timeline), Timeline: timeline,
		Key: key, Value: value, Seq: p.options.NextSequence + uint64(index)}
	it.index++
	return true
}

func (it *eventIterator) Entry() runfile.Entry {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	return it.entry
}
func (it *eventIterator) Err() error {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	return it.err
}
func (it *eventIterator) Close() error {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	it.closed = true
	it.entry = runfile.Entry{}
	return nil
}
