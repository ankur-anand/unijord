package runingest

import (
	"github.com/ankur-anand/isledb/internal/runcontract"
	"github.com/ankur-anand/isledb/internal/runfile"
)

type headIterator struct {
	input  *positionedInput
	index  int
	closed bool
	err    error
	entry  runfile.Entry
	key    [runcontract.MaxKeyBytes - 8]byte
	value  [runcontract.HeadBytes]byte
}

var _ runfile.EntryIterator = (*headIterator)(nil)
var _ runfile.TimelineCatalog = (*positionedInput)(nil)

func (it *headIterator) Next() bool {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	it.entry = runfile.Entry{}
	if it.closed || it.input.closed || p.disposed {
		it.err = ErrPositionClosed
		return false
	}
	if it.err != nil || it.index == len(p.headOrder) {
		return false
	}
	id := p.headOrder[it.index]
	timeline := p.slot.catalog.bytes(timelineID(id))
	h := p.heads[id]
	key, err := runcontract.EncodeKey(it.key[:], runcontract.Key{Kind: runcontract.Heads,
		Namespace: p.options.Namespace, Shard: p.options.Shard, Timeline: timeline})
	if err != nil {
		it.err = err
		return false
	}
	value, err := runcontract.EncodeHead(it.value[:], h.head)
	if err != nil {
		it.err = err
		return false
	}
	it.entry = runfile.Entry{TimelineID: runfile.TimelineID(id), Timeline: timeline,
		Key: key, Value: value, Seq: p.options.NextSequence + uint64(h.lastRecord)}
	it.index++
	return true
}

func (it *headIterator) Entry() runfile.Entry {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	return it.entry
}
func (it *headIterator) Err() error {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	return it.err
}
func (it *headIterator) Close() error {
	p := it.input.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	it.closed = true
	it.entry = runfile.Entry{}
	return nil
}
