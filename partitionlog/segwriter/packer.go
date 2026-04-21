package segwriter

import (
	"context"
	"fmt"
	"hash"
	"hash/crc32"
	"sort"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/cespare/xxhash/v2"
)

type packerOptions struct {
	PartSize          int
	UploadParallelism int
	UploadQueueSize   int
	HashAlgo          segformat.HashAlgo
	UploadLimiter     UploadLimiter
}

type packer struct {
	txn Txn

	partSize int
	limiter  UploadLimiter

	ctx    context.Context
	cancel context.CancelFunc

	parts   chan Part
	results chan uploadResult
	wg      sync.WaitGroup

	buf        []byte
	offset     uint64
	nextPart   int
	enqueued   int
	collected  int
	receipts   []PartReceipt
	firstErr   error
	bodySealed bool
	completed  bool
	aborted    bool
	partsClose bool

	hasher digest64
}

type uploadResult struct {
	receipt PartReceipt
	err     error
}

type digest64 interface {
	hash.Hash
	Sum64() uint64
}

type crc32Digest struct {
	hash.Hash32
}

func (d crc32Digest) Sum64() uint64 {
	return uint64(d.Sum32())
}

func newPacker(ctx context.Context, txn Txn, opts packerOptions) (*packer, error) {
	if txn == nil {
		return nil, fmt.Errorf("%w: txn is nil", ErrInvalidOptions)
	}
	if opts.PartSize <= 0 {
		return nil, fmt.Errorf("%w: part_size must be positive", ErrInvalidOptions)
	}
	if opts.UploadParallelism <= 0 {
		opts.UploadParallelism = 1
	}
	if opts.UploadQueueSize <= 0 {
		opts.UploadQueueSize = opts.UploadParallelism
	}
	if err := opts.HashAlgo.Validate(); err != nil {
		return nil, err
	}
	hasher, err := newDigest(opts.HashAlgo)
	if err != nil {
		return nil, err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	p := &packer{
		txn:      txn,
		partSize: opts.PartSize,
		limiter:  opts.UploadLimiter,
		ctx:      workerCtx,
		cancel:   cancel,
		parts:    make(chan Part, opts.UploadQueueSize),
		results:  make(chan uploadResult, opts.UploadParallelism),
		hasher:   hasher,
	}
	for i := 0; i < opts.UploadParallelism; i++ {
		p.wg.Add(1)
		go p.uploadWorker()
	}
	return p, nil
}

func (p *packer) Offset() uint64 {
	return p.offset
}

func (p *packer) WriteBody(ctx context.Context, b []byte) error {
	if p.bodySealed {
		return ErrBodySealed
	}
	return p.write(ctx, b, true)
}

func (p *packer) BodyHash() uint64 {
	p.bodySealed = true
	return p.hasher.Sum64()
}

func (p *packer) WriteFinal(ctx context.Context, b []byte) error {
	if !p.bodySealed {
		return ErrBodyNotSealed
	}
	return p.write(ctx, b, false)
}

func (p *packer) Complete(ctx context.Context) (CommittedObject, error) {
	if p.completed {
		return CommittedObject{}, ErrPackerClosed
	}
	if p.aborted {
		return CommittedObject{}, ErrPackerAborted
	}
	if !p.bodySealed {
		return CommittedObject{}, ErrBodyNotSealed
	}
	if p.offset == 0 {
		return CommittedObject{}, ErrEmptyObject
	}
	if err := p.pollResults(); err != nil {
		p.stopWorkers()
		return CommittedObject{}, err
	}
	if err := p.flushPart(ctx); err != nil {
		p.stopWorkers()
		return CommittedObject{}, err
	}
	p.closeParts()
	if err := p.collectUntilDone(ctx); err != nil {
		p.stopWorkers()
		return CommittedObject{}, err
	}
	p.wg.Wait()
	if p.firstErr != nil {
		return CommittedObject{}, p.firstErr
	}
	sort.Slice(p.receipts, func(i, j int) bool {
		return p.receipts[i].Number < p.receipts[j].Number
	})
	obj, err := p.txn.Complete(ctx, p.receipts)
	if err != nil {
		return CommittedObject{}, err
	}
	p.completed = true
	p.cancel()
	return obj, nil
}

func (p *packer) Abort(ctx context.Context) error {
	if p.completed || p.aborted {
		return nil
	}
	p.aborted = true
	p.stopWorkers()
	return p.txn.Abort(ctx)
}

func (p *packer) Err() error {
	_ = p.pollResults()
	return p.firstErr
}

func (p *packer) write(ctx context.Context, b []byte, hashBody bool) error {
	if p.completed {
		return ErrPackerClosed
	}
	if p.aborted {
		return ErrPackerAborted
	}
	if err := p.pollResults(); err != nil {
		return err
	}
	for len(b) > 0 {
		if p.buf == nil {
			p.buf = make([]byte, 0, initialPartCapacity(p.partSize, len(b)))
		}
		n := p.partSize - len(p.buf)
		if n > len(b) {
			n = len(b)
		}
		chunk := b[:n]
		p.buf = append(p.buf, chunk...)
		if hashBody {
			if _, err := p.hasher.Write(chunk); err != nil {
				return err
			}
		}
		p.offset += uint64(n)
		b = b[n:]
		if len(p.buf) == p.partSize {
			if err := p.flushPart(ctx); err != nil {
				return err
			}
		}
	}
	return p.pollResults()
}

func (p *packer) flushPart(ctx context.Context) error {
	if len(p.buf) == 0 {
		return p.pollResults()
	}
	if err := p.pollResults(); err != nil {
		return err
	}
	p.nextPart++
	part := Part{
		Number: p.nextPart,
		Bytes:  p.buf,
	}
	p.buf = nil
	select {
	case p.parts <- part:
		p.enqueued++
		return p.pollResults()
	case <-ctx.Done():
		p.setFirstErr(ctx.Err())
		return ctx.Err()
	case <-p.ctx.Done():
		err := p.ctx.Err()
		p.setFirstErr(err)
		return err
	}
}

func initialPartCapacity(partSize int, pendingBytes int) int {
	if pendingBytes >= partSize {
		return partSize
	}
	capacity := 64 << 10
	if pendingBytes > capacity {
		capacity = pendingBytes
	}
	if capacity > partSize {
		capacity = partSize
	}
	return capacity
}

func (p *packer) uploadWorker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case part, ok := <-p.parts:
			if !ok {
				return
			}
			receipt, err := p.uploadPart(part)
			result := uploadResult{receipt: receipt, err: err}
			select {
			case p.results <- result:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *packer) uploadPart(part Part) (PartReceipt, error) {
	if p.limiter != nil {
		if err := p.limiter.Acquire(p.ctx); err != nil {
			return PartReceipt{}, err
		}
		defer p.limiter.Release()
	}
	return p.txn.UploadPart(p.ctx, part)
}

func (p *packer) pollResults() error {
	for {
		select {
		case result := <-p.results:
			p.recordResult(result)
		default:
			return p.firstErr
		}
	}
}

func (p *packer) collectUntilDone(ctx context.Context) error {
	for p.collected < p.enqueued && p.firstErr == nil {
		select {
		case result := <-p.results:
			p.recordResult(result)
		case <-ctx.Done():
			p.setFirstErr(ctx.Err())
		}
	}
	if p.firstErr != nil {
		return p.firstErr
	}
	return nil
}

func (p *packer) recordResult(result uploadResult) {
	p.collected++
	if result.err != nil {
		p.setFirstErr(result.err)
		return
	}
	p.receipts = append(p.receipts, result.receipt)
}

func (p *packer) setFirstErr(err error) {
	if err == nil {
		return
	}
	if p.firstErr == nil {
		p.firstErr = err
		p.cancel()
	}
}

func (p *packer) stopWorkers() {
	p.cancel()
	p.closeParts()
	p.wg.Wait()
}

func (p *packer) closeParts() {
	if p.partsClose {
		return
	}
	close(p.parts)
	p.partsClose = true
}

func newDigest(algo segformat.HashAlgo) (digest64, error) {
	switch algo {
	case segformat.HashCRC32C:
		return crc32Digest{Hash32: crc32.New(crc32.MakeTable(crc32.Castagnoli))}, nil
	case segformat.HashXXH64:
		return xxhash.New(), nil
	default:
		return nil, fmt.Errorf("%w: %d", segformat.ErrUnsupportedHashAlgo, uint16(algo))
	}
}
