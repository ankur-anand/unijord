package segwriter

import (
	"context"
	"errors"
	"fmt"
	"hash"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/cespare/xxhash/v2"
)

// packer preserves segment byte order, tracks offsets, and computes the body
// hash. Multipart splitting and upload concurrency belong to the sink.
type packer struct {
	txn Txn

	offset     uint64
	firstErr   error
	bodySealed bool
	completed  bool
	aborted    bool
	abortErr   error

	hasher digest64
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

func newPacker(txn Txn, hashAlgo segformat.HashAlgo) (*packer, error) {
	if txn == nil {
		return nil, fmt.Errorf("%w: txn is nil", ErrInvalidOptions)
	}
	if err := hashAlgo.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	hasher, err := newDigest(hashAlgo)
	if err != nil {
		return nil, err
	}
	return &packer{txn: txn, hasher: hasher}, nil
}

func (p *packer) Offset() uint64 {
	return p.offset
}

func (p *packer) WriteBody(ctx context.Context, bytes []byte) error {
	if p.bodySealed {
		return ErrBodySealed
	}
	if err := p.write(ctx, bytes); err != nil {
		return err
	}
	if _, err := p.hasher.Write(bytes); err != nil {
		p.setFirstErr(err)
		return err
	}
	return nil
}

func (p *packer) BodyHash() uint64 {
	p.bodySealed = true
	return p.hasher.Sum64()
}

func (p *packer) WriteFinal(ctx context.Context, bytes []byte) error {
	if !p.bodySealed {
		return ErrBodyNotSealed
	}
	return p.write(ctx, bytes)
}

func (p *packer) Complete(ctx context.Context) (CommittedObject, error) {
	if p.completed {
		return CommittedObject{}, ErrPackerClosed
	}
	if p.aborted {
		return CommittedObject{}, ErrPackerAborted
	}
	if p.firstErr != nil {
		return CommittedObject{}, p.firstErr
	}
	if !p.bodySealed {
		return CommittedObject{}, ErrBodyNotSealed
	}
	if p.offset == 0 {
		return CommittedObject{}, ErrEmptyObject
	}
	if err := ctx.Err(); err != nil {
		return CommittedObject{}, err
	}

	obj, err := p.txn.Commit(ctx)
	if err != nil {
		return CommittedObject{}, err
	}
	if obj.URI == "" {
		err := fmt.Errorf("%w: commit returned an empty object URI", ErrSinkContract)
		p.setFirstErr(err)
		return CommittedObject{}, errors.Join(err, p.abortAfterFailure())
	}
	if obj.SizeBytes != p.offset {
		err := fmt.Errorf("%w: commit size=%d accepted_bytes=%d", ErrSinkContract, obj.SizeBytes, p.offset)
		p.setFirstErr(err)
		return CommittedObject{}, errors.Join(err, p.abortAfterFailure())
	}
	p.completed = true
	return obj, nil
}

func (p *packer) Abort(ctx context.Context) error {
	if p.completed {
		return nil
	}
	if p.aborted && p.abortErr == nil {
		return nil
	}
	p.aborted = true
	p.abortErr = p.txn.Abort(ctx)
	return p.abortErr
}

func (p *packer) write(ctx context.Context, bytes []byte) error {
	if p.completed {
		return ErrPackerClosed
	}
	if p.firstErr != nil {
		return p.firstErr
	}
	if p.aborted {
		return ErrPackerAborted
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(bytes) == 0 {
		return nil
	}
	if err := p.txn.Write(ctx, bytes); err != nil {
		p.setFirstErr(err)
		return err
	}
	p.offset += uint64(len(bytes))
	return nil
}

func (p *packer) setFirstErr(err error) {
	if err != nil && p.firstErr == nil {
		p.firstErr = err
	}
}

func (p *packer) abortAfterFailure() error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return p.Abort(ctx)
}

func newDigest(algo segformat.HashAlgo) (digest64, error) {
	switch algo {
	case segformat.HashCRC32C:
		return crc32Digest{Hash32: segformat.NewCRC32C()}, nil
	case segformat.HashXXH64:
		return xxhash.New(), nil
	default:
		return nil, fmt.Errorf("%w: %d", segformat.ErrUnsupportedHashAlgo, uint16(algo))
	}
}
