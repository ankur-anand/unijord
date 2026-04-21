package segmentio

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// SegmentSink starts a staged write transaction for one validated segment.
type SegmentSink interface {
	Begin(ctx context.Context, plan AssemblyPlan) (SegmentTxn, error)
}

// SegmentTxn is a transactional segment upload session.
//
// UploadPart must fully consume part.Body before returning success. The caller
// retains ownership of the backing bytes and may immediately reuse that memory
// after UploadPart returns.
type SegmentTxn interface {
	UploadPart(ctx context.Context, part UploadPart) (PartReceipt, error)
	Complete(ctx context.Context, parts []PartReceipt) (CommittedObject, error)
	Abort(ctx context.Context) error
}

// UploadPart describes one emitted multipart transport chunk. Body is only
// valid for the duration of SegmentTxn.UploadPart.
type UploadPart struct {
	PartNumber int32
	SizeBytes  int64
	Body       io.Reader
}

// PartReceipt is returned by a sink after one part upload succeeds.
type PartReceipt struct {
	PartNumber int32
	ETag       string
}

// CommittedObject describes the final durable object produced by a sink.
type CommittedObject struct {
	ObjectID  string
	SizeBytes uint64
	ETag      string
}

type segmentEmitter struct {
	ctx          context.Context
	txn          SegmentTxn
	partSize     int
	buf          bytes.Buffer
	scratch      []byte
	partNumber   int32
	receipts     []PartReceipt
	totalWritten uint64
}

func newSegmentEmitter(ctx context.Context, txn SegmentTxn, partSize int) *segmentEmitter {
	if partSize <= 0 {
		partSize = DefaultUploadPartSize
	}
	return &segmentEmitter{
		ctx:      ctx,
		txn:      txn,
		partSize: partSize,
		scratch:  make([]byte, 32<<10),
	}
}

func (e *segmentEmitter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		remaining := e.partSize - e.buf.Len()
		if remaining == 0 {
			if err := e.flushPart(); err != nil {
				return written, err
			}
			remaining = e.partSize
		}

		if remaining > len(p) {
			remaining = len(p)
		}
		n, err := e.buf.Write(p[:remaining])
		written += n
		e.totalWritten += uint64(n)
		if err != nil {
			return written, err
		}
		p = p[n:]

		if e.buf.Len() == e.partSize {
			if err := e.flushPart(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

func (e *segmentEmitter) writeReader(r io.Reader) error {
	_, err := io.CopyBuffer(e, r, e.scratch)
	return err
}

func (e *segmentEmitter) Complete() (CommittedObject, error) {
	if e.buf.Len() > 0 {
		if err := e.flushPart(); err != nil {
			return CommittedObject{}, err
		}
	}
	return e.txn.Complete(e.ctx, e.receipts)
}

func (e *segmentEmitter) flushPart() error {
	if e.buf.Len() == 0 {
		return nil
	}

	e.partNumber++
	body := bytes.NewReader(e.buf.Bytes())

	receipt, err := e.txn.UploadPart(e.ctx, UploadPart{
		PartNumber: e.partNumber,
		SizeBytes:  int64(body.Len()),
		Body:       body,
	})
	if err != nil {
		return err
	}
	if body.Len() != 0 {
		return fmt.Errorf("%w: sink did not fully consume upload part %d", ErrInvalidSegment, e.partNumber)
	}

	e.buf.Reset()
	e.receipts = append(e.receipts, receipt)
	return nil
}

func emitAssembledSegment(emitter *segmentEmitter, assembled assembledSegment) error {
	if _, err := emitter.Write(assembled.Header.marshalBinary()); err != nil {
		return fmt.Errorf("emit header: %w", err)
	}

	for i, block := range assembled.Blocks {
		reader, err := block.Descriptor.Payload.Open()
		if err != nil {
			return fmt.Errorf("open block payload %d: %w", i, err)
		}
		if err := emitter.writeReader(reader); err != nil {
			_ = reader.Close()
			return fmt.Errorf("emit block payload %d: %w", i, err)
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("close block payload %d: %w", i, err)
		}
	}

	indexBuf := make([]byte, BlockIndexEntrySize)
	for i, block := range assembled.Blocks {
		block.Entry.marshalTo(indexBuf)
		if _, err := emitter.Write(indexBuf); err != nil {
			return fmt.Errorf("emit block index entry %d: %w", i, err)
		}
	}

	if _, err := emitter.Write(assembled.Footer.marshalBinary()); err != nil {
		return fmt.Errorf("emit footer: %w", err)
	}

	if emitter.totalWritten != assembled.Plan.TotalSize {
		return fmt.Errorf("%w: emitted size=%d want=%d", ErrInvalidSegment, emitter.totalWritten, assembled.Plan.TotalSize)
	}

	return nil
}

type bufferSegmentSink struct {
	bytes []byte
}

func newBufferSegmentSink() *bufferSegmentSink {
	return &bufferSegmentSink{}
}

func (s *bufferSegmentSink) Begin(_ context.Context, plan AssemblyPlan) (SegmentTxn, error) {
	return &bufferSegmentTxn{
		sink:  s,
		plan:  plan,
		parts: make(map[int32][]byte),
	}, nil
}

func (s *bufferSegmentSink) Bytes() []byte {
	return append([]byte(nil), s.bytes...)
}

type bufferSegmentTxn struct {
	sink    *bufferSegmentSink
	plan    AssemblyPlan
	parts   map[int32][]byte
	aborted bool
}

func (t *bufferSegmentTxn) UploadPart(_ context.Context, part UploadPart) (PartReceipt, error) {
	if t.aborted {
		return PartReceipt{}, ErrWriterAborted
	}

	limited := io.LimitReader(part.Body, part.SizeBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return PartReceipt{}, err
	}
	if int64(len(data)) != part.SizeBytes {
		return PartReceipt{}, fmt.Errorf(
			"%w: uploaded part size=%d want=%d",
			ErrInvalidSegment,
			len(data),
			part.SizeBytes,
		)
	}

	t.parts[part.PartNumber] = data
	return PartReceipt{
		PartNumber: part.PartNumber,
		ETag:       fmt.Sprintf("%08x", crc32Checksum(data)),
	}, nil
}

func (t *bufferSegmentTxn) Complete(_ context.Context, parts []PartReceipt) (CommittedObject, error) {
	if t.aborted {
		return CommittedObject{}, ErrWriterAborted
	}

	buf := bytes.NewBuffer(make([]byte, 0, plannedBufferCap(int64(t.plan.TotalSize))))
	for i, receipt := range parts {
		expectedPartNumber := int32(i + 1)
		if receipt.PartNumber != expectedPartNumber {
			return CommittedObject{}, fmt.Errorf(
				"%w: receipt part_number=%d want=%d",
				ErrInvalidSegment,
				receipt.PartNumber,
				expectedPartNumber,
			)
		}
		payload, ok := t.parts[receipt.PartNumber]
		if !ok {
			return CommittedObject{}, fmt.Errorf("%w: missing uploaded part %d", ErrInvalidSegment, receipt.PartNumber)
		}
		buf.Write(payload)
	}

	if uint64(buf.Len()) != t.plan.TotalSize {
		return CommittedObject{}, fmt.Errorf(
			"%w: committed size=%d want=%d",
			ErrInvalidSegment,
			buf.Len(),
			t.plan.TotalSize,
		)
	}

	t.sink.bytes = buf.Bytes()
	return CommittedObject{
		ObjectID:  "buffer://segment",
		SizeBytes: uint64(len(t.sink.bytes)),
		ETag:      fmt.Sprintf("%08x", crc32Checksum(t.sink.bytes)),
	}, nil
}

func (t *bufferSegmentTxn) Abort(_ context.Context) error {
	t.aborted = true
	t.parts = nil
	return nil
}

func plannedBufferCap(sizeBytes int64) int {
	const maxInt = int64(^uint(0) >> 1)
	if sizeBytes <= 0 || sizeBytes > maxInt {
		return HeaderSize + FooterSize
	}
	return int(sizeBytes)
}
