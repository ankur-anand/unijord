package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

type segmentTxn struct {
	mu       sync.Mutex
	upload   multipart.Upload
	receipts map[int]multipart.Receipt
}

var _ segwriter.Txn = (*segmentTxn)(nil)

func newSegmentTxn(upload multipart.Upload) *segmentTxn {
	return &segmentTxn{
		upload:   upload,
		receipts: make(map[int]multipart.Receipt),
	}
}

func (t *segmentTxn) UploadPart(ctx context.Context, part segwriter.Part) (segwriter.PartReceipt, error) {
	receipt, err := t.upload.UploadPart(ctx, multipart.Part{Number: part.Number, Bytes: part.Bytes})
	if err != nil {
		return segwriter.PartReceipt{}, mapMultipartSegmentError(err)
	}
	t.mu.Lock()
	t.receipts[receipt.Number] = receipt
	t.mu.Unlock()
	return segwriter.PartReceipt{
		Number: receipt.Number,
		Token:  receipt.Token,
	}, nil
}

func (t *segmentTxn) Complete(ctx context.Context, receipts []segwriter.PartReceipt) (segwriter.CommittedObject, error) {
	multipartReceipts, err := t.receiptsForComplete(receipts)
	if err != nil {
		return segwriter.CommittedObject{}, err
	}
	attrs, err := t.upload.Complete(ctx, multipartReceipts)
	if err != nil {
		return segwriter.CommittedObject{}, mapMultipartSegmentError(err)
	}
	return segwriter.CommittedObject{
		URI:       attrs.Key,
		SizeBytes: attrs.SizeBytes,
		Token:     attrs.Token,
	}, nil
}

func (t *segmentTxn) Abort(ctx context.Context) error {
	return mapMultipartSegmentError(t.upload.Abort(ctx))
}

func (t *segmentTxn) receiptsForComplete(receipts []segwriter.PartReceipt) ([]multipart.Receipt, error) {
	multipartReceipts := make([]multipart.Receipt, 0, len(receipts))
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, receipt := range receipts {
		full := t.receipts[receipt.Number]
		if full.Number == 0 {
			return nil, fmt.Errorf("%w: missing receipt for part %d", segwriter.ErrInvalidOptions, receipt.Number)
		}
		if receipt.Token != "" && full.Token != receipt.Token {
			return nil, fmt.Errorf("%w: receipt token mismatch for part %d", segwriter.ErrInvalidOptions, receipt.Number)
		}
		multipartReceipts = append(multipartReceipts, full)
	}
	return multipartReceipts, nil
}

func mapMultipartSegmentError(err error) error {
	switch {
	case errors.Is(err, multipart.ErrAborted):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnAborted, err)
	case errors.Is(err, multipart.ErrCompleted):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnCompleted, err)
	case errors.Is(err, multipart.ErrInvalidStore):
		return fmt.Errorf("%w: %w", segwriter.ErrInvalidOptions, err)
	default:
		return err
	}
}
