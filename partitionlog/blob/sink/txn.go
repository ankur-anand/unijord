package sink

import (
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	uploadstream "github.com/ankur-anand/unijord/partitionlog/blob/sink/stream"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

type segmentTxn struct {
	upload uploadstream.Upload
}

var _ segwriter.Txn = (*segmentTxn)(nil)

func newSegmentTxn(upload uploadstream.Upload) *segmentTxn {
	return &segmentTxn{upload: upload}
}

func (t *segmentTxn) Write(ctx context.Context, bytes []byte) error {
	return mapStreamSegmentError(t.upload.Write(ctx, bytes))
}

func (t *segmentTxn) Commit(ctx context.Context) (segwriter.CommittedObject, error) {
	attrs, err := t.upload.Commit(ctx)
	if err != nil {
		return segwriter.CommittedObject{}, mapStreamSegmentError(err)
	}
	return segwriter.CommittedObject{
		URI:       attrs.Key,
		SizeBytes: attrs.SizeBytes,
		Token:     attrs.Token,
	}, nil
}

func (t *segmentTxn) Abort(ctx context.Context) error {
	return mapStreamSegmentError(t.upload.Abort(ctx))
}

func mapStreamSegmentError(err error) error {
	switch {
	case errors.Is(err, uploadstream.ErrCommitIndeterminate), errors.Is(err, multipart.ErrCommitIndeterminate):
		mapped := fmt.Errorf("%w: %w", segwriter.ErrTxnCommitIndeterminate, err)
		if errors.Is(err, uploadstream.ErrBackendContract) {
			return fmt.Errorf("%w: %w", segwriter.ErrSinkContract, mapped)
		}
		return mapped
	case errors.Is(err, uploadstream.ErrAborted):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnAborted, err)
	case errors.Is(err, uploadstream.ErrClosed):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnCompleted, err)
	case errors.Is(err, uploadstream.ErrInvalidOptions), errors.Is(err, uploadstream.ErrLimitExceeded):
		return fmt.Errorf("%w: %w", segwriter.ErrInvalidOptions, err)
	case errors.Is(err, uploadstream.ErrBackendContract):
		return fmt.Errorf("%w: %w", segwriter.ErrSinkContract, err)
	default:
		return mapMultipartSegmentError(err)
	}
}

func mapMultipartSegmentError(err error) error {
	switch {
	case errors.Is(err, multipart.ErrCommitIndeterminate):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnCommitIndeterminate, err)
	case errors.Is(err, multipart.ErrCleaned):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnAborted, err)
	case errors.Is(err, multipart.ErrCommitted):
		return fmt.Errorf("%w: %w", segwriter.ErrTxnCompleted, err)
	case errors.Is(err, multipart.ErrInvalidStore):
		return fmt.Errorf("%w: %w", segwriter.ErrInvalidOptions, err)
	default:
		return err
	}
}
