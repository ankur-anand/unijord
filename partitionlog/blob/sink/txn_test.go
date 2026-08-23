package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/blob/sink/multipart"
	uploadstream "github.com/ankur-anand/unijord/partitionlog/blob/sink/stream"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

func TestSegmentTxnMapsIndeterminateCommit(t *testing.T) {
	t.Parallel()

	cause := errors.New("commit response lost")
	uploadErr := errors.Join(uploadstream.ErrCommitIndeterminate, multipart.ErrCommitIndeterminate, cause)
	txn := newSegmentTxn(errorUpload{commitErr: uploadErr})

	_, err := txn.Commit(context.Background())
	for _, target := range []error{
		segwriter.ErrTxnCommitIndeterminate,
		uploadstream.ErrCommitIndeterminate,
		multipart.ErrCommitIndeterminate,
		cause,
	} {
		if !errors.Is(err, target) {
			t.Fatalf("Commit() error = %v, want errors.Is(%v)", err, target)
		}
	}
}

func TestSegmentTxnPreservesBackendContractOnIndeterminateCommit(t *testing.T) {
	t.Parallel()

	uploadErr := errors.Join(uploadstream.ErrCommitIndeterminate, uploadstream.ErrBackendContract)
	txn := newSegmentTxn(errorUpload{commitErr: uploadErr})

	_, err := txn.Commit(context.Background())
	for _, target := range []error{segwriter.ErrTxnCommitIndeterminate, segwriter.ErrSinkContract} {
		if !errors.Is(err, target) {
			t.Fatalf("Commit() error = %v, want errors.Is(%v)", err, target)
		}
	}
}

type errorUpload struct {
	commitErr error
}

func (errorUpload) Write(context.Context, []byte) error { return nil }

func (u errorUpload) Commit(context.Context) (multipart.ObjectAttrs, error) {
	return multipart.ObjectAttrs{}, u.commitErr
}

func (errorUpload) Abort(context.Context) error { return nil }
