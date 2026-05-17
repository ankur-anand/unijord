package multipart

import (
	"context"
	"errors"
	"fmt"
)

const (
	defaultContentType = "application/octet-stream"
)

var (
	ErrInvalidStore = errors.New("blobsink/multipart: invalid object store")
	ErrAborted      = errors.New("blobsink/multipart: upload aborted")
	ErrCompleted    = errors.New("blobsink/multipart: upload completed")
	// ErrPreconditionFailed means the final object already exists or a
	// provider-side conditional write failed.
	ErrPreconditionFailed = errors.New("blobsink/multipart: precondition failed")
)

type Store interface {
	BeginMultipart(ctx context.Context, key string, opts Options) (Upload, error)
}

type Upload interface {
	UploadPart(context.Context, Part) (Receipt, error)
	Complete(context.Context, []Receipt) (ObjectAttrs, error)
	Abort(context.Context) error
}

type Options struct {
	ContentType   string
	StagingPrefix string
}

type Part struct {
	Number int
	Bytes  []byte
}

type Receipt struct {
	Number    int
	Token     string
	SizeBytes uint64
}

type ObjectAttrs struct {
	Key       string
	SizeBytes uint64
	Token     string
}

func NormalizeOptions(key string, opts Options) (Options, error) {
	if key == "" {
		return Options{}, fmt.Errorf("%w: empty key", ErrInvalidStore)
	}
	if opts.ContentType == "" {
		opts.ContentType = defaultContentType
	}
	if opts.StagingPrefix == "" {
		opts.StagingPrefix = key + ".staging"
	}
	return opts, nil
}

func ValidatePart(part Part) error {
	if part.Number <= 0 {
		return fmt.Errorf("%w: invalid part number %d", ErrInvalidStore, part.Number)
	}
	if len(part.Bytes) == 0 {
		return fmt.Errorf("%w: empty part %d", ErrInvalidStore, part.Number)
	}
	return nil
}

func ValidateReceipts(receipts []Receipt) error {
	if len(receipts) == 0 {
		return fmt.Errorf("%w: no receipts", ErrInvalidStore)
	}
	for i, receipt := range receipts {
		want := i + 1
		if receipt.Number != want {
			return fmt.Errorf("%w: receipt number=%d want=%d", ErrInvalidStore, receipt.Number, want)
		}
	}
	return nil
}
