package segwriter

import (
	"context"
	"errors"
)

func Encode(ctx context.Context, records []Record, opts Options) ([]byte, Metadata, error) {
	sink := NewMemorySink("memory://encode")
	w, err := New(opts, sink)
	if err != nil {
		return nil, Metadata{}, err
	}
	for _, record := range records {
		if err := w.Append(ctx, record); err != nil {
			return nil, Metadata{}, errors.Join(err, w.Abort(context.WithoutCancel(ctx)))
		}
	}
	result, err := w.Close(ctx)
	if err != nil {
		return nil, Metadata{}, err
	}
	return sink.Bytes(), result.Metadata, nil
}
