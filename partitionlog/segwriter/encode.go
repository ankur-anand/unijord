package segwriter

import "context"

func Encode(ctx context.Context, records []Record, opts Options) ([]byte, Metadata, error) {
	sink := NewMemorySink("memory://encode")
	w, err := New(opts, sink)
	if err != nil {
		return nil, Metadata{}, err
	}
	for _, record := range records {
		if err := w.Append(ctx, record); err != nil {
			_ = w.Abort(ctx)
			return nil, Metadata{}, err
		}
	}
	result, err := w.Close(ctx)
	if err != nil {
		return nil, Metadata{}, err
	}
	return sink.Bytes(), result.Metadata, nil
}
