package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
)

var _ csession.RetentionManager = (*Catalog)(nil)

// retentionFile remains JSON because it is a small mutable policy mailbox,
// not part of the catformat tree. The authoritative applied state is committed
// in the binary head.
type retentionFile struct {
	Version       uint16 `json:"version"`
	StreamID      string `json:"stream_id,omitempty"`
	Partition     uint32 `json:"partition"`
	PolicyVersion uint64 `json:"policy_version"`
	BeforeLSN     uint64 `json:"before_lsn"`
	CreatedUnixMS int64  `json:"created_unix_ms"`
}

func (c *Catalog) RequestRetention(ctx context.Context, partition uint32, request csession.RetentionRequest) (csession.RetentionRequest, error) {
	if err := ctx.Err(); err != nil {
		return csession.RetentionRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return csession.RetentionRequest{}, err
	}

	path := RetentionRequestPath(c.opts.Prefix, c.opts.StreamID, partition)
	current, token, found, err := c.loadRetentionFile(ctx, partition)
	if err != nil {
		return csession.RetentionRequest{}, err
	}
	if found {
		if existing, done, err := compareRetentionRequest(current.request(), request); done || err != nil {
			return existing, err
		}
	}

	candidate := retentionFile{
		Version: request.Version, StreamID: c.opts.StreamID, Partition: partition,
		PolicyVersion: request.PolicyVersion, BeforeLSN: request.BeforeLSN, CreatedUnixMS: request.CreatedUnixMS,
	}
	body, err := json.Marshal(candidate)
	if err != nil {
		return csession.RetentionRequest{}, err
	}

	backoff := c.opts.WriterCommitInitialBackoff
	var lastCASErr error
	for attempt := 0; attempt < c.opts.WriterCommitMaxAttempts; attempt++ {
		obj, swapped, casErr := c.backend.CompareAndSwap(ctx, path, token, body)
		if casErr != nil {
			lastCASErr = casErr
		} else if swapped {
			return request, nil
		} else {
			observed, err := decodeRetentionFile(obj.Body, c.opts.StreamID, partition)
			if err != nil {
				return csession.RetentionRequest{}, err
			}
			if existing, done, err := compareRetentionRequest(observed.request(), request); done || err != nil {
				return existing, err
			}
			token = obj.Token
			lastCASErr = nil
		}

		if attempt+1 == c.opts.WriterCommitMaxAttempts {
			break
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			return csession.RetentionRequest{}, fmt.Errorf("%w: request retention partition=%d: %w", csession.ErrCommitIndeterminate, partition, errors.Join(lastCASErr, err))
		}
		backoff = growBackoff(backoff, c.opts.WriterCommitMaxBackoff)
	}

	observed, _, found, err := c.loadRetentionFile(ctx, partition)
	if err != nil {
		return csession.RetentionRequest{}, fmt.Errorf("%w: request retention partition=%d: %w", csession.ErrCommitIndeterminate, partition, errors.Join(lastCASErr, err))
	}
	if found {
		if existing, done, err := compareRetentionRequest(observed.request(), request); done || err != nil {
			return existing, err
		}
	}
	if lastCASErr != nil {
		return csession.RetentionRequest{}, fmt.Errorf("request retention partition=%d: %w", partition, lastCASErr)
	}
	return csession.RetentionRequest{}, fmt.Errorf("%w: retention request CAS did not apply partition=%d", csession.ErrConflict, partition)
}

func (c *Catalog) LoadRetentionRequest(ctx context.Context, partition uint32) (csession.RetentionRequest, bool, error) {
	if err := ctx.Err(); err != nil {
		return csession.RetentionRequest{}, false, err
	}
	file, _, found, err := c.loadRetentionFile(ctx, partition)
	if err != nil || !found {
		return csession.RetentionRequest{}, found, err
	}
	return file.request(), true, nil
}

func (c *Catalog) loadRetentionFile(ctx context.Context, partition uint32) (retentionFile, string, bool, error) {
	obj, err := c.backend.Get(ctx, RetentionRequestPath(c.opts.Prefix, c.opts.StreamID, partition))
	if errors.Is(err, ErrObjectNotFound) {
		return retentionFile{}, "", false, nil
	}
	if err != nil {
		return retentionFile{}, "", false, err
	}
	file, err := decodeRetentionFile(obj.Body, c.opts.StreamID, partition)
	if err != nil {
		return retentionFile{}, "", false, err
	}
	return file, obj.Token, true, nil
}

func decodeRetentionFile(body []byte, streamID string, partition uint32) (retentionFile, error) {
	var file retentionFile
	if err := json.Unmarshal(body, &file); err != nil {
		return retentionFile{}, fmt.Errorf("%w: decode retention partition=%d: %v", ErrCorruptCatalog, partition, err)
	}
	if file.StreamID != streamID {
		return retentionFile{}, fmt.Errorf("%w: retention stream_id=%q want=%q", ErrCorruptCatalog, file.StreamID, streamID)
	}
	if file.Partition != partition {
		return retentionFile{}, fmt.Errorf("%w: retention partition=%d want=%d", ErrCorruptCatalog, file.Partition, partition)
	}
	if err := file.request().Validate(); err != nil {
		return retentionFile{}, fmt.Errorf("%w: %v", ErrCorruptCatalog, err)
	}
	return file, nil
}

func (f retentionFile) request() csession.RetentionRequest {
	return csession.RetentionRequest{
		Version: f.Version, PolicyVersion: f.PolicyVersion,
		BeforeLSN: f.BeforeLSN, CreatedUnixMS: f.CreatedUnixMS,
	}
}

func compareRetentionRequest(current, requested csession.RetentionRequest) (csession.RetentionRequest, bool, error) {
	switch {
	case current.PolicyVersion == requested.PolicyVersion && current.BeforeLSN == requested.BeforeLSN:
		return current, true, nil
	case requested.PolicyVersion <= current.PolicyVersion:
		return csession.RetentionRequest{}, true, fmt.Errorf("%w: policy_version=%d current=%d", csession.ErrRetentionRegression, requested.PolicyVersion, current.PolicyVersion)
	case requested.BeforeLSN < current.BeforeLSN:
		return csession.RetentionRequest{}, true, fmt.Errorf("%w: before_lsn=%d current=%d", csession.ErrRetentionRegression, requested.BeforeLSN, current.BeforeLSN)
	default:
		return csession.RetentionRequest{}, false, nil
	}
}

func sleepBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func growBackoff(current, maximum time.Duration) time.Duration {
	if current <= 0 || maximum <= 0 {
		return current
	}
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}
