package lifecycle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

const stateVersion = 1

type stateFile struct {
	Version      int    `json:"version"`
	StreamID     string `json:"stream_id"`
	Partition    uint32 `json:"partition"`
	OwnerID      string `json:"owner_id,omitempty"`
	LeaseUntilMS int64  `json:"lease_until_unix_ms,omitempty"`

	RetentionVersion uint64 `json:"retention_version,omitempty"`
	SafeFloorLSN     uint64 `json:"safe_floor_lsn,omitempty"`
	PendingFloorLSN  uint64 `json:"pending_floor_lsn,omitempty"`
	PendingSinceMS   int64  `json:"pending_since_unix_ms,omitempty"`
	HasPendingFloor  bool   `json:"has_pending_floor,omitempty"`

	SegmentReclaimedThroughLSN uint64 `json:"segment_reclaimed_through_lsn,omitempty"`
	SegmentAfterKey            string `json:"segment_after_key,omitempty"`

	PageReclaimedThroughLSN uint64 `json:"page_reclaimed_through_lsn,omitempty"`
	PageLevel               uint8  `json:"page_level,omitempty"`
	PageAfterKey            string `json:"page_after_key,omitempty"`
	MaxPageLevel            uint8  `json:"max_page_level,omitempty"`

	StagingAfterKey string `json:"staging_after_key,omitempty"`
	StagingEpoch    uint64 `json:"staging_epoch,omitempty"`

	OrphanSegmentAfterKey string             `json:"orphan_segment_after_key,omitempty"`
	OrphanPageLevel       uint8              `json:"orphan_page_level,omitempty"`
	OrphanPageAfterKey    string             `json:"orphan_page_after_key,omitempty"`
	PageQuarantine        []quarantineObject `json:"page_quarantine,omitempty"`

	UpdatedMS int64 `json:"updated_unix_ms"`
}

type quarantineObject struct {
	Key                string `json:"key"`
	SizeBytes          uint64 `json:"size_bytes,omitempty"`
	ObservedGeneration uint64 `json:"observed_generation"`
	ObservedMS         int64  `json:"observed_unix_ms"`
}

func randomOwnerID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}

func (r *Reclaimer) acquire(ctx context.Context, partition uint32, now time.Time) (stateFile, string, error) {
	path := catalogblob.GCStatePath(r.opts.CatalogPrefix, r.opts.StreamID, partition)
	owner := hex.EncodeToString(r.opts.OwnerID[:])
	for attempt := 0; attempt < r.opts.CASAttempts; attempt++ {
		state, token, err := r.loadState(ctx, path, partition)
		if err != nil {
			return stateFile{}, "", err
		}
		if state.OwnerID != "" && state.OwnerID != owner && state.LeaseUntilMS > now.UnixMilli() {
			return stateFile{}, "", fmt.Errorf("%w: owner=%s until=%d", ErrLeaseHeld, state.OwnerID, state.LeaseUntilMS)
		}
		state.OwnerID = owner
		state.LeaseUntilMS = now.Add(r.leaseDuration()).UnixMilli()
		state.UpdatedMS = now.UnixMilli()
		body, err := marshalState(state, r.opts.StreamID, partition)
		if err != nil {
			return stateFile{}, "", err
		}
		obj, swapped, err := r.backend.CompareAndSwap(ctx, path, token, body)
		if err != nil {
			return stateFile{}, "", err
		}
		if swapped {
			return state, obj.Token, nil
		}
	}
	return stateFile{}, "", fmt.Errorf("%w: acquire retries exhausted", ErrLeaseLost)
}

func (r *Reclaimer) loadState(ctx context.Context, path string, partition uint32) (stateFile, string, error) {
	obj, err := r.backend.Get(ctx, path)
	if errors.Is(err, blobstore.ErrObjectNotFound) {
		return stateFile{Version: stateVersion, StreamID: r.opts.StreamID, Partition: partition}, "", nil
	}
	if err != nil {
		return stateFile{}, "", err
	}
	state, err := decodeState(obj.Body, r.opts.StreamID, partition)
	if err != nil {
		return stateFile{}, "", err
	}
	return state, obj.Token, nil
}

func (r *Reclaimer) saveState(ctx context.Context, state *stateFile, token *string) error {
	now := r.now().UTC()
	state.OwnerID = hex.EncodeToString(r.opts.OwnerID[:])
	state.LeaseUntilMS = now.Add(r.leaseDuration()).UnixMilli()
	state.UpdatedMS = now.UnixMilli()
	body, err := marshalState(*state, r.opts.StreamID, state.Partition)
	if err != nil {
		return err
	}
	path := catalogblob.GCStatePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition)
	obj, swapped, err := r.backend.CompareAndSwap(ctx, path, *token, body)
	if err != nil {
		return err
	}
	if !swapped {
		return fmt.Errorf("%w: state CAS conflict", ErrLeaseLost)
	}
	*token = obj.Token
	return nil
}

func (r *Reclaimer) checkLease(state *stateFile) error {
	if state == nil {
		return fmt.Errorf("%w: missing state", ErrLeaseLost)
	}
	owner := hex.EncodeToString(r.opts.OwnerID[:])
	if state.OwnerID != owner {
		return fmt.Errorf("%w: owner=%s current=%s", ErrLeaseLost, owner, state.OwnerID)
	}
	if state.LeaseUntilMS <= r.now().UTC().UnixMilli() {
		return fmt.Errorf("%w: lease expired at=%d", ErrLeaseLost, state.LeaseUntilMS)
	}
	return nil
}

func (r *Reclaimer) release(ctx context.Context, state *stateFile, token *string) error {
	state.OwnerID = ""
	state.LeaseUntilMS = 0
	state.UpdatedMS = r.now().UTC().UnixMilli()
	body, err := marshalState(*state, r.opts.StreamID, state.Partition)
	if err != nil {
		return err
	}
	path := catalogblob.GCStatePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition)
	obj, swapped, err := r.backend.CompareAndSwap(ctx, path, *token, body)
	if err != nil {
		return err
	}
	if !swapped {
		return fmt.Errorf("%w: release CAS conflict", ErrLeaseLost)
	}
	*token = obj.Token
	return nil
}

func marshalState(state stateFile, streamID string, partition uint32) ([]byte, error) {
	if err := validateState(state, streamID, partition); err != nil {
		return nil, err
	}
	body, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: marshal state: %w", err)
	}
	return body, nil
}

func decodeState(body []byte, streamID string, partition uint32) (stateFile, error) {
	var state stateFile
	if err := json.Unmarshal(body, &state); err != nil {
		return stateFile{}, fmt.Errorf("%w: decode: %v", ErrCorruptState, err)
	}
	if err := validateState(state, streamID, partition); err != nil {
		return stateFile{}, err
	}
	return state, nil
}

func validateState(state stateFile, streamID string, partition uint32) error {
	switch {
	case state.Version != stateVersion:
		return fmt.Errorf("%w: version=%d", ErrCorruptState, state.Version)
	case state.StreamID != streamID:
		return fmt.Errorf("%w: stream_id=%q want=%q", ErrCorruptState, state.StreamID, streamID)
	case state.Partition != partition:
		return fmt.Errorf("%w: partition=%d want=%d", ErrCorruptState, state.Partition, partition)
	case state.SafeFloorLSN < state.SegmentReclaimedThroughLSN:
		return fmt.Errorf("%w: segment reclaimed=%d exceeds safe floor=%d", ErrCorruptState, state.SegmentReclaimedThroughLSN, state.SafeFloorLSN)
	case state.SafeFloorLSN < state.PageReclaimedThroughLSN:
		return fmt.Errorf("%w: page reclaimed=%d exceeds safe floor=%d", ErrCorruptState, state.PageReclaimedThroughLSN, state.SafeFloorLSN)
	case state.HasPendingFloor && state.PendingFloorLSN <= state.SafeFloorLSN:
		return fmt.Errorf("%w: pending floor=%d safe=%d", ErrCorruptState, state.PendingFloorLSN, state.SafeFloorLSN)
	case !state.HasPendingFloor && (state.PendingFloorLSN != 0 || state.PendingSinceMS != 0):
		return fmt.Errorf("%w: pending fields without pending floor", ErrCorruptState)
	case state.PageLevel > state.MaxPageLevel && state.PageReclaimedThroughLSN < state.SafeFloorLSN:
		return fmt.Errorf("%w: page level=%d max=%d", ErrCorruptState, state.PageLevel, state.MaxPageLevel)
	case state.OrphanPageLevel > state.MaxPageLevel && state.OrphanPageAfterKey != "":
		return fmt.Errorf("%w: orphan page level=%d max=%d", ErrCorruptState, state.OrphanPageLevel, state.MaxPageLevel)
	case len(state.PageQuarantine) > maxQuarantineEntries:
		return fmt.Errorf("%w: quarantine entries=%d max=%d", ErrCorruptState, len(state.PageQuarantine), maxQuarantineEntries)
	}
	seenQuarantine := make(map[string]struct{}, len(state.PageQuarantine))
	for _, candidate := range state.PageQuarantine {
		if candidate.Key == "" || candidate.ObservedGeneration == 0 || candidate.ObservedMS <= 0 {
			return fmt.Errorf("%w: invalid quarantine candidate", ErrCorruptState)
		}
		if _, exists := seenQuarantine[candidate.Key]; exists {
			return fmt.Errorf("%w: duplicate quarantine key=%q", ErrCorruptState, candidate.Key)
		}
		seenQuarantine[candidate.Key] = struct{}{}
	}
	if state.OwnerID != "" {
		decoded, err := hex.DecodeString(state.OwnerID)
		if err != nil || len(decoded) != 16 {
			return fmt.Errorf("%w: invalid owner_id", ErrCorruptState)
		}
	}
	return nil
}

func (r *Reclaimer) observeHead(state *stateFile, snapshot catalogblob.MaintenanceSnapshot, now time.Time) bool {
	changed := false
	if snapshot.Head.AppliedRetentionVersion > state.RetentionVersion {
		state.RetentionVersion = snapshot.Head.AppliedRetentionVersion
		changed = true
	}
	if snapshot.MaxIndexLevel > state.MaxPageLevel {
		state.MaxPageLevel = snapshot.MaxIndexLevel
		changed = true
	}
	if state.HasPendingFloor && !now.Before(time.UnixMilli(state.PendingSinceMS).Add(r.opts.DeleteDelay)) {
		state.SafeFloorLSN = state.PendingFloorLSN
		state.PendingFloorLSN = 0
		state.PendingSinceMS = 0
		state.HasPendingFloor = false
		changed = true
	}
	if !state.HasPendingFloor && snapshot.Head.OldestLSN > state.SafeFloorLSN {
		state.PendingFloorLSN = snapshot.Head.OldestLSN
		state.PendingSinceMS = now.UnixMilli()
		state.HasPendingFloor = true
		changed = true
	}
	return changed
}
