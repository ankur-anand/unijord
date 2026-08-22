package lifecycle

import (
	"context"
	"errors"
	"fmt"

	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

// RunPartition performs bounded lifecycle work for one known partition.
func (r *Reclaimer) RunPartition(ctx context.Context, partition uint32) (result Result, err error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	parentCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, r.opts.MaxPassDuration)
	defer cancel()
	now := r.now().UTC()
	state, token, err := r.acquire(ctx, partition, now)
	if err != nil {
		return Result{}, err
	}
	acquiredState := state
	defer func() {
		releaseState := &state
		if r.opts.DryRun {
			releaseState = &acquiredState
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(parentCtx), r.leaseReleaseTimeout())
		defer releaseCancel()
		releaseErr := r.release(releaseCtx, releaseState, &token)
		if releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("lifecycle: release lease: %w", releaseErr))
		}
	}()

	snapshot, err := r.catalog.LoadMaintenanceSnapshot(ctx, partition)
	if err != nil {
		return Result{}, err
	}
	if err := r.validateSnapshot(snapshot, partition); err != nil {
		return Result{}, err
	}
	if r.observeHead(&state, snapshot, now) && !r.opts.DryRun {
		if err := r.saveState(ctx, &state, &token); err != nil {
			return Result{}, err
		}
	}

	budget := runBudget{opts: r.opts, result: &result}
	if state.SegmentReclaimedThroughLSN < state.SafeFloorLSN && budget.available() {
		if err := r.reclaimSegments(ctx, &state, &token, &budget); err != nil {
			return Result{}, err
		}
	}
	if state.PageReclaimedThroughLSN < state.SafeFloorLSN && budget.available() {
		if err := r.reclaimPages(ctx, &state, &token, &budget); err != nil {
			return Result{}, err
		}
	}
	if budget.available() {
		if err := r.reclaimStaging(ctx, snapshot, &state, &token, &budget); err != nil {
			return Result{}, err
		}
	}

	result.SafeFloorLSN = state.SafeFloorLSN
	result.ReclaimedThroughLSN = min(state.SegmentReclaimedThroughLSN, state.PageReclaimedThroughLSN)
	result.HasMore = state.SegmentReclaimedThroughLSN < state.SafeFloorLSN ||
		state.PageReclaimedThroughLSN < state.SafeFloorLSN || state.HasPendingFloor || budget.exhausted
	return result, nil
}

func (r *Reclaimer) validateSnapshot(snapshot catalogblob.MaintenanceSnapshot, partition uint32) error {
	if snapshot.Head.StreamID != r.opts.StreamID {
		return fmt.Errorf("lifecycle: head stream_id=%q want=%q", snapshot.Head.StreamID, r.opts.StreamID)
	}
	if snapshot.Head.Partition != partition {
		return fmt.Errorf("lifecycle: head partition=%d want=%d", snapshot.Head.Partition, partition)
	}
	if snapshot.Head.OldestLSN > snapshot.Head.NextLSN {
		return fmt.Errorf("lifecycle: invalid head oldest=%d next=%d", snapshot.Head.OldestLSN, snapshot.Head.NextLSN)
	}
	return nil
}

func (r *Reclaimer) recheckFloor(ctx context.Context, partition uint32, safeFloor uint64) (catalogblob.MaintenanceSnapshot, error) {
	snapshot, err := r.catalog.LoadMaintenanceSnapshot(ctx, partition)
	if err != nil {
		return catalogblob.MaintenanceSnapshot{}, err
	}
	if err := r.validateSnapshot(snapshot, partition); err != nil {
		return catalogblob.MaintenanceSnapshot{}, err
	}
	if snapshot.Head.OldestLSN < safeFloor {
		return catalogblob.MaintenanceSnapshot{}, fmt.Errorf("lifecycle: head retention regressed oldest=%d safe=%d", snapshot.Head.OldestLSN, safeFloor)
	}
	return snapshot, nil
}

type runBudget struct {
	opts      Options
	result    *Result
	exhausted bool
}

func (b *runBudget) available() bool {
	return !b.exhausted && b.result.ScannedObjects < b.opts.MaxObjectsPerRun && b.result.DeletedObjects < b.opts.MaxDeletesPerRun
}

func (b *runBudget) listLimit() int {
	remaining := b.opts.MaxObjectsPerRun - b.result.ScannedObjects
	if remaining <= 0 {
		b.exhausted = true
		return 0
	}
	return min(remaining, b.opts.ListPageSize)
}

func (b *runBudget) recordScan(count int) {
	b.result.ScannedObjects += count
	if b.result.ScannedObjects >= b.opts.MaxObjectsPerRun {
		b.exhausted = true
	}
}

func (b *runBudget) recordCandidate() {
	b.result.CandidateObjects++
}

func (b *runBudget) recordDelete(size uint64) {
	b.result.DeletedObjects++
	b.result.DeletedBytes += size
	if b.result.DeletedObjects >= b.opts.MaxDeletesPerRun || b.result.DeletedBytes >= b.opts.MaxDeleteBytes {
		b.exhausted = true
	}
}

func (b *runBudget) invalid() {
	b.result.InvalidObjects++
}

func objectSize(info ObjectInfo) uint64 {
	if info.SizeBytes <= 0 {
		return 0
	}
	return uint64(info.SizeBytes)
}
