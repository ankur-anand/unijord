package lifecycle

import (
	"context"
	"fmt"
	"time"

	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

// ScrubPartition performs bounded orphan discovery and quarantine work for
// one partition. It is intentionally separate from normal retention GC
// because reachability checks are more expensive than ordered range deletion.
func (r *Reclaimer) ScrubPartition(ctx context.Context, partition uint32) (result Result, err error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
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
		releaseErr := r.release(ctx, releaseState, &token)
		if err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()

	snapshot, err := r.catalog.LoadMaintenanceSnapshot(ctx, partition)
	if err != nil {
		return Result{}, err
	}
	if err := r.validateSnapshot(snapshot, partition); err != nil {
		return Result{}, err
	}
	if snapshot.MaxIndexLevel > state.MaxPageLevel && !r.opts.DryRun {
		state.MaxPageLevel = snapshot.MaxIndexLevel
		if err := r.saveState(ctx, &state, &token); err != nil {
			return Result{}, err
		}
	}

	budget := runBudget{opts: r.opts, result: &result}
	if err := r.processPageQuarantine(ctx, &state, &token, &budget); err != nil {
		return Result{}, err
	}
	segmentMore := false
	if budget.available() {
		segmentMore, err = r.scrubSegmentOrphans(ctx, snapshot, &state, &token, &budget)
		if err != nil {
			return Result{}, err
		}
	}
	pageMore := false
	if budget.available() && !segmentMore {
		pageMore, err = r.scrubPageOrphans(ctx, snapshot, &state, &token, &budget)
		if err != nil {
			return Result{}, err
		}
	}

	result.SafeFloorLSN = state.SafeFloorLSN
	result.ReclaimedThroughLSN = min(state.SegmentReclaimedThroughLSN, state.PageReclaimedThroughLSN)
	result.PendingQuarantine = len(state.PageQuarantine)
	result.HasMore = segmentMore || pageMore || budget.exhausted
	return result, nil
}

func (r *Reclaimer) processPageQuarantine(ctx context.Context, state *stateFile, token *string, budget *runBudget) error {
	if len(state.PageQuarantine) == 0 {
		return nil
	}
	now := r.now().UTC()
	remaining := make([]quarantineObject, 0, len(state.PageQuarantine))
	changed := false
	for i, candidate := range state.PageQuarantine {
		if now.Before(timeFromUnixMilli(candidate.ObservedMS).Add(r.opts.DeleteDelay)) {
			remaining = append(remaining, candidate)
			continue
		}
		if !budget.available() {
			remaining = append(remaining, state.PageQuarantine[i:]...)
			break
		}
		budget.recordScan(1)
		parsed, err := catalogblob.ParsePagePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, candidate.Key)
		if err != nil {
			return fmt.Errorf("%w: quarantine page: %v", ErrCorruptState, err)
		}
		snapshot, reachable, err := r.catalog.IsPageReachable(ctx, state.Partition, candidate.Key)
		if err != nil {
			return err
		}
		if err := r.validateSnapshot(snapshot, state.Partition); err != nil {
			return err
		}
		if snapshot.Generation < candidate.ObservedGeneration {
			return fmt.Errorf("lifecycle: catalog generation regressed current=%d observed=%d", snapshot.Generation, candidate.ObservedGeneration)
		}
		if reachable {
			changed = true
			continue
		}
		if parsed.Generation >= snapshot.Generation {
			remaining = append(remaining, candidate)
			continue
		}
		budget.recordCandidate()
		if !budget.canDelete(candidate.SizeBytes) {
			remaining = append(remaining, candidate)
			remaining = append(remaining, state.PageQuarantine[i+1:]...)
			break
		}
		if r.opts.DryRun {
			remaining = append(remaining, candidate)
			continue
		}
		if err := r.backend.Delete(ctx, candidate.Key); err != nil {
			return err
		}
		budget.recordDelete(candidate.SizeBytes)
		changed = true
	}
	if r.opts.DryRun || !changed {
		return nil
	}
	state.PageQuarantine = remaining
	return r.saveState(ctx, state, token)
}

func (r *Reclaimer) scrubSegmentOrphans(ctx context.Context, snapshot catalogblob.MaintenanceSnapshot, state *stateFile, token *string, budget *runBudget) (bool, error) {
	prefix := r.layout.SegmentPrefix(r.opts.StreamID, state.Partition)
	afterKey := state.OrphanSegmentAfterKey
	if afterKey == "" {
		afterKey = prefix
	}
	now := r.now().UTC()

	for budget.available() {
		page, err := r.backend.List(ctx, ListOptions{Prefix: prefix, AfterKey: afterKey, Limit: budget.listLimit()})
		if err != nil {
			return false, err
		}
		if err := validateObjectPage(page, afterKey); err != nil {
			return false, err
		}
		budget.recordScan(len(page.Objects))
		if len(page.Objects) == 0 {
			return false, r.completeOrphanSegments(ctx, state, token)
		}

		lastProcessed := afterKey
		for _, object := range page.Objects {
			parsed, err := r.layout.ParseSegmentKey(r.opts.StreamID, state.Partition, object.Key)
			if err != nil {
				budget.invalid()
				lastProcessed = object.Key
				continue
			}
			if !orphanSegmentEligible(parsed.BaseLSN, parsed.WriterEpoch, snapshot) || !oldEnough(object.CreatedAt, now, r.opts.DeleteDelay) {
				lastProcessed = object.Key
				continue
			}
			reachable, err := r.segmentReferenced(ctx, state.Partition, object.Key, parsed.BaseLSN)
			if err != nil {
				return false, err
			}
			if reachable {
				lastProcessed = object.Key
				continue
			}
			budget.recordCandidate()
			size := objectSize(object)
			if !budget.canDelete(size) {
				return true, r.checkpointOrphanSegment(ctx, state, token, lastProcessed)
			}
			if r.opts.DryRun {
				lastProcessed = object.Key
				continue
			}
			fresh, err := r.catalog.LoadMaintenanceSnapshot(ctx, state.Partition)
			if err != nil {
				return false, err
			}
			if err := r.validateSnapshot(fresh, state.Partition); err != nil {
				return false, err
			}
			if !orphanSegmentEligible(parsed.BaseLSN, parsed.WriterEpoch, fresh) {
				lastProcessed = object.Key
				continue
			}
			reachable, err = r.segmentReferenced(ctx, state.Partition, object.Key, parsed.BaseLSN)
			if err != nil {
				return false, err
			}
			if reachable {
				lastProcessed = object.Key
				continue
			}
			if err := r.backend.Delete(ctx, object.Key); err != nil {
				_ = r.checkpointOrphanSegment(ctx, state, token, lastProcessed)
				return false, err
			}
			budget.recordDelete(size)
			lastProcessed = object.Key
		}

		afterKey = lastProcessed
		if r.opts.DryRun {
			if !page.HasMore {
				return false, nil
			}
			continue
		}
		state.OrphanSegmentAfterKey = lastProcessed
		if !page.HasMore {
			return false, r.completeOrphanSegments(ctx, state, token)
		}
		if err := r.saveState(ctx, state, token); err != nil {
			return false, err
		}
	}
	return true, nil
}

func orphanSegmentEligible(baseLSN, writerEpoch uint64, snapshot catalogblob.MaintenanceSnapshot) bool {
	if writerEpoch > snapshot.Head.WriterEpoch {
		return false
	}
	return baseLSN < snapshot.Head.NextLSN || writerEpoch < snapshot.Head.WriterEpoch
}

func (r *Reclaimer) segmentReferenced(ctx context.Context, partition uint32, key string, baseLSN uint64) (bool, error) {
	segment, found, err := r.catalog.FindSegment(ctx, partition, baseLSN)
	if err != nil || !found {
		return false, err
	}
	return segment.BaseLSN == baseLSN && segment.URI == key, nil
}

func (r *Reclaimer) checkpointOrphanSegment(ctx context.Context, state *stateFile, token *string, afterKey string) error {
	if r.opts.DryRun || afterKey == state.OrphanSegmentAfterKey {
		return nil
	}
	state.OrphanSegmentAfterKey = afterKey
	return r.saveState(ctx, state, token)
}

func (r *Reclaimer) completeOrphanSegments(ctx context.Context, state *stateFile, token *string) error {
	if r.opts.DryRun {
		return nil
	}
	state.OrphanSegmentAfterKey = ""
	return r.saveState(ctx, state, token)
}

func (r *Reclaimer) scrubPageOrphans(ctx context.Context, snapshot catalogblob.MaintenanceSnapshot, state *stateFile, token *string, budget *runBudget) (bool, error) {
	level := state.OrphanPageLevel
	afterKey := state.OrphanPageAfterKey
	quarantined := make(map[string]struct{}, len(state.PageQuarantine))
	for _, candidate := range state.PageQuarantine {
		quarantined[candidate.Key] = struct{}{}
	}

	for level <= state.MaxPageLevel && budget.available() {
		prefix := catalogblob.PageLevelPrefix(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, level)
		if afterKey == "" {
			afterKey = prefix
		}
		page, err := r.backend.List(ctx, ListOptions{Prefix: prefix, AfterKey: afterKey, Limit: budget.listLimit()})
		if err != nil {
			return false, err
		}
		if err := validateObjectPage(page, afterKey); err != nil {
			return false, err
		}
		budget.recordScan(len(page.Objects))
		if len(page.Objects) == 0 {
			level++
			afterKey = ""
			if level > state.MaxPageLevel {
				return false, r.completeOrphanPages(ctx, state, token)
			}
			if err := r.checkpointOrphanPage(ctx, state, token, level, ""); err != nil {
				return false, err
			}
			continue
		}

		lastProcessed := afterKey
		for _, object := range page.Objects {
			parsed, err := catalogblob.ParsePagePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, object.Key)
			if err != nil || parsed.Level != level {
				budget.invalid()
				lastProcessed = object.Key
				continue
			}
			if parsed.Generation >= snapshot.Generation {
				lastProcessed = object.Key
				continue
			}
			if _, exists := quarantined[object.Key]; exists {
				lastProcessed = object.Key
				continue
			}
			fresh, reachable, err := r.catalog.IsPageReachable(ctx, state.Partition, object.Key)
			if err != nil {
				return false, err
			}
			if err := r.validateSnapshot(fresh, state.Partition); err != nil {
				return false, err
			}
			if reachable || parsed.Generation >= fresh.Generation {
				lastProcessed = object.Key
				continue
			}
			budget.recordCandidate()
			if len(state.PageQuarantine) >= r.opts.MaxQuarantine {
				return false, r.checkpointOrphanPage(ctx, state, token, level, lastProcessed)
			}
			resultSize := objectSize(object)
			if !r.opts.DryRun {
				state.PageQuarantine = append(state.PageQuarantine, quarantineObject{
					Key: object.Key, SizeBytes: resultSize,
					ObservedGeneration: fresh.Generation, ObservedMS: r.now().UTC().UnixMilli(),
				})
				quarantined[object.Key] = struct{}{}
			}
			budget.result.QuarantinedObjects++
			lastProcessed = object.Key
		}

		afterKey = lastProcessed
		if r.opts.DryRun {
			if !page.HasMore {
				level++
				afterKey = ""
			}
			continue
		}
		if !page.HasMore {
			level++
			afterKey = ""
			if level > state.MaxPageLevel {
				return false, r.completeOrphanPages(ctx, state, token)
			}
		}
		if err := r.checkpointOrphanPage(ctx, state, token, level, afterKey); err != nil {
			return false, err
		}
	}
	return budget.exhausted, nil
}

func (r *Reclaimer) checkpointOrphanPage(ctx context.Context, state *stateFile, token *string, level uint8, afterKey string) error {
	if r.opts.DryRun {
		return nil
	}
	state.OrphanPageLevel = level
	state.OrphanPageAfterKey = afterKey
	return r.saveState(ctx, state, token)
}

func (r *Reclaimer) completeOrphanPages(ctx context.Context, state *stateFile, token *string) error {
	if r.opts.DryRun {
		return nil
	}
	state.OrphanPageLevel = 0
	state.OrphanPageAfterKey = ""
	return r.saveState(ctx, state, token)
}

func timeFromUnixMilli(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}
