package lifecycle

import (
	"context"
	"fmt"
)

func (r *Reclaimer) reclaimSegments(ctx context.Context, state *stateFile, token *string, budget *runBudget) error {
	afterKey := state.SegmentAfterKey
	if afterKey == "" {
		afterKey = r.layout.SegmentLowerBound(r.opts.StreamID, state.Partition, state.SegmentReclaimedThroughLSN)
	}
	prefix := r.layout.SegmentPrefix(r.opts.StreamID, state.Partition)

	for budget.available() {
		limit := budget.listLimit()
		if limit == 0 {
			return nil
		}
		page, err := r.backend.List(ctx, ListOptions{Prefix: prefix, AfterKey: afterKey, Limit: limit})
		if err != nil {
			return err
		}
		if err := validateObjectPage(page, afterKey); err != nil {
			return err
		}
		budget.recordScan(len(page.Objects))
		if len(page.Objects) == 0 {
			return r.completeSegments(ctx, state, token)
		}

		lastProcessed := afterKey
		candidates := make([]deleteCandidate, 0, len(page.Objects))
		var scheduledBytes uint64
		reachedFloor := false
		budgetStopped := false
		for _, object := range page.Objects {
			parsed, err := r.layout.ParseSegmentKey(r.opts.StreamID, state.Partition, object.Key)
			if err != nil {
				budget.invalid()
				lastProcessed = object.Key
				continue
			}
			if parsed.BaseLSN >= state.SafeFloorLSN {
				reachedFloor = true
				break
			}
			budget.recordCandidate()
			size := objectSize(object)
			if !r.opts.DryRun && !budget.canScheduleDelete(size, uint64(len(candidates)), scheduledBytes) {
				budgetStopped = true
				break
			}
			if r.opts.DryRun {
				lastProcessed = object.Key
				continue
			}
			candidates = append(candidates, deleteCandidate{key: object.Key, size: size, beforeKey: lastProcessed})
			scheduledBytes += size
			lastProcessed = object.Key
		}
		if len(candidates) > 0 {
			if _, err := r.recheckFloor(ctx, state.Partition, state.SafeFloorLSN); err != nil {
				return err
			}
			checkpoint, err := r.executeDeletes(ctx, state, candidates, budget)
			if err != nil {
				if checkpoint != state.SegmentAfterKey {
					state.SegmentAfterKey = checkpoint
					_ = r.saveState(ctx, state, token)
				}
				return err
			}
		}
		if reachedFloor {
			return r.completeSegments(ctx, state, token)
		}
		if budgetStopped {
			if !r.opts.DryRun && lastProcessed != state.SegmentAfterKey {
				state.SegmentAfterKey = lastProcessed
				return r.saveState(ctx, state, token)
			}
			return nil
		}

		afterKey = lastProcessed
		if r.opts.DryRun {
			if !page.HasMore {
				return nil
			}
			continue
		}
		state.SegmentAfterKey = lastProcessed
		if !page.HasMore {
			return r.completeSegments(ctx, state, token)
		}
		if err := r.saveState(ctx, state, token); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reclaimer) completeSegments(ctx context.Context, state *stateFile, token *string) error {
	if r.opts.DryRun {
		return nil
	}
	state.SegmentReclaimedThroughLSN = state.SafeFloorLSN
	state.SegmentAfterKey = ""
	return r.saveState(ctx, state, token)
}

func validateObjectPage(page ObjectPage, afterKey string) error {
	previous := afterKey
	for _, object := range page.Objects {
		if object.Key == "" || object.Key <= previous {
			return fmt.Errorf("lifecycle: non-increasing object page key=%q previous=%q", object.Key, previous)
		}
		previous = object.Key
	}
	if page.HasMore {
		if len(page.Objects) == 0 || page.NextAfterKey != page.Objects[len(page.Objects)-1].Key {
			return fmt.Errorf("lifecycle: invalid next after key %q", page.NextAfterKey)
		}
	} else if page.NextAfterKey != "" {
		return fmt.Errorf("lifecycle: terminal page has next after key %q", page.NextAfterKey)
	}
	return nil
}
