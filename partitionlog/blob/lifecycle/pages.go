package lifecycle

import (
	"context"

	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

func (r *Reclaimer) reclaimPages(ctx context.Context, state *stateFile, token *string, budget *runBudget) error {
	level := state.PageLevel
	afterKey := state.PageAfterKey

	for level <= state.MaxPageLevel && budget.available() {
		if afterKey == "" {
			afterKey = catalogblob.PageLowerBound(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, level, state.PageReclaimedThroughLSN)
		}
		limit := budget.listLimit()
		if limit == 0 {
			return nil
		}
		prefix := catalogblob.PageLevelPrefix(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, level)
		page, err := r.backend.List(ctx, ListOptions{Prefix: prefix, AfterKey: afterKey, Limit: limit})
		if err != nil {
			return err
		}
		if err := validateObjectPage(page, afterKey); err != nil {
			return err
		}
		budget.recordScan(len(page.Objects))
		if len(page.Objects) == 0 {
			level++
			afterKey = ""
			if level > state.MaxPageLevel {
				return r.completePages(ctx, state, token)
			}
			if err := r.advancePageLevel(ctx, state, token, level); err != nil {
				return err
			}
			continue
		}

		lastProcessed := afterKey
		levelComplete := false
		budgetStopped := false
		candidates := make([]deleteCandidate, 0, len(page.Objects))
		var scheduledBytes uint64
		for _, object := range page.Objects {
			parsed, err := catalogblob.ParsePagePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, object.Key)
			if err != nil || parsed.Level != level {
				budget.invalid()
				lastProcessed = object.Key
				continue
			}
			if parsed.SeqHi >= state.SafeFloorLSN {
				levelComplete = true
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
				var checkpointErr error
				if checkpoint != state.PageAfterKey {
					state.PageAfterKey = checkpoint
					checkpointErr = r.saveState(ctx, state, token)
				}
				return joinDeleteCheckpointError(err, checkpointErr)
			}
		}
		if budgetStopped {
			if !r.opts.DryRun && lastProcessed != state.PageAfterKey {
				state.PageAfterKey = lastProcessed
				return r.saveState(ctx, state, token)
			}
			return nil
		}

		if r.opts.DryRun {
			if levelComplete || !page.HasMore {
				level++
				afterKey = ""
			} else {
				afterKey = lastProcessed
			}
			continue
		}
		if levelComplete || !page.HasMore {
			level++
			afterKey = ""
			if level > state.MaxPageLevel {
				return r.completePages(ctx, state, token)
			}
			if err := r.advancePageLevel(ctx, state, token, level); err != nil {
				return err
			}
			continue
		}
		state.PageAfterKey = lastProcessed
		afterKey = lastProcessed
		if err := r.saveState(ctx, state, token); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reclaimer) completePages(ctx context.Context, state *stateFile, token *string) error {
	if r.opts.DryRun {
		return nil
	}
	state.PageReclaimedThroughLSN = state.SafeFloorLSN
	state.PageLevel = 0
	state.PageAfterKey = ""
	return r.saveState(ctx, state, token)
}

func (r *Reclaimer) advancePageLevel(ctx context.Context, state *stateFile, token *string, level uint8) error {
	if r.opts.DryRun {
		return nil
	}
	state.PageLevel = level
	state.PageAfterKey = ""
	return r.saveState(ctx, state, token)
}
