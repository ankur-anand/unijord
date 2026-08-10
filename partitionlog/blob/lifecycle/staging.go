package lifecycle

import (
	"context"
	"time"

	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

func (r *Reclaimer) reclaimStaging(ctx context.Context, snapshot catalogblob.MaintenanceSnapshot, state *stateFile, token *string, budget *runBudget) error {
	if snapshot.Head.WriterEpoch == 0 {
		return nil
	}
	afterKey := state.StagingAfterKey
	if state.StagingEpoch != snapshot.Head.WriterEpoch {
		afterKey = ""
		if !r.opts.DryRun {
			state.StagingEpoch = snapshot.Head.WriterEpoch
			state.StagingAfterKey = ""
			if err := r.saveState(ctx, state, token); err != nil {
				return err
			}
		}
	}
	prefix := r.layout.PartitionStagingPrefix(r.opts.StreamID, state.Partition)
	if afterKey == "" {
		afterKey = prefix
	}
	now := r.now().UTC()

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
			return r.completeStaging(ctx, state, token)
		}

		lastProcessed := afterKey
		checkedHead := false
		for _, object := range page.Objects {
			parsed, err := r.layout.ParseStagingKey(r.opts.StreamID, state.Partition, object.Key)
			if err != nil {
				budget.invalid()
				lastProcessed = object.Key
				continue
			}
			if parsed.WriterEpoch >= snapshot.Head.WriterEpoch || !oldEnough(object.CreatedAt, now, r.opts.DeleteDelay) {
				lastProcessed = object.Key
				continue
			}
			budget.recordCandidate()
			size := objectSize(object)
			if !budget.canDelete(size) {
				if !r.opts.DryRun && lastProcessed != state.StagingAfterKey {
					state.StagingAfterKey = lastProcessed
					return r.saveState(ctx, state, token)
				}
				return nil
			}
			if r.opts.DryRun {
				lastProcessed = object.Key
				continue
			}
			if !checkedHead {
				fresh, err := r.catalog.LoadMaintenanceSnapshot(ctx, state.Partition)
				if err != nil {
					return err
				}
				if err := r.validateSnapshot(fresh, state.Partition); err != nil {
					return err
				}
				if fresh.Head.WriterEpoch < snapshot.Head.WriterEpoch {
					return nil
				}
				checkedHead = true
			}
			if err := r.backend.Delete(ctx, object.Key); err != nil {
				if lastProcessed != state.StagingAfterKey {
					state.StagingAfterKey = lastProcessed
					_ = r.saveState(ctx, state, token)
				}
				return err
			}
			budget.recordDelete(size)
			lastProcessed = object.Key
		}

		afterKey = lastProcessed
		if r.opts.DryRun {
			if !page.HasMore {
				return nil
			}
			continue
		}
		state.StagingAfterKey = lastProcessed
		if !page.HasMore {
			return r.completeStaging(ctx, state, token)
		}
		if err := r.saveState(ctx, state, token); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reclaimer) completeStaging(ctx context.Context, state *stateFile, token *string) error {
	if r.opts.DryRun {
		return nil
	}
	// Resetting means the next scheduled pass can catch a stale object that
	// became visible after this listing completed.
	state.StagingAfterKey = ""
	return r.saveState(ctx, state, token)
}

func oldEnough(createdAt, now time.Time, delay time.Duration) bool {
	return !createdAt.IsZero() && !createdAt.After(now) && !createdAt.Add(delay).After(now)
}
