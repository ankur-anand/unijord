package lifecycle

import (
	"context"
	"fmt"
	"sync"
)

// batchDeleteBackend is an optional backend capability. Results align with
// keys; a nil entry means that key was deleted or already absent.
type batchDeleteBackend interface {
	DeleteBatch(ctx context.Context, keys []string) []error
}

type deleteCandidate struct {
	key       string
	size      uint64
	beforeKey string
}

func (r *Reclaimer) executeDeletes(ctx context.Context, candidates []deleteCandidate, budget *runBudget) (string, error) {
	if len(candidates) == 0 || r.opts.DryRun {
		return "", nil
	}

	var firstFailed int = -1
	var firstErr error
	failed := 0
	for start := 0; start < len(candidates); start += r.opts.DeleteBatchSize {
		end := min(start+r.opts.DeleteBatchSize, len(candidates))
		errs := r.deleteWave(ctx, candidates[start:end])
		for i, err := range errs {
			candidate := candidates[start+i]
			if err == nil {
				budget.recordDelete(candidate.size)
				continue
			}
			failed++
			if firstFailed < 0 {
				firstFailed = start + i
				firstErr = err
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	if firstFailed < 0 {
		return "", nil
	}
	return candidates[firstFailed].beforeKey, fmt.Errorf(
		"lifecycle: delete failed objects=%d first_key=%q: %w",
		failed,
		candidates[firstFailed].key,
		firstErr,
	)
}

func (r *Reclaimer) deleteWave(ctx context.Context, candidates []deleteCandidate) []error {
	keys := make([]string, len(candidates))
	for i := range candidates {
		keys[i] = candidates[i].key
	}
	if backend, ok := r.backend.(batchDeleteBackend); ok {
		if err := r.waitForDeleteBudget(ctx, len(keys)); err != nil {
			return repeatedDeleteError(len(keys), err)
		}
		errs := backend.DeleteBatch(ctx, keys)
		if len(errs) == len(keys) {
			return errs
		}
		err := fmt.Errorf("lifecycle: batch delete returned %d results for %d keys", len(errs), len(keys))
		errs = make([]error, len(keys))
		for i := range errs {
			errs[i] = err
		}
		return errs
	}

	errs := make([]error, len(candidates))
	jobs := make(chan int, len(candidates))
	for i := range candidates {
		jobs <- i
	}
	close(jobs)

	workers := min(r.opts.DeleteConcurrency, len(candidates))
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := r.waitForDeleteBudget(ctx, 1); err != nil {
					errs[i] = err
					continue
				}
				errs[i] = r.backend.Delete(ctx, candidates[i].key)
			}
		}()
	}
	wg.Wait()
	return errs
}

func (r *Reclaimer) waitForDeleteBudget(ctx context.Context, objects int) error {
	if r.opts.DeleteRateLimiter == nil {
		return nil
	}
	if err := r.opts.DeleteRateLimiter.Wait(ctx, objects); err != nil {
		return fmt.Errorf("lifecycle: wait for delete rate budget: %w", err)
	}
	return nil
}

func repeatedDeleteError(count int, err error) []error {
	errs := make([]error, count)
	for i := range errs {
		errs[i] = err
	}
	return errs
}

func (b *runBudget) canScheduleDelete(size, scheduledCount, scheduledBytes uint64) bool {
	if uint64(b.result.DeletedObjects)+scheduledCount >= uint64(b.opts.MaxDeletesPerRun) {
		b.exhausted = true
		return false
	}
	if size > ^uint64(0)-scheduledBytes || b.result.DeletedBytes > ^uint64(0)-scheduledBytes {
		b.exhausted = true
		return false
	}
	usedBytes := b.result.DeletedBytes + scheduledBytes
	if b.result.DeletedObjects+int(scheduledCount) > 0 {
		if usedBytes >= b.opts.MaxDeleteBytes || size > b.opts.MaxDeleteBytes-usedBytes {
			b.exhausted = true
			return false
		}
	}
	return true
}
