package lifecycle

import (
	"context"
	"fmt"
	"time"

	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
)

type segmentScrubObject struct {
	object ObjectInfo
	parsed segmentsink.SegmentObjectKey
}

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
	remainingBudget := r.opts.MaxObjectsPerRun - budget.result.ScannedObjects
	if remainingBudget <= 0 {
		budget.exhausted = true
		return nil
	}
	type quarantineCheck struct {
		parsed catalogblob.PageObjectKey
	}
	checksByLevel := make(map[uint8][]catalogPageScrubObject)
	checks := make(map[int]quarantineCheck)
	for i, candidate := range state.PageQuarantine {
		if len(checks) >= remainingBudget {
			break
		}
		if now.Before(timeFromUnixMilli(candidate.ObservedMS).Add(r.opts.DeleteDelay)) {
			continue
		}
		parsed, err := catalogblob.ParsePagePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, candidate.Key)
		if err != nil {
			return fmt.Errorf("%w: quarantine page: %v", ErrCorruptState, err)
		}
		checks[i] = quarantineCheck{parsed: parsed}
		checksByLevel[parsed.Level] = append(checksByLevel[parsed.Level], catalogPageScrubObject{
			object: ObjectInfo{Key: candidate.Key},
			parsed: parsed,
		})
	}
	type levelReachability struct {
		snapshot  catalogblob.MaintenanceSnapshot
		reachable map[string]struct{}
	}
	levels := make(map[uint8]levelReachability, len(checksByLevel))
	for level, objects := range checksByLevel {
		snapshot, reachable, err := r.reachablePagesForObjectPage(ctx, state.Partition, level, objects)
		if err != nil {
			return err
		}
		levels[level] = levelReachability{snapshot: snapshot, reachable: reachable}
	}

	remaining := make([]quarantineObject, 0, len(state.PageQuarantine))
	changed := false
	deleteCandidates := make([]deleteCandidate, 0, len(checks))
	var scheduledBytes uint64
	for i, candidate := range state.PageQuarantine {
		if now.Before(timeFromUnixMilli(candidate.ObservedMS).Add(r.opts.DeleteDelay)) {
			remaining = append(remaining, candidate)
			continue
		}
		check, selected := checks[i]
		if !selected || !budget.available() {
			remaining = append(remaining, candidate)
			continue
		}
		budget.recordScan(1)
		level := levels[check.parsed.Level]
		snapshot := level.snapshot
		if snapshot.Generation < candidate.ObservedGeneration {
			return fmt.Errorf("lifecycle: catalog generation regressed current=%d observed=%d", snapshot.Generation, candidate.ObservedGeneration)
		}
		if _, reachable := level.reachable[candidate.Key]; reachable {
			changed = true
			continue
		}
		if check.parsed.Generation >= snapshot.Generation {
			remaining = append(remaining, candidate)
			continue
		}
		budget.recordCandidate()
		if !r.opts.DryRun && !budget.canScheduleDelete(candidate.SizeBytes, uint64(len(deleteCandidates)), scheduledBytes) {
			remaining = append(remaining, candidate)
			continue
		}
		if r.opts.DryRun {
			remaining = append(remaining, candidate)
			continue
		}
		deleteCandidates = append(deleteCandidates, deleteCandidate{
			key: candidate.Key, size: candidate.SizeBytes,
		})
		scheduledBytes += candidate.SizeBytes
	}
	if len(deleteCandidates) > 0 {
		if _, err := r.executeDeletes(ctx, deleteCandidates, budget); err != nil {
			return err
		}
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

		parsedObjects := make([]segmentScrubObject, 0, len(page.Objects))
		for _, object := range page.Objects {
			parsed, err := r.layout.ParseSegmentKey(r.opts.StreamID, state.Partition, object.Key)
			if err != nil {
				budget.invalid()
				continue
			}
			parsedObjects = append(parsedObjects, segmentScrubObject{object: object, parsed: parsed})
		}

		fresh := snapshot
		referenced := map[string]struct{}{}
		if len(parsedObjects) > 0 {
			var err error
			fresh, referenced, err = r.referencedSegmentsForObjectPage(ctx, state.Partition, parsedObjects)
			if err != nil {
				return false, err
			}
		}

		lastProcessed := afterKey
		candidates := make([]deleteCandidate, 0, len(parsedObjects))
		var scheduledBytes uint64
		budgetStopped := false
		for _, candidate := range parsedObjects {
			object := candidate.object
			parsed := candidate.parsed
			if !oldEnough(object.CreatedAt, now, r.opts.DeleteDelay) || !orphanSegmentEligible(parsed.BaseLSN, parsed.WriterEpoch, state.SafeFloorLSN, fresh) {
				lastProcessed = object.Key
				continue
			}
			if _, ok := referenced[object.Key]; ok {
				lastProcessed = object.Key
				continue
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
			checkpoint, err := r.executeDeletes(ctx, candidates, budget)
			if err != nil {
				_ = r.checkpointOrphanSegment(ctx, state, token, checkpoint)
				return false, err
			}
		}
		if budgetStopped {
			return true, r.checkpointOrphanSegment(ctx, state, token, lastProcessed)
		}
		if len(page.Objects) > 0 && lastProcessed < page.Objects[len(page.Objects)-1].Key {
			lastProcessed = page.Objects[len(page.Objects)-1].Key
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

func (r *Reclaimer) referencedSegmentsForObjectPage(ctx context.Context, partition uint32, objects []segmentScrubObject) (catalogblob.MaintenanceSnapshot, map[string]struct{}, error) {
	fromLSN := objects[0].parsed.BaseLSN
	throughLSN := fromLSN
	for _, object := range objects[1:] {
		if object.parsed.BaseLSN < fromLSN {
			fromLSN = object.parsed.BaseLSN
		}
		if object.parsed.BaseLSN > throughLSN {
			throughLSN = object.parsed.BaseLSN
		}
	}
	physicalKeys := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		physicalKeys[object.object.Key] = struct{}{}
	}
	referenced := make(map[string]struct{}, len(objects))
	var observed catalogblob.MaintenanceSnapshot
	hasObserved := false
	for next := fromLSN; ; {
		snapshot, page, err := r.catalog.ListMaintenanceSegments(ctx, catalog.ListSegmentsRequest{
			Partition: partition,
			FromLSN:   next,
			Limit:     catalog.MaxSegmentPageLimit,
		})
		if err != nil {
			return catalogblob.MaintenanceSnapshot{}, nil, err
		}
		if err := r.validateSnapshot(snapshot, partition); err != nil {
			return catalogblob.MaintenanceSnapshot{}, nil, err
		}
		if !hasObserved {
			observed = snapshot
			hasObserved = true
		} else if snapshot.Generation != observed.Generation {
			return catalogblob.MaintenanceSnapshot{}, nil, fmt.Errorf("lifecycle: catalog changed during segment scrub generation=%d current=%d", observed.Generation, snapshot.Generation)
		}
		for _, segment := range page.Segments {
			if segment.BaseLSN > throughLSN {
				break
			}
			if _, ok := physicalKeys[segment.URI]; ok {
				referenced[segment.URI] = struct{}{}
			}
		}
		if !page.HasMore || page.NextLSN > throughLSN {
			return observed, referenced, nil
		}
		if page.NextLSN <= next {
			return catalogblob.MaintenanceSnapshot{}, nil, fmt.Errorf("lifecycle: non-advancing catalog segment page next_lsn=%d from_lsn=%d", page.NextLSN, next)
		}
		next = page.NextLSN
	}
}

func orphanSegmentEligible(baseLSN, writerEpoch, safeFloorLSN uint64, snapshot catalogblob.MaintenanceSnapshot) bool {
	// Retained-history reclamation owns this range because it waits for the
	// delayed safe floor. Treating old retained objects as ordinary orphans
	// would let object creation age bypass that read-safety delay.
	if baseLSN < snapshot.Head.OldestLSN && baseLSN >= safeFloorLSN {
		return false
	}
	if writerEpoch > snapshot.Head.WriterEpoch {
		return false
	}
	return baseLSN < snapshot.Head.NextLSN || writerEpoch < snapshot.Head.WriterEpoch
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

		parsedObjects := make([]catalogPageScrubObject, 0, len(page.Objects))
		for _, object := range page.Objects {
			parsed, err := catalogblob.ParsePagePath(r.opts.CatalogPrefix, r.opts.StreamID, state.Partition, object.Key)
			if err != nil || parsed.Level != level {
				budget.invalid()
				continue
			}
			parsedObjects = append(parsedObjects, catalogPageScrubObject{object: object, parsed: parsed})
		}

		fresh := snapshot
		reachable := map[string]struct{}{}
		if len(parsedObjects) > 0 {
			var err error
			fresh, reachable, err = r.reachablePagesForObjectPage(ctx, state.Partition, level, parsedObjects)
			if err != nil {
				return false, err
			}
		}

		lastProcessed := afterKey
		for _, candidate := range parsedObjects {
			object := candidate.object
			parsed := candidate.parsed
			if parsed.Generation >= fresh.Generation {
				lastProcessed = object.Key
				continue
			}
			if _, exists := quarantined[object.Key]; exists {
				lastProcessed = object.Key
				continue
			}
			if _, ok := reachable[object.Key]; ok {
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
		if len(page.Objects) > 0 && lastProcessed < page.Objects[len(page.Objects)-1].Key {
			lastProcessed = page.Objects[len(page.Objects)-1].Key
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

type catalogPageScrubObject struct {
	object ObjectInfo
	parsed catalogblob.PageObjectKey
}

func (r *Reclaimer) reachablePagesForObjectPage(ctx context.Context, partition uint32, level uint8, objects []catalogPageScrubObject) (catalogblob.MaintenanceSnapshot, map[string]struct{}, error) {
	fromSeqLo := objects[0].parsed.SeqLo
	throughSeqLo := fromSeqLo
	physicalKeys := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		physicalKeys[object.object.Key] = struct{}{}
		if object.parsed.SeqLo < fromSeqLo {
			fromSeqLo = object.parsed.SeqLo
		}
		if object.parsed.SeqLo > throughSeqLo {
			throughSeqLo = object.parsed.SeqLo
		}
	}
	reachable := make(map[string]struct{}, len(objects))
	var observed catalogblob.MaintenanceSnapshot
	hasObserved := false
	for next := fromSeqLo; ; {
		snapshot, page, err := r.catalog.ListMaintenancePages(ctx, catalogblob.MaintenancePageRequest{
			Partition: partition,
			Level:     level,
			FromSeqLo: next,
			Limit:     catalog.MaxSegmentPageLimit,
		})
		if err != nil {
			return catalogblob.MaintenanceSnapshot{}, nil, err
		}
		if err := r.validateSnapshot(snapshot, partition); err != nil {
			return catalogblob.MaintenanceSnapshot{}, nil, err
		}
		if !hasObserved {
			observed = snapshot
			hasObserved = true
		} else if snapshot.Generation != observed.Generation {
			return catalogblob.MaintenanceSnapshot{}, nil, fmt.Errorf("lifecycle: catalog changed during page scrub generation=%d current=%d", observed.Generation, snapshot.Generation)
		}
		for _, path := range page.Paths {
			if _, ok := physicalKeys[path]; ok {
				reachable[path] = struct{}{}
			}
		}
		if !page.HasMore || page.NextSeqLo > throughSeqLo {
			return observed, reachable, nil
		}
		if page.NextSeqLo <= next {
			return catalogblob.MaintenanceSnapshot{}, nil, fmt.Errorf("lifecycle: non-advancing catalog page next_seq_lo=%d from_seq_lo=%d", page.NextSeqLo, next)
		}
		next = page.NextSeqLo
	}
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
