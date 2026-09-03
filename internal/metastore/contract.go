package metastore

import "fmt"

// ValidateChunkPublicationResult verifies the implementation-independent
// shape of a successful publication result. Heads are aligned with
// publication.Mutations and describe the exact acknowledged post-state, even
// when Replayed is true and the durable timelines have since advanced.
func ValidateChunkPublicationResult(publication ChunkPublication, result ChunkPublicationResult) error {
	if err := ValidateChunkPublication(publication); err != nil {
		return err
	}
	if len(result.Heads) != len(publication.Mutations) {
		return fmt.Errorf("%w: result heads=%d mutations=%d", ErrCorrupt,
			len(result.Heads), len(publication.Mutations))
	}
	for i, mutation := range publication.Mutations {
		head := result.Heads[i]
		if err := validateMutationHead(publication.Fence.Shard.Shard, mutation, head); err != nil {
			return fmt.Errorf("%w: result head=%d: %v", ErrCorrupt, i, err)
		}
	}
	return nil
}

// ValidateCommitDirectChunkResult verifies a direct commit acknowledgement.
// Receipts are aligned with request.Operations; Heads are aligned with
// request.Publication.Mutations.
func ValidateCommitDirectChunkResult(request CommitDirectChunkRequest, result CommitDirectChunkResult) error {
	if err := ValidateCommitDirectChunk(request); err != nil {
		return err
	}
	publicationResult := ChunkPublicationResult{
		Heads:             result.Heads,
		Replayed:          result.Replayed,
		WriterFenceActive: result.WriterFenceActive,
	}
	if err := ValidateChunkPublicationResult(request.Publication, publicationResult); err != nil {
		return err
	}
	if len(result.Receipts) != len(request.Operations) {
		return fmt.Errorf("%w: direct receipts=%d operations=%d", ErrCorrupt,
			len(result.Receipts), len(request.Operations))
	}
	heads := make(map[[32]byte]TimelineHead, len(result.Heads))
	for _, head := range result.Heads {
		heads[head.Key.Hash()] = head
	}
	for i, operation := range request.Operations {
		receipt := result.Receipts[i]
		wantHead, exists := heads[operation.Timeline.Hash()]
		if !exists || receipt.Producer != operation.Producer ||
			!receipt.Timeline.Equal(operation.Timeline) ||
			receipt.FirstLSN != operation.FirstLSN || receipt.LastLSN != operation.LastLSN ||
			receipt.NextLSN != operation.LastLSN+1 || receipt.Replayed != result.Replayed ||
			!SameTimelineHead(wantHead, receipt.Head) {
			return fmt.Errorf("%w: direct receipt=%d does not acknowledge operation", ErrCorrupt, i)
		}
	}
	return nil
}

// ValidateCommitDirectSealResult verifies the stable result of a direct seal.
func ValidateCommitDirectSealResult(request CommitDirectSealRequest, result CommitDirectSealResult) error {
	if err := ValidateCommitDirectSeal(request); err != nil {
		return err
	}
	if result.Producer != request.Producer || !result.Head.Key.Equal(request.Timeline) ||
		result.Head.Shard != request.Fence.Shard.Shard ||
		result.Head.NextLSN != request.ExpectedNextLSN || result.Head.State != TimelineSealed ||
		result.Head.Revision == 0 {
		return fmt.Errorf("%w: direct seal result does not acknowledge request", ErrCorrupt)
	}
	return ValidateTimelineHead(result.Head)
}

// ValidateCommitKafkaChunkResult verifies Kafka cursor advancement and the
// exact acknowledged timeline post-state.
func ValidateCommitKafkaChunkResult(request CommitKafkaChunkRequest, result CommitKafkaChunkResult) error {
	if err := ValidateCommitKafkaChunk(request); err != nil {
		return err
	}
	publicationResult := ChunkPublicationResult{
		Heads:             result.Heads,
		Replayed:          result.Replayed,
		WriterFenceActive: result.WriterFenceActive,
	}
	if err := ValidateChunkPublicationResult(request.Publication, publicationResult); err != nil {
		return err
	}
	if result.NextOffset != request.Source.ResultingNextOffset {
		return fmt.Errorf("%w: Kafka result offset=%d want=%d", ErrCorrupt,
			result.NextOffset, request.Source.ResultingNextOffset)
	}
	return nil
}

// SameTimelineHead compares logical head state rather than internal identity
// pointers.
func SameTimelineHead(a, b TimelineHead) bool {
	return a.Key.Equal(b.Key) && a.Shard == b.Shard && a.NextLSN == b.NextLSN &&
		a.LastTimestampMS == b.LastTimestampMS && a.State == b.State && a.Revision == b.Revision
}

func validateMutationHead(shard uint32, mutation TimelineMutation, head TimelineHead) error {
	state := TimelineOpen
	if mutation.SealAfterAppend {
		state = TimelineSealed
	}
	if !head.Key.Equal(mutation.Key) || head.Shard != shard || head.NextLSN != mutation.LastLSN+1 ||
		head.LastTimestampMS != mutation.LastTimestampMS || head.State != state || head.Revision == 0 {
		return fmt.Errorf("head does not match mutation post-state")
	}
	return ValidateTimelineHead(head)
}
