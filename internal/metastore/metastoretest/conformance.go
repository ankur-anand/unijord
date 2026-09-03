// Package metastoretest contains the reusable black-box conformance suite for
// metastore backends. A backend package should call RunDirect and RunKafka from
// its own tests with a fresh isolated harness for each subtest.
package metastoretest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ankur-anand/unijord/internal/chunkref"
	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/ankur-anand/unijord/internal/ujtc"
)

type DirectStore interface {
	metastore.Reader
	metastore.DirectActivator
	metastore.DirectCommitter
}

// DirectHarness exposes only the failure injection required to prove commit
// ambiguity. FailNextDirectChunkAfterApply must let the next direct chunk
// transaction commit durably, then return an error matching both
// metastore.ErrOutcomeUnknown and cause.
type DirectHarness interface {
	Store() DirectStore
	FailNextDirectChunkAfterApply(cause error)
	FailNextDirectSealAfterApply(cause error)
}

type DirectFactory func(testing.TB) DirectHarness

type KafkaStore interface {
	metastore.Reader
	metastore.KafkaActivator
	metastore.KafkaCommitter
}

type KafkaBindingFixture struct {
	Mapping           metastore.KafkaPartitionMapping
	ConfigurationHash [32]byte
	InitialOffset     int64
	LeaderEpoch       metastore.KafkaLeaderEpoch
}

// KafkaHarness installs test-only durable binding state and exposes one
// after-commit failure point. Production methods remain the narrow metastore
// interfaces.
type KafkaHarness interface {
	Store() KafkaStore
	InstallKafkaBinding(context.Context, KafkaBindingFixture) error
	FailNextKafkaChunkAfterApply(cause error)
}

type KafkaFactory func(testing.TB) KafkaHarness

var errLostCommitReply = errors.New("metastoretest: lost commit reply")

// RunDirect proves stable replay results, authority precedence, cancellation,
// and indeterminate-commit recovery for the direct protocol.
func RunDirect(t *testing.T, factory DirectFactory) {
	t.Helper()
	t.Run("replay-after-advance-seal-and-takeover", func(t *testing.T) {
		harness := factory(t)
		store := harness.Store()
		namespace := metastore.CopyNamespace([]byte("metastoretest-direct"))
		shard := metastore.ShardKey{Namespace: namespace, Shard: 7}
		firstLease := claimDirect(t, store, shard, "writer-a")
		timeline := metastore.CopyTimelineKey(namespace, []byte("timeline-a"))
		producer := testProducer(1)

		first := directRequest(firstLease.State.Fence, producer, timeline, 0, 0, false)
		firstResult, err := store.CommitDirectChunk(context.Background(), first)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitDirectChunkResult(first, firstResult))
		if firstResult.Replayed || !firstResult.WriterFenceActive {
			t.Fatalf("first commit flags = replayed:%t active:%t", firstResult.Replayed, firstResult.WriterFenceActive)
		}

		producer.Sequence++
		second := directRequest(firstLease.State.Fence, producer, timeline, 1, 1, true)
		second.Publication.Chunk.Sequence = 1
		secondResult, err := store.CommitDirectChunk(context.Background(), second)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitDirectChunkResult(second, secondResult))

		replay, err := store.CommitDirectChunk(context.Background(), first)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitDirectChunkResult(first, replay))
		if !replay.Replayed || !replay.WriterFenceActive || replay.Heads[0].State != metastore.TimelineOpen ||
			replay.Heads[0].NextLSN != 1 {
			t.Fatalf("replay did not preserve original acknowledgement: %+v", replay)
		}

		_ = claimDirect(t, store, shard, "writer-b")
		replay, err = store.CommitDirectChunk(context.Background(), first)
		requireNoError(t, err)
		if !replay.Replayed || replay.WriterFenceActive {
			t.Fatalf("replay after takeover flags = replayed:%t active:%t", replay.Replayed, replay.WriterFenceActive)
		}
		conflictingReplay := first
		conflictingReplay.Operations = append([]metastore.DirectAppendOperation(nil), first.Operations...)
		conflictingReplay.Operations[0].OperationHash = testDigest(98)
		if _, err := store.CommitDirectChunk(context.Background(), conflictingReplay); !errors.Is(err, metastore.ErrStaleWriter) {
			t.Fatalf("direct stale-writer replay-conflict precedence error = %v", err)
		}

		producer.Sequence++
		stale := directRequest(firstLease.State.Fence, producer, timeline, 2, 2, false)
		stale.Publication.Chunk.Sequence = 0 // A collision must not hide stale authority.
		if _, err := store.CommitDirectChunk(context.Background(), stale); !errors.Is(err, metastore.ErrStaleWriter) {
			t.Fatalf("stale writer precedence error = %v", err)
		}
	})

	t.Run("cancellation-and-unknown-outcome", func(t *testing.T) {
		harness := factory(t)
		store := harness.Store()
		namespace := metastore.CopyNamespace([]byte("metastoretest-direct-cancel"))
		shard := metastore.ShardKey{Namespace: namespace, Shard: 11}
		lease := claimDirect(t, store, shard, "writer-a")
		timeline := metastore.CopyTimelineKey(namespace, []byte("timeline-a"))
		request := directRequest(lease.State.Fence, testProducer(1), timeline, 0, 0, false)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.CommitDirectChunk(ctx, request); !errors.Is(err, context.Canceled) ||
			errors.Is(err, metastore.ErrOutcomeUnknown) {
			t.Fatalf("pre-attempt cancellation error = %v", err)
		}
		if _, err := store.Head(context.Background(), timeline); !errors.Is(err, metastore.ErrNotFound) {
			t.Fatalf("canceled commit changed head: %v", err)
		}

		harness.FailNextDirectChunkAfterApply(errLostCommitReply)
		if _, err := store.CommitDirectChunk(context.Background(), request); !errors.Is(err, metastore.ErrOutcomeUnknown) || !errors.Is(err, errLostCommitReply) {
			t.Fatalf("ambiguous direct commit error = %v", err)
		}
		replay, err := store.CommitDirectChunk(context.Background(), request)
		requireNoError(t, err)
		if !replay.Replayed {
			t.Fatal("exact retry did not reconcile direct commit")
		}
		requireNoError(t, metastore.ValidateCommitDirectChunkResult(request, replay))
	})

	t.Run("seal-replay-and-unknown-outcome", func(t *testing.T) {
		harness := factory(t)
		store := harness.Store()
		namespace := metastore.CopyNamespace([]byte("metastoretest-direct-seal"))
		shard := metastore.ShardKey{Namespace: namespace, Shard: 13}
		lease := claimDirect(t, store, shard, "writer-a")
		timeline := metastore.CopyTimelineKey(namespace, []byte("timeline-a"))
		producer := testProducer(1)
		appendRequest := directRequest(lease.State.Fence, producer, timeline, 0, 0, false)
		_, err := store.CommitDirectChunk(context.Background(), appendRequest)
		requireNoError(t, err)

		producer.Sequence++
		seal := metastore.CommitDirectSealRequest{
			Producer: producer, OperationHash: testDigest(71), Fence: lease.State.Fence,
			Timeline: timeline, ExpectedNextLSN: 1,
		}
		harness.FailNextDirectSealAfterApply(context.DeadlineExceeded)
		if _, err := store.CommitDirectSeal(context.Background(), seal); !errors.Is(err, metastore.ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ambiguous direct seal error = %v", err)
		}
		replay, err := store.CommitDirectSeal(context.Background(), seal)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitDirectSealResult(seal, replay))
		if !replay.Replayed || !replay.WriterFenceActive {
			t.Fatalf("direct seal replay flags = %+v", replay)
		}

		_ = claimDirect(t, store, shard, "writer-b")
		replay, err = store.CommitDirectSeal(context.Background(), seal)
		requireNoError(t, err)
		if !replay.Replayed || replay.WriterFenceActive {
			t.Fatalf("direct seal replay after takeover flags = %+v", replay)
		}
		conflict := seal
		conflict.OperationHash = testDigest(72)
		if _, err := store.CommitDirectSeal(context.Background(), conflict); !errors.Is(err, metastore.ErrStaleWriter) {
			t.Fatalf("direct seal stale-writer precedence error = %v", err)
		}
	})
}

// RunKafka proves the same publication rules while also pinning durable
// partition-to-shard mapping and atomic Kafka-offset acknowledgement.
func RunKafka(t *testing.T, factory KafkaFactory) {
	t.Helper()
	t.Run("mapping-replay-authority-and-unknown-outcome", func(t *testing.T) {
		harness := factory(t)
		store := harness.Store()
		namespace := metastore.CopyNamespace([]byte("metastoretest-kafka"))
		mapping := metastore.KafkaPartitionMapping{
			BindingID: testBindingID(1), TopicID: testTopicID(2), Partition: 17,
			MappingGeneration: 1, Shard: metastore.ShardKey{Namespace: namespace, Shard: 103},
		}
		fixture := KafkaBindingFixture{
			Mapping: mapping, ConfigurationHash: testDigest(3), InitialOffset: 500,
			LeaderEpoch: metastore.KafkaLeaderEpoch{Value: 4, Valid: true},
		}
		requireNoError(t, harness.InstallKafkaBinding(context.Background(), fixture))
		firstLease := activateKafka(t, store, fixture, "connector-a")
		timeline := metastore.CopyTimelineKey(namespace, []byte("order-a"))

		first := kafkaRequest(firstLease, timeline, 500, 502, 0, 0, false)
		firstResult, err := store.CommitKafkaChunk(context.Background(), first)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitKafkaChunkResult(first, firstResult))

		secondLease := firstLease
		secondLease.NextOffset = 502
		secondLease.Writer.NextChunkSequence = 1
		second := kafkaRequest(secondLease, timeline, 502, 503, 1, 1, true)
		secondResult, err := store.CommitKafkaChunk(context.Background(), second)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitKafkaChunkResult(second, secondResult))

		replay, err := store.CommitKafkaChunk(context.Background(), first)
		requireNoError(t, err)
		requireNoError(t, metastore.ValidateCommitKafkaChunkResult(first, replay))
		if !replay.Replayed || !replay.AuthorityActive() || replay.NextOffset != 502 ||
			replay.Heads[0].State != metastore.TimelineOpen || replay.Heads[0].NextLSN != 1 {
			t.Fatalf("Kafka replay did not preserve original acknowledgement: %+v", replay)
		}

		newLease := activateKafka(t, store, fixture, "connector-b")
		replay, err = store.CommitKafkaChunk(context.Background(), first)
		requireNoError(t, err)
		if !replay.Replayed || replay.SourceOwnerActive || replay.WriterFenceActive {
			t.Fatalf("Kafka replay after takeover flags = %+v", replay)
		}
		conflictingReplay := first
		conflictingReplay.Publication.Chunk.SHA256 = testDigest(99)
		if _, err := store.CommitKafkaChunk(context.Background(), conflictingReplay); !errors.Is(err, metastore.ErrStaleSource) {
			t.Fatalf("Kafka stale source precedence error = %v", err)
		}

		staleWriter := kafkaRequest(newLease, timeline, 503, 504, 2, 2, false)
		staleWriter.Publication.Fence = firstLease.Writer.Fence
		staleWriter.Publication.Chunk.WriterEpoch = firstLease.Writer.Fence.Epoch
		staleWriter.Publication.Chunk.Sequence = 0
		if _, err := store.CommitKafkaChunk(context.Background(), staleWriter); !errors.Is(err, metastore.ErrStaleWriter) {
			t.Fatalf("Kafka stale writer precedence error = %v", err)
		}

		unknownTimeline := metastore.CopyTimelineKey(namespace, []byte("order-b"))
		unknown := kafkaRequest(newLease, unknownTimeline, 503, 504, 2, 0, false)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.CommitKafkaChunk(ctx, unknown); !errors.Is(err, context.Canceled) ||
			errors.Is(err, metastore.ErrOutcomeUnknown) {
			t.Fatalf("pre-attempt Kafka cancellation error = %v", err)
		}
		harness.FailNextKafkaChunkAfterApply(context.DeadlineExceeded)
		if _, err := store.CommitKafkaChunk(context.Background(), unknown); !errors.Is(err, metastore.ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ambiguous Kafka commit error = %v", err)
		}
		unknownReplay, err := store.CommitKafkaChunk(context.Background(), unknown)
		requireNoError(t, err)
		if !unknownReplay.Replayed || unknownReplay.NextOffset != 504 {
			t.Fatalf("Kafka exact retry did not reconcile: %+v", unknownReplay)
		}
	})
}

func claimDirect(t *testing.T, store DirectStore, shard metastore.ShardKey, owner string) metastore.DirectShardLease {
	t.Helper()
	leases, err := store.ClaimDirectShards(context.Background(), metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString(owner), Shards: []metastore.ShardKey{shard},
	})
	requireNoError(t, err)
	if len(leases) != 1 {
		t.Fatalf("direct leases=%d want=1", len(leases))
	}
	requireNoError(t, metastore.ValidateDirectShardClaimResult(metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString(owner), Shards: []metastore.ShardKey{shard},
	}, leases))
	return leases[0]
}

func activateKafka(t *testing.T, store KafkaStore, fixture KafkaBindingFixture, owner string) metastore.KafkaPartitionLease {
	t.Helper()
	leases, err := store.ActivateKafkaPartitions(context.Background(), metastore.KafkaActivationRequest{
		BindingID: fixture.Mapping.BindingID, TopicID: fixture.Mapping.TopicID,
		ConfigurationHash: fixture.ConfigurationHash, Owner: metastore.OwnerIDFromString(owner),
		Partitions: []int32{fixture.Mapping.Partition},
	})
	requireNoError(t, err)
	if len(leases) != 1 {
		t.Fatalf("Kafka leases=%d want=1", len(leases))
	}
	requireNoError(t, metastore.ValidateKafkaActivationResult(metastore.KafkaActivationRequest{
		BindingID: fixture.Mapping.BindingID, TopicID: fixture.Mapping.TopicID,
		ConfigurationHash: fixture.ConfigurationHash, Owner: metastore.OwnerIDFromString(owner),
		Partitions: []int32{fixture.Mapping.Partition},
	}, leases))
	return leases[0]
}

func directRequest(fence metastore.WriterFence, producer metastore.ProducerPosition, timeline metastore.TimelineKey,
	firstLSN, lastLSN uint64, seal bool,
) metastore.CommitDirectChunkRequest {
	publication := publicationAtSequence(fence, timeline, firstLSN, firstLSN, lastLSN, seal)
	return metastore.CommitDirectChunkRequest{
		Publication: publication,
		Operations: []metastore.DirectAppendOperation{{
			Producer: producer, OperationHash: testDigest(byte(40 + producer.Sequence)), Timeline: timeline,
			FirstLSN: firstLSN, LastLSN: lastLSN, SealAfterAppend: seal,
		}},
	}
}

func kafkaRequest(lease metastore.KafkaPartitionLease, timeline metastore.TimelineKey,
	firstOffset, nextOffset int64, chunkSequence, firstLSN uint64, seal bool,
) metastore.CommitKafkaChunkRequest {
	return metastore.CommitKafkaChunkRequest{
		Source: metastore.KafkaSourceExpectation{
			Mapping: lease.Mapping, OwnerEpoch: lease.SourceOwnerEpoch,
			ExpectedNextOffset: firstOffset, ResultingNextOffset: nextOffset, LeaderEpoch: lease.LeaderEpoch,
		},
		Publication: publicationAtSequence(lease.Writer.Fence, timeline, chunkSequence, firstLSN, firstLSN, seal),
	}
}

func publicationAtSequence(fence metastore.WriterFence, timeline metastore.TimelineKey,
	sequence, firstLSN, lastLSN uint64, seal bool,
) metastore.ChunkPublication {
	records := uint32(lastLSN - firstLSN + 1)
	return metastore.ChunkPublication{
		Fence: fence,
		Chunk: chunkref.Ref{
			Key: fmt.Sprintf("metastoretest/chunk-%020d.ujtc", sequence), FormatVersion: ujtc.Version,
			NamespaceHash: fence.Shard.Namespace.Hash(), Shard: fence.Shard.Shard,
			WriterEpoch: fence.Epoch, Sequence: sequence, RecordCount: records, TimelineCount: 1,
			SizeBytes:      ujtc.HeaderSize + ujtc.RecordHeaderSize + uint64(records),
			MinTimestampMS: 100 + int64(firstLSN), MaxTimestampMS: 100 + int64(lastLSN),
			SHA256: testDigest(byte(10 + sequence)),
		},
		Mutations: []metastore.TimelineMutation{{
			Key: timeline, ExpectedNextLSN: firstLSN, LastLSN: lastLSN,
			FirstTimestampMS: 100 + int64(firstLSN), LastTimestampMS: 100 + int64(lastLSN),
			SealAfterAppend: seal,
		}},
	}
}

func testProducer(value byte) metastore.ProducerPosition {
	var producerID metastore.ProducerID
	var incarnationID metastore.ProducerIncarnationID
	producerID[len(producerID)-1] = value
	incarnationID[len(incarnationID)-1] = value + 1
	return metastore.ProducerPosition{ProducerID: producerID, IncarnationID: incarnationID, Epoch: 1}
}

func testBindingID(value byte) metastore.KafkaBindingID {
	var result metastore.KafkaBindingID
	result[len(result)-1] = value
	return result
}

func testTopicID(value byte) metastore.KafkaTopicID {
	var result metastore.KafkaTopicID
	result[len(result)-1] = value
	return result
}

func testDigest(value byte) [32]byte {
	var result [32]byte
	result[len(result)-1] = value
	return result
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
