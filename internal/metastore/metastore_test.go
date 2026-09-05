package metastore_test

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/chunkref"
	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/ankur-anand/unijord/internal/packref"
	"github.com/ankur-anand/unijord/internal/timelineindex"
	"github.com/ankur-anand/unijord/internal/ujpk"
	"github.com/ankur-anand/unijord/internal/ujtc"
)

func TestTimelineIdentityOwnsBytesAndKeepsExistingHashContract(t *testing.T) {
	namespaceBytes := []byte("tenant-a")
	timelineBytes := []byte("order-8891")
	namespace := metastore.CopyNamespace(namespaceBytes)
	key := metastore.CopyTimelineKey(namespace, timelineBytes)

	namespaceBytes[0] = 'X'
	timelineBytes[0] = 'X'
	if got := string(namespace.Bytes()); got != "tenant-a" {
		t.Fatalf("namespace after source mutation = %q", got)
	}
	if got := string(key.Bytes()); got != "order-8891" {
		t.Fatalf("timeline after source mutation = %q", got)
	}

	legacyNamespace := timelineindex.CopyNamespace([]byte("tenant-a"))
	legacyKey := timelineindex.CopyKey(legacyNamespace, []byte("order-8891"))
	if key.Hash() != legacyKey.Hash() {
		t.Fatal("metastore timeline hash changed existing UJPK routing identity")
	}
	if namespace.Hash() != legacyNamespace.Hash() {
		t.Fatal("metastore namespace hash changed UJTC namespace identity")
	}
	if err := metastore.ValidateTimelineKey(key); err != nil {
		t.Fatalf("ValidateTimelineKey() error = %v", err)
	}
}

func TestOwnerIdentityOwnsBytes(t *testing.T) {
	source := []byte("writer-a")
	owner := metastore.CopyOwnerID(source)
	source[0] = 'X'
	if got := string(owner.Bytes()); got != "writer-a" {
		t.Fatalf("owner after source mutation = %q", got)
	}
	returned := owner.Bytes()
	returned[0] = 'Y'
	if got := string(owner.Bytes()); got != "writer-a" {
		t.Fatalf("owner after returned-byte mutation = %q", got)
	}
	if err := metastore.ValidateOwnerID(owner); err != nil {
		t.Fatalf("ValidateOwnerID() error = %v", err)
	}
}

func TestChunkPublicationValidationAndHashAreCanonical(t *testing.T) {
	publication := validPublication()
	if err := metastore.ValidateChunkPublication(publication); err != nil {
		t.Fatalf("ValidateChunkPublication() error = %v", err)
	}
	if got, want := metastore.HashChunkPublication(publication),
		digestHex(t, "4c6e96d103d3380b175dece26ef146f210dcdf06cf80ca12996e464b7055cc2e"); got != want {
		t.Fatalf("chunk publication compatibility hash = %x want=%x", got, want)
	}

	reordered := publication
	reordered.Mutations = []metastore.TimelineMutation{publication.Mutations[1], publication.Mutations[0]}
	if got, want := metastore.HashChunkPublication(reordered), metastore.HashChunkPublication(publication); got != want {
		t.Fatalf("hash depends on mutation input order: got=%x want=%x", got, want)
	}

	sealed := publication
	sealed.Mutations = append([]metastore.TimelineMutation(nil), publication.Mutations...)
	sealed.Mutations[0].SealAfterAppend = true
	if metastore.HashChunkPublication(sealed) == metastore.HashChunkPublication(publication) {
		t.Fatal("seal transition is absent from publication identity")
	}

	successorOwner := publication
	successorOwner.Fence.Owner = metastore.OwnerIDFromString("writer-b")
	if metastore.HashChunkPublication(successorOwner) == metastore.HashChunkPublication(publication) {
		t.Fatal("writer owner is absent from publication identity")
	}

	duplicate := publication
	duplicate.Mutations = []metastore.TimelineMutation{publication.Mutations[0], publication.Mutations[0]}
	if err := metastore.ValidateChunkPublication(duplicate); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate timeline error = %v", err)
	}
}

func TestNewChunkPublicationEnforcesFenceBeforeSequenceAndTailCapacity(t *testing.T) {
	publication := validPublication()
	state := metastore.ShardState{Fence: publication.Fence, NextChunkSequence: 0, MaterializedBefore: 0}
	if err := metastore.ValidateNewChunkPublication(state, publication); err != nil {
		t.Fatalf("ValidateNewChunkPublication() error = %v", err)
	}

	stale := state
	stale.Fence.Owner = metastore.OwnerIDFromString("writer-b")
	stale.NextChunkSequence = 99
	if err := metastore.ValidateNewChunkPublication(stale, publication); !errors.Is(err, metastore.ErrStaleWriter) {
		t.Fatalf("stale fence precedence error = %v", err)
	}

	full := publication
	full.Chunk.Sequence = metastore.MaxActiveTailChunks
	fullState := metastore.ShardState{
		Fence: publication.Fence, NextChunkSequence: metastore.MaxActiveTailChunks, MaterializedBefore: 0,
	}
	if err := metastore.ValidateNewChunkPublication(fullState, full); !errors.Is(err, metastore.ErrTailFull) {
		t.Fatalf("tail capacity error = %v", err)
	}
}

func TestDirectCommitOperationsExactlyCoverTimelineMutation(t *testing.T) {
	publication := oneTimelinePublication(4)
	request := metastore.CommitDirectChunkRequest{
		Publication: publication,
		Operations: []metastore.DirectAppendOperation{
			{Producer: producer(1), OperationHash: digest(11), Timeline: publication.Mutations[0].Key,
				FirstLSN: 0, LastLSN: 1},
			{Producer: producer(2), OperationHash: digest(12), Timeline: publication.Mutations[0].Key,
				FirstLSN: 2, LastLSN: 3},
		},
	}
	if err := metastore.ValidateCommitDirectChunk(request); err != nil {
		t.Fatalf("ValidateCommitDirectChunk() error = %v", err)
	}
	if got, want := metastore.HashDirectChunkCommit(request),
		digestHex(t, "9d7aec5eb71b316f0bf5d8d12c539da8042e0c699a299550169b7ea1ca8f593f"); got != want {
		t.Fatalf("direct chunk compatibility hash = %x want=%x", got, want)
	}

	reordered := request
	reordered.Operations = []metastore.DirectAppendOperation{request.Operations[1], request.Operations[0]}
	if got, want := metastore.HashDirectChunkCommit(reordered), metastore.HashDirectChunkCommit(request); got != want {
		t.Fatalf("direct hash depends on operation input order: got=%x want=%x", got, want)
	}

	gapped := request
	gapped.Operations = append([]metastore.DirectAppendOperation(nil), request.Operations...)
	gapped.Operations[1].FirstLSN = 3
	if err := metastore.ValidateCommitDirectChunk(gapped); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("gapped operations error = %v", err)
	}

	duplicateProducer := request
	duplicateProducer.Operations = append([]metastore.DirectAppendOperation(nil), request.Operations...)
	duplicateProducer.Operations[1].Producer = duplicateProducer.Operations[0].Producer
	if err := metastore.ValidateCommitDirectChunk(duplicateProducer); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate producer error = %v", err)
	}
}

func TestDirectSealCommitHasCanonicalIdentity(t *testing.T) {
	publication := oneTimelinePublication(1)
	request := metastore.CommitDirectSealRequest{
		Producer: producer(1), OperationHash: digest(12), Fence: publication.Fence,
		Timeline: publication.Mutations[0].Key, ExpectedNextLSN: 1,
	}
	if err := metastore.ValidateCommitDirectSeal(request); err != nil {
		t.Fatalf("ValidateCommitDirectSeal() error = %v", err)
	}
	base := metastore.HashDirectSealCommit(request)
	if want := digestHex(t, "3ee25e3a375c3b7afaf27ffa357475cdf9754dbeb2e47ae6c54fad7ca36b06e8"); base != want {
		t.Fatalf("direct seal compatibility hash = %x want=%x", base, want)
	}
	changedOwner := request
	changedOwner.Fence.Owner = metastore.OwnerIDFromString("writer-b")
	if metastore.HashDirectSealCommit(changedOwner) == base {
		t.Fatal("seal hash omits writer owner")
	}
	changedLSN := request
	changedLSN.ExpectedNextLSN++
	if metastore.HashDirectSealCommit(changedLSN) == base {
		t.Fatal("seal hash omits expected next LSN")
	}
	result := metastore.CommitDirectSealResult{
		Producer: request.Producer,
		Head: metastore.TimelineHead{
			Key: request.Timeline, Shard: request.Fence.Shard.Shard, NextLSN: request.ExpectedNextLSN,
			LastTimestampMS: 10, State: metastore.TimelineSealed, Revision: 2,
		},
		Replayed: true,
	}
	if err := metastore.ValidateCommitDirectSealResult(request, result); err != nil {
		t.Fatalf("ValidateCommitDirectSealResult() error = %v", err)
	}
}

func TestKafkaCommitBindsSourceIntervalToPublication(t *testing.T) {
	publication := validPublication()
	request := metastore.CommitKafkaChunkRequest{
		Source: metastore.KafkaSourceExpectation{
			Mapping: metastore.KafkaPartitionMapping{
				BindingID: bindingID(1), TopicID: topicID(2), Partition: 17, MappingGeneration: 3,
				Shard: publication.Fence.Shard,
			},
			OwnerEpoch:         4,
			ExpectedNextOffset: 500, ResultingNextOffset: 520,
			LeaderEpoch: metastore.KafkaLeaderEpoch{Value: 8, Valid: true},
		},
		Publication: publication,
	}
	if err := metastore.ValidateCommitKafkaChunk(request); err != nil {
		t.Fatalf("ValidateCommitKafkaChunk() error = %v", err)
	}
	if got, want := metastore.HashKafkaChunkCommit(request),
		digestHex(t, "6781bf29633791a99f5de22c8feeec4c2544c6f8618e2c3ec0d12ac8bdf2aa3e"); got != want {
		t.Fatalf("Kafka chunk compatibility hash = %x want=%x", got, want)
	}

	changed := request
	changed.Source.ResultingNextOffset++
	if metastore.HashKafkaChunkCommit(changed) == metastore.HashKafkaChunkCommit(request) {
		t.Fatal("Kafka source interval is absent from commit identity")
	}

	invalid := request
	invalid.Source.ResultingNextOffset = invalid.Source.ExpectedNextOffset
	if err := metastore.ValidateCommitKafkaChunk(invalid); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("non-progressing Kafka cursor error = %v", err)
	}

	wrongShard := request
	wrongShard.Source.Mapping.Shard.Shard++
	if err := metastore.ValidateCommitKafkaChunk(wrongShard); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("wrong mapped shard error = %v", err)
	}

	changedMapping := request
	changedMapping.Source.Mapping.MappingGeneration++
	if metastore.HashKafkaChunkCommit(changedMapping) == metastore.HashKafkaChunkCommit(request) {
		t.Fatal("Kafka mapping generation is absent from commit identity")
	}

	durable := metastore.KafkaPartitionLease{
		Mapping: request.Source.Mapping, SourceOwnerEpoch: request.Source.OwnerEpoch,
		NextOffset: request.Source.ExpectedNextOffset,
		Writer: metastore.ShardState{
			Fence: request.Publication.Fence, NextChunkSequence: request.Publication.Chunk.Sequence,
		},
	}
	if err := metastore.ValidateNewKafkaChunk(durable, request); err != nil {
		t.Fatalf("ValidateNewKafkaChunk() error = %v", err)
	}
	staleSource := durable
	staleSource.SourceOwnerEpoch++
	staleSource.Writer.NextChunkSequence++
	if err := metastore.ValidateNewKafkaChunk(staleSource, request); !errors.Is(err, metastore.ErrStaleSource) {
		t.Fatalf("stale source precedence error = %v", err)
	}
	staleWriter := durable
	staleWriter.Writer.Fence.Owner = metastore.OwnerIDFromString("writer-b")
	staleWriter.NextOffset++
	if err := metastore.ValidateNewKafkaChunk(staleWriter, request); !errors.Is(err, metastore.ErrStaleWriter) {
		t.Fatalf("stale Kafka writer precedence error = %v", err)
	}
}

func TestKafkaPartitionMappingIsOneToOne(t *testing.T) {
	namespace := metastore.CopyNamespace([]byte("tenant-a"))
	mappings := []metastore.KafkaPartitionMapping{
		{BindingID: bindingID(1), TopicID: topicID(2), Partition: 0, MappingGeneration: 1,
			Shard: metastore.ShardKey{Namespace: namespace, Shard: 100}},
		{BindingID: bindingID(1), TopicID: topicID(2), Partition: 1, MappingGeneration: 1,
			Shard: metastore.ShardKey{Namespace: namespace, Shard: 103}},
	}
	if err := metastore.ValidateKafkaPartitionMappings(2, mappings); err != nil {
		t.Fatalf("ValidateKafkaPartitionMappings() error = %v", err)
	}

	duplicatePartition := append([]metastore.KafkaPartitionMapping(nil), mappings...)
	duplicatePartition[1].Partition = duplicatePartition[0].Partition
	if err := metastore.ValidateKafkaPartitionMappings(2, duplicatePartition); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate partition error = %v", err)
	}

	duplicateShard := append([]metastore.KafkaPartitionMapping(nil), mappings...)
	duplicateShard[1].Shard = duplicateShard[0].Shard
	if err := metastore.ValidateKafkaPartitionMappings(2, duplicateShard); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate shard error = %v", err)
	}

	missingPartition := mappings[:1]
	if err := metastore.ValidateKafkaPartitionMappings(2, missingPartition); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("incomplete partition mapping error = %v", err)
	}
}

func TestKafkaActivationKeepsPartitionAndWriterMappingTogether(t *testing.T) {
	namespace := metastore.CopyNamespace([]byte("tenant-a"))
	owner := metastore.OwnerIDFromString("connector-a")
	request := metastore.KafkaActivationRequest{
		BindingID: bindingID(1), TopicID: topicID(2), ConfigurationHash: digest(3),
		Owner: owner, Partitions: []int32{17},
	}
	mapping := metastore.KafkaPartitionMapping{
		BindingID: request.BindingID, TopicID: request.TopicID, Partition: 17, MappingGeneration: 1,
		Shard: metastore.ShardKey{Namespace: namespace, Shard: 103},
	}
	lease := metastore.KafkaPartitionLease{
		Mapping: mapping, SourceOwnerEpoch: 2, NextOffset: 500,
		Writer: metastore.ShardState{
			Fence: metastore.WriterFence{Shard: mapping.Shard, Epoch: 4, Owner: owner},
		},
	}
	if err := metastore.ValidateKafkaActivationResult(request, []metastore.KafkaPartitionLease{lease}); err != nil {
		t.Fatalf("ValidateKafkaActivationResult() error = %v", err)
	}

	crossWired := lease
	crossWired.Writer.Fence.Shard.Shard = 3
	if err := metastore.ValidateKafkaActivationResult(request, []metastore.KafkaPartitionLease{crossWired}); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("cross-wired activation error = %v", err)
	}
}

func TestPackPublicationRequiresExactSourceCoverage(t *testing.T) {
	publication := validPublication()
	publication.Chunk.RecordCount = 4
	publication.Chunk.MaxTimestampMS = 13
	publication.Mutations[1].LastLSN = 1
	publication.Mutations[1].LastTimestampMS = 13
	pack := packref.Ref{
		ID: objectID(9), Key: "packs/9.ujpk", FormatVersion: ujpk.Version,
		NamespaceHash: publication.Chunk.NamespaceHash, Shard: publication.Chunk.Shard,
		FirstChunkSequence: 0, LastChunkSequence: 0,
		RecordCount: publication.Chunk.RecordCount, TimelineCount: publication.Chunk.TimelineCount,
		SizeBytes: 4096, SHA256: digest(21),
	}
	request := metastore.PublishPackRequest{
		Shard: publication.Fence.Shard, ExpectedMaterializedBefore: 0, Pack: pack,
		Ranges: []metastore.TimelinePackRange{
			{Key: publication.Mutations[0].Key, FirstLSN: 0, LastLSN: 1},
			{Key: publication.Mutations[1].Key, FirstLSN: 0, LastLSN: 1},
		},
		SourceChunks: []metastore.SourceChunkCoverage{{
			Chunk: publication.Chunk, Mutations: publication.Mutations,
		}},
		DeleteNotBeforeMS: 1000,
	}
	if err := metastore.ValidatePublishPack(request); err != nil {
		t.Fatalf("ValidatePublishPack() error = %v", err)
	}
	if got, want := metastore.HashPackPublication(request),
		digestHex(t, "0b1dc72aec987654dc0623ddad43e552fe1b94b36ebabca40dedf98069d3646f"); got != want {
		t.Fatalf("pack compatibility hash = %x want=%x", got, want)
	}
	if got, want := metastore.HashSourceChunkCoverage(request.SourceChunks[0]),
		digestHex(t, "2a5c819685158ceedf3a41f21cee267b3bf5f01fc41bb23986959b3ee199cfec"); got != want {
		t.Fatalf("source coverage compatibility hash = %x want=%x", got, want)
	}

	overclaim := request
	overclaim.Ranges = append([]metastore.TimelinePackRange(nil), request.Ranges...)
	overclaim.Ranges[0].LastLSN = 2
	overclaim.Ranges[1].LastLSN = 0 // Total records remain four, but per-timeline coverage is false.
	if err := metastore.ValidatePublishPack(overclaim); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("overclaimed range error = %v", err)
	}

	negativeDeadline := request
	negativeDeadline.DeleteNotBeforeMS = -1
	if err := metastore.ValidatePublishPack(negativeDeadline); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("negative delete deadline error = %v", err)
	}
	changedDeadline := request
	changedDeadline.DeleteNotBeforeMS++
	if metastore.HashPackPublication(changedDeadline) != metastore.HashPackPublication(request) {
		t.Fatal("cleanup deadline changed canonical pack publication identity")
	}
}

func TestActivationRequestsRejectDuplicateOwnershipDomains(t *testing.T) {
	namespace := metastore.CopyNamespace([]byte("tenant-a"))
	direct := metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("runtime-a"),
		Shards: []metastore.ShardKey{
			{Namespace: namespace, Shard: 3},
			{Namespace: namespace, Shard: 3},
		},
	}
	if err := metastore.ValidateDirectShardClaim(direct); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate direct shard error = %v", err)
	}
	materializer := metastore.MaterializerClaimRequest{Owner: direct.Owner, Shards: direct.Shards}
	if err := metastore.ValidateMaterializerClaim(materializer); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate materializer shard error = %v", err)
	}

	kafka := metastore.KafkaActivationRequest{
		BindingID: bindingID(1), TopicID: topicID(2), ConfigurationHash: digest(3),
		Owner:      metastore.OwnerIDFromString("runtime-a"),
		Partitions: []int32{4, 7, 4},
	}
	if err := metastore.ValidateKafkaActivation(kafka); !errors.Is(err, metastore.ErrInvalidRequest) {
		t.Fatalf("duplicate Kafka partition error = %v", err)
	}
}

func TestCommitResultsDescribeTheAcknowledgedPostState(t *testing.T) {
	publication := oneTimelinePublication(1)
	head := metastore.TimelineHead{
		Key: publication.Mutations[0].Key, Shard: publication.Fence.Shard.Shard,
		NextLSN: 1, LastTimestampMS: publication.Mutations[0].LastTimestampMS,
		State: metastore.TimelineOpen, Revision: 1,
	}
	publicationResult := metastore.ChunkPublicationResult{Heads: []metastore.TimelineHead{head}}
	if err := metastore.ValidateChunkPublicationResult(publication, publicationResult); err != nil {
		t.Fatalf("ValidateChunkPublicationResult() error = %v", err)
	}

	request := metastore.CommitDirectChunkRequest{
		Publication: publication,
		Operations: []metastore.DirectAppendOperation{{
			Producer: producer(1), OperationHash: digest(2), Timeline: publication.Mutations[0].Key,
			FirstLSN: 0, LastLSN: 0,
		}},
	}
	direct := metastore.CommitDirectChunkResult{
		Heads: []metastore.TimelineHead{head},
		Receipts: []metastore.DirectReceipt{{
			Producer: request.Operations[0].Producer, Timeline: request.Operations[0].Timeline,
			FirstLSN: 0, LastLSN: 0, NextLSN: 1, Head: head,
		}},
	}
	if err := metastore.ValidateCommitDirectChunkResult(request, direct); err != nil {
		t.Fatalf("ValidateCommitDirectChunkResult() error = %v", err)
	}

	bad := publicationResult
	bad.Heads = append([]metastore.TimelineHead(nil), publicationResult.Heads...)
	bad.Heads[0].NextLSN = 2
	if err := metastore.ValidateChunkPublicationResult(publication, bad); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("invalid acknowledged head error = %v", err)
	}
}

func TestReadPlanRequiresForwardProgress(t *testing.T) {
	publication := oneTimelinePublication(3)
	key := publication.Mutations[0].Key
	pack := packref.Ref{
		ID: objectID(3), Key: "packs/3.ujpk", FormatVersion: ujpk.Version,
		NamespaceHash: key.Namespace().Hash(), Shard: publication.Fence.Shard.Shard,
		FirstChunkSequence: 0, LastChunkSequence: 0, RecordCount: 3, TimelineCount: 1,
		SizeBytes: 4096, SHA256: digest(4),
	}
	plan := metastore.ReadPlan{
		Head: metastore.TimelineHead{
			Key: key, Shard: publication.Fence.Shard.Shard, NextLSN: 3,
			LastTimestampMS: 12, State: metastore.TimelineOpen, Revision: 1,
		},
		PackRanges:     []metastore.TimelinePackRange{{Key: key, FirstLSN: 0, LastLSN: 0, Pack: pack}},
		MorePackRanges: true, NextPackLSN: 1,
	}
	if err := metastore.ValidateReadPlan(key, 0, 1, plan); err != nil {
		t.Fatalf("ValidateReadPlan() error = %v", err)
	}

	emptyMore := plan
	emptyMore.PackRanges = nil
	if err := metastore.ValidateReadPlan(key, 0, 1, emptyMore); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("empty more page error = %v", err)
	}
	nonAdvancing := plan
	nonAdvancing.NextPackLSN = 0
	if err := metastore.ValidateReadPlan(key, 0, 1, nonAdvancing); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("non-advancing page error = %v", err)
	}
	belowRequest := plan
	belowRequest.MorePackRanges = false
	belowRequest.NextPackLSN = 0
	if err := metastore.ValidateReadPlan(key, 1, 1, belowRequest); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("range below requested LSN error = %v", err)
	}
}

func validPublication() metastore.ChunkPublication {
	namespace := metastore.CopyNamespace([]byte("tenant-a"))
	keyA := metastore.CopyTimelineKey(namespace, []byte("A"))
	keyB := metastore.CopyTimelineKey(namespace, []byte("B"))
	return metastore.ChunkPublication{
		Fence: metastore.WriterFence{
			Shard: metastore.ShardKey{Namespace: namespace, Shard: 3},
			Epoch: 7, Owner: metastore.OwnerIDFromString("writer-a"),
		},
		Chunk: chunkref.Ref{
			Key: "chunks/0.ujtc", FormatVersion: ujtc.Version,
			NamespaceHash: namespace.Hash(), Shard: 3, WriterEpoch: 7, Sequence: 0,
			RecordCount: 3, TimelineCount: 2,
			SizeBytes:      ujtc.HeaderSize + ujtc.RecordHeaderSize + 32,
			MinTimestampMS: 10, MaxTimestampMS: 12, SHA256: digest(7),
		},
		Mutations: []metastore.TimelineMutation{
			{Key: keyA, ExpectedNextLSN: 0, LastLSN: 1, FirstTimestampMS: 10, LastTimestampMS: 11},
			{Key: keyB, ExpectedNextLSN: 0, LastLSN: 0, FirstTimestampMS: 12, LastTimestampMS: 12},
		},
	}
}

func oneTimelinePublication(records uint32) metastore.ChunkPublication {
	namespace := metastore.CopyNamespace([]byte("tenant-a"))
	key := metastore.CopyTimelineKey(namespace, []byte("A"))
	return metastore.ChunkPublication{
		Fence: metastore.WriterFence{
			Shard: metastore.ShardKey{Namespace: namespace, Shard: 3},
			Epoch: 7, Owner: metastore.OwnerIDFromString("writer-a"),
		},
		Chunk: chunkref.Ref{
			Key: "chunks/0.ujtc", FormatVersion: ujtc.Version,
			NamespaceHash: namespace.Hash(), Shard: 3, WriterEpoch: 7, Sequence: 0,
			RecordCount: records, TimelineCount: 1,
			SizeBytes:      ujtc.HeaderSize + ujtc.RecordHeaderSize + uint64(records),
			MinTimestampMS: 10, MaxTimestampMS: 10 + int64(records) - 1, SHA256: digest(7),
		},
		Mutations: []metastore.TimelineMutation{{
			Key: key, ExpectedNextLSN: 0, LastLSN: uint64(records) - 1,
			FirstTimestampMS: 10, LastTimestampMS: 10 + int64(records) - 1,
		}},
	}
}

func producer(value byte) metastore.ProducerPosition {
	return metastore.ProducerPosition{
		ProducerID: producerID(value), IncarnationID: producerIncarnationID(value + 20), Epoch: 1,
	}
}

func producerID(value byte) metastore.ProducerID {
	var result metastore.ProducerID
	result[len(result)-1] = value
	return result
}

func producerIncarnationID(value byte) metastore.ProducerIncarnationID {
	var result metastore.ProducerIncarnationID
	result[len(result)-1] = value
	return result
}

func bindingID(value byte) metastore.KafkaBindingID {
	var result metastore.KafkaBindingID
	result[len(result)-1] = value
	return result
}

func topicID(value byte) metastore.KafkaTopicID {
	var result metastore.KafkaTopicID
	result[len(result)-1] = value
	return result
}

func objectID(value byte) [16]byte {
	var result [16]byte
	result[len(result)-1] = value
	return result
}

func digest(value byte) [32]byte {
	var result [32]byte
	result[len(result)-1] = value
	return result
}

func digestHex(t *testing.T, value string) [32]byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("invalid test digest %q: bytes=%d error=%v", value, len(decoded), err)
	}
	var result [32]byte
	copy(result[:], decoded)
	return result
}
