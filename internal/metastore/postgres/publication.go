package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

// applyNewChunkPublication is the connector-neutral SQL kernel. The caller
// owns tx and must persist its direct-producer or Kafka checkpoint in the same
// transaction before committing. Exact replay is deliberately handled by that
// ingress-specific caller before this function is reached.
func applyNewChunkPublication(ctx context.Context, tx pgx.Tx,
	publication metastore.ChunkPublication,
) (metastore.ChunkPublicationResult, error) {
	if err := metastore.ValidateChunkPublication(publication); err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	state, err := lockPublicationShard(ctx, tx, publication.Fence.Shard)
	if err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	keys := make([]metastore.TimelineKey, len(publication.Mutations))
	for i := range publication.Mutations {
		keys[i] = publication.Mutations[i].Key
	}
	stored, err := queryHeads(ctx, tx, keys, true)
	if err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	plan, err := planNewPublication(state, publication, stored)
	if err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	publicationHash := metastore.HashChunkPublication(publication)
	if err := insertChunk(ctx, tx, publication.Chunk, publicationHash); err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	if err := writeHeads(ctx, tx, publication.Chunk.NamespaceHash,
		publication.Chunk.Shard, plan.heads); err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	if err := advanceChunkSequence(ctx, tx, state, plan.nextChunkSequence); err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	result := metastore.ChunkPublicationResult{
		Heads: headsFromPlan(plan), WriterFenceActive: true,
	}
	if err := metastore.ValidateChunkPublicationResult(publication, result); err != nil {
		return metastore.ChunkPublicationResult{}, err
	}
	return result, nil
}

func lockPublicationShard(ctx context.Context, tx pgx.Tx,
	key metastore.ShardKey,
) (metastore.ShardState, error) {
	hash := key.Namespace.Hash()
	var namespaceBytes, epochBytes, ownerBytes, nextBytes []byte
	err := tx.QueryRow(ctx, `
		SELECT n.namespace_key, w.writer_epoch, w.writer_owner, w.next_chunk_sequence
		FROM unijord_metastore.shard_writers w
		JOIN unijord_metastore.namespaces n USING (namespace_hash)
		WHERE w.namespace_hash = $1 AND w.shard = $2
		FOR UPDATE OF w`, hash[:], int64(key.Shard)).Scan(
		&namespaceBytes, &epochBytes, &ownerBytes, &nextBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return metastore.ShardState{}, classifyMissingShardControl(ctx, tx, key)
	}
	if err != nil {
		return metastore.ShardState{}, fmt.Errorf("metastore/postgres: lock shard writer: %w", err)
	}
	if !bytes.Equal(namespaceBytes, key.Namespace.Bytes()) {
		return metastore.ShardState{}, fmt.Errorf("%w: namespace digest collision", metastore.ErrCorrupt)
	}

	var materializedBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT materialized_before
		FROM unijord_metastore.shard_materializers
		WHERE namespace_hash = $1 AND shard = $2`, hash[:], int64(key.Shard)).Scan(&materializedBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return metastore.ShardState{}, fmt.Errorf("%w: materializer control row is missing", metastore.ErrCorrupt)
	}
	if err != nil {
		return metastore.ShardState{}, fmt.Errorf("metastore/postgres: read materializer cursor: %w", err)
	}

	epoch, err := decodeUint64(epochBytes)
	if err != nil {
		return metastore.ShardState{}, err
	}
	next, err := decodeUint64(nextBytes)
	if err != nil {
		return metastore.ShardState{}, err
	}
	materialized, err := decodeUint64(materializedBytes)
	if err != nil {
		return metastore.ShardState{}, err
	}
	state := metastore.ShardState{
		Fence: metastore.WriterFence{
			Shard: key, Epoch: epoch, Owner: metastore.OwnerIDFromString(string(ownerBytes)),
		},
		NextChunkSequence: next, MaterializedBefore: materialized,
	}
	if err := metastore.ValidateShardState(state); err != nil {
		return metastore.ShardState{}, fmt.Errorf("%w: invalid durable shard state: %v",
			metastore.ErrCorrupt, err)
	}
	return state, nil
}

func classifyMissingShardControl(ctx context.Context, tx pgx.Tx,
	key metastore.ShardKey,
) error {
	hash := key.Namespace.Hash()
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM unijord_metastore.shards
			WHERE namespace_hash = $1 AND shard = $2
		)`, hash[:], int64(key.Shard)).Scan(&exists); err != nil {
		return fmt.Errorf("metastore/postgres: check missing shard control: %w", err)
	}
	if !exists {
		return metastore.ErrNotFound
	}
	return fmt.Errorf("%w: writer control row is missing", metastore.ErrCorrupt)
}

func advanceChunkSequence(ctx context.Context, tx pgx.Tx, before metastore.ShardState,
	next uint64,
) error {
	hash := before.Fence.Shard.Namespace.Hash()
	tag, err := tx.Exec(ctx, `
		UPDATE unijord_metastore.shard_writers
		SET next_chunk_sequence = $3, updated_at = transaction_timestamp()
		WHERE namespace_hash = $1 AND shard = $2 AND next_chunk_sequence = $4`,
		hash[:], int64(before.Fence.Shard.Shard), encodeUint64(next),
		encodeUint64(before.NextChunkSequence))
	if err != nil {
		return fmt.Errorf("metastore/postgres: advance chunk sequence: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: shard writer changed while locked", metastore.ErrCorrupt)
	}
	return nil
}
