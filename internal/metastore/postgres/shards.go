package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

// Shard reads one consistent writer/materializer state without taking a
// publication lock.
func (s *Store) Shard(ctx context.Context, key metastore.ShardKey) (metastore.ShardState, error) {
	if err := s.checkContext(ctx); err != nil {
		return metastore.ShardState{}, err
	}
	if err := metastore.ValidateShardKey(key); err != nil {
		return metastore.ShardState{}, err
	}
	hash := key.Namespace.Hash()
	var namespaceBytes, epochBytes, ownerBytes, nextBytes, materializedBytes []byte
	var shard int64
	err := s.pool.QueryRow(ctx, `
		SELECT n.namespace_key, s.shard, w.writer_epoch, w.writer_owner,
		       w.next_chunk_sequence, m.materialized_before
		FROM unijord_metastore.shards s
		JOIN unijord_metastore.namespaces n USING (namespace_hash)
		LEFT JOIN unijord_metastore.shard_writers w USING (namespace_hash, shard)
		LEFT JOIN unijord_metastore.shard_materializers m USING (namespace_hash, shard)
		WHERE s.namespace_hash = $1 AND s.shard = $2`, hash[:], int64(key.Shard)).Scan(
		&namespaceBytes, &shard, &epochBytes, &ownerBytes, &nextBytes, &materializedBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return metastore.ShardState{}, metastore.ErrNotFound
	}
	if err != nil {
		return metastore.ShardState{}, fmt.Errorf("metastore/postgres: read shard: %w", err)
	}
	if shard < 0 || shard > math.MaxUint32 || uint32(shard) != key.Shard ||
		!bytes.Equal(namespaceBytes, key.Namespace.Bytes()) {
		return metastore.ShardState{}, fmt.Errorf("%w: shard identity mismatch", metastore.ErrCorrupt)
	}
	if epochBytes == nil || ownerBytes == nil || nextBytes == nil || materializedBytes == nil {
		return metastore.ShardState{}, fmt.Errorf("%w: shard control row is missing", metastore.ErrCorrupt)
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
		Fence: metastore.WriterFence{Shard: key, Epoch: epoch,
			Owner: metastore.OwnerIDFromString(string(ownerBytes))},
		NextChunkSequence: next, MaterializedBefore: materialized,
	}
	if err := metastore.ValidateShardState(state); err != nil {
		return metastore.ShardState{}, fmt.Errorf("%w: invalid durable shard state: %v",
			metastore.ErrCorrupt, err)
	}
	return state, nil
}
