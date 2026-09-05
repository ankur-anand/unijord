package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

func lockShardIdentities(ctx context.Context, tx pgx.Tx, items []claimItem) error {
	keys := make([]int64, 0, len(items))
	for _, item := range items {
		value := binary.BigEndian.Uint64(item.identity.namespaceHash[:8]) ^ uint64(item.identity.shard)
		keys = append(keys, int64(value))
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	rows, err := tx.Query(ctx, `
		SELECT pg_advisory_xact_lock(lock_key)
		FROM unnest($1::bigint[]) AS requested(lock_key)
		ORDER BY lock_key`, keys)
	if err != nil {
		return fmt.Errorf("metastore/postgres: lock shard identities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ignored any
		if err := rows.Scan(&ignored); err != nil {
			return fmt.Errorf("metastore/postgres: scan shard identity lock: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("metastore/postgres: iterate shard identity locks: %w", err)
	}
	return nil
}

func ensureNamespaces(ctx context.Context, tx pgx.Tx, claims []namespaceClaim) error {
	hashes := make([][]byte, len(claims))
	keys := make([][]byte, len(claims))
	for i, claim := range claims {
		hashes[i] = claim.hash[:]
		keys[i] = claim.namespace.Bytes()
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO unijord_metastore.namespaces(namespace_hash, namespace_key)
		SELECT namespace_hash, namespace_key
		FROM unnest($1::bytea[], $2::bytea[]) AS requested(namespace_hash, namespace_key)
		ON CONFLICT (namespace_hash) DO NOTHING`, hashes, keys); err != nil {
		return fmt.Errorf("metastore/postgres: insert namespaces: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT requested.namespace_hash, stored.namespace_key
		FROM unnest($1::bytea[]) AS requested(namespace_hash)
		JOIN unijord_metastore.namespaces stored USING (namespace_hash)
		ORDER BY requested.namespace_hash`, hashes)
	if err != nil {
		return fmt.Errorf("metastore/postgres: verify namespaces: %w", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var hashBytes, exact []byte
		if err := rows.Scan(&hashBytes, &exact); err != nil {
			return fmt.Errorf("metastore/postgres: scan namespace: %w", err)
		}
		hash, err := decodeHash(hashBytes)
		if err != nil {
			return err
		}
		if seen >= len(claims) || claims[seen].hash != hash ||
			!bytes.Equal(claims[seen].namespace.Bytes(), exact) {
			return fmt.Errorf("%w: namespace digest collision", metastore.ErrCorrupt)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("metastore/postgres: iterate namespaces: %w", err)
	}
	if seen != len(claims) {
		return fmt.Errorf("%w: namespace rows=%d want=%d", metastore.ErrCorrupt, seen, len(claims))
	}
	return nil
}

func ensureShards(ctx context.Context, tx pgx.Tx, items []claimItem) ([]claimItem, error) {
	hashes, shards := claimArrays(items)
	rows, err := tx.Query(ctx, `
		INSERT INTO unijord_metastore.shards(namespace_hash, shard)
		SELECT namespace_hash, shard
		FROM unnest($1::bytea[], $2::bigint[]) AS requested(namespace_hash, shard)
		ON CONFLICT (namespace_hash, shard) DO NOTHING
		RETURNING namespace_hash, shard`, hashes, shards)
	if err != nil {
		return nil, fmt.Errorf("metastore/postgres: insert shards: %w", err)
	}
	defer rows.Close()
	byIdentity := make(map[shardIdentity]claimItem, len(items))
	for _, item := range items {
		byIdentity[item.identity] = item
	}
	created := make([]claimItem, 0, len(items))
	for rows.Next() {
		var hashBytes []byte
		var shard int64
		if err := rows.Scan(&hashBytes, &shard); err != nil {
			return nil, fmt.Errorf("metastore/postgres: scan inserted shard: %w", err)
		}
		hash, err := decodeHash(hashBytes)
		if err != nil || shard < 0 || shard > math.MaxUint32 {
			return nil, fmt.Errorf("%w: invalid inserted shard identity", metastore.ErrCorrupt)
		}
		item, exists := byIdentity[shardIdentity{namespaceHash: hash, shard: uint32(shard)}]
		if !exists {
			return nil, fmt.Errorf("%w: inserted unexpected shard", metastore.ErrCorrupt)
		}
		created = append(created, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metastore/postgres: iterate inserted shards: %w", err)
	}
	slices.SortFunc(created, compareClaimItems)
	return created, nil
}

func initializeShardControls(ctx context.Context, tx pgx.Tx, created []claimItem, owner metastore.OwnerID) error {
	if len(created) == 0 {
		return nil
	}
	hashes, shards := claimArrays(created)
	epochOne := encodeUint64(1)
	zero := encodeUint64(0)
	if tag, err := tx.Exec(ctx, `
		INSERT INTO unijord_metastore.shard_writers(
			namespace_hash, shard, writer_epoch, writer_owner, next_chunk_sequence)
		SELECT namespace_hash, shard, $3, $4, $5
		FROM unnest($1::bytea[], $2::bigint[]) AS requested(namespace_hash, shard)`,
		hashes, shards, epochOne, owner.Bytes(), zero); err != nil {
		return fmt.Errorf("metastore/postgres: initialize shard writers: %w", err)
	} else if tag.RowsAffected() != int64(len(created)) {
		return fmt.Errorf("%w: initialized writer rows=%d want=%d", metastore.ErrCorrupt,
			tag.RowsAffected(), len(created))
	}
	if tag, err := tx.Exec(ctx, `
		INSERT INTO unijord_metastore.shard_materializers(
			namespace_hash, shard, materializer_epoch, materializer_owner, materialized_before)
		SELECT namespace_hash, shard, $3, NULL, $3
		FROM unnest($1::bytea[], $2::bigint[]) AS requested(namespace_hash, shard)`,
		hashes, shards, zero); err != nil {
		return fmt.Errorf("metastore/postgres: initialize shard materializers: %w", err)
	} else if tag.RowsAffected() != int64(len(created)) {
		return fmt.Errorf("%w: initialized materializer rows=%d want=%d", metastore.ErrCorrupt,
			tag.RowsAffected(), len(created))
	}
	return nil
}

func lockShardWriters(ctx context.Context, tx pgx.Tx, items []claimItem) (map[shardIdentity]metastore.ShardState, error) {
	hashes, shards := claimArrays(items)
	rows, err := tx.Query(ctx, `
		SELECT w.namespace_hash, w.shard, w.writer_epoch, w.writer_owner,
		       w.next_chunk_sequence, m.materialized_before
		FROM unijord_metastore.shard_writers w
		JOIN unijord_metastore.shard_materializers m USING (namespace_hash, shard)
		JOIN unnest($1::bytea[], $2::bigint[]) AS requested(namespace_hash, shard)
		  ON requested.namespace_hash = w.namespace_hash AND requested.shard = w.shard
		ORDER BY w.namespace_hash, w.shard
		FOR UPDATE OF w`, hashes, shards)
	if err != nil {
		return nil, fmt.Errorf("metastore/postgres: lock shard writers: %w", err)
	}
	defer rows.Close()
	itemsByID := make(map[shardIdentity]claimItem, len(items))
	for _, item := range items {
		itemsByID[item.identity] = item
	}
	states := make(map[shardIdentity]metastore.ShardState, len(items))
	for rows.Next() {
		var hashBytes, epochBytes, ownerBytes, nextBytes, materializedBytes []byte
		var shard int64
		if err := rows.Scan(&hashBytes, &shard, &epochBytes, &ownerBytes, &nextBytes, &materializedBytes); err != nil {
			return nil, fmt.Errorf("metastore/postgres: scan shard writer: %w", err)
		}
		hash, err := decodeHash(hashBytes)
		if err != nil || shard < 0 || shard > math.MaxUint32 {
			return nil, fmt.Errorf("%w: invalid shard writer identity", metastore.ErrCorrupt)
		}
		identity := shardIdentity{namespaceHash: hash, shard: uint32(shard)}
		item, exists := itemsByID[identity]
		if !exists {
			return nil, fmt.Errorf("%w: locked unexpected shard writer", metastore.ErrCorrupt)
		}
		epoch, err := decodeUint64(epochBytes)
		if err != nil {
			return nil, err
		}
		next, err := decodeUint64(nextBytes)
		if err != nil {
			return nil, err
		}
		materialized, err := decodeUint64(materializedBytes)
		if err != nil {
			return nil, err
		}
		state := metastore.ShardState{
			Fence: metastore.WriterFence{Shard: item.key, Epoch: epoch,
				Owner: metastore.OwnerIDFromString(string(ownerBytes))},
			NextChunkSequence: next, MaterializedBefore: materialized,
		}
		if err := metastore.ValidateShardState(state); err != nil {
			return nil, fmt.Errorf("%w: invalid durable shard state: %v", metastore.ErrCorrupt, err)
		}
		if _, duplicate := states[identity]; duplicate {
			return nil, fmt.Errorf("%w: duplicate shard writer row", metastore.ErrCorrupt)
		}
		states[identity] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metastore/postgres: iterate shard writers: %w", err)
	}
	if len(states) != len(items) {
		return nil, fmt.Errorf("%w: shard control rows=%d want=%d", metastore.ErrCorrupt,
			len(states), len(items))
	}
	return states, nil
}

func updateWriterFences(ctx context.Context, tx pgx.Tx, states []metastore.ShardState) error {
	if len(states) == 0 {
		return nil
	}
	slices.SortFunc(states, func(a, b metastore.ShardState) int {
		aHash := a.Fence.Shard.Namespace.Hash()
		bHash := b.Fence.Shard.Namespace.Hash()
		if order := bytes.Compare(aHash[:], bHash[:]); order != 0 {
			return order
		}
		return compareUint32(a.Fence.Shard.Shard, b.Fence.Shard.Shard)
	})
	hashes := make([][]byte, len(states))
	shards := make([]int64, len(states))
	epochs := make([][]byte, len(states))
	owners := make([][]byte, len(states))
	for i, state := range states {
		hash := state.Fence.Shard.Namespace.Hash()
		hashes[i] = hash[:]
		shards[i] = int64(state.Fence.Shard.Shard)
		epochs[i] = encodeUint64(state.Fence.Epoch)
		owners[i] = state.Fence.Owner.Bytes()
	}
	tag, err := tx.Exec(ctx, `
		UPDATE unijord_metastore.shard_writers stored
		SET writer_epoch = requested.writer_epoch,
		    writer_owner = requested.writer_owner,
		    updated_at = transaction_timestamp()
		FROM unnest($1::bytea[], $2::bigint[], $3::bytea[], $4::bytea[])
		  AS requested(namespace_hash, shard, writer_epoch, writer_owner)
		WHERE stored.namespace_hash = requested.namespace_hash
		  AND stored.shard = requested.shard`, hashes, shards, epochs, owners)
	if err != nil {
		return fmt.Errorf("metastore/postgres: update writer fences: %w", err)
	}
	if tag.RowsAffected() != int64(len(states)) {
		return fmt.Errorf("%w: updated writer rows=%d want=%d", metastore.ErrCorrupt,
			tag.RowsAffected(), len(states))
	}
	return nil
}

func compareClaimItems(a, b claimItem) int {
	if order := bytes.Compare(a.identity.namespaceHash[:], b.identity.namespaceHash[:]); order != 0 {
		return order
	}
	return compareUint32(a.identity.shard, b.identity.shard)
}
