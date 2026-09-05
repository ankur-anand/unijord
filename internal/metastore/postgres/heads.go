package postgres

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

// Head returns one timeline's logical high-water and lifecycle state.
func (s *Store) Head(ctx context.Context, key metastore.TimelineKey) (metastore.TimelineHead, error) {
	if err := s.checkContext(ctx); err != nil {
		return metastore.TimelineHead{}, err
	}
	if err := metastore.ValidateTimelineKey(key); err != nil {
		return metastore.TimelineHead{}, err
	}
	stored, err := queryHeads(ctx, s.pool, []metastore.TimelineKey{key}, false)
	if err != nil {
		return metastore.TimelineHead{}, err
	}
	head, exists := stored[key.Hash()]
	if !exists {
		return metastore.TimelineHead{}, metastore.ErrNotFound
	}
	return head, nil
}

// LookupHeads performs one set-oriented lookup and returns results aligned
// with keys. Missing timelines are represented by Found=false.
func (s *Store) LookupHeads(ctx context.Context, keys []metastore.TimelineKey) ([]metastore.HeadLookup, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	if err := metastore.ValidateHeadLookup(keys); err != nil {
		return nil, err
	}
	stored, err := queryHeads(ctx, s.pool, keys, false)
	if err != nil {
		return nil, err
	}
	result := make([]metastore.HeadLookup, len(keys))
	for i, key := range keys {
		if head, exists := stored[key.Hash()]; exists {
			result[i] = metastore.HeadLookup{Found: true, Head: head}
		}
	}
	return result, nil
}

func queryHeads(ctx context.Context, db querier, keys []metastore.TimelineKey,
	forUpdate bool,
) (map[[32]byte]metastore.TimelineHead, error) {
	requested := make(map[[32]byte]metastore.TimelineKey, len(keys))
	hashes := make([][]byte, 0, len(keys))
	for _, key := range keys {
		hash := key.Hash()
		if prior, exists := requested[hash]; exists {
			if !prior.Equal(key) {
				return nil, fmt.Errorf("%w: requested timeline digest collision", metastore.ErrCorrupt)
			}
			continue
		}
		requested[hash] = key
		hashes = append(hashes, hash[:])
	}

	query := `
		SELECT h.key_hash, n.namespace_key, h.timeline_key, h.shard,
		       h.next_lsn, h.last_timestamp_ms, h.state, h.revision
		FROM unijord_metastore.timeline_heads h
		JOIN unijord_metastore.namespaces n USING (namespace_hash)
		WHERE h.key_hash = ANY($1::bytea[])
		ORDER BY h.key_hash`
	if forUpdate {
		query += " FOR UPDATE OF h"
	}
	rows, err := db.Query(ctx, query, hashes)
	if err != nil {
		return nil, fmt.Errorf("metastore/postgres: read timeline heads: %w", err)
	}
	defer rows.Close()

	result := make(map[[32]byte]metastore.TimelineHead, len(keys))
	for rows.Next() {
		var hashBytes, namespaceBytes, timelineBytes, nextBytes, revisionBytes []byte
		var shard, timestamp int64
		var state int16
		if err := rows.Scan(&hashBytes, &namespaceBytes, &timelineBytes, &shard,
			&nextBytes, &timestamp, &state, &revisionBytes); err != nil {
			return nil, fmt.Errorf("metastore/postgres: scan timeline head: %w", err)
		}
		hash, err := decodeHash(hashBytes)
		if err != nil {
			return nil, err
		}
		key, exists := requested[hash]
		if !exists || !bytes.Equal(namespaceBytes, key.Namespace().Bytes()) ||
			!bytes.Equal(timelineBytes, key.Bytes()) {
			return nil, fmt.Errorf("%w: timeline digest collision", metastore.ErrCorrupt)
		}
		head, err := decodeTimelineHead(key, shard, nextBytes, timestamp, state, revisionBytes)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result[hash]; duplicate {
			return nil, fmt.Errorf("%w: duplicate timeline head", metastore.ErrCorrupt)
		}
		result[hash] = head
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metastore/postgres: iterate timeline heads: %w", err)
	}
	return result, nil
}

func decodeTimelineHead(key metastore.TimelineKey, shard int64, nextBytes []byte,
	timestamp int64, state int16, revisionBytes []byte,
) (metastore.TimelineHead, error) {
	if shard < 0 || shard > math.MaxUint32 {
		return metastore.TimelineHead{}, fmt.Errorf("%w: timeline shard=%d", metastore.ErrCorrupt, shard)
	}
	next, err := decodeUint64(nextBytes)
	if err != nil {
		return metastore.TimelineHead{}, err
	}
	revision, err := decodeUint64(revisionBytes)
	if err != nil {
		return metastore.TimelineHead{}, err
	}
	head := metastore.TimelineHead{
		Key: key, Shard: uint32(shard), NextLSN: next, LastTimestampMS: timestamp,
		State: metastore.TimelineState(state), Revision: revision,
	}
	if err := metastore.ValidateTimelineHead(head); err != nil {
		return metastore.TimelineHead{}, fmt.Errorf("%w: invalid stored timeline head: %v",
			metastore.ErrCorrupt, err)
	}
	return head, nil
}

func writeHeads(ctx context.Context, tx pgx.Tx, namespaceHash [32]byte, shard uint32,
	writes []headWrite,
) error {
	var insertHashes, insertKeys, insertNext, insertRevisions [][]byte
	var insertTimestamps []int64
	var insertStates []int16
	var updateHashes, updateNext, updateRevisions [][]byte
	var updateTimestamps []int64
	var updateStates []int16

	for _, write := range writes {
		if err := metastore.ValidateTimelineHead(write.head); err != nil {
			return fmt.Errorf("%w: planned timeline head: %v", metastore.ErrCorrupt, err)
		}
		hashBytes := bytes.Clone(write.hash[:])
		if write.insert {
			insertHashes = append(insertHashes, hashBytes)
			insertKeys = append(insertKeys, write.head.Key.Bytes())
			insertNext = append(insertNext, encodeUint64(write.head.NextLSN))
			insertTimestamps = append(insertTimestamps, write.head.LastTimestampMS)
			insertStates = append(insertStates, int16(write.head.State))
			insertRevisions = append(insertRevisions, encodeUint64(write.head.Revision))
			continue
		}
		updateHashes = append(updateHashes, hashBytes)
		updateNext = append(updateNext, encodeUint64(write.head.NextLSN))
		updateTimestamps = append(updateTimestamps, write.head.LastTimestampMS)
		updateStates = append(updateStates, int16(write.head.State))
		updateRevisions = append(updateRevisions, encodeUint64(write.head.Revision))
	}

	if len(insertHashes) != 0 {
		// Missing heads cannot be row-locked by queryHeads. Acquire their
		// unique-index locks in hash order as well, even across shard writers.
		tag, err := tx.Exec(ctx, `
			INSERT INTO unijord_metastore.timeline_heads(
				key_hash, namespace_hash, timeline_key, shard, next_lsn,
				last_timestamp_ms, state, revision)
			SELECT input.key_hash, $1, input.timeline_key, $2, input.next_lsn,
			       input.last_timestamp_ms, input.state, input.revision
			FROM unnest($3::bytea[], $4::bytea[], $5::bytea[], $6::bigint[],
			            $7::smallint[], $8::bytea[])
				  AS input(key_hash, timeline_key, next_lsn, last_timestamp_ms, state, revision)
				ORDER BY input.key_hash
				ON CONFLICT (key_hash) DO NOTHING`, namespaceHash[:], int64(shard),
			insertHashes, insertKeys, insertNext, insertTimestamps, insertStates, insertRevisions)
		if err != nil {
			return fmt.Errorf("metastore/postgres: insert timeline heads: %w", err)
		}
		if tag.RowsAffected() != int64(len(insertHashes)) {
			return fmt.Errorf("%w: timeline was concurrently created", metastore.ErrConflict)
		}
	}
	if len(updateHashes) != 0 {
		tag, err := tx.Exec(ctx, `
			UPDATE unijord_metastore.timeline_heads stored
			SET next_lsn = input.next_lsn,
			    last_timestamp_ms = input.last_timestamp_ms,
			    state = input.state,
			    revision = input.revision,
			    updated_at = transaction_timestamp()
			FROM unnest($1::bytea[], $2::bytea[], $3::bigint[], $4::smallint[], $5::bytea[])
			  AS input(key_hash, next_lsn, last_timestamp_ms, state, revision)
			WHERE stored.key_hash = input.key_hash`, updateHashes, updateNext,
			updateTimestamps, updateStates, updateRevisions)
		if err != nil {
			return fmt.Errorf("metastore/postgres: update timeline heads: %w", err)
		}
		if tag.RowsAffected() != int64(len(updateHashes)) {
			return fmt.Errorf("%w: updated timeline heads=%d want=%d", metastore.ErrCorrupt,
				tag.RowsAffected(), len(updateHashes))
		}
	}
	return nil
}
