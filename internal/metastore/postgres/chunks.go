package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/chunkref"
	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

type storedChunk struct {
	ref             chunkref.Ref
	publicationHash [32]byte
}

func insertChunk(ctx context.Context, tx pgx.Tx, ref chunkref.Ref,
	publicationHash [32]byte,
) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO unijord_metastore.chunks(
			namespace_hash, shard, sequence, object_key, format_version,
			writer_epoch, record_count, timeline_count, object_size,
			min_timestamp_ms, max_timestamp_ms, object_sha256, publication_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (namespace_hash, shard, sequence) DO NOTHING`,
		ref.NamespaceHash[:], int64(ref.Shard), encodeUint64(ref.Sequence), ref.Key,
		int32(ref.FormatVersion), encodeUint64(ref.WriterEpoch), int64(ref.RecordCount),
		int64(ref.TimelineCount), encodeUint64(ref.SizeBytes), ref.MinTimestampMS,
		ref.MaxTimestampMS, ref.SHA256[:], publicationHash[:])
	if err != nil {
		return fmt.Errorf("metastore/postgres: insert chunk: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: chunk sequence=%d already exists",
			metastore.ErrCorrupt, ref.Sequence)
	}
	return nil
}

func queryChunk(ctx context.Context, db querier, namespaceHash [32]byte,
	shard uint32, sequence uint64,
) (storedChunk, error) {
	row := db.QueryRow(ctx, `
		SELECT object_key, format_version, writer_epoch, sequence,
		       record_count, timeline_count, object_size,
		       min_timestamp_ms, max_timestamp_ms, object_sha256,
		       publication_hash
		FROM unijord_metastore.chunks
		WHERE namespace_hash = $1 AND shard = $2 AND sequence = $3`,
		namespaceHash[:], int64(shard), encodeUint64(sequence))
	item, err := scanChunk(row, namespaceHash, shard)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedChunk{}, metastore.ErrNotFound
	}
	if err != nil {
		return storedChunk{}, fmt.Errorf("metastore/postgres: read chunk: %w", err)
	}
	return item, nil
}

func scanChunk(row pgx.Row, namespaceHash [32]byte, shard uint32) (storedChunk, error) {
	var item storedChunk
	var writerEpochBytes, sequenceBytes, sizeBytes, objectHashBytes, publicationHashBytes []byte
	var formatVersion, recordCount, timelineCount int64
	if err := row.Scan(&item.ref.Key, &formatVersion, &writerEpochBytes, &sequenceBytes,
		&recordCount, &timelineCount, &sizeBytes, &item.ref.MinTimestampMS,
		&item.ref.MaxTimestampMS, &objectHashBytes, &publicationHashBytes); err != nil {
		return storedChunk{}, err
	}
	if formatVersion < 0 || formatVersion > math.MaxUint16 ||
		recordCount < 0 || recordCount > math.MaxUint32 ||
		timelineCount < 0 || timelineCount > math.MaxUint32 {
		return storedChunk{}, fmt.Errorf("%w: invalid stored chunk scalar", metastore.ErrCorrupt)
	}
	writerEpoch, err := decodeUint64(writerEpochBytes)
	if err != nil {
		return storedChunk{}, err
	}
	sequence, err := decodeUint64(sequenceBytes)
	if err != nil {
		return storedChunk{}, err
	}
	size, err := decodeUint64(sizeBytes)
	if err != nil {
		return storedChunk{}, err
	}
	objectHash, err := decodeHash(objectHashBytes)
	if err != nil {
		return storedChunk{}, err
	}
	publicationHash, err := decodeHash(publicationHashBytes)
	if err != nil {
		return storedChunk{}, err
	}
	item.ref.FormatVersion = uint16(formatVersion)
	item.ref.NamespaceHash = namespaceHash
	item.ref.Shard = shard
	item.ref.WriterEpoch = writerEpoch
	item.ref.Sequence = sequence
	item.ref.RecordCount = uint32(recordCount)
	item.ref.TimelineCount = uint32(timelineCount)
	item.ref.SizeBytes = size
	item.ref.SHA256 = objectHash
	item.publicationHash = publicationHash
	if err := chunkref.Validate(item.ref); err != nil {
		return storedChunk{}, fmt.Errorf("%w: invalid stored chunk: %v", metastore.ErrCorrupt, err)
	}
	return item, nil
}
