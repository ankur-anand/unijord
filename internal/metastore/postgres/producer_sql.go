package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

func insertProducer(ctx context.Context, tx pgx.Tx, request metastore.OpenProducerRequest) error {
	hash := request.Key.Namespace.Hash()
	if err := ensureNamespaces(ctx, tx, []namespaceClaim{{hash: hash, namespace: request.Key.Namespace}}); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO unijord_metastore.producers(
			namespace_hash, producer_id, incarnation_id, epoch, next_sequence, state)
		VALUES ($1, $2, $3, $4, $5, 1)
		ON CONFLICT (namespace_hash, producer_id) DO NOTHING`,
		hash[:], request.Key.ID[:], request.IncarnationID[:], encodeUint64(1), encodeUint64(0))
	if err != nil {
		return fmt.Errorf("metastore/postgres: insert producer: %w", err)
	}
	return nil
}

func queryProducer(ctx context.Context, db querier, key metastore.ProducerKey, lock bool) (metastore.ProducerState, error) {
	sql := `SELECT n.namespace_key, p.incarnation_id, p.epoch, p.next_sequence, p.state
		FROM unijord_metastore.producers p
		JOIN unijord_metastore.namespaces n USING (namespace_hash)
		WHERE p.namespace_hash=$1 AND p.producer_id=$2`
	if lock {
		sql += " FOR UPDATE OF p"
	}
	hash := key.Namespace.Hash()
	var exact, incarnation, epochBytes, nextBytes []byte
	var status int16
	err := db.QueryRow(ctx, sql, hash[:], key.ID[:]).Scan(&exact, &incarnation, &epochBytes, &nextBytes, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return metastore.ProducerState{}, metastore.ErrNotFound
	}
	if err != nil {
		return metastore.ProducerState{}, fmt.Errorf("metastore/postgres: read producer: %w", err)
	}
	if !bytes.Equal(exact, key.Namespace.Bytes()) {
		return metastore.ProducerState{}, fmt.Errorf("%w: namespace digest collision", metastore.ErrCorrupt)
	}
	if len(incarnation) != 16 || (status != int16(metastore.ProducerOpen) && status != int16(metastore.ProducerClosed)) {
		return metastore.ProducerState{}, fmt.Errorf("%w: invalid durable producer", metastore.ErrCorrupt)
	}
	epoch, err := decodeUint64(epochBytes)
	if err != nil {
		return metastore.ProducerState{}, err
	}
	next, err := decodeUint64(nextBytes)
	if err != nil {
		return metastore.ProducerState{}, err
	}
	state := metastore.ProducerState{
		Fence:        metastore.ProducerFence{Key: key, Epoch: epoch},
		NextSequence: next, Status: metastore.ProducerStatus(status),
	}
	copy(state.Fence.IncarnationID[:], incarnation)
	if err := metastore.ValidateProducerState(state); err != nil {
		return metastore.ProducerState{}, err
	}
	return state, nil
}

func updateProducer(ctx context.Context, tx pgx.Tx, state metastore.ProducerState) error {
	hash := state.Fence.Key.Namespace.Hash()
	tag, err := tx.Exec(ctx, `UPDATE unijord_metastore.producers
		SET incarnation_id=$3, epoch=$4, state=$5, updated_at=transaction_timestamp()
		WHERE namespace_hash=$1 AND producer_id=$2`,
		hash[:], state.Fence.Key.ID[:], state.Fence.IncarnationID[:], encodeUint64(state.Fence.Epoch), int16(state.Status))
	if err != nil {
		return fmt.Errorf("metastore/postgres: update producer: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: locked producer disappeared", metastore.ErrCorrupt)
	}
	return nil
}
