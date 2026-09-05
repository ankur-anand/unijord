-- Producer authority is namespace-scoped, not bound to one shard or service
-- process. Operation receipts and expiration are separate protocol work.
CREATE TABLE unijord_metastore.producers (
    namespace_hash  bytea NOT NULL,
    producer_id     bytea NOT NULL
        CHECK (octet_length(producer_id) = 16)
        CHECK (producer_id <> decode(repeat('00', 16), 'hex')),
    incarnation_id  bytea NOT NULL
        CHECK (octet_length(incarnation_id) = 16)
        CHECK (incarnation_id <> decode(repeat('00', 16), 'hex')),
    epoch           bytea NOT NULL
        CHECK (octet_length(epoch) = 8)
        CHECK (epoch <> decode('0000000000000000', 'hex')),
    next_sequence   bytea NOT NULL
        CHECK (octet_length(next_sequence) = 8),
    state           smallint NOT NULL CHECK (state IN (1, 2)),
    created_at      timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at      timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (namespace_hash, producer_id),
    FOREIGN KEY (namespace_hash)
        REFERENCES unijord_metastore.namespaces(namespace_hash)
        ON DELETE CASCADE
);

INSERT INTO unijord_metastore.schema_migrations(version) VALUES (4);
