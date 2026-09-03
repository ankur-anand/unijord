CREATE SCHEMA IF NOT EXISTS unijord_metastore;

CREATE TABLE IF NOT EXISTS unijord_metastore.schema_migrations (
    version     integer PRIMARY KEY CHECK (version > 0),
    applied_at  timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE IF NOT EXISTS unijord_metastore.namespaces (
    namespace_hash  bytea PRIMARY KEY
        CHECK (octet_length(namespace_hash) = 32)
        CHECK (namespace_hash <> decode(repeat('00', 32), 'hex')),
    namespace_key   bytea NOT NULL
        CHECK (octet_length(namespace_key) BETWEEN 1 AND 1024),
    created_at      timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE IF NOT EXISTS unijord_metastore.shards (
    namespace_hash  bytea NOT NULL,
    shard            bigint NOT NULL CHECK (shard BETWEEN 0 AND 4294967295),
    created_at       timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (namespace_hash, shard),
    FOREIGN KEY (namespace_hash)
        REFERENCES unijord_metastore.namespaces(namespace_hash)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS unijord_metastore.shard_writers (
    namespace_hash      bytea NOT NULL,
    shard               bigint NOT NULL CHECK (shard BETWEEN 0 AND 4294967295),
    writer_epoch        bytea NOT NULL
        CHECK (octet_length(writer_epoch) = 8)
        CHECK (writer_epoch <> decode('0000000000000000', 'hex')),
    writer_owner        bytea NOT NULL
        CHECK (octet_length(writer_owner) BETWEEN 1 AND 256),
    next_chunk_sequence bytea NOT NULL
        CHECK (octet_length(next_chunk_sequence) = 8),
    updated_at          timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (namespace_hash, shard),
    FOREIGN KEY (namespace_hash, shard)
        REFERENCES unijord_metastore.shards(namespace_hash, shard)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS unijord_metastore.shard_materializers (
    namespace_hash      bytea NOT NULL,
    shard               bigint NOT NULL CHECK (shard BETWEEN 0 AND 4294967295),
    materializer_epoch  bytea NOT NULL
        CHECK (octet_length(materializer_epoch) = 8),
    materializer_owner  bytea,
    materialized_before bytea NOT NULL
        CHECK (octet_length(materialized_before) = 8),
    updated_at          timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (namespace_hash, shard),
    FOREIGN KEY (namespace_hash, shard)
        REFERENCES unijord_metastore.shards(namespace_hash, shard)
        ON DELETE CASCADE,
    CHECK (
        (materializer_epoch = decode('0000000000000000', 'hex') AND materializer_owner IS NULL)
        OR
        (materializer_epoch <> decode('0000000000000000', 'hex')
            AND octet_length(materializer_owner) BETWEEN 1 AND 256)
    )
);

INSERT INTO unijord_metastore.schema_migrations(version)
VALUES (1)
ON CONFLICT (version) DO NOTHING;
