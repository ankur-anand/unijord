CREATE TABLE IF NOT EXISTS unijord_metastore.timeline_heads (
    key_hash          bytea PRIMARY KEY
        CHECK (octet_length(key_hash) = 32)
        CHECK (key_hash <> decode(repeat('00', 32), 'hex')),
    namespace_hash    bytea NOT NULL,
    timeline_key      bytea NOT NULL
        CHECK (octet_length(timeline_key) BETWEEN 1 AND 512),
    shard             bigint NOT NULL CHECK (shard BETWEEN 0 AND 4294967295),
    next_lsn          bytea NOT NULL
        CHECK (octet_length(next_lsn) = 8)
        CHECK (next_lsn <> decode('0000000000000000', 'hex')),
    last_timestamp_ms bigint NOT NULL,
    state             smallint NOT NULL CHECK (state IN (1, 2)),
    revision          bytea NOT NULL
        CHECK (octet_length(revision) = 8)
        CHECK (revision <> decode('0000000000000000', 'hex')),
    created_at        timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at        timestamptz NOT NULL DEFAULT transaction_timestamp(),
    FOREIGN KEY (namespace_hash, shard)
        REFERENCES unijord_metastore.shards(namespace_hash, shard)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS timeline_heads_namespace_list
    ON unijord_metastore.timeline_heads(namespace_hash, key_hash);

CREATE TABLE IF NOT EXISTS unijord_metastore.chunks (
    namespace_hash   bytea NOT NULL,
    shard            bigint NOT NULL CHECK (shard BETWEEN 0 AND 4294967295),
    sequence         bytea NOT NULL CHECK (octet_length(sequence) = 8),
    object_key       text NOT NULL CHECK (object_key <> ''),
    format_version   integer NOT NULL CHECK (format_version BETWEEN 1 AND 65535),
    writer_epoch     bytea NOT NULL
        CHECK (octet_length(writer_epoch) = 8)
        CHECK (writer_epoch <> decode('0000000000000000', 'hex')),
    record_count     bigint NOT NULL CHECK (record_count BETWEEN 1 AND 1048576),
    timeline_count   bigint NOT NULL CHECK (
        timeline_count BETWEEN 1 AND record_count
    ),
    object_size      bytea NOT NULL CHECK (octet_length(object_size) = 8),
    min_timestamp_ms bigint NOT NULL,
    max_timestamp_ms bigint NOT NULL,
    object_sha256    bytea NOT NULL
        CHECK (octet_length(object_sha256) = 32)
        CHECK (object_sha256 <> decode(repeat('00', 32), 'hex')),
    publication_hash bytea NOT NULL CHECK (octet_length(publication_hash) = 32),
    created_at       timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (namespace_hash, shard, sequence),
    FOREIGN KEY (namespace_hash, shard)
        REFERENCES unijord_metastore.shards(namespace_hash, shard)
        ON DELETE CASCADE,
    CHECK (min_timestamp_ms <= max_timestamp_ms)
);

INSERT INTO unijord_metastore.schema_migrations(version)
VALUES (2)
ON CONFLICT (version) DO NOTHING;
