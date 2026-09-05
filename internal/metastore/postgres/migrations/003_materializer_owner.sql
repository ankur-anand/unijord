-- The original epoch/owner CHECK can evaluate to NULL for a nonzero epoch
-- with a NULL owner. CHECK accepts NULL, so explicitly reject that case.
-- Keep the original constraint: it also enforces owner length and requires
-- unclaimed (epoch zero) rows to have no owner.
ALTER TABLE unijord_metastore.shard_materializers
    ADD CONSTRAINT shard_materializers_claimed_owner_required
    CHECK (
        materializer_epoch = decode('0000000000000000', 'hex')
        OR materializer_owner IS NOT NULL
    );

INSERT INTO unijord_metastore.schema_migrations(version)
VALUES (3);
