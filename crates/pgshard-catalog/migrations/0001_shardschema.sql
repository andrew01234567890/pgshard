BEGIN;

DO $pgshard_requirements$
BEGIN
    IF current_setting('server_version_num')::integer < 180000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '0A000',
            MESSAGE = 'pgshard requires PostgreSQL 18 or newer';
    END IF;

    IF current_database() <> 'shardschema' THEN
        RAISE EXCEPTION USING
            ERRCODE = '3D000',
            MESSAGE = 'the pgshard catalog must be installed in the dedicated shardschema database';
    END IF;

    IF getdatabaseencoding() <> 'UTF8' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'the shardschema database must use UTF8 encoding';
    END IF;
END
$pgshard_requirements$;

CREATE SCHEMA IF NOT EXISTS pgshard_catalog;
REVOKE ALL ON SCHEMA pgshard_catalog FROM PUBLIC;

DO $pgshard_roles$
DECLARE
    role_can_login boolean;
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_reader') THEN
        CREATE ROLE pgshard_catalog_reader NOLOGIN;
    ELSE
        SELECT rolcanlogin INTO role_can_login
          FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_reader';
        IF role_can_login THEN
            RAISE EXCEPTION USING ERRCODE = '42501',
                MESSAGE = 'pre-existing pgshard_catalog_reader role must be NOLOGIN';
        END IF;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_admin') THEN
        CREATE ROLE pgshard_catalog_admin NOLOGIN;
    ELSE
        SELECT rolcanlogin INTO role_can_login
          FROM pg_catalog.pg_roles WHERE rolname = 'pgshard_catalog_admin';
        IF role_can_login THEN
            RAISE EXCEPTION USING ERRCODE = '42501',
                MESSAGE = 'pre-existing pgshard_catalog_admin role must be NOLOGIN';
        END IF;
    END IF;
END
$pgshard_roles$;

GRANT pgshard_catalog_reader TO pgshard_catalog_admin;
GRANT USAGE ON SCHEMA pgshard_catalog TO pgshard_catalog_reader;
GRANT USAGE ON SCHEMA pgshard_catalog TO pgshard_catalog_admin;

DO $pgshard_domains$
BEGIN
    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'sql_identifier'
    ) THEN
        CREATE DOMAIN pgshard_catalog.sql_identifier AS text
            CHECK (
                octet_length(VALUE) BETWEEN 1 AND 63
            );
    END IF;

    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'resource_name'
    ) THEN
        CREATE DOMAIN pgshard_catalog.resource_name AS text
            CHECK (
                VALUE ~ '^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$'
                AND octet_length(VALUE) BETWEEN 1 AND 63
            );
    END IF;

    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'uint64_boundary'
    ) THEN
        CREATE DOMAIN pgshard_catalog.uint64_boundary AS numeric(20, 0)
            CHECK (VALUE >= 0 AND VALUE <= 18446744073709551616);
    END IF;

    IF NOT EXISTS (
        SELECT
        FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
        WHERE n.nspname = 'pgshard_catalog' AND t.typname = 'replication_slot_name'
    ) THEN
        CREATE DOMAIN pgshard_catalog.replication_slot_name AS text
            CHECK (
                VALUE ~ '^[a-z0-9_]+$'
                AND octet_length(VALUE) BETWEEN 1 AND 63
            );
    END IF;
END
$pgshard_domains$;

CREATE TABLE IF NOT EXISTS pgshard_catalog.cluster_configuration (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    cluster_id uuid NOT NULL DEFAULT gen_random_uuid(),
    home_shard_id pgshard_catalog.resource_name NOT NULL DEFAULT 'shard-0000',
    hash_algorithm text NOT NULL DEFAULT 'xxh3_64'
        CHECK (hash_algorithm = 'xxh3_64'),
    hash_version smallint NOT NULL DEFAULT 1 CHECK (hash_version = 1),
    hash_seed numeric(20, 0) NOT NULL DEFAULT 0
        CHECK (hash_seed >= 0 AND hash_seed <= 18446744073709551615),
    text_key_encoding text NOT NULL DEFAULT 'UTF8'
        CHECK (text_key_encoding = 'UTF8'),
    text_key_collation text NOT NULL DEFAULT 'C'
        CHECK (text_key_collation = 'C'),
    installed_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

COMMENT ON TABLE pgshard_catalog.cluster_configuration IS
    'Immutable routing hash contract; this catalog intentionally stores no credentials or password material.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.cluster_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    catalog_epoch bigint NOT NULL DEFAULT 0 CHECK (catalog_epoch >= 0),
    changed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CHECK (catalog_epoch < 9223372036854775807)
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.logical_databases (
    logical_database_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    database_name pgshard_catalog.sql_identifier NOT NULL UNIQUE,
    schema_epoch bigint NOT NULL DEFAULT 1 CHECK (schema_epoch > 0),
    authorization_epoch bigint NOT NULL DEFAULT 1 CHECK (authorization_epoch > 0),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.shards (
    shard_id pgshard_catalog.resource_name PRIMARY KEY,
    shard_number bigint NOT NULL UNIQUE CHECK (shard_number BETWEEN 0 AND 4294967295),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('provisioning', 'active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.shard_restore_incarnations (
    restore_incarnation uuid PRIMARY KEY
        CHECK (restore_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
    shard_id pgshard_catalog.resource_name NOT NULL
        REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'retired')),
    installed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    retired_at timestamptz,
    UNIQUE (restore_incarnation, shard_id),
    CHECK ((state = 'active') = (retired_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS shard_restore_incarnations_one_active
    ON pgshard_catalog.shard_restore_incarnations(shard_id)
    WHERE state = 'active';

COMMENT ON TABLE pgshard_catalog.shard_restore_incarnations IS
    'Permanent shard history. Bootstrap and each coordinated restore allocate a fresh active UUID.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.slot_sync_probes (
    probe_generation uuid PRIMARY KEY
        CHECK (probe_generation <> '00000000-0000-0000-0000-000000000000'::uuid),
    shard_id pgshard_catalog.resource_name NOT NULL
        REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
    restore_incarnation uuid NOT NULL
        CHECK (restore_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
    system_identifier numeric(20, 0) NOT NULL
        CHECK (system_identifier BETWEEN 1 AND 18446744073709551615),
    database_oid bigint NOT NULL CHECK (database_oid BETWEEN 1 AND 4294967295),
    database_name pgshard_catalog.sql_identifier NOT NULL,
    source_timeline bigint NOT NULL CHECK (source_timeline BETWEEN 1 AND 4294967295),
    slot_name pgshard_catalog.replication_slot_name NOT NULL,
    consistent_point pg_lsn,
    creation_receipt_id uuid,
    cleanup_receipt_id uuid,
    state text NOT NULL DEFAULT 'allocated'
        CHECK (state IN ('allocated', 'active', 'retiring', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    retiring_at timestamptz,
    retired_at timestamptz,
    UNIQUE (shard_id, slot_name),
    FOREIGN KEY (restore_incarnation, shard_id)
        REFERENCES pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id)
        ON DELETE RESTRICT,
    CHECK (right(slot_name::text, 32) = replace(probe_generation::text, '-', '')),
    CHECK (consistent_point IS NULL OR consistent_point > '0/0')
);

-- v0.50 forward migration. CREATE TABLE IF NOT EXISTS does not add columns to
-- catalogs installed by v0.49 and earlier, so upgrade the existing relation
-- before any function or grant below references receipt identity. Receiptless
-- active/retiring rows cannot be assigned an honest create-attempt capability:
-- operators must finish their cleanup with the previous release first.
ALTER TABLE pgshard_catalog.slot_sync_probes
    ADD COLUMN IF NOT EXISTS creation_receipt_id uuid;
ALTER TABLE pgshard_catalog.slot_sync_probes
    ADD COLUMN IF NOT EXISTS cleanup_receipt_id uuid;

DO $pgshard_slot_sync_probe_receipts$
BEGIN
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.slot_sync_probes
         WHERE (state = 'active' AND creation_receipt_id IS NULL)
            OR (state = 'retiring' AND cleanup_receipt_id IS NULL)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'receiptless live slot-sync probes block catalog upgrade',
            DETAIL = 'v0.49 and earlier active or retiring probes have no exact create-attempt receipt',
            HINT = 'finish retiring every live probe with the previous release, then retry the migration';
    END IF;

    IF NOT EXISTS (
        SELECT
          FROM pg_catalog.pg_constraint
         WHERE conrelid = 'pgshard_catalog.slot_sync_probes'::regclass
           AND conname = 'slot_sync_probes_receipt_ids_nonzero'
    ) THEN
        ALTER TABLE pgshard_catalog.slot_sync_probes
            ADD CONSTRAINT slot_sync_probes_receipt_ids_nonzero
            CHECK (
                (
                    creation_receipt_id IS NULL
                    OR creation_receipt_id <> '00000000-0000-0000-0000-000000000000'::uuid
                )
                AND (
                    cleanup_receipt_id IS NULL
                    OR cleanup_receipt_id <> '00000000-0000-0000-0000-000000000000'::uuid
                )
            ) NOT VALID;
    END IF;
    ALTER TABLE pgshard_catalog.slot_sync_probes
        VALIDATE CONSTRAINT slot_sync_probes_receipt_ids_nonzero;

    IF NOT EXISTS (
        SELECT
          FROM pg_catalog.pg_constraint
         WHERE conrelid = 'pgshard_catalog.slot_sync_probes'::regclass
           AND conname = 'slot_sync_probes_receipt_lifecycle'
    ) THEN
        ALTER TABLE pgshard_catalog.slot_sync_probes
            ADD CONSTRAINT slot_sync_probes_receipt_lifecycle
            CHECK (
                (
                    state = 'allocated'
                    AND consistent_point IS NULL
                    AND creation_receipt_id IS NULL
                    AND cleanup_receipt_id IS NULL
                    AND activated_at IS NULL
                    AND retiring_at IS NULL
                    AND retired_at IS NULL
                )
                OR
                (
                    state = 'active'
                    AND consistent_point IS NOT NULL
                    AND creation_receipt_id IS NOT NULL
                    AND cleanup_receipt_id IS NULL
                    AND activated_at IS NOT NULL
                    AND retiring_at IS NULL
                    AND retired_at IS NULL
                )
                OR
                (
                    state = 'retiring'
                    AND retiring_at IS NOT NULL
                    AND retired_at IS NULL
                    AND cleanup_receipt_id IS NOT NULL
                    AND (
                        creation_receipt_id IS NULL
                        OR cleanup_receipt_id = creation_receipt_id
                    )
                    AND (
                        (
                            consistent_point IS NULL
                            AND creation_receipt_id IS NULL
                            AND activated_at IS NULL
                        )
                        OR (
                            consistent_point IS NOT NULL
                            AND creation_receipt_id IS NOT NULL
                            AND activated_at IS NOT NULL
                        )
                    )
                )
                OR
                (
                    state = 'retired'
                    AND retiring_at IS NOT NULL
                    AND retired_at IS NOT NULL
                    AND (
                        (
                            cleanup_receipt_id IS NOT NULL
                            AND (
                                creation_receipt_id IS NULL
                                OR cleanup_receipt_id = creation_receipt_id
                            )
                            AND (
                                (
                                    consistent_point IS NULL
                                    AND creation_receipt_id IS NULL
                                    AND activated_at IS NULL
                                )
                                OR (
                                    consistent_point IS NOT NULL
                                    AND creation_receipt_id IS NOT NULL
                                    AND activated_at IS NOT NULL
                                )
                            )
                        )
                        OR (
                            creation_receipt_id IS NULL
                            AND cleanup_receipt_id IS NULL
                            AND (
                                (consistent_point IS NULL AND activated_at IS NULL)
                                OR (consistent_point IS NOT NULL AND activated_at IS NOT NULL)
                            )
                        )
                    )
                )
            ) NOT VALID;
    END IF;
    ALTER TABLE pgshard_catalog.slot_sync_probes
        VALIDATE CONSTRAINT slot_sync_probes_receipt_lifecycle;
END
$pgshard_slot_sync_probe_receipts$;

CREATE UNIQUE INDEX IF NOT EXISTS slot_sync_probes_one_live_per_shard
    ON pgshard_catalog.slot_sync_probes(shard_id)
    WHERE state IN ('allocated', 'active', 'retiring');

COMMENT ON TABLE pgshard_catalog.slot_sync_probes IS
    'Permanent per-shard failover-slot probe allocations used only to attest continuous slot synchronization. Consumer checkpoints never depend on probe progress.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.routing_epochs (
    routing_epoch bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_database_id uuid NOT NULL
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    range_revision bigint NOT NULL DEFAULT 0
        CHECK (range_revision >= 0 AND range_revision < 9223372036854775807),
    state text NOT NULL DEFAULT 'staged'
        CHECK (state IN ('staged', 'active', 'superseded')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    superseded_at timestamptz,
    UNIQUE (logical_database_id, routing_epoch),
    CHECK ((state = 'staged') = (activated_at IS NULL)),
    CHECK ((state = 'superseded') = (superseded_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS routing_epochs_one_active_per_database
    ON pgshard_catalog.routing_epochs(logical_database_id)
    WHERE state = 'active';

CREATE TABLE IF NOT EXISTS pgshard_catalog.routing_ranges (
    routing_epoch bigint NOT NULL
        REFERENCES pgshard_catalog.routing_epochs(routing_epoch) ON DELETE RESTRICT,
    range_start pgshard_catalog.uint64_boundary NOT NULL,
    range_end pgshard_catalog.uint64_boundary NOT NULL,
    shard_id pgshard_catalog.resource_name NOT NULL
        REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
    PRIMARY KEY (routing_epoch, range_start),
    CHECK (range_start < range_end),
    CHECK (range_start < 18446744073709551616)
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.active_routing_epochs (
    logical_database_id uuid PRIMARY KEY
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    routing_epoch bigint NOT NULL UNIQUE,
    activated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    FOREIGN KEY (logical_database_id, routing_epoch)
        REFERENCES pgshard_catalog.routing_epochs(logical_database_id, routing_epoch)
        ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS pgshard_catalog.registered_tables (
    registered_table_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logical_database_id uuid NOT NULL
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    schema_name pgshard_catalog.sql_identifier NOT NULL,
    table_name pgshard_catalog.sql_identifier NOT NULL,
    shard_key_column pgshard_catalog.sql_identifier NOT NULL,
    shard_key_type text NOT NULL CHECK (shard_key_type IN ('bigint', 'uuid', 'text', 'bytea')),
    shard_key_encoding text,
    shard_key_collation text,
    hash_version smallint NOT NULL DEFAULT 1 CHECK (hash_version = 1),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (logical_database_id, schema_name, table_name),
    CHECK (
        (shard_key_type = 'text' AND shard_key_encoding = 'UTF8' AND shard_key_collation = 'C')
        OR
        (shard_key_type <> 'text' AND shard_key_encoding IS NULL AND shard_key_collation IS NULL)
    )
);

COMMENT ON COLUMN pgshard_catalog.registered_tables.shard_key_collation IS
    'Text shard keys use PostgreSQL C collation so byte-distinct UTF8 keys remain routing-distinct.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.logical_consumers (
    consumer_id uuid PRIMARY KEY DEFAULT gen_random_uuid()
        CHECK (consumer_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    logical_database_id uuid NOT NULL
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    consumer_name pgshard_catalog.resource_name NOT NULL,
    purpose text NOT NULL
        CHECK (purpose IN ('change-stream', 'reshard-materializer', 'internal-materialization')),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (logical_database_id, consumer_name),
    UNIQUE (consumer_id, logical_database_id)
);

COMMENT ON TABLE pgshard_catalog.logical_consumers IS
    'Permanent identities for public streams, reshard materializers, and internal materializations.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.logical_consumer_shards (
    consumer_id uuid NOT NULL,
    logical_database_id uuid NOT NULL,
    shard_id pgshard_catalog.resource_name NOT NULL
        REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
    ownership_fence bigint NOT NULL DEFAULT 1
        CHECK (ownership_fence > 0 AND ownership_fence < 9223372036854775807),
    state text NOT NULL DEFAULT 'provisioning'
        CHECK (state IN ('provisioning', 'ready', 'fenced', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (consumer_id, logical_database_id, shard_id),
    FOREIGN KEY (consumer_id, logical_database_id)
        REFERENCES pgshard_catalog.logical_consumers(consumer_id, logical_database_id)
        ON DELETE RESTRICT
);

COMMENT ON TABLE pgshard_catalog.logical_consumer_shards IS
    'Stable per-consumer shard fence. A row cannot become ready without a current checkpoint and active source attachment.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.logical_consumer_checkpoints (
    checkpoint_generation uuid PRIMARY KEY
        CHECK (checkpoint_generation <> '00000000-0000-0000-0000-000000000000'::uuid),
    consumer_id uuid NOT NULL,
    logical_database_id uuid NOT NULL,
    shard_id pgshard_catalog.resource_name NOT NULL,
    restore_incarnation uuid NOT NULL
        CHECK (restore_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
    system_identifier numeric(20, 0) NOT NULL
        CHECK (system_identifier BETWEEN 1 AND 18446744073709551615),
    database_oid bigint NOT NULL CHECK (database_oid BETWEEN 1 AND 4294967295),
    source_timeline bigint NOT NULL CHECK (source_timeline BETWEEN 1 AND 4294967295),
    checkpoint_lsn pg_lsn NOT NULL DEFAULT '0/0',
    checkpoint_ordinal bigint NOT NULL DEFAULT 0 CHECK (checkpoint_ordinal >= 0),
    snapshot_required boolean NOT NULL DEFAULT true,
    state text NOT NULL DEFAULT 'current' CHECK (state IN ('current', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    retired_at timestamptz,
    FOREIGN KEY (consumer_id, logical_database_id, shard_id)
        REFERENCES pgshard_catalog.logical_consumer_shards(
            consumer_id,
            logical_database_id,
            shard_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (restore_incarnation, shard_id)
        REFERENCES pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id)
        ON DELETE RESTRICT,
    CHECK ((state = 'current') = (retired_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS logical_consumer_checkpoints_one_current
    ON pgshard_catalog.logical_consumer_checkpoints(
        consumer_id,
        logical_database_id,
        shard_id
    )
    WHERE state = 'current';

COMMENT ON TABLE pgshard_catalog.logical_consumer_checkpoints IS
    'Never-reused checkpoints bound to one restore, system, database, and timeline lineage. Retired rows remain permanent resume-token tombstones.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.logical_consumer_attachments (
    attachment_generation uuid PRIMARY KEY
        CHECK (attachment_generation <> '00000000-0000-0000-0000-000000000000'::uuid),
    consumer_id uuid NOT NULL,
    logical_database_id uuid NOT NULL,
    shard_id pgshard_catalog.resource_name NOT NULL,
    restore_incarnation uuid NOT NULL
        CHECK (restore_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
    system_identifier numeric(20, 0) NOT NULL
        CHECK (system_identifier BETWEEN 1 AND 18446744073709551615),
    database_oid bigint NOT NULL CHECK (database_oid BETWEEN 1 AND 4294967295),
    database_name pgshard_catalog.sql_identifier NOT NULL,
    selected_source_member_ordinal integer NOT NULL
        CHECK (selected_source_member_ordinal BETWEEN 0 AND 65535),
    selected_source_role text NOT NULL
        CHECK (selected_source_role IN ('primary-anchor', 'standby-decoder')),
    selected_source_timeline bigint NOT NULL
        CHECK (selected_source_timeline BETWEEN 1 AND 4294967295),
    state text NOT NULL DEFAULT 'staged'
        CHECK (state IN ('staged', 'active', 'retiring', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    retired_at timestamptz,
    UNIQUE (
        attachment_generation,
        consumer_id,
        logical_database_id,
        shard_id
    ),
    FOREIGN KEY (consumer_id, logical_database_id, shard_id)
        REFERENCES pgshard_catalog.logical_consumer_shards(
            consumer_id,
            logical_database_id,
            shard_id
        ) ON DELETE RESTRICT,
    FOREIGN KEY (restore_incarnation, shard_id)
        REFERENCES pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id)
        ON DELETE RESTRICT,
    CHECK (
        (state = 'staged' AND activated_at IS NULL AND retired_at IS NULL)
        OR
        (state IN ('active', 'retiring') AND activated_at IS NOT NULL AND retired_at IS NULL)
        OR
        (state = 'retired' AND retired_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS logical_consumer_attachments_one_staged
    ON pgshard_catalog.logical_consumer_attachments(
        consumer_id,
        logical_database_id,
        shard_id
    )
    WHERE state = 'staged';

CREATE UNIQUE INDEX IF NOT EXISTS logical_consumer_attachments_one_active
    ON pgshard_catalog.logical_consumer_attachments(
        consumer_id,
        logical_database_id,
        shard_id
    )
    WHERE state = 'active';

COMMENT ON TABLE pgshard_catalog.logical_consumer_attachments IS
    'Immutable source-identity generations. Replacement creates a new generation instead of rebinding restored or forked WAL.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.managed_replication_slots (
    slot_generation uuid PRIMARY KEY
        CHECK (slot_generation <> '00000000-0000-0000-0000-000000000000'::uuid),
    attachment_generation uuid NOT NULL,
    consumer_id uuid NOT NULL,
    logical_database_id uuid NOT NULL,
    shard_id pgshard_catalog.resource_name NOT NULL,
    slot_role text NOT NULL CHECK (slot_role IN ('primary-anchor', 'standby-decoder')),
    member_ordinal integer CHECK (member_ordinal BETWEEN 0 AND 65535),
    slot_name pgshard_catalog.replication_slot_name NOT NULL,
    consistent_point pg_lsn,
    two_phase_at pg_lsn,
    state text NOT NULL DEFAULT 'allocated'
        CHECK (state IN ('allocated', 'active', 'retiring', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    retired_at timestamptz,
    FOREIGN KEY (
        attachment_generation,
        consumer_id,
        logical_database_id,
        shard_id
    ) REFERENCES pgshard_catalog.logical_consumer_attachments(
        attachment_generation,
        consumer_id,
        logical_database_id,
        shard_id
    ) ON DELETE RESTRICT,
    CHECK (
        (slot_role = 'primary-anchor' AND member_ordinal IS NULL)
        OR
        (slot_role = 'standby-decoder' AND member_ordinal IS NOT NULL)
    ),
    CHECK (right(slot_name::text, 32) = replace(slot_generation::text, '-', '')),
    CHECK (consistent_point IS NULL OR consistent_point > '0/0'),
    CHECK (two_phase_at IS NULL OR two_phase_at > '0/0'),
    CHECK (
        (
            state = 'allocated'
            AND consistent_point IS NULL
            AND two_phase_at IS NULL
            AND activated_at IS NULL
            AND retired_at IS NULL
        )
        OR
        (
            state IN ('active', 'retiring')
            AND consistent_point IS NOT NULL
            AND two_phase_at IS NOT NULL
            AND activated_at IS NOT NULL
            AND retired_at IS NULL
        )
        OR
        (
            state = 'retired'
            AND retired_at IS NOT NULL
            AND (
                (
                    consistent_point IS NULL
                    AND two_phase_at IS NULL
                    AND activated_at IS NULL
                )
                OR
                (
                    consistent_point IS NOT NULL
                    AND two_phase_at IS NOT NULL
                    AND activated_at IS NOT NULL
                )
            )
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS managed_replication_slots_one_live_role_per_member
    ON pgshard_catalog.managed_replication_slots(
        attachment_generation,
        slot_role,
        member_ordinal
    )
    WHERE slot_role = 'standby-decoder' AND state IN ('allocated', 'active', 'retiring');

CREATE UNIQUE INDEX IF NOT EXISTS managed_replication_slots_one_live_primary_anchor
    ON pgshard_catalog.managed_replication_slots(attachment_generation)
    WHERE slot_role = 'primary-anchor' AND state IN ('allocated', 'active', 'retiring');

COMMENT ON TABLE pgshard_catalog.managed_replication_slots IS
    'Permanent managed-slot allocations. Names encode the full simple UUID generation and are never reused within a shard.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.managed_slot_creation_attempts (
    creation_receipt_id uuid PRIMARY KEY
        CHECK (creation_receipt_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    slot_generation uuid NOT NULL
        CHECK (slot_generation <> '00000000-0000-0000-0000-000000000000'::uuid),
    slot_name pgshard_catalog.replication_slot_name NOT NULL,
    allocation_kind text NOT NULL CHECK (allocation_kind IN ('probe', 'consumer')),
    slot_role text NOT NULL CHECK (slot_role IN ('primary-anchor', 'standby-decoder')),
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'abandoned', 'activated', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    resolved_at timestamptz,
    CHECK (right(slot_name::text, 32) = replace(slot_generation::text, '-', '')),
    CHECK ((state = 'pending') = (resolved_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS managed_slot_creation_attempts_one_pending
    ON pgshard_catalog.managed_slot_creation_attempts(slot_generation)
    WHERE state = 'pending';

CREATE UNIQUE INDEX IF NOT EXISTS managed_slot_creation_attempts_one_activated
    ON pgshard_catalog.managed_slot_creation_attempts(slot_generation)
    WHERE state = 'activated';

COMMENT ON TABLE pgshard_catalog.managed_slot_creation_attempts IS
    'Permanent create-attempt ledger. A pending row is a durable barrier against owner retirement after the orchestration session or advisory fence is lost.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.operation_tombstones (
    operation_kind pgshard_catalog.sql_identifier NOT NULL,
    operation_id uuid NOT NULL,
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    outcome_code pgshard_catalog.sql_identifier NOT NULL,
    result_fingerprint bytea CHECK (result_fingerprint IS NULL OR octet_length(result_fingerprint) = 32),
    completed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (operation_kind, operation_id)
);

COMMENT ON TABLE pgshard_catalog.operation_tombstones IS
    'Permanent idempotency records. Only fixed-size fingerprints are retained; request/result bodies and secrets are forbidden.';

-- Disable existing statement triggers before idempotent seed statements. PostgreSQL
-- fires AFTER STATEMENT triggers even when ON CONFLICT inserts zero rows.
DROP TRIGGER IF EXISTS cluster_state_notify ON pgshard_catalog.cluster_state;
DROP TRIGGER IF EXISTS logical_databases_touch_catalog ON pgshard_catalog.logical_databases;
DROP TRIGGER IF EXISTS shards_touch_catalog ON pgshard_catalog.shards;
DROP TRIGGER IF EXISTS registered_tables_touch_catalog ON pgshard_catalog.registered_tables;
DROP TRIGGER IF EXISTS shard_restore_incarnations_touch_catalog
    ON pgshard_catalog.shard_restore_incarnations;

INSERT INTO pgshard_catalog.cluster_configuration(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO pgshard_catalog.cluster_state(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state)
VALUES ('shard-0000', 0, 'active')
ON CONFLICT (shard_id) DO NOTHING;

-- Backfill only shards with no restore history. A retired-only shard is between
-- explicit restore rotations and migration replay must not invent its successor.
INSERT INTO pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id)
SELECT gen_random_uuid(), shards.shard_id
  FROM pgshard_catalog.shards AS shards
 WHERE NOT EXISTS (
     SELECT
       FROM pgshard_catalog.shard_restore_incarnations AS incarnations
      WHERE incarnations.shard_id = shards.shard_id
 );

CREATE OR REPLACE FUNCTION pgshard_catalog.reject_all_changes()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = format('%I.%I is immutable', TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_routing_epoch_history()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'staged' THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated routing epochs are permanent';
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.state = 'staged' THEN
        IF NEW.routing_epoch <> OLD.routing_epoch
           OR NEW.logical_database_id <> OLD.logical_database_id
           OR NEW.created_at <> OLD.created_at
           OR NEW.state NOT IN ('staged', 'active')
           OR (
               NEW.state = 'staged'
               AND (
                   NEW.activated_at IS NOT NULL
                   OR NEW.superseded_at IS NOT NULL
                   OR NEW.range_revision NOT IN (OLD.range_revision, OLD.range_revision + 1)
               )
           )
           OR (
               NEW.state = 'active'
               AND (
                   NEW.activated_at IS NULL
                   OR NEW.superseded_at IS NOT NULL
                   OR NEW.range_revision <> OLD.range_revision
               )
           ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid staged routing epoch transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'active'
       AND NEW.state = 'superseded'
       AND NEW.routing_epoch = OLD.routing_epoch
       AND NEW.logical_database_id = OLD.logical_database_id
       AND NEW.range_revision = OLD.range_revision
       AND NEW.created_at = OLD.created_at
       AND NEW.activated_at = OLD.activated_at
       AND NEW.superseded_at IS NOT NULL THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated routing epoch history is immutable';
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_routing_range_history()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    protected_epoch bigint;
    epoch_state text;
BEGIN
    protected_epoch := CASE WHEN TG_OP = 'DELETE' THEN OLD.routing_epoch ELSE NEW.routing_epoch END;

    SELECT state
      INTO epoch_state
      FROM pgshard_catalog.routing_epochs
     WHERE routing_epoch = protected_epoch
     FOR KEY SHARE;

    IF epoch_state IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'routing epoch does not exist';
    END IF;

    IF epoch_state <> 'staged' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated routing ranges are immutable';
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.routing_epoch <> NEW.routing_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a routing range cannot move between epochs';
    END IF;

    -- Version the parent row in the same transaction as every child mutation.
    -- An activation that took an older REPEATABLE READ snapshot must then fail
    -- with a serialization error when it tries to lock the changed parent,
    -- instead of validating stale ranges and publishing an invalid map.
    UPDATE pgshard_catalog.routing_epochs
       SET range_revision = range_revision + 1
     WHERE routing_epoch = protected_epoch;

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.notify_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF NEW.catalog_epoch IS DISTINCT FROM OLD.catalog_epoch THEN
        PERFORM pg_catalog.pg_notify(
            'pgshard_catalog_changed',
            NEW.catalog_epoch::text
        );
    END IF;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.touch_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    UPDATE pgshard_catalog.cluster_state
       SET catalog_epoch = catalog_epoch + 1,
           changed_at = statement_timestamp()
     WHERE singleton;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.lock_catalog_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    PERFORM 1
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.lock_managed_slot_target(target_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF target_name IS NULL OR target_name = '' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'managed slot target is required';
    END IF;
    IF NOT pg_catalog.pg_try_advisory_xact_lock(
        pg_catalog.hashtextextended(target_name, 1346851656)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55P03',
            MESSAGE = 'managed slot target fence is busy';
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.lock_managed_slot_targets(target_names text[])
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    target_name text;
BEGIN
    FOR target_name IN
        SELECT DISTINCT names.name
          FROM pg_catalog.unnest(target_names) AS names(name)
         WHERE names.name IS NOT NULL
         ORDER BY names.name
    LOOP
        PERFORM pgshard_catalog.lock_managed_slot_target(target_name);
    END LOOP;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.begin_managed_slot_creation_attempt(
    expected_slot_generation uuid,
    expected_slot_name text,
    expected_slot_role text,
    expected_system_identifier numeric,
    expected_database_oid bigint,
    expected_source_timeline bigint,
    expected_restore_incarnation uuid,
    expected_catalog_epoch bigint,
    requested_creation_receipt_id uuid
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    observed_catalog_epoch bigint;
    allocation_kind text;
    existing_attempt pgshard_catalog.managed_slot_creation_attempts%ROWTYPE;
BEGIN
    IF expected_slot_generation IS NULL
       OR expected_slot_generation = '00000000-0000-0000-0000-000000000000'::uuid
       OR requested_creation_receipt_id IS NULL
       OR requested_creation_receipt_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'managed slot creation identities must be non-nil';
    END IF;

    SELECT catalog_epoch
      INTO observed_catalog_epoch
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;
    IF observed_catalog_epoch IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'catalog state is missing';
    END IF;
    IF observed_catalog_epoch IS DISTINCT FROM expected_catalog_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'managed slot creation used a stale catalog epoch';
    END IF;

    -- This fails fast rather than retaining the cluster-state row while
    -- waiting. The Rust caller waits at session scope, then invokes this
    -- function again while owning the same target key.
    PERFORM pgshard_catalog.lock_managed_slot_target(expected_slot_name);

    SELECT 'probe'
      INTO allocation_kind
      FROM pgshard_catalog.slot_sync_probes AS probes
      JOIN pgshard_catalog.shard_restore_incarnations AS restores
        ON restores.restore_incarnation = probes.restore_incarnation
       AND restores.shard_id = probes.shard_id
      JOIN pgshard_catalog.shards AS shards ON shards.shard_id = probes.shard_id
     WHERE probes.probe_generation = expected_slot_generation
       AND probes.slot_name::text = expected_slot_name
       AND expected_slot_role = 'primary-anchor'
       AND probes.state = 'allocated'
       AND probes.creation_receipt_id IS NULL
       AND probes.cleanup_receipt_id IS NULL
       AND probes.system_identifier = expected_system_identifier
       AND probes.database_oid = expected_database_oid
       AND probes.source_timeline = expected_source_timeline
       AND probes.restore_incarnation = expected_restore_incarnation
       AND restores.state = 'active'
       AND shards.state IN ('provisioning', 'active')
     FOR KEY SHARE OF probes, restores, shards;

    IF allocation_kind IS NULL THEN
        SELECT 'consumer'
          INTO allocation_kind
          FROM pgshard_catalog.managed_replication_slots AS slots
          JOIN pgshard_catalog.logical_consumer_attachments AS attachments
            ON attachments.attachment_generation = slots.attachment_generation
           AND attachments.consumer_id = slots.consumer_id
           AND attachments.logical_database_id = slots.logical_database_id
           AND attachments.shard_id = slots.shard_id
          JOIN pgshard_catalog.logical_consumer_shards AS consumer_shards
            ON consumer_shards.consumer_id = slots.consumer_id
           AND consumer_shards.logical_database_id = slots.logical_database_id
           AND consumer_shards.shard_id = slots.shard_id
          JOIN pgshard_catalog.logical_consumers AS consumers
            ON consumers.consumer_id = slots.consumer_id
           AND consumers.logical_database_id = slots.logical_database_id
          JOIN pgshard_catalog.logical_databases AS databases
            ON databases.logical_database_id = slots.logical_database_id
          JOIN pgshard_catalog.shard_restore_incarnations AS restores
            ON restores.restore_incarnation = attachments.restore_incarnation
           AND restores.shard_id = attachments.shard_id
          JOIN pgshard_catalog.shards AS shards ON shards.shard_id = attachments.shard_id
         WHERE slots.slot_generation = expected_slot_generation
           AND slots.slot_name::text = expected_slot_name
           AND slots.slot_role = expected_slot_role
           AND slots.state = 'allocated'
           AND attachments.state = 'staged'
           AND attachments.system_identifier = expected_system_identifier
           AND attachments.database_oid = expected_database_oid
           AND attachments.selected_source_timeline = expected_source_timeline
           AND attachments.restore_incarnation = expected_restore_incarnation
           AND consumer_shards.state IN ('provisioning', 'fenced')
           AND consumers.state = 'active'
           AND databases.state = 'active'
           AND restores.state = 'active'
           AND shards.state IN ('provisioning', 'active')
         FOR KEY SHARE OF slots, attachments, consumer_shards, consumers, databases, restores, shards;
    END IF;

    IF allocation_kind IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot allocation is not eligible for creation';
    END IF;

    SELECT *
      INTO existing_attempt
      FROM pgshard_catalog.managed_slot_creation_attempts
     WHERE creation_receipt_id = requested_creation_receipt_id
     FOR KEY SHARE;
    IF FOUND THEN
        IF existing_attempt.slot_generation = expected_slot_generation
           AND existing_attempt.slot_name::text = expected_slot_name
           AND existing_attempt.allocation_kind = allocation_kind
           AND existing_attempt.slot_role = expected_slot_role
           AND existing_attempt.state = 'pending' THEN
            RETURN allocation_kind;
        END IF;
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'creation receipt identity was already used';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts
         WHERE slot_generation = expected_slot_generation
           AND state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot already has an unresolved creation attempt';
    END IF;

    INSERT INTO pgshard_catalog.managed_slot_creation_attempts(
        creation_receipt_id,
        slot_generation,
        slot_name,
        allocation_kind,
        slot_role
    ) VALUES (
        requested_creation_receipt_id,
        expected_slot_generation,
        expected_slot_name,
        allocation_kind,
        expected_slot_role
    );
    RETURN allocation_kind;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.abandon_managed_slot_creation_attempt(
    expected_slot_generation uuid,
    expected_slot_name text,
    expected_creation_receipt_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    changed bigint;
    existing_attempt pgshard_catalog.managed_slot_creation_attempts%ROWTYPE;
BEGIN
    PERFORM 1
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'catalog state is missing';
    END IF;

    SELECT *
      INTO existing_attempt
      FROM pgshard_catalog.managed_slot_creation_attempts
     WHERE creation_receipt_id = expected_creation_receipt_id
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF existing_attempt.slot_generation IS DISTINCT FROM expected_slot_generation
       OR existing_attempt.slot_name::text IS DISTINCT FROM expected_slot_name THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot creation attempt cannot be abandoned';
    END IF;
    IF existing_attempt.state = 'abandoned' THEN
        RETURN;
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_target(expected_slot_name);

    UPDATE pgshard_catalog.managed_slot_creation_attempts
       SET state = 'abandoned', resolved_at = statement_timestamp()
     WHERE creation_receipt_id = expected_creation_receipt_id
       AND slot_generation = expected_slot_generation
       AND slot_name::text = expected_slot_name
       AND state = 'pending';
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed = 1 THEN
        RETURN;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot creation attempt cannot be abandoned';
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_managed_slot_creation_attempt()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot creation attempts are permanent';
    END IF;
    IF NOT (
        TG_OP = 'UPDATE'
        AND OLD.allocation_kind = 'probe'
        AND OLD.state = 'activated'
        AND NEW.state = 'retired'
    ) THEN
        PERFORM pgshard_catalog.lock_managed_slot_target(
            CASE WHEN TG_OP = 'INSERT' THEN NEW.slot_name::text ELSE OLD.slot_name::text END
        );
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'pending' OR NEW.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a managed slot creation attempt must start pending';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.creation_receipt_id IS DISTINCT FROM OLD.creation_receipt_id
       OR NEW.slot_generation IS DISTINCT FROM OLD.slot_generation
       OR NEW.slot_name IS DISTINCT FROM OLD.slot_name
       OR NEW.allocation_kind IS DISTINCT FROM OLD.allocation_kind
       OR NEW.slot_role IS DISTINCT FROM OLD.slot_role
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot creation attempt identity is immutable';
    END IF;
    IF NOT (
        (OLD.state = 'pending' AND NEW.state IN ('abandoned', 'activated', 'retired'))
        OR (OLD.state = 'activated' AND NEW.state = 'retired')
    ) OR NEW.resolved_at IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid managed slot creation attempt transition';
    END IF;
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_shard_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    becoming_unavailable boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shard identities are permanent';
    END IF;

    IF NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.shard_number IS DISTINCT FROM OLD.shard_number
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shard identity is immutable';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_targets(ARRAY(
        SELECT probes.slot_name::text
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.shard_id = OLD.shard_id
        UNION
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.shard_id = OLD.shard_id
    ));
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.slot_sync_probes AS probes
            ON probes.probe_generation = attempts.slot_generation
         WHERE probes.shard_id = OLD.shard_id
           AND attempts.state = 'pending'
        UNION ALL
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.managed_replication_slots AS slots
            ON slots.slot_generation = attempts.slot_generation
         WHERE slots.shard_id = OLD.shard_id
           AND attempts.state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'shard lifecycle is blocked by a pending managed slot creation';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'provisioning' AND NEW.state = 'active')
        OR (OLD.state = 'active' AND NEW.state = 'draining')
        OR (OLD.state = 'draining' AND NEW.state IN ('active', 'retired'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid shard lifecycle transition';
    END IF;

    becoming_unavailable := NEW.state NOT IN ('active', 'draining');

    IF becoming_unavailable AND EXISTS (
        SELECT
          FROM pgshard_catalog.routing_ranges AS ranges
          JOIN pgshard_catalog.routing_epochs AS epochs
            ON epochs.routing_epoch = ranges.routing_epoch
         WHERE ranges.shard_id = OLD.shard_id
           AND epochs.state = 'active'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('shard %s is referenced by active routing', OLD.shard_id);
    END IF;

    IF NEW.state = 'retired' AND EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
         WHERE consumer_shards.shard_id = OLD.shard_id
           AND consumer_shards.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('shard %s still has non-retired logical consumers', OLD.shard_id);
    END IF;

    IF NEW.state = 'retired' AND EXISTS (
        SELECT
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.shard_id = OLD.shard_id
           AND probes.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('shard %s still has a non-retired slot-sync probe', OLD.shard_id);
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.install_initial_shard_restore_incarnation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    INSERT INTO pgshard_catalog.shard_restore_incarnations(restore_incarnation, shard_id)
    VALUES (gen_random_uuid(), NEW.shard_id);
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_shard_restore_incarnation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shard restore incarnations are permanent';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'active' OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a shard restore incarnation must start active';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.restore_incarnation IS DISTINCT FROM OLD.restore_incarnation
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.installed_at IS DISTINCT FROM OLD.installed_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'shard restore incarnation identity is immutable';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_targets(ARRAY(
        SELECT probes.slot_name::text
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.restore_incarnation = OLD.restore_incarnation
           AND probes.shard_id = OLD.shard_id
        UNION
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
          JOIN pgshard_catalog.logical_consumer_attachments AS attachments
            ON attachments.attachment_generation = slots.attachment_generation
         WHERE attachments.restore_incarnation = OLD.restore_incarnation
           AND attachments.shard_id = OLD.shard_id
    ));
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.slot_sync_probes AS probes
            ON probes.probe_generation = attempts.slot_generation
         WHERE probes.restore_incarnation = OLD.restore_incarnation
           AND probes.shard_id = OLD.shard_id
           AND attempts.state = 'pending'
        UNION ALL
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.managed_replication_slots AS slots
            ON slots.slot_generation = attempts.slot_generation
          JOIN pgshard_catalog.logical_consumer_attachments AS attachments
            ON attachments.attachment_generation = slots.attachment_generation
         WHERE attachments.restore_incarnation = OLD.restore_incarnation
           AND attachments.shard_id = OLD.shard_id
           AND attempts.state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'restore lifecycle is blocked by a pending managed slot creation';
    END IF;

    IF OLD.state = 'retired' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired shard restore incarnation is immutable';
    END IF;

    IF NEW.state = OLD.state AND NEW.retired_at IS NOT DISTINCT FROM OLD.retired_at THEN
        RETURN NEW;
    END IF;

    IF NEW.state <> 'retired' OR NEW.retired_at IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid shard restore incarnation transition';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
         WHERE consumer_shards.shard_id = NEW.shard_id
           AND consumer_shards.state = 'ready'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'restore incarnation retirement requires every consumer to be fenced';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_checkpoints AS checkpoints
         WHERE checkpoints.shard_id = NEW.shard_id
           AND checkpoints.restore_incarnation = NEW.restore_incarnation
           AND checkpoints.state = 'current'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'restore incarnation retains a current logical consumer checkpoint';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_attachments AS attachments
         WHERE attachments.shard_id = NEW.shard_id
           AND attachments.restore_incarnation = NEW.restore_incarnation
           AND attachments.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'restore incarnation retains non-retired logical consumer attachment';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.shard_id = NEW.shard_id
           AND probes.restore_incarnation = NEW.restore_incarnation
           AND probes.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'restore incarnation retains a non-retired slot-sync probe';
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_slot_sync_probe()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    restore_state text;
    shard_state text;
    attempts_changed bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe generations are permanent';
    END IF;

    -- Final retirement is already covered by the live, connection-bound
    -- absence fence. Every earlier transition takes the database-enforced
    -- target lock in cluster-state-before-target order.
    IF NOT (
        TG_OP = 'UPDATE'
        AND OLD.state = 'retiring'
        AND NEW.state = 'retired'
    ) THEN
        PERFORM pgshard_catalog.lock_managed_slot_target(
            CASE WHEN TG_OP = 'INSERT' THEN NEW.slot_name::text ELSE OLD.slot_name::text END
        );
    END IF;

    SELECT incarnations.state, shards.state
      INTO restore_state, shard_state
      FROM pgshard_catalog.shard_restore_incarnations AS incarnations
      JOIN pgshard_catalog.shards AS shards
        ON shards.shard_id = incarnations.shard_id
     WHERE incarnations.restore_incarnation = NEW.restore_incarnation
       AND incarnations.shard_id = NEW.shard_id
     FOR KEY SHARE OF incarnations, shards;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'allocated'
           OR NEW.consistent_point IS NOT NULL
           OR NEW.creation_receipt_id IS NOT NULL
           OR NEW.cleanup_receipt_id IS NOT NULL
           OR NEW.activated_at IS NOT NULL
           OR NEW.retiring_at IS NOT NULL
           OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a slot-sync probe must start allocated';
        END IF;
        IF restore_state IS DISTINCT FROM 'active'
           OR shard_state IS NULL
           OR shard_state NOT IN ('provisioning', 'active') THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probes require an active shard restore';
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.managed_replication_slots AS slots
             WHERE slots.slot_generation = NEW.probe_generation
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'replication-slot generations cannot be reused across managed roles';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.probe_generation IS DISTINCT FROM OLD.probe_generation
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.restore_incarnation IS DISTINCT FROM OLD.restore_incarnation
       OR NEW.system_identifier IS DISTINCT FROM OLD.system_identifier
       OR NEW.database_oid IS DISTINCT FROM OLD.database_oid
       OR NEW.database_name IS DISTINCT FROM OLD.database_name
       OR NEW.source_timeline IS DISTINCT FROM OLD.source_timeline
       OR NEW.slot_name IS DISTINCT FROM OLD.slot_name
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe allocation identity is immutable';
    END IF;

    IF OLD.state = 'retired' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired slot-sync probes are immutable';
    END IF;

    IF NEW.state = OLD.state THEN
        IF NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
           OR NEW.creation_receipt_id IS DISTINCT FROM OLD.creation_receipt_id
           OR NEW.cleanup_receipt_id IS DISTINCT FROM OLD.cleanup_receipt_id
           OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
           OR NEW.retiring_at IS DISTINCT FROM OLD.retiring_at
           OR NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe lifecycle history is immutable';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'allocated' AND NEW.state = 'active' THEN
        IF restore_state IS DISTINCT FROM 'active'
           OR shard_state IS NULL
           OR shard_state NOT IN ('provisioning', 'active')
           OR NEW.consistent_point IS NULL
           OR NEW.creation_receipt_id IS NULL
           OR NEW.cleanup_receipt_id IS NOT NULL
           OR NEW.activated_at IS NULL
           OR NEW.retiring_at IS NOT NULL
           OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe activation is incomplete or misplaced';
        END IF;
        UPDATE pgshard_catalog.managed_slot_creation_attempts
           SET state = 'activated', resolved_at = statement_timestamp()
         WHERE creation_receipt_id = NEW.creation_receipt_id
           AND slot_generation = NEW.probe_generation
           AND slot_name = NEW.slot_name
           AND allocation_kind = 'probe'
           AND slot_role = 'primary-anchor'
           AND state = 'pending';
        GET DIAGNOSTICS attempts_changed = ROW_COUNT;
        IF attempts_changed <> 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'slot-sync probe activation requires its exact pending creation attempt';
        END IF;
    ELSIF OLD.state IN ('allocated', 'active') AND NEW.state = 'retiring' THEN
        IF NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
           OR NEW.creation_receipt_id IS DISTINCT FROM OLD.creation_receipt_id
           OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
           OR NEW.cleanup_receipt_id IS NULL
           OR (
               OLD.creation_receipt_id IS NOT NULL
               AND NEW.cleanup_receipt_id IS DISTINCT FROM OLD.creation_receipt_id
           )
           OR NEW.retiring_at IS NULL
           OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe retirement must preserve activation history';
        END IF;
        IF OLD.state = 'allocated' AND EXISTS (
            SELECT
              FROM pgshard_catalog.managed_slot_creation_attempts
             WHERE slot_generation = NEW.probe_generation
               AND state = 'pending'
        ) THEN
            UPDATE pgshard_catalog.managed_slot_creation_attempts
               SET state = 'activated', resolved_at = statement_timestamp()
             WHERE creation_receipt_id = NEW.cleanup_receipt_id
               AND slot_generation = NEW.probe_generation
               AND slot_name = NEW.slot_name
               AND allocation_kind = 'probe'
               AND slot_role = 'primary-anchor'
               AND state = 'pending';
            GET DIAGNOSTICS attempts_changed = ROW_COUNT;
            IF attempts_changed <> 1 THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = 'slot-sync probe retirement requires its exact pending creation attempt';
            END IF;
        END IF;
    ELSIF OLD.state = 'retiring' AND NEW.state = 'retired' THEN
        IF NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
           OR NEW.creation_receipt_id IS DISTINCT FROM OLD.creation_receipt_id
           OR NEW.cleanup_receipt_id IS DISTINCT FROM OLD.cleanup_receipt_id
           OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
           OR NEW.retiring_at IS DISTINCT FROM OLD.retiring_at
           OR NEW.retired_at IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe retirement is incomplete';
        END IF;
        UPDATE pgshard_catalog.managed_slot_creation_attempts
           SET state = 'retired', resolved_at = statement_timestamp()
         WHERE slot_generation = NEW.probe_generation
           AND slot_name = NEW.slot_name
           AND allocation_kind = 'probe'
           AND state = 'activated';
    ELSE
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid slot-sync probe transition';
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_database_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    becoming_retired boolean;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical database identities are permanent';
    END IF;

    IF NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.database_name IS DISTINCT FROM OLD.database_name
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.schema_epoch < OLD.schema_epoch
       OR NEW.authorization_epoch < OLD.authorization_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical database identity and epochs are monotonic';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_targets(ARRAY(
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.logical_database_id = OLD.logical_database_id
    ));
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.managed_replication_slots AS slots
            ON slots.slot_generation = attempts.slot_generation
         WHERE slots.logical_database_id = OLD.logical_database_id
           AND attempts.state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'logical database lifecycle is blocked by a pending managed slot creation';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'active' AND NEW.state = 'draining')
        OR (OLD.state = 'draining' AND NEW.state IN ('active', 'retired'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid logical database lifecycle transition';
    END IF;

    becoming_retired := NEW.state = 'retired';

    IF becoming_retired AND EXISTS (
        SELECT FROM pgshard_catalog.active_routing_epochs
         WHERE logical_database_id = OLD.logical_database_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical database %s still has active routing',
                OLD.logical_database_id
            );
    END IF;

    IF becoming_retired AND EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumers AS consumers
         WHERE consumers.logical_database_id = OLD.logical_database_id
           AND consumers.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format(
                'logical database %s still has non-retired logical consumers',
                OLD.logical_database_id
            );
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_logical_consumer_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical consumer identities are permanent';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'active' OR NOT EXISTS (
            SELECT
              FROM pgshard_catalog.logical_databases AS databases
             WHERE databases.logical_database_id = NEW.logical_database_id
               AND databases.state = 'active'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'a logical consumer must start under an active logical database';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.consumer_name IS DISTINCT FROM OLD.consumer_name
       OR NEW.purpose IS DISTINCT FROM OLD.purpose
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical consumer identity is immutable';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_targets(ARRAY(
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.consumer_id = OLD.consumer_id
           AND slots.logical_database_id = OLD.logical_database_id
    ));
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.managed_replication_slots AS slots
            ON slots.slot_generation = attempts.slot_generation
         WHERE slots.consumer_id = OLD.consumer_id
           AND slots.logical_database_id = OLD.logical_database_id
           AND attempts.state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'logical consumer lifecycle is blocked by a pending managed slot creation';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'active' AND NEW.state = 'draining')
        OR (OLD.state = 'draining' AND NEW.state IN ('active', 'retired'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid logical consumer lifecycle transition';
    END IF;

    IF NEW.state = 'retired' AND EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
         WHERE consumer_shards.consumer_id = OLD.consumer_id
           AND consumer_shards.logical_database_id = OLD.logical_database_id
           AND consumer_shards.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('logical consumer %s still has non-retired shards', OLD.consumer_id);
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_logical_consumer_shard_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical consumer shard identities are permanent';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.ownership_fence <> 1 OR NEW.state <> 'provisioning' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'a logical consumer shard must start provisioning at ownership fence 1';
        END IF;
        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.logical_consumers AS consumers
              JOIN pgshard_catalog.shards AS shards ON shards.shard_id = NEW.shard_id
             WHERE consumers.consumer_id = NEW.consumer_id
               AND consumers.logical_database_id = NEW.logical_database_id
               AND consumers.state = 'active'
               AND shards.state IN ('active', 'draining')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'logical consumer shard requires active consumer and available shard';
        END IF;
        NEW.updated_at := statement_timestamp();
        RETURN NEW;
    END IF;

    IF NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical consumer shard identity is immutable';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_targets(ARRAY(
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.consumer_id = OLD.consumer_id
           AND slots.logical_database_id = OLD.logical_database_id
           AND slots.shard_id = OLD.shard_id
    ));
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.managed_replication_slots AS slots
            ON slots.slot_generation = attempts.slot_generation
         WHERE slots.consumer_id = OLD.consumer_id
           AND slots.logical_database_id = OLD.logical_database_id
           AND slots.shard_id = OLD.shard_id
           AND attempts.state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'consumer ownership fencing is blocked by a pending managed slot creation';
    END IF;

    IF NEW.ownership_fence NOT IN (OLD.ownership_fence, OLD.ownership_fence + 1) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'logical consumer ownership fence must advance by one';
    END IF;

    IF NEW.ownership_fence <> OLD.ownership_fence AND NEW.state <> 'fenced' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'ownership can only advance while the consumer is fenced';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'provisioning' AND NEW.state = 'fenced')
        OR (OLD.state = 'ready' AND NEW.state = 'fenced')
        OR (OLD.state = 'fenced' AND NEW.state IN ('ready', 'retired'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid logical consumer shard lifecycle transition';
    END IF;

    IF OLD.state = 'ready' AND NEW.state = 'fenced'
       AND NEW.ownership_fence <> OLD.ownership_fence + 1 THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'fencing a ready consumer must advance ownership';
    END IF;

    IF NEW.state = 'ready' AND NOT EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_checkpoints AS checkpoints
         WHERE checkpoints.consumer_id = NEW.consumer_id
           AND checkpoints.logical_database_id = NEW.logical_database_id
           AND checkpoints.shard_id = NEW.shard_id
           AND checkpoints.state = 'current'
           AND NOT checkpoints.snapshot_required
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'a ready logical consumer shard requires a resumable current checkpoint';
    END IF;

    IF NEW.state = 'ready' AND NOT EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_attachments AS attachments
         WHERE attachments.consumer_id = NEW.consumer_id
           AND attachments.logical_database_id = NEW.logical_database_id
           AND attachments.shard_id = NEW.shard_id
           AND attachments.state = 'active'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'a ready logical consumer shard requires an active source attachment';
    END IF;

    IF NEW.state = 'retired' AND EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_checkpoints AS checkpoints
         WHERE checkpoints.consumer_id = NEW.consumer_id
           AND checkpoints.logical_database_id = NEW.logical_database_id
           AND checkpoints.shard_id = NEW.shard_id
           AND checkpoints.state = 'current'
        UNION ALL
        SELECT
          FROM pgshard_catalog.logical_consumer_attachments AS attachments
         WHERE attachments.consumer_id = NEW.consumer_id
           AND attachments.logical_database_id = NEW.logical_database_id
           AND attachments.shard_id = NEW.shard_id
           AND attachments.state <> 'retired'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'a logical consumer shard retains current checkpoint or attachment state';
    END IF;

    NEW.updated_at := statement_timestamp();
    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_logical_consumer_checkpoint()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'checkpoint generations are permanent';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'current'
           OR NEW.retired_at IS NOT NULL
           OR NEW.checkpoint_lsn <> '0/0'
           OR NEW.checkpoint_ordinal <> 0
           OR NOT NEW.snapshot_required THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'a checkpoint generation must start current at zero and require a snapshot';
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
             WHERE consumer_shards.consumer_id = NEW.consumer_id
               AND consumer_shards.logical_database_id = NEW.logical_database_id
               AND consumer_shards.shard_id = NEW.shard_id
               AND consumer_shards.state = 'retired'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a retired consumer shard cannot gain a checkpoint';
        END IF;
        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.shard_restore_incarnations AS incarnations
             WHERE incarnations.restore_incarnation = NEW.restore_incarnation
               AND incarnations.shard_id = NEW.shard_id
               AND incarnations.state = 'active'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'a checkpoint generation requires the active shard restore incarnation';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.checkpoint_generation IS DISTINCT FROM OLD.checkpoint_generation
       OR NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.restore_incarnation IS DISTINCT FROM OLD.restore_incarnation
       OR NEW.system_identifier IS DISTINCT FROM OLD.system_identifier
       OR NEW.database_oid IS DISTINCT FROM OLD.database_oid
       OR NEW.source_timeline IS DISTINCT FROM OLD.source_timeline
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'checkpoint generation identity is immutable';
    END IF;

    IF OLD.state = 'retired' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired checkpoint generations are immutable';
    END IF;

    IF NEW.checkpoint_lsn < OLD.checkpoint_lsn
       OR NEW.checkpoint_ordinal < OLD.checkpoint_ordinal THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'durable checkpoint progress cannot regress';
    END IF;

    IF (
        NEW.checkpoint_lsn IS DISTINCT FROM OLD.checkpoint_lsn
        OR NEW.snapshot_required IS DISTINCT FROM OLD.snapshot_required
    ) AND NEW.checkpoint_ordinal <= OLD.checkpoint_ordinal THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'checkpoint LSN or snapshot changes must advance the checkpoint ordinal';
    END IF;

    IF NOT OLD.snapshot_required AND NEW.snapshot_required THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'requiring a new snapshot must allocate a new checkpoint generation';
    END IF;

    IF NEW.state NOT IN (OLD.state, 'retired') THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid checkpoint generation transition';
    END IF;

    IF NEW.state = 'retired' THEN
        IF NEW.retired_at IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired checkpoint requires a timestamp';
        END IF;
        IF NEW.checkpoint_lsn IS DISTINCT FROM OLD.checkpoint_lsn
           OR NEW.checkpoint_ordinal IS DISTINCT FROM OLD.checkpoint_ordinal
           OR NEW.snapshot_required IS DISTINCT FROM OLD.snapshot_required THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'checkpoint retirement cannot advance durable progress';
        END IF;
        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
             WHERE consumer_shards.consumer_id = NEW.consumer_id
               AND consumer_shards.logical_database_id = NEW.logical_database_id
               AND consumer_shards.shard_id = NEW.shard_id
               AND consumer_shards.state = 'fenced'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'checkpoint retirement requires a fenced consumer';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.retired_at IS NOT NULL THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'current checkpoint cannot have a retirement timestamp';
    END IF;

    IF (
        NEW.checkpoint_lsn IS DISTINCT FROM OLD.checkpoint_lsn
        OR NEW.checkpoint_ordinal IS DISTINCT FROM OLD.checkpoint_ordinal
        OR NEW.snapshot_required IS DISTINCT FROM OLD.snapshot_required
    ) AND NOT EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_attachments AS attachments
          JOIN pgshard_catalog.managed_replication_slots AS selected_slots
            ON selected_slots.attachment_generation = attachments.attachment_generation
           AND selected_slots.consumer_id = attachments.consumer_id
           AND selected_slots.logical_database_id = attachments.logical_database_id
           AND selected_slots.shard_id = attachments.shard_id
          JOIN pgshard_catalog.managed_replication_slots AS anchor_slots
            ON anchor_slots.attachment_generation = attachments.attachment_generation
           AND anchor_slots.slot_role = 'primary-anchor'
           AND anchor_slots.state = 'active'
         WHERE attachments.consumer_id = NEW.consumer_id
           AND attachments.logical_database_id = NEW.logical_database_id
           AND attachments.shard_id = NEW.shard_id
           AND attachments.restore_incarnation = NEW.restore_incarnation
           AND attachments.system_identifier = NEW.system_identifier
           AND attachments.database_oid = NEW.database_oid
           AND attachments.selected_source_timeline = NEW.source_timeline
           AND attachments.state = 'active'
           AND selected_slots.slot_role = attachments.selected_source_role
           AND (
               attachments.selected_source_role = 'primary-anchor'
               OR selected_slots.member_ordinal = attachments.selected_source_member_ordinal
           )
           AND selected_slots.state = 'active'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'checkpoint progress requires active matching source slots';
    END IF;

    IF OLD.snapshot_required AND NOT NEW.snapshot_required AND NOT EXISTS (
        SELECT
          FROM pgshard_catalog.logical_consumer_attachments AS attachments
          JOIN pgshard_catalog.managed_replication_slots AS selected_slots
            ON selected_slots.attachment_generation = attachments.attachment_generation
           AND selected_slots.consumer_id = attachments.consumer_id
           AND selected_slots.logical_database_id = attachments.logical_database_id
           AND selected_slots.shard_id = attachments.shard_id
          JOIN pgshard_catalog.managed_replication_slots AS anchor_slots
            ON anchor_slots.attachment_generation = attachments.attachment_generation
           AND anchor_slots.slot_role = 'primary-anchor'
           AND anchor_slots.state = 'active'
         WHERE attachments.consumer_id = NEW.consumer_id
           AND attachments.logical_database_id = NEW.logical_database_id
           AND attachments.shard_id = NEW.shard_id
           AND attachments.restore_incarnation = NEW.restore_incarnation
           AND attachments.system_identifier = NEW.system_identifier
           AND attachments.database_oid = NEW.database_oid
           AND attachments.selected_source_timeline = NEW.source_timeline
           AND attachments.state = 'active'
           AND selected_slots.slot_role = attachments.selected_source_role
           AND (
               attachments.selected_source_role = 'primary-anchor'
               OR selected_slots.member_ordinal = attachments.selected_source_member_ordinal
           )
           AND selected_slots.state = 'active'
           AND selected_slots.consistent_point <= NEW.checkpoint_lsn
           AND selected_slots.two_phase_at <= NEW.checkpoint_lsn
           AND anchor_slots.consistent_point <= NEW.checkpoint_lsn
           AND anchor_slots.two_phase_at <= NEW.checkpoint_lsn
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'snapshot completion is behind a managed slot activation boundary';
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_logical_consumer_attachment()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source attachment generations are permanent';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'staged' OR NEW.activated_at IS NOT NULL OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a source attachment must start staged';
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
             WHERE consumer_shards.consumer_id = NEW.consumer_id
               AND consumer_shards.logical_database_id = NEW.logical_database_id
               AND consumer_shards.shard_id = NEW.shard_id
               AND consumer_shards.state = 'retired'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired consumer shard cannot gain an attachment';
        END IF;
        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.shard_restore_incarnations AS incarnations
             WHERE incarnations.restore_incarnation = NEW.restore_incarnation
               AND incarnations.shard_id = NEW.shard_id
               AND incarnations.state = 'active'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source attachment requires the active shard restore incarnation';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.attachment_generation IS DISTINCT FROM OLD.attachment_generation
       OR NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.restore_incarnation IS DISTINCT FROM OLD.restore_incarnation
       OR NEW.system_identifier IS DISTINCT FROM OLD.system_identifier
       OR NEW.database_oid IS DISTINCT FROM OLD.database_oid
       OR NEW.database_name IS DISTINCT FROM OLD.database_name
       OR NEW.selected_source_member_ordinal IS DISTINCT FROM OLD.selected_source_member_ordinal
       OR NEW.selected_source_role IS DISTINCT FROM OLD.selected_source_role
       OR NEW.selected_source_timeline IS DISTINCT FROM OLD.selected_source_timeline
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source attachment identity is immutable';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_targets(ARRAY(
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.attachment_generation = OLD.attachment_generation
    ));
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
          JOIN pgshard_catalog.managed_replication_slots AS slots
            ON slots.slot_generation = attempts.slot_generation
         WHERE slots.attachment_generation = OLD.attachment_generation
           AND attempts.state = 'pending'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'source attachment lifecycle is blocked by a pending managed slot creation';
    END IF;

    IF OLD.state = 'retired' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired source attachments are immutable';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'staged' AND NEW.state IN ('active', 'retired'))
        OR (OLD.state = 'active' AND NEW.state = 'retiring')
        OR (OLD.state = 'retiring' AND NEW.state = 'retired')
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid source attachment transition';
    END IF;

    IF NEW.state = OLD.state THEN
        IF NEW.activated_at IS DISTINCT FROM OLD.activated_at
           OR NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'source attachment lifecycle timestamps are immutable';
        END IF;
    ELSIF OLD.state = 'staged' AND NEW.state = 'active' THEN
        IF NEW.activated_at IS NULL OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'active attachment requires an activation timestamp';
        END IF;

        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.shard_restore_incarnations AS incarnations
             WHERE incarnations.restore_incarnation = NEW.restore_incarnation
               AND incarnations.shard_id = NEW.shard_id
               AND incarnations.state = 'active'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source attachment restore incarnation is not active';
        END IF;

        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.managed_replication_slots AS slots
             WHERE slots.attachment_generation = NEW.attachment_generation
               AND slots.slot_role = 'primary-anchor'
               AND slots.state = 'active'
        ) OR NOT EXISTS (
            SELECT
              FROM pgshard_catalog.managed_replication_slots AS slots
             WHERE slots.attachment_generation = NEW.attachment_generation
               AND slots.slot_role = NEW.selected_source_role
               AND (
                   NEW.selected_source_role = 'primary-anchor'
                   OR slots.member_ordinal = NEW.selected_source_member_ordinal
               )
               AND slots.state = 'active'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'source attachment requires active anchor and selected source slots';
        END IF;

        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.logical_consumer_checkpoints AS checkpoints
              JOIN pgshard_catalog.managed_replication_slots AS selected_slots
                ON selected_slots.attachment_generation = NEW.attachment_generation
               AND selected_slots.slot_role = NEW.selected_source_role
               AND (
                   NEW.selected_source_role = 'primary-anchor'
                   OR selected_slots.member_ordinal = NEW.selected_source_member_ordinal
               )
               AND selected_slots.state = 'active'
              JOIN pgshard_catalog.managed_replication_slots AS anchor_slots
                ON anchor_slots.attachment_generation = NEW.attachment_generation
               AND anchor_slots.slot_role = 'primary-anchor'
               AND anchor_slots.state = 'active'
             WHERE checkpoints.consumer_id = NEW.consumer_id
               AND checkpoints.logical_database_id = NEW.logical_database_id
               AND checkpoints.shard_id = NEW.shard_id
               AND checkpoints.restore_incarnation = NEW.restore_incarnation
               AND checkpoints.system_identifier = NEW.system_identifier
               AND checkpoints.database_oid = NEW.database_oid
               AND checkpoints.source_timeline = NEW.selected_source_timeline
               AND checkpoints.state = 'current'
               AND (
                   checkpoints.snapshot_required
                   OR (
                       selected_slots.consistent_point <= checkpoints.checkpoint_lsn
                       AND selected_slots.two_phase_at <= checkpoints.checkpoint_lsn
                       AND anchor_slots.consistent_point <= checkpoints.checkpoint_lsn
                       AND anchor_slots.two_phase_at <= checkpoints.checkpoint_lsn
                   )
               )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'selected source cannot resume the durable checkpoint';
        END IF;
    ELSIF OLD.state = 'active' AND NEW.state = 'retiring' THEN
        IF NEW.activated_at IS DISTINCT FROM OLD.activated_at OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'source retirement cannot rewrite activation history';
        END IF;
        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.logical_consumer_shards AS consumer_shards
             WHERE consumer_shards.consumer_id = NEW.consumer_id
               AND consumer_shards.logical_database_id = NEW.logical_database_id
               AND consumer_shards.shard_id = NEW.shard_id
               AND consumer_shards.state = 'fenced'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source retirement requires a fenced consumer';
        END IF;
    ELSIF NEW.state = 'retired' THEN
        IF NEW.retired_at IS NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired attachment requires a timestamp';
        END IF;
        IF NEW.activated_at IS DISTINCT FROM OLD.activated_at THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'source retirement cannot rewrite activation history';
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.managed_replication_slots AS slots
             WHERE slots.attachment_generation = NEW.attachment_generation
               AND slots.state <> 'retired'
        ) THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'attachment retains non-retired managed slots';
        END IF;
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_managed_replication_slot()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    attachment_state text;
    attempts_changed bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot names and generations are permanent';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_target(
        CASE WHEN TG_OP = 'INSERT' THEN NEW.slot_name::text ELSE OLD.slot_name::text END
    );

    SELECT state
      INTO attachment_state
      FROM pgshard_catalog.logical_consumer_attachments
     WHERE attachment_generation = NEW.attachment_generation
     FOR KEY SHARE;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'allocated'
           OR NEW.consistent_point IS NOT NULL
           OR NEW.two_phase_at IS NOT NULL
           OR NEW.activated_at IS NOT NULL
           OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'a managed slot must start allocated';
        END IF;
        IF attachment_state <> 'staged' THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slots can only be allocated to staged attachments';
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.slot_sync_probes AS probes
             WHERE probes.probe_generation = NEW.slot_generation
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'replication-slot generations cannot be reused across managed roles';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.slot_generation IS DISTINCT FROM OLD.slot_generation
       OR NEW.attachment_generation IS DISTINCT FROM OLD.attachment_generation
       OR NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.slot_role IS DISTINCT FROM OLD.slot_role
       OR NEW.member_ordinal IS DISTINCT FROM OLD.member_ordinal
       OR NEW.slot_name IS DISTINCT FROM OLD.slot_name
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot allocation identity is immutable';
    END IF;

    IF OLD.state = 'retired' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'retired managed slots are immutable';
    END IF;

    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'allocated' AND NEW.state IN ('active', 'retired'))
        OR (OLD.state = 'active' AND NEW.state = 'retiring')
        OR (OLD.state = 'active' AND NEW.state = 'retired' AND attachment_state = 'staged')
        OR (OLD.state = 'retiring' AND NEW.state = 'retired')
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'invalid managed slot transition';
    END IF;

    IF OLD.state = 'allocated' AND NEW.state = 'active' THEN
        IF attachment_state <> 'staged'
           OR NEW.consistent_point IS NULL
           OR NEW.two_phase_at IS NULL
           OR NEW.activated_at IS NULL
           OR NEW.retired_at IS NOT NULL THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot activation is incomplete or misplaced';
        END IF;
        UPDATE pgshard_catalog.managed_slot_creation_attempts
           SET state = 'activated', resolved_at = statement_timestamp()
         WHERE slot_generation = NEW.slot_generation
           AND slot_name = NEW.slot_name
           AND allocation_kind = 'consumer'
           AND slot_role = NEW.slot_role
           AND state = 'pending';
        GET DIAGNOSTICS attempts_changed = ROW_COUNT;
        IF attempts_changed <> 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'managed slot activation requires one exact pending creation attempt';
        END IF;
    ELSIF OLD.state = 'active' AND NEW.state = 'retiring' THEN
        IF attachment_state <> 'retiring' THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot retirement requires a retiring attachment';
        END IF;
    ELSIF NEW.state = 'retired' THEN
        IF NEW.retired_at IS NULL OR attachment_state NOT IN ('staged', 'retiring') THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot retirement is incomplete or misplaced';
        END IF;
        IF OLD.state = 'allocated'
           AND (
               NEW.consistent_point IS NOT NULL
               OR NEW.two_phase_at IS NOT NULL
               OR NEW.activated_at IS NOT NULL
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'an unactivated managed slot cannot fabricate activation history';
        END IF;
        IF OLD.state = 'allocated' THEN
            UPDATE pgshard_catalog.managed_slot_creation_attempts
               SET state = 'retired', resolved_at = statement_timestamp()
             WHERE slot_generation = NEW.slot_generation
               AND slot_name = NEW.slot_name
               AND allocation_kind = 'consumer'
               AND slot_role = NEW.slot_role
               AND state = 'pending';
        ELSE
            UPDATE pgshard_catalog.managed_slot_creation_attempts
               SET state = 'retired', resolved_at = statement_timestamp()
             WHERE slot_generation = NEW.slot_generation
               AND slot_name = NEW.slot_name
               AND allocation_kind = 'consumer'
               AND slot_role = NEW.slot_role
               AND state = 'activated';
        END IF;
    END IF;

    IF OLD.state <> 'allocated'
       AND (
           NEW.consistent_point IS DISTINCT FROM OLD.consistent_point
           OR NEW.two_phase_at IS DISTINCT FROM OLD.two_phase_at
           OR NEW.activated_at IS DISTINCT FROM OLD.activated_at
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'activated managed slot history is immutable';
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.advance_logical_consumer_checkpoint(
    target_checkpoint_generation uuid,
    expected_ownership_fence bigint,
    expected_checkpoint_ordinal bigint,
    new_checkpoint_lsn pg_lsn,
    new_checkpoint_ordinal bigint,
    new_snapshot_required boolean
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    observed_ownership_fence bigint;
    observed_checkpoint_ordinal bigint;
BEGIN
    -- Match the lock order used by every catalog mutation before locking the
    -- owner and checkpoint rows. A waiting stale owner must observe a fence
    -- committed by the transaction that held this global catalog lock.
    PERFORM
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;

    SELECT consumer_shards.ownership_fence, checkpoints.checkpoint_ordinal
      INTO observed_ownership_fence, observed_checkpoint_ordinal
      FROM pgshard_catalog.logical_consumer_checkpoints AS checkpoints
      JOIN pgshard_catalog.logical_consumer_shards AS consumer_shards
        ON consumer_shards.consumer_id = checkpoints.consumer_id
       AND consumer_shards.logical_database_id = checkpoints.logical_database_id
       AND consumer_shards.shard_id = checkpoints.shard_id
     WHERE checkpoints.checkpoint_generation = target_checkpoint_generation
       AND checkpoints.state = 'current'
     FOR UPDATE OF consumer_shards, checkpoints;

    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'current logical consumer checkpoint does not exist';
    END IF;

    IF observed_ownership_fence IS DISTINCT FROM expected_ownership_fence THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'logical consumer ownership fence compare-and-swap failed: expected %s, observed %s',
                coalesce(expected_ownership_fence::text, 'NULL'),
                observed_ownership_fence
            );
    END IF;

    IF observed_checkpoint_ordinal IS DISTINCT FROM expected_checkpoint_ordinal THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'logical consumer checkpoint compare-and-swap failed: expected ordinal %s, observed %s',
                coalesce(expected_checkpoint_ordinal::text, 'NULL'),
                observed_checkpoint_ordinal
            );
    END IF;

    UPDATE pgshard_catalog.logical_consumer_checkpoints
       SET checkpoint_lsn = new_checkpoint_lsn,
           checkpoint_ordinal = new_checkpoint_ordinal,
           snapshot_required = new_snapshot_required
     WHERE checkpoint_generation = target_checkpoint_generation;

    RETURN new_checkpoint_ordinal;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.validate_routing_epoch(target_routing_epoch bigint)
RETURNS void
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pgshard_catalog
AS $function$
DECLARE
    expected_start numeric(20, 0) := 0;
    range_count bigint := 0;
    current_range record;
BEGIN
    IF NOT EXISTS (
        SELECT FROM pgshard_catalog.routing_epochs
        WHERE routing_epoch = target_routing_epoch AND state = 'staged'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'routing epoch is not staged';
    END IF;

    FOR current_range IN
        SELECT range_start, range_end, shard_id
          FROM pgshard_catalog.routing_ranges
         WHERE routing_epoch = target_routing_epoch
         ORDER BY range_start, range_end
    LOOP
        IF current_range.range_start > expected_start THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format('routing epoch has a gap at %s', expected_start);
        ELSIF current_range.range_start < expected_start THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format('routing epoch overlaps at %s', current_range.range_start);
        END IF;

        IF NOT EXISTS (
            SELECT FROM pgshard_catalog.shards
            WHERE shard_id = current_range.shard_id AND state IN ('active', 'draining')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format('routing epoch references unavailable shard %s', current_range.shard_id);
        END IF;

        expected_start := current_range.range_end;
        range_count := range_count + 1;
    END LOOP;

    IF range_count = 0 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'routing epoch has no ranges';
    END IF;

    IF expected_start <> 18446744073709551616 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = format('routing epoch ends at %s instead of 18446744073709551616', expected_start);
    END IF;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.activate_routing_epoch(
    target_logical_database_id uuid,
    target_routing_epoch bigint,
    expected_active_routing_epoch bigint,
    expected_catalog_epoch bigint
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    observed_catalog_epoch bigint;
    observed_active_routing_epoch bigint;
    target_database_id uuid;
    resulting_catalog_epoch bigint;
BEGIN
    SELECT catalog_epoch
      INTO STRICT observed_catalog_epoch
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;

    IF observed_catalog_epoch IS DISTINCT FROM expected_catalog_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'catalog epoch compare-and-swap failed: expected %s, observed %s',
                coalesce(expected_catalog_epoch::text, 'NULL'),
                observed_catalog_epoch
            );
    END IF;

    SELECT routing_epoch
      INTO observed_active_routing_epoch
      FROM pgshard_catalog.active_routing_epochs
     WHERE logical_database_id = target_logical_database_id
     FOR UPDATE;

    IF observed_active_routing_epoch IS DISTINCT FROM expected_active_routing_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'active routing epoch compare-and-swap failed: expected %s, observed %s',
                coalesce(expected_active_routing_epoch::text, 'NULL'),
                coalesce(observed_active_routing_epoch::text, 'NULL')
            );
    END IF;

    SELECT epochs.logical_database_id
      INTO target_database_id
      FROM pgshard_catalog.routing_epochs AS epochs
      JOIN pgshard_catalog.logical_databases AS databases
        ON databases.logical_database_id = epochs.logical_database_id
     WHERE epochs.routing_epoch = target_routing_epoch
       AND epochs.state = 'staged'
       AND databases.state <> 'retired'
     FOR UPDATE OF epochs;

    IF target_database_id IS NULL OR target_database_id <> target_logical_database_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'target routing epoch is not staged for the requested logical database';
    END IF;

    IF observed_active_routing_epoch IS NOT NULL
       AND target_routing_epoch <= observed_active_routing_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = format(
                'routing epoch must advance: active %s, target %s',
                observed_active_routing_epoch,
                target_routing_epoch
            );
    END IF;

    PERFORM pgshard_catalog.validate_routing_epoch(target_routing_epoch);

    IF observed_active_routing_epoch IS NOT NULL THEN
        UPDATE pgshard_catalog.routing_epochs
           SET state = 'superseded',
               superseded_at = statement_timestamp()
         WHERE routing_epoch = observed_active_routing_epoch;
    END IF;

    UPDATE pgshard_catalog.routing_epochs
       SET state = 'active',
           activated_at = statement_timestamp()
     WHERE routing_epoch = target_routing_epoch;

    INSERT INTO pgshard_catalog.active_routing_epochs(
        logical_database_id,
        routing_epoch,
        activated_at
    )
    VALUES (target_logical_database_id, target_routing_epoch, statement_timestamp())
    ON CONFLICT (logical_database_id) DO UPDATE
        SET routing_epoch = EXCLUDED.routing_epoch,
            activated_at = EXCLUDED.activated_at;

    UPDATE pgshard_catalog.cluster_state
       SET catalog_epoch = greatest(catalog_epoch + 1, target_routing_epoch),
           changed_at = statement_timestamp()
     WHERE singleton
     RETURNING catalog_epoch INTO resulting_catalog_epoch;

    RETURN resulting_catalog_epoch;
END
$function$;

DROP TRIGGER IF EXISTS cluster_configuration_immutable ON pgshard_catalog.cluster_configuration;
CREATE TRIGGER cluster_configuration_immutable
BEFORE UPDATE OR DELETE ON pgshard_catalog.cluster_configuration
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.reject_all_changes();

DROP TRIGGER IF EXISTS routing_epoch_history_immutable ON pgshard_catalog.routing_epochs;
CREATE TRIGGER routing_epoch_history_immutable
BEFORE UPDATE OR DELETE ON pgshard_catalog.routing_epochs
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_routing_epoch_history();

DROP TRIGGER IF EXISTS routing_range_history_immutable ON pgshard_catalog.routing_ranges;
CREATE TRIGGER routing_range_history_immutable
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.routing_ranges
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_routing_range_history();

DROP TRIGGER IF EXISTS operation_tombstone_immutable ON pgshard_catalog.operation_tombstones;
CREATE TRIGGER operation_tombstone_immutable
BEFORE UPDATE OR DELETE ON pgshard_catalog.operation_tombstones
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.reject_all_changes();

DROP TRIGGER IF EXISTS cluster_state_notify ON pgshard_catalog.cluster_state;
CREATE TRIGGER cluster_state_notify
AFTER UPDATE ON pgshard_catalog.cluster_state
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.notify_catalog_state();

DROP TRIGGER IF EXISTS logical_databases_touch_catalog ON pgshard_catalog.logical_databases;
CREATE TRIGGER logical_databases_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_databases
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS logical_databases_lock_catalog ON pgshard_catalog.logical_databases;
CREATE TRIGGER logical_databases_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_databases
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_databases_protect_active_routing ON pgshard_catalog.logical_databases;
CREATE TRIGGER logical_databases_protect_active_routing
BEFORE UPDATE OR DELETE ON pgshard_catalog.logical_databases
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_database_lifecycle();

DROP TRIGGER IF EXISTS shards_touch_catalog ON pgshard_catalog.shards;
CREATE TRIGGER shards_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS shards_lock_catalog ON pgshard_catalog.shards;
CREATE TRIGGER shards_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS shards_protect_active_routing ON pgshard_catalog.shards;
CREATE TRIGGER shards_protect_active_routing
BEFORE UPDATE OR DELETE ON pgshard_catalog.shards
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_shard_lifecycle();

DROP TRIGGER IF EXISTS shards_install_restore_incarnation ON pgshard_catalog.shards;
CREATE TRIGGER shards_install_restore_incarnation
AFTER INSERT ON pgshard_catalog.shards
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.install_initial_shard_restore_incarnation();

DROP TRIGGER IF EXISTS shard_restore_incarnations_touch_catalog
    ON pgshard_catalog.shard_restore_incarnations;
CREATE TRIGGER shard_restore_incarnations_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.shard_restore_incarnations
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS shard_restore_incarnations_lock_catalog
    ON pgshard_catalog.shard_restore_incarnations;
CREATE TRIGGER shard_restore_incarnations_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.shard_restore_incarnations
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS shard_restore_incarnations_protect_history
    ON pgshard_catalog.shard_restore_incarnations;
CREATE TRIGGER shard_restore_incarnations_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.shard_restore_incarnations
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_shard_restore_incarnation();

DROP TRIGGER IF EXISTS slot_sync_probes_touch_catalog
    ON pgshard_catalog.slot_sync_probes;
CREATE TRIGGER slot_sync_probes_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.slot_sync_probes
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS slot_sync_probes_lock_catalog
    ON pgshard_catalog.slot_sync_probes;
CREATE TRIGGER slot_sync_probes_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.slot_sync_probes
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS slot_sync_probes_protect_history
    ON pgshard_catalog.slot_sync_probes;
CREATE TRIGGER slot_sync_probes_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.slot_sync_probes
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_slot_sync_probe();

DROP TRIGGER IF EXISTS registered_tables_touch_catalog ON pgshard_catalog.registered_tables;
CREATE TRIGGER registered_tables_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.registered_tables
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS registered_tables_lock_catalog ON pgshard_catalog.registered_tables;
CREATE TRIGGER registered_tables_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.registered_tables
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_consumers_touch_catalog ON pgshard_catalog.logical_consumers;
CREATE TRIGGER logical_consumers_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumers
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS logical_consumers_lock_catalog ON pgshard_catalog.logical_consumers;
CREATE TRIGGER logical_consumers_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumers
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_consumers_protect_lifecycle ON pgshard_catalog.logical_consumers;
CREATE TRIGGER logical_consumers_protect_lifecycle
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumers
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_logical_consumer_lifecycle();

DROP TRIGGER IF EXISTS logical_consumer_shards_touch_catalog
    ON pgshard_catalog.logical_consumer_shards;
CREATE TRIGGER logical_consumer_shards_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS logical_consumer_shards_lock_catalog
    ON pgshard_catalog.logical_consumer_shards;
CREATE TRIGGER logical_consumer_shards_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_consumer_shards_protect_lifecycle
    ON pgshard_catalog.logical_consumer_shards;
CREATE TRIGGER logical_consumer_shards_protect_lifecycle
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_shards
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_logical_consumer_shard_lifecycle();

DROP TRIGGER IF EXISTS logical_consumer_checkpoints_touch_catalog
    ON pgshard_catalog.logical_consumer_checkpoints;
CREATE TRIGGER logical_consumer_checkpoints_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_checkpoints
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS logical_consumer_checkpoints_lock_catalog
    ON pgshard_catalog.logical_consumer_checkpoints;
CREATE TRIGGER logical_consumer_checkpoints_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_checkpoints
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_consumer_checkpoints_protect_history
    ON pgshard_catalog.logical_consumer_checkpoints;
CREATE TRIGGER logical_consumer_checkpoints_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_checkpoints
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_logical_consumer_checkpoint();

DROP TRIGGER IF EXISTS logical_consumer_attachments_touch_catalog
    ON pgshard_catalog.logical_consumer_attachments;
CREATE TRIGGER logical_consumer_attachments_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_attachments
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS logical_consumer_attachments_lock_catalog
    ON pgshard_catalog.logical_consumer_attachments;
CREATE TRIGGER logical_consumer_attachments_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_attachments
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS logical_consumer_attachments_protect_history
    ON pgshard_catalog.logical_consumer_attachments;
CREATE TRIGGER logical_consumer_attachments_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.logical_consumer_attachments
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_logical_consumer_attachment();

DROP TRIGGER IF EXISTS managed_replication_slots_touch_catalog
    ON pgshard_catalog.managed_replication_slots;
CREATE TRIGGER managed_replication_slots_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.managed_replication_slots
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS managed_replication_slots_lock_catalog
    ON pgshard_catalog.managed_replication_slots;
CREATE TRIGGER managed_replication_slots_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.managed_replication_slots
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS managed_replication_slots_protect_history
    ON pgshard_catalog.managed_replication_slots;
CREATE TRIGGER managed_replication_slots_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.managed_replication_slots
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_managed_replication_slot();

DROP TRIGGER IF EXISTS managed_slot_creation_attempts_protect_history
    ON pgshard_catalog.managed_slot_creation_attempts;
CREATE TRIGGER managed_slot_creation_attempts_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.managed_slot_creation_attempts
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_managed_slot_creation_attempt();

REVOKE ALL ON ALL TABLES IN SCHEMA pgshard_catalog FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pgshard_catalog FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgshard_catalog FROM PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA pgshard_catalog TO pgshard_catalog_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgshard_catalog TO pgshard_catalog_reader;

GRANT EXECUTE ON FUNCTION pgshard_catalog.begin_managed_slot_creation_attempt(
    uuid, text, text, numeric, bigint, bigint, uuid, bigint, uuid
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.abandon_managed_slot_creation_attempt(
    uuid, text, uuid
) TO pgshard_catalog_admin;

GRANT INSERT (database_name) ON pgshard_catalog.logical_databases TO pgshard_catalog_admin;
GRANT INSERT (shard_id, shard_number, state), UPDATE (state)
    ON pgshard_catalog.shards TO pgshard_catalog_admin;
GRANT INSERT (restore_incarnation, shard_id), UPDATE (state, retired_at)
    ON pgshard_catalog.shard_restore_incarnations TO pgshard_catalog_admin;
GRANT INSERT (
    probe_generation,
    shard_id,
    restore_incarnation,
    system_identifier,
    database_oid,
    database_name,
    source_timeline,
    slot_name
), UPDATE (
    consistent_point,
    creation_receipt_id,
    cleanup_receipt_id,
    state,
    activated_at,
    retiring_at,
    retired_at
)
    ON pgshard_catalog.slot_sync_probes TO pgshard_catalog_admin;
GRANT INSERT (logical_database_id) ON pgshard_catalog.routing_epochs TO pgshard_catalog_admin;
GRANT INSERT, UPDATE, DELETE ON pgshard_catalog.routing_ranges TO pgshard_catalog_admin;
GRANT INSERT (
    logical_database_id,
    schema_name,
    table_name,
    shard_key_column,
    shard_key_type,
    shard_key_encoding,
    shard_key_collation,
    state
) ON pgshard_catalog.registered_tables TO pgshard_catalog_admin;
GRANT INSERT (logical_database_id, consumer_name, purpose), UPDATE (state)
    ON pgshard_catalog.logical_consumers TO pgshard_catalog_admin;
GRANT INSERT (consumer_id, logical_database_id, shard_id), UPDATE (ownership_fence, state)
    ON pgshard_catalog.logical_consumer_shards TO pgshard_catalog_admin;
GRANT INSERT (
    checkpoint_generation,
    consumer_id,
    logical_database_id,
    shard_id,
    restore_incarnation,
    system_identifier,
    database_oid,
    source_timeline
), UPDATE (state, retired_at)
    ON pgshard_catalog.logical_consumer_checkpoints TO pgshard_catalog_admin;
GRANT INSERT (
    attachment_generation,
    consumer_id,
    logical_database_id,
    shard_id,
    restore_incarnation,
    system_identifier,
    database_oid,
    database_name,
    selected_source_member_ordinal,
    selected_source_role,
    selected_source_timeline
), UPDATE (state, activated_at, retired_at)
    ON pgshard_catalog.logical_consumer_attachments TO pgshard_catalog_admin;
GRANT INSERT (
    slot_generation,
    attachment_generation,
    consumer_id,
    logical_database_id,
    shard_id,
    slot_role,
    member_ordinal,
    slot_name
), UPDATE (consistent_point, two_phase_at, state, activated_at, retired_at)
    ON pgshard_catalog.managed_replication_slots TO pgshard_catalog_admin;
GRANT INSERT ON pgshard_catalog.operation_tombstones TO pgshard_catalog_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pgshard_catalog TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.validate_routing_epoch(bigint)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.activate_routing_epoch(uuid, bigint, bigint, bigint)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.advance_logical_consumer_checkpoint(
    uuid,
    bigint,
    bigint,
    pg_lsn,
    bigint,
    boolean
) TO pgshard_catalog_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog GRANT SELECT ON TABLES TO pgshard_catalog_reader;

COMMIT;
