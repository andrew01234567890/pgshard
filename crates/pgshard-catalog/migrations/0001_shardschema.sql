BEGIN;

-- Pin every caller-settable part of the migration environment before the
-- requirements check. A restored database or bootstrap role must not be able
-- to shadow pg_catalog routines or select an executable table access method.
SET LOCAL search_path = pg_catalog;
SET LOCAL row_security = off;
SET LOCAL check_function_bodies = on;
SET LOCAL default_tablespace = '';
SET LOCAL temp_tablespaces = '';
SET LOCAL default_table_access_method = heap;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_indexonlyscan = off;
SET LOCAL enable_bitmapscan = off;

-- Takeover validation uses statement snapshots.  Override a non-default
-- session characteristic before the transaction takes its first snapshot.  An
-- already-active stronger-isolation snapshot fails closed; an enclosing READ
-- COMMITTED transaction continues to receive a fresh snapshot per statement.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DO $pgshard_requirements$
BEGIN
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION USING
            ERRCODE = '25000',
            MESSAGE = 'the shardschema migration requires READ COMMITTED isolation';
    END IF;

    IF NOT EXISTS (
        SELECT
          FROM pg_catalog.pg_roles AS roles
         WHERE roles.rolname = current_user
           AND roles.rolsuper
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pgshard catalog migration requires a superuser bootstrap principal';
    END IF;

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

-- A restored dedicated catalog database must not contain database-wide DDL
-- hooks. DO is not an event-trigger-supported command, so the requirements
-- check can reject a non-superuser with the stable migration error before these
-- privileged settings. Disable hooks and trigger suppression before the first
-- supported DDL or catalog DML, then reject their persisted presence.
SET LOCAL event_triggers = off;
SET LOCAL session_replication_role = origin;

DO $pgshard_reject_event_triggers$
BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_event_trigger) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing shardschema contains an unsupported event trigger';
    END IF;
END
$pgshard_reject_event_triggers$;

DO $pgshard_reject_executable_catalog_relations$
DECLARE
    heap_access_method oid;
    unsafe_relation text;
BEGIN
    SELECT methods.oid
      INTO STRICT heap_access_method
      FROM pg_catalog.pg_am AS methods
     WHERE methods.amname = 'heap'
       AND methods.amtype = 't';

    SELECT relations.relname
      INTO unsafe_relation
      FROM pg_catalog.pg_class AS relations
      JOIN pg_catalog.pg_namespace AS namespaces
        ON namespaces.oid = relations.relnamespace
     WHERE namespaces.nspname = 'pgshard_catalog'
       AND (
           relations.relkind IN ('p', 'v', 'm', 'f')
           OR (relations.relkind = 'r' AND relations.relam <> heap_access_method)
       )
     ORDER BY relations.oid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains an executable or noncanonical relation',
            DETAIL = unsafe_relation;
    END IF;
END
$pgshard_reject_executable_catalog_relations$;

-- Freeze every pre-existing trigger/FK-capable catalog relation before
-- inspecting attached triggers or foreign keys.  CREATE TRIGGER and
-- referential-constraint DDL require a conflicting relation lock.  Fail rather
-- than wait while holding an earlier relation: normal catalog DML takes its
-- target relation before cluster_state, so a partial blocking lock set can
-- deadlock that order.  The caller retries the complete transaction after a
-- quiet window.  A successful pass retains every lock through ownership
-- transfer, ACL reset, and trigger recreation.
DO $pgshard_lock_existing_catalog_relations$
DECLARE
    catalog_relation record;
BEGIN
    FOR catalog_relation IN
        SELECT namespaces.nspname, relations.relname
          FROM pg_catalog.pg_class AS relations
          JOIN pg_catalog.pg_namespace AS namespaces
            ON namespaces.oid = relations.relnamespace
         WHERE namespaces.nspname = 'pgshard_catalog'
           AND relations.relkind = 'r'
         ORDER BY relations.oid
    LOOP
        EXECUTE pg_catalog.format(
            'LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE NOWAIT',
            catalog_relation.nspname,
            catalog_relation.relname
        );
    END LOOP;
END
$pgshard_lock_existing_catalog_relations$;

-- Catalog rows remain hostile until ownership and executable metadata have
-- both been validated. Base-table locks above stabilize constraints, defaults,
-- and indexes. Released definitions depend only on pinned built-in operators,
-- routines, and btree operator classes, whose dependencies PostgreSQL omits
-- from pg_depend. Any dependency recorded against an executable object is
-- therefore user-created and must be rejected before catalog rows are read or
-- seed DML can evaluate it.
DO $pgshard_reject_executable_relation_metadata$
DECLARE
    catalog_schema_oid oid;
    metadata_hash text;
    unsafe_metadata text;
BEGIN
    SELECT namespaces.oid
      INTO catalog_schema_oid
      FROM pg_catalog.pg_namespace AS namespaces
     WHERE namespaces.nspname = 'pgshard_catalog';
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT pg_catalog.format(
               'relation %I.%I inherits from %I.%I',
               child_namespaces.nspname,
               children.relname,
               parent_namespaces.nspname,
               parents.relname
           )
      INTO unsafe_metadata
      FROM pg_catalog.pg_inherits AS inheritance
      JOIN pg_catalog.pg_class AS parents
        ON parents.oid = inheritance.inhparent
      JOIN pg_catalog.pg_class AS children
        ON children.oid = inheritance.inhrelid
      JOIN pg_catalog.pg_namespace AS parent_namespaces
        ON parent_namespaces.oid = parents.relnamespace
      JOIN pg_catalog.pg_namespace AS child_namespaces
        ON child_namespaces.oid = children.relnamespace
     WHERE parents.relnamespace = catalog_schema_oid
        OR children.relnamespace = catalog_schema_oid
     ORDER BY inheritance.inhseqno, children.oid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains external inherited relations',
            DETAIL = unsafe_metadata;
    END IF;

    WITH metadata AS (
        SELECT pg_catalog.format(
                   'constraint|%s|%s|%s|%s|%s|%s|%s|%s',
                   CASE
                       WHEN relations.oid IS NOT NULL THEN
                           pg_catalog.format(
                               '%I.%I',
                               relation_namespaces.nspname,
                               relations.relname
                           )
                       ELSE pg_catalog.format(
                           '%I.%I',
                           type_namespaces.nspname,
                           types.typname
                       )
                   END,
                   constraints.conname,
                   constraints.contype,
                   constraints.condeferrable,
                   constraints.condeferred,
                   constraints.convalidated,
                   constraints.connoinherit,
                   pg_catalog.pg_get_constraintdef(constraints.oid, false)
               ) AS object
          FROM pg_catalog.pg_constraint AS constraints
          LEFT JOIN pg_catalog.pg_class AS relations
            ON relations.oid = constraints.conrelid
          LEFT JOIN pg_catalog.pg_namespace AS relation_namespaces
            ON relation_namespaces.oid = relations.relnamespace
          LEFT JOIN pg_catalog.pg_type AS types
            ON types.oid = constraints.contypid
          LEFT JOIN pg_catalog.pg_namespace AS type_namespaces
            ON type_namespaces.oid = types.typnamespace
         WHERE constraints.connamespace = catalog_schema_oid
            OR relations.relnamespace = catalog_schema_oid
        UNION ALL
        SELECT pg_catalog.format(
                   'default|%I.%I|%s|%I|%s',
                   namespaces.nspname,
                   relations.relname,
                   attributes.attnum,
                   attributes.attname,
                   pg_catalog.pg_get_expr(
                       defaults.adbin,
                       defaults.adrelid,
                       false
                   )
               )
          FROM pg_catalog.pg_attrdef AS defaults
          JOIN pg_catalog.pg_class AS relations
            ON relations.oid = defaults.adrelid
          JOIN pg_catalog.pg_namespace AS namespaces
            ON namespaces.oid = relations.relnamespace
          JOIN pg_catalog.pg_attribute AS attributes
            ON attributes.attrelid = defaults.adrelid
           AND attributes.attnum = defaults.adnum
         WHERE relations.relnamespace = catalog_schema_oid
        UNION ALL
        SELECT pg_catalog.format(
                   'index|%I.%I|%I.%I|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
                   table_namespaces.nspname,
                   tables.relname,
                   index_namespaces.nspname,
                   index_relations.relname,
                   access_methods.amname,
                   indexes.indisvalid,
                   indexes.indisready,
                   indexes.indislive,
                   indexes.indisunique,
                   indexes.indisprimary,
                   indexes.indisexclusion,
                   indexes.indisreplident,
                   indexes.indisclustered,
                   pg_catalog.pg_get_indexdef(indexes.indexrelid, 0, false)
               )
          FROM pg_catalog.pg_index AS indexes
          JOIN pg_catalog.pg_class AS tables
            ON tables.oid = indexes.indrelid
          JOIN pg_catalog.pg_namespace AS table_namespaces
            ON table_namespaces.oid = tables.relnamespace
          JOIN pg_catalog.pg_class AS index_relations
            ON index_relations.oid = indexes.indexrelid
          JOIN pg_catalog.pg_namespace AS index_namespaces
            ON index_namespaces.oid = index_relations.relnamespace
          JOIN pg_catalog.pg_am AS access_methods
            ON access_methods.oid = index_relations.relam
         WHERE tables.relnamespace = catalog_schema_oid
    )
    SELECT pg_catalog.encode(
               pg_catalog.sha256(
                   pg_catalog.convert_to(
                       pg_catalog.string_agg(
                           object,
                           E'\n' ORDER BY object COLLATE "C"
                       ),
                       'UTF8'
                   )
               ),
               'hex'
           )
      INTO metadata_hash
      FROM metadata;
    IF metadata_hash NOT IN (
        '692f622a91f66a3bfeb303e32dfface8309e93c411c9b1fe8cbdd81a2e4e420e',
        '8bf2ce2a858b56e603b85bff106a20d62af435c549ecbc6eb7ad61c12ca65981',
        '858ff7c989f5aabb02fe978100b1cba3090cef600636cc5b7fc0a0fb2c9e3a11',
        '5cef33e65f629aee202b314a13081a54bd64df19e8efbfc710df7abb1a97f32e',
        '0b8dd28bbbad4d55039f300d1969ba0737c5f783b286d3f7b206a94ea7b08efb',
        '34beddb4cdfaf101bdb63b4f69283793afea56d50afb26116f039e012fec96d6'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains noncanonical executable relation metadata',
            DETAIL = COALESCE(metadata_hash, 'no released executable metadata');
    END IF;

    WITH expression_objects(classid, objid, identity) AS (
        SELECT 'pg_catalog.pg_constraint'::pg_catalog.regclass,
               constraints.oid,
               pg_catalog.pg_describe_object(
                   'pg_catalog.pg_constraint'::pg_catalog.regclass,
                   constraints.oid,
                   0
               )
          FROM pg_catalog.pg_constraint AS constraints
          LEFT JOIN pg_catalog.pg_class AS relations
            ON relations.oid = constraints.conrelid
         WHERE constraints.connamespace = catalog_schema_oid
            OR relations.relnamespace = catalog_schema_oid
        UNION ALL
        SELECT 'pg_catalog.pg_attrdef'::pg_catalog.regclass,
               defaults.oid,
               pg_catalog.pg_describe_object(
                   'pg_catalog.pg_attrdef'::pg_catalog.regclass,
                   defaults.oid,
                   0
               )
          FROM pg_catalog.pg_attrdef AS defaults
          JOIN pg_catalog.pg_class AS relations
            ON relations.oid = defaults.adrelid
         WHERE relations.relnamespace = catalog_schema_oid
        UNION ALL
        SELECT 'pg_catalog.pg_class'::pg_catalog.regclass,
               indexes.indexrelid,
               pg_catalog.pg_describe_object(
                   'pg_catalog.pg_class'::pg_catalog.regclass,
                   indexes.indexrelid,
                   0
               )
          FROM pg_catalog.pg_index AS indexes
          JOIN pg_catalog.pg_class AS relations
            ON relations.oid = indexes.indrelid
         WHERE relations.relnamespace = catalog_schema_oid
    )
    SELECT pg_catalog.format(
               '%s references %s',
               objects.identity,
               pg_catalog.pg_describe_object(
                   dependencies.refclassid,
                   dependencies.refobjid,
                   dependencies.refobjsubid
               )
           )
      INTO unsafe_metadata
      FROM expression_objects AS objects
      JOIN pg_catalog.pg_depend AS dependencies
        ON dependencies.classid = objects.classid
       AND dependencies.objid = objects.objid
     WHERE dependencies.refclassid IN (
               'pg_catalog.pg_proc'::pg_catalog.regclass,
               'pg_catalog.pg_operator'::pg_catalog.regclass,
               'pg_catalog.pg_opclass'::pg_catalog.regclass,
               'pg_catalog.pg_opfamily'::pg_catalog.regclass
           )
     ORDER BY objects.identity, dependencies.refclassid,
              dependencies.refobjid, dependencies.refobjsubid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains executable relation metadata',
            DETAIL = unsafe_metadata;
    END IF;

    SELECT pg_catalog.format(
               'index %I.%I',
               index_namespaces.nspname,
               index_relations.relname
           )
      INTO unsafe_metadata
      FROM pg_catalog.pg_index AS indexes
      JOIN pg_catalog.pg_class AS relations
        ON relations.oid = indexes.indrelid
      JOIN pg_catalog.pg_class AS index_relations
        ON index_relations.oid = indexes.indexrelid
      JOIN pg_catalog.pg_namespace AS index_namespaces
        ON index_namespaces.oid = index_relations.relnamespace
      JOIN pg_catalog.pg_am AS access_methods
        ON access_methods.oid = index_relations.relam
     WHERE relations.relnamespace = catalog_schema_oid
       AND (access_methods.amname <> 'btree' OR indexes.indexprs IS NOT NULL)
     ORDER BY index_relations.oid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains executable relation metadata',
            DETAIL = unsafe_metadata;
    END IF;
END
$pgshard_reject_executable_relation_metadata$;

DO $pgshard_role_bootstrap$
DECLARE
    bootstrap_superuser_oid CONSTANT pg_catalog.oid := 10;
    catalog_schema_oid oid;
    catalog_schema_owner oid;
    catalog_schema_owner_name name;
    catalog_schema_owner_is_superuser boolean;
    dependent_membership record;
    owner_membership record;
    expected_sequence record;
    mismatched_object text;
    sequence_next_value numeric;
    sequence_maximum_value numeric;
    sequence_maximum_generated_value numeric;
BEGIN
    SELECT namespaces.oid, owners.oid, owners.rolname, owners.rolsuper
      INTO catalog_schema_oid,
           catalog_schema_owner,
           catalog_schema_owner_name,
           catalog_schema_owner_is_superuser
      FROM pg_catalog.pg_namespace AS namespaces
      JOIN pg_catalog.pg_roles AS owners ON owners.oid = namespaces.nspowner
     WHERE namespaces.nspname = 'pgshard_catalog';

    IF NOT FOUND THEN
        IF EXISTS (
            SELECT
              FROM pg_catalog.pg_roles AS roles
             WHERE roles.rolname IN (
                       'pgshard_catalog_owner',
                       'pgshard_catalog_reader',
                       'pgshard_catalog_admin'
                   )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501',
                MESSAGE = 'pgshard catalog roles exist before catalog bootstrap';
        END IF;
        RETURN;
    END IF;

    SELECT objects.object_identity
      INTO mismatched_object
      FROM (
          SELECT pg_catalog.format('relation %I', relations.relname) AS object_identity,
                 relations.relowner AS object_owner
            FROM pg_catalog.pg_class AS relations
           WHERE relations.relnamespace = catalog_schema_oid
          UNION ALL
          SELECT pg_catalog.format('routine %I', routines.proname), routines.proowner
            FROM pg_catalog.pg_proc AS routines
           WHERE routines.pronamespace = catalog_schema_oid
          UNION ALL
          SELECT pg_catalog.format('type %I', types.typname), types.typowner
            FROM pg_catalog.pg_type AS types
           WHERE types.typnamespace = catalog_schema_oid
          UNION ALL
          SELECT pg_catalog.format('collation %I', collations.collname), collations.collowner
            FROM pg_catalog.pg_collation AS collations
           WHERE collations.collnamespace = catalog_schema_oid
      ) AS objects
     WHERE objects.object_owner <> catalog_schema_owner
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog objects must share the schema owner',
            DETAIL = mismatched_object;
    END IF;

    -- The released catalog contains only relations, routines, types and
    -- collations as independently owned schema objects. Reject any other
    -- namespaced object class rather than transferring only part of its state.
    SELECT pg_catalog.pg_describe_object(
               dependencies.classid,
               dependencies.objid,
               dependencies.objsubid
           )
      INTO mismatched_object
      FROM pg_catalog.pg_depend AS dependencies
     WHERE dependencies.refclassid =
               'pg_catalog.pg_namespace'::pg_catalog.regclass
       AND dependencies.refobjid = catalog_schema_oid
       AND dependencies.refobjsubid = 0
       AND dependencies.deptype = 'n'
       AND dependencies.classid NOT IN (
               'pg_catalog.pg_class'::pg_catalog.regclass,
               'pg_catalog.pg_proc'::pg_catalog.regclass,
               'pg_catalog.pg_type'::pg_catalog.regclass,
               'pg_catalog.pg_collation'::pg_catalog.regclass
           )
     ORDER BY dependencies.classid, dependencies.objid, dependencies.objsubid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains an unsupported schema object',
            DETAIL = mismatched_object;
    END IF;

    -- Rules depend on their target relation rather than directly on the
    -- namespace. The catalog defines no rewrite rules, so reject every one
    -- before trusted ownership or ACL transition.
    SELECT pg_catalog.format(
               'rule %I on pgshard_catalog.%I',
               rewrite_rules.rulename,
               relations.relname
           )
      INTO mismatched_object
      FROM pg_catalog.pg_rewrite AS rewrite_rules
      JOIN pg_catalog.pg_class AS relations
        ON relations.oid = rewrite_rules.ev_class
     WHERE relations.relnamespace = catalog_schema_oid
     ORDER BY relations.oid, rewrite_rules.oid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains an unsupported rewrite rule',
            DETAIL = mismatched_object;
    END IF;

    -- Both released identity sequences have fixed bigint parameters and
    -- internal ownership dependencies. CREATE TABLE IF NOT EXISTS cannot
    -- recreate or repair a damaged sequence on upgrade.
    SELECT pg_catalog.format('sequence pgshard_catalog.%I', sequences.relname)
      INTO mismatched_object
      FROM pg_catalog.pg_sequence AS sequence_metadata
      JOIN pg_catalog.pg_class AS sequences
        ON sequences.oid = sequence_metadata.seqrelid
      LEFT JOIN pg_catalog.pg_depend AS ownership
        ON ownership.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
       AND ownership.objid = sequences.oid
       AND ownership.objsubid = 0
       AND ownership.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
       AND ownership.refobjsubid > 0
       AND ownership.deptype IN ('a', 'i')
      LEFT JOIN pg_catalog.pg_class AS owned_relations
        ON owned_relations.oid = ownership.refobjid
      LEFT JOIN pg_catalog.pg_attribute AS owned_attributes
        ON owned_attributes.attrelid = ownership.refobjid
       AND owned_attributes.attnum = ownership.refobjsubid
     WHERE sequences.relnamespace = catalog_schema_oid
       AND NOT (
           (sequences.relname, owned_relations.relname, owned_attributes.attname) IN (
               ('routing_epochs_routing_epoch_seq', 'routing_epochs', 'routing_epoch'),
               ('registered_tables_registered_table_id_seq', 'registered_tables', 'registered_table_id')
           )
           AND sequence_metadata.seqtypid = 'pg_catalog.int8'::pg_catalog.regtype
           AND sequence_metadata.seqstart = 1
           AND sequence_metadata.seqincrement = 1
           AND sequence_metadata.seqmax = 9223372036854775807
           AND sequence_metadata.seqmin = 1
           AND sequence_metadata.seqcache = 1
           AND NOT sequence_metadata.seqcycle
       )
     ORDER BY sequences.oid
     LIMIT 1;
    IF FOUND OR NOT (
        (
            SELECT pg_catalog.count(*) = 2
              FROM pg_catalog.pg_sequence AS sequence_metadata
              JOIN pg_catalog.pg_class AS sequences
                ON sequences.oid = sequence_metadata.seqrelid
             WHERE sequences.relnamespace = catalog_schema_oid
        )
        OR (
            NOT EXISTS (
                SELECT
                  FROM pg_catalog.pg_sequence AS sequence_metadata
                  JOIN pg_catalog.pg_class AS sequences
                    ON sequences.oid = sequence_metadata.seqrelid
                 WHERE sequences.relnamespace = catalog_schema_oid
            )
            AND pg_catalog.to_regclass('pgshard_catalog.routing_epochs') IS NULL
            AND pg_catalog.to_regclass('pgshard_catalog.registered_tables') IS NULL
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains an unsupported identity sequence',
            DETAIL = COALESCE(mismatched_object, 'expected both released identity sequences');
    END IF;

    -- Sequence calls are intentionally not transactional, so a restored or
    -- manually rewound last_value can be structurally canonical while making
    -- the next identity value collide with an existing row. Validate the
    -- effective next values before any migration DDL or seed DML.
    FOR expected_sequence IN
        SELECT *
          FROM (VALUES
              (
                  'routing_epochs_routing_epoch_seq'::pg_catalog.name,
                  'routing_epochs'::pg_catalog.name,
                  'routing_epoch'::pg_catalog.name
              ),
              (
                  'registered_tables_registered_table_id_seq'::pg_catalog.name,
                  'registered_tables'::pg_catalog.name,
                  'registered_table_id'::pg_catalog.name
              )
          ) AS expected_sequence(sequence_name, relation_name, column_name)
    LOOP
        IF pg_catalog.to_regclass(
               pg_catalog.format(
                   'pgshard_catalog.%I',
                   expected_sequence.sequence_name
               )
           ) IS NOT NULL THEN
            EXECUTE pg_catalog.format(
                'SELECT CASE WHEN is_called THEN last_value::numeric + 1 '
                'ELSE last_value::numeric END '
                'FROM pgshard_catalog.%I',
                expected_sequence.sequence_name
            ) INTO sequence_next_value;
            EXECUTE pg_catalog.format(
                'SELECT COALESCE(pg_catalog.max(%I)::numeric, 0) '
                'FROM pgshard_catalog.%I',
                expected_sequence.column_name,
                expected_sequence.relation_name
            ) INTO sequence_maximum_value;
            SELECT sequences.seqmax::numeric
              INTO sequence_maximum_generated_value
              FROM pg_catalog.pg_sequence AS sequences
             WHERE sequences.seqrelid = pg_catalog.to_regclass(
                       pg_catalog.format(
                           'pgshard_catalog.%I',
                           expected_sequence.sequence_name
                       )
                   );
            IF sequence_next_value <= sequence_maximum_value
               OR sequence_next_value > sequence_maximum_generated_value THEN
                RAISE EXCEPTION USING
                    ERRCODE = '42501',
                    MESSAGE = 'pre-existing pgshard_catalog contains unsafe identity sequence progress',
                    DETAIL = pg_catalog.format(
                        'sequence pgshard_catalog.%I would generate %s outside the safe range above existing maximum %s and at most %s',
                        expected_sequence.sequence_name,
                        sequence_next_value,
                        sequence_maximum_value,
                        sequence_maximum_generated_value
                    );
            END IF;
        END IF;
    END LOOP;

    -- A disabled internal FK trigger can admit an incarnation for a shard
    -- that does not exist. Validate both directions before replacing any
    -- function body or running seed DML; the ordinary forward FK check alone
    -- is not sufficient once restored bytes are treated as hostile input.
    IF pg_catalog.to_regclass('pgshard_catalog.shards') IS NOT NULL
       AND pg_catalog.to_regclass(
               'pgshard_catalog.shard_restore_incarnations'
           ) IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
              FROM pgshard_catalog.shards AS shards
             WHERE NOT EXISTS (
                       SELECT
                         FROM pgshard_catalog.shard_restore_incarnations AS history
                        WHERE history.shard_id = shards.shard_id
                   )
                OR (shards.state = 'active') IS DISTINCT FROM EXISTS (
                       SELECT
                         FROM pgshard_catalog.shard_restore_incarnations AS incarnations
                        WHERE incarnations.shard_id = shards.shard_id
                          AND incarnations.state = 'active'
                   )
            UNION ALL
            SELECT 1
              FROM pgshard_catalog.shard_restore_incarnations AS incarnations
              LEFT JOIN pgshard_catalog.shards AS shards
                ON shards.shard_id = incarnations.shard_id
             WHERE shards.shard_id IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501',
                MESSAGE = 'pre-existing pgshard_catalog contains invalid restore lineage';
        END IF;
    END IF;

    -- A role with TRIGGER or REFERENCES can attach executable or referential
    -- triggers without owning a catalog relation. Those rows do not depend
    -- directly on the namespace, so validate them separately before any
    -- trusted ownership or ACL transition.
    SELECT pg_catalog.format(
               'trigger %I on pgshard_catalog.%I',
               triggers.tgname,
               relations.relname
           )
      INTO mismatched_object
      FROM pg_catalog.pg_trigger AS triggers
      JOIN pg_catalog.pg_class AS relations
        ON relations.oid = triggers.tgrelid
      JOIN pg_catalog.pg_proc AS routines
        ON routines.oid = triggers.tgfoid
      JOIN pg_catalog.pg_namespace AS routine_namespaces
        ON routine_namespaces.oid = routines.pronamespace
     WHERE relations.relnamespace = catalog_schema_oid
       AND (
           (
               NOT triggers.tgisinternal
               AND (
                   routine_namespaces.nspname <> 'pgshard_catalog'
                   OR routines.pronargs <> 0
                   OR triggers.tgenabled <> 'O'
                   OR triggers.tgconstraint <> 0
                   OR triggers.tgconstrrelid <> 0
                   OR triggers.tgconstrindid <> 0
                   OR triggers.tgdeferrable
                   OR triggers.tginitdeferred
                   OR triggers.tgparentid <> 0
                   OR triggers.tgnargs <> 0
                   OR pg_catalog.octet_length(triggers.tgargs) <> 0
                   OR triggers.tgattr <> ''::pg_catalog.int2vector
                   OR triggers.tgqual IS NOT NULL
                   OR triggers.tgoldtable IS NOT NULL
                   OR triggers.tgnewtable IS NOT NULL
                   OR NOT EXISTS (
                       SELECT
                         FROM (VALUES
                             ('cluster_configuration', 'cluster_configuration_immutable', 'reject_all_changes', 27),
                             ('cluster_state', 'cluster_state_notify', 'notify_catalog_state', 17),
                             ('database_shard_placements', 'database_shard_placements_lock_catalog', 'lock_catalog_state', 30),
                             ('database_shard_placements', 'database_shard_placements_protect_history', 'protect_database_shard_placement', 31),
                             ('database_shard_placements', 'database_shard_placements_touch_catalog', 'touch_catalog_state', 28),
                             ('database_shards', 'database_shards_lock_catalog', 'lock_catalog_state', 30),
                             ('database_shards', 'database_shards_protect_lifecycle', 'protect_database_shard_lifecycle', 31),
                             ('database_shards', 'database_shards_touch_catalog', 'touch_catalog_state', 28),
                             ('logical_consumer_attachments', 'logical_consumer_attachments_lock_catalog', 'lock_catalog_state', 30),
                             ('logical_consumer_attachments', 'logical_consumer_attachments_protect_history', 'protect_logical_consumer_attachment', 31),
                             ('logical_consumer_attachments', 'logical_consumer_attachments_touch_catalog', 'touch_catalog_state', 28),
                             ('logical_consumer_checkpoints', 'logical_consumer_checkpoints_lock_catalog', 'lock_catalog_state', 30),
                             ('logical_consumer_checkpoints', 'logical_consumer_checkpoints_protect_history', 'protect_logical_consumer_checkpoint', 31),
                             ('logical_consumer_checkpoints', 'logical_consumer_checkpoints_touch_catalog', 'touch_catalog_state', 28),
                             ('logical_consumer_shards', 'logical_consumer_shards_lock_catalog', 'lock_catalog_state', 30),
                             ('logical_consumer_shards', 'logical_consumer_shards_protect_lifecycle', 'protect_logical_consumer_shard_lifecycle', 31),
                             ('logical_consumer_shards', 'logical_consumer_shards_touch_catalog', 'touch_catalog_state', 28),
                             ('logical_consumers', 'logical_consumers_lock_catalog', 'lock_catalog_state', 30),
                             ('logical_consumers', 'logical_consumers_protect_lifecycle', 'protect_logical_consumer_lifecycle', 31),
                             ('logical_consumers', 'logical_consumers_touch_catalog', 'touch_catalog_state', 28),
                             ('logical_databases', 'logical_databases_lock_catalog', 'lock_catalog_state', 30),
                             ('logical_databases', 'logical_databases_protect_active_routing', 'protect_database_lifecycle', 27),
                             ('logical_databases', 'logical_databases_touch_catalog', 'touch_catalog_state', 28),
                             ('managed_replication_slots', 'managed_replication_slots_lock_catalog', 'lock_catalog_state', 30),
                             ('managed_replication_slots', 'managed_replication_slots_protect_history', 'protect_managed_replication_slot', 31),
                             ('managed_replication_slots', 'managed_replication_slots_touch_catalog', 'touch_catalog_state', 28),
                             ('managed_slot_creation_attempts', 'managed_slot_creation_attempts_protect_history', 'protect_managed_slot_creation_attempt', 31),
                             ('operation_tombstones', 'operation_tombstone_immutable', 'reject_all_changes', 27),
                             ('registered_tables', 'registered_tables_lock_catalog', 'lock_catalog_state', 30),
                             ('registered_tables', 'registered_tables_touch_catalog', 'touch_catalog_state', 28),
                             ('routing_epochs', 'routing_epoch_history_immutable', 'protect_routing_epoch_history', 27),
                             ('routing_ranges', 'routing_range_history_immutable', 'protect_routing_range_history', 31),
                             ('shard_restore_incarnations', 'shard_restore_incarnations_lock_catalog', 'lock_catalog_state', 30),
                             ('shard_restore_incarnations', 'shard_restore_incarnations_protect_history', 'protect_shard_restore_incarnation', 31),
                             ('shard_restore_incarnations', 'shard_restore_incarnations_touch_catalog', 'touch_catalog_state', 28),
                             ('shards', 'shards_install_restore_incarnation', 'install_initial_shard_restore_incarnation', 5),
                             ('shards', 'shards_lock_catalog', 'lock_catalog_state', 30),
                             ('shards', 'shards_protect_active_routing', 'protect_shard_lifecycle', 27),
                             ('shards', 'shards_touch_catalog', 'touch_catalog_state', 28),
                             ('slot_sync_probes', 'slot_sync_probes_lock_catalog', 'lock_catalog_state', 30),
                             ('slot_sync_probes', 'slot_sync_probes_protect_history', 'protect_slot_sync_probe', 31),
                             ('slot_sync_probes', 'slot_sync_probes_touch_catalog', 'touch_catalog_state', 28)
                         ) AS allowed_triggers(
                             relation_name,
                             trigger_name,
                             routine_name,
                             trigger_type
                         )
                        WHERE allowed_triggers.relation_name = relations.relname
                          AND allowed_triggers.trigger_name = triggers.tgname
                          AND allowed_triggers.routine_name = routines.proname
                          AND allowed_triggers.trigger_type = triggers.tgtype
                   )
               )
           )
           OR (
               triggers.tgisinternal
               AND (
                   triggers.tgenabled <> 'O'
                   OR routine_namespaces.nspname <> 'pg_catalog'
                   OR pg_catalog.left(routines.proname, 8) <> 'RI_FKey_'
                   OR NOT EXISTS (
                       SELECT
                         FROM pg_catalog.pg_constraint AS constraints
                         JOIN pg_catalog.pg_class AS source_relations
                           ON source_relations.oid = constraints.conrelid
                         JOIN pg_catalog.pg_class AS referenced_relations
                           ON referenced_relations.oid = constraints.confrelid
                        WHERE constraints.oid = triggers.tgconstraint
                          AND constraints.contype = 'f'
                          AND constraints.connamespace = catalog_schema_oid
                          AND source_relations.relnamespace = catalog_schema_oid
                          AND referenced_relations.relnamespace = catalog_schema_oid
                   )
               )
           )
       )
     ORDER BY relations.oid, triggers.oid
     LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog contains an unsupported attached trigger',
            DETAIL = mismatched_object;
    END IF;

    IF EXISTS (
        SELECT
          FROM pg_catalog.pg_default_acl AS defaults
         WHERE defaults.defaclnamespace = catalog_schema_oid
           AND (
               defaults.defaclrole <> catalog_schema_owner
               OR defaults.defaclobjtype <> 'r'
               OR EXISTS (
                   SELECT
                     FROM pg_catalog.aclexplode(defaults.defaclacl) AS acl
                     LEFT JOIN pg_catalog.pg_roles AS grantees
                       ON grantees.oid = acl.grantee
                    WHERE grantees.rolname IS DISTINCT FROM 'pgshard_catalog_reader'
                       OR acl.privilege_type <> 'SELECT'
                       OR acl.is_grantable
               )
           )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog default privileges do not match the released boundary';
    END IF;

    IF catalog_schema_owner_name IN (
        'pgshard_catalog_reader',
        'pgshard_catalog_admin'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog schema has an unsafe fixed-role owner';
    END IF;

    IF catalog_schema_owner_name <> 'pgshard_catalog_owner'
       AND NOT catalog_schema_owner_is_superuser THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog schema owner must be a superuser';
    END IF;

    -- PostgreSQL 18 can leave a formerly non-superuser CREATEROLE owner with
    -- ADMIN OPTION on roles it created. A superuser-owned released catalog can
    -- instead have explicit GRANTED BY history under that owner. Re-home safe
    -- downstream grants under the bootstrap principal before removing every
    -- legacy-owner membership with CASCADE.
    IF catalog_schema_owner_name <> 'pgshard_catalog_owner' THEN
        IF (
            SELECT pg_catalog.count(*)
              FROM pg_catalog.pg_roles AS roles
             WHERE roles.rolname IN (
                       'pgshard_catalog_reader',
                       'pgshard_catalog_admin'
                   )
        ) <> 2 THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501',
                MESSAGE = 'legacy pgshard_catalog schema requires both released fixed roles';
        END IF;

        IF EXISTS (
            SELECT
              FROM pg_catalog.pg_auth_members AS memberships
             WHERE memberships.roleid IN (
                       'pgshard_catalog_reader'::pg_catalog.regrole,
                       'pgshard_catalog_admin'::pg_catalog.regrole
                   )
               AND memberships.member <> catalog_schema_owner
               AND memberships.admin_option
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501',
                MESSAGE = 'pre-existing pgshard catalog role has a delegable membership';
        END IF;

        -- PostgreSQL records every superuser-issued role grant under the
        -- bootstrap superuser. If that role already owns the released catalog,
        -- its downstream grants are already rooted at the destination grantor;
        -- regranting and then revoking that same row would delete it.
        IF catalog_schema_owner <> bootstrap_superuser_oid THEN
            FOR dependent_membership IN
                SELECT granted_roles.rolname AS granted_role_name,
                       member_roles.rolname AS member_role_name,
                       pg_catalog.bool_or(memberships.inherit_option) AS inherit_option,
                       pg_catalog.bool_or(memberships.set_option) AS set_option
                  FROM pg_catalog.pg_auth_members AS memberships
                  JOIN pg_catalog.pg_roles AS granted_roles
                    ON granted_roles.oid = memberships.roleid
                  JOIN pg_catalog.pg_roles AS member_roles
                    ON member_roles.oid = memberships.member
                 WHERE memberships.roleid IN (
                           'pgshard_catalog_reader'::pg_catalog.regrole,
                           'pgshard_catalog_admin'::pg_catalog.regrole
                       )
                   AND memberships.member <> catalog_schema_owner
                   AND EXISTS (
                       SELECT
                         FROM pg_catalog.pg_auth_members AS legacy_grants
                        WHERE legacy_grants.roleid = memberships.roleid
                          AND legacy_grants.member = memberships.member
                          AND legacy_grants.grantor = catalog_schema_owner
                   )
                 GROUP BY granted_roles.rolname, member_roles.rolname
            LOOP
                EXECUTE pg_catalog.format(
                    'GRANT %I TO %I WITH ADMIN FALSE, INHERIT %s, SET %s',
                    dependent_membership.granted_role_name,
                    dependent_membership.member_role_name,
                    CASE WHEN dependent_membership.inherit_option THEN 'TRUE' ELSE 'FALSE' END,
                    CASE WHEN dependent_membership.set_option THEN 'TRUE' ELSE 'FALSE' END
                );
            END LOOP;

            FOR dependent_membership IN
                SELECT granted_roles.rolname AS granted_role_name,
                       member_roles.rolname AS member_role_name
                  FROM pg_catalog.pg_auth_members AS memberships
                  JOIN pg_catalog.pg_roles AS granted_roles
                    ON granted_roles.oid = memberships.roleid
                  JOIN pg_catalog.pg_roles AS member_roles
                    ON member_roles.oid = memberships.member
                 WHERE memberships.roleid IN (
                           'pgshard_catalog_reader'::pg_catalog.regrole,
                           'pgshard_catalog_admin'::pg_catalog.regrole
                       )
                   AND memberships.member <> catalog_schema_owner
                   AND memberships.grantor = catalog_schema_owner
            LOOP
                EXECUTE pg_catalog.format(
                    'REVOKE %I FROM %I GRANTED BY %I CASCADE',
                    dependent_membership.granted_role_name,
                    dependent_membership.member_role_name,
                    catalog_schema_owner_name
                );
            END LOOP;
        END IF;

        FOR owner_membership IN
            SELECT granted_roles.rolname AS granted_role_name,
                   grantors.rolname AS grantor_name
              FROM pg_catalog.pg_auth_members AS memberships
              JOIN pg_catalog.pg_roles AS granted_roles
                ON granted_roles.oid = memberships.roleid
              JOIN pg_catalog.pg_roles AS grantors
                ON grantors.oid = memberships.grantor
             WHERE memberships.roleid IN (
                       'pgshard_catalog_reader'::pg_catalog.regrole,
                       'pgshard_catalog_admin'::pg_catalog.regrole
                   )
               AND memberships.member = catalog_schema_owner
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE %I FROM %I GRANTED BY %I CASCADE',
                owner_membership.granted_role_name,
                catalog_schema_owner_name,
                owner_membership.grantor_name
            );
        END LOOP;
    END IF;
END
$pgshard_role_bootstrap$;

DO $pgshard_roles$
DECLARE
    role_name text;
    role_attributes record;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'pgshard_catalog_owner',
        'pgshard_catalog_reader',
        'pgshard_catalog_admin'
    ]
    LOOP
        SELECT roles.rolsuper,
               roles.rolinherit,
               roles.rolcreaterole,
               roles.rolcreatedb,
               roles.rolcanlogin,
               roles.rolreplication,
               roles.rolbypassrls,
               roles.rolconnlimit,
               roles.rolpassword,
               roles.rolvaliduntil,
               EXISTS (
                   SELECT
                     FROM pg_catalog.pg_db_role_setting AS settings
                    WHERE settings.setrole = roles.oid
               ) AS has_role_settings
          INTO role_attributes
          FROM pg_catalog.pg_authid AS roles
         WHERE roles.rolname = role_name;
        IF NOT FOUND THEN
            EXECUTE pg_catalog.format('CREATE ROLE %I NOLOGIN', role_name);
        ELSIF role_attributes.rolsuper
           OR NOT role_attributes.rolinherit
           OR role_attributes.rolcreaterole
           OR role_attributes.rolcreatedb
           OR role_attributes.rolcanlogin
           OR role_attributes.rolreplication
           OR role_attributes.rolbypassrls
           OR role_attributes.rolconnlimit <> -1
           OR role_attributes.rolpassword IS NOT NULL
           OR role_attributes.rolvaliduntil IS NOT NULL
           OR role_attributes.has_role_settings THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501',
                MESSAGE = pg_catalog.format(
                    'pre-existing %s role has unsafe attributes',
                    role_name
                );
        END IF;
    END LOOP;

    -- A formerly non-superuser CREATEROLE principal can retain ADMIN OPTION on
    -- roles it created. Reject that and every other delegable membership before
    -- these fixed roles receive catalog privileges.
    IF EXISTS (
        SELECT
          FROM pg_catalog.pg_auth_members AS memberships
          JOIN pg_catalog.pg_roles AS granted_roles
            ON granted_roles.oid = memberships.roleid
         WHERE granted_roles.rolname IN (
                   'pgshard_catalog_owner',
                   'pgshard_catalog_reader',
                   'pgshard_catalog_admin'
               )
           AND memberships.admin_option
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard catalog role has a delegable membership';
    END IF;

    IF EXISTS (
        SELECT
          FROM pg_catalog.pg_auth_members AS memberships
          JOIN pg_catalog.pg_roles AS member_roles
            ON member_roles.oid = memberships.member
          JOIN pg_catalog.pg_roles AS granted_roles
            ON granted_roles.oid = memberships.roleid
         WHERE member_roles.rolname = 'pgshard_catalog_reader'
            OR (
                member_roles.rolname = 'pgshard_catalog_admin'
                AND granted_roles.rolname <> 'pgshard_catalog_reader'
            )
            OR (
                member_roles.rolname = 'pgshard_catalog_owner'
                AND granted_roles.rolname <> 'pg_read_all_stats'
            )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard catalog role inherits an unexpected role';
    END IF;

    IF EXISTS (
        SELECT
          FROM pg_catalog.pg_auth_members AS memberships
          JOIN pg_catalog.pg_roles AS granted_roles
            ON granted_roles.oid = memberships.roleid
         WHERE granted_roles.rolname = 'pgshard_catalog_owner'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'pre-existing pgshard_catalog_owner role has a member';
    END IF;
END
$pgshard_roles$;

GRANT pg_read_all_stats TO pgshard_catalog_owner;
GRANT pgshard_catalog_reader TO pgshard_catalog_admin;
CREATE SCHEMA IF NOT EXISTS pgshard_catalog AUTHORIZATION pgshard_catalog_owner;
REVOKE ALL ON SCHEMA pgshard_catalog FROM PUBLIC;

DO $pgshard_owner_takeover$
DECLARE
    catalog_schema_oid oid;
    legacy_owner oid;
    legacy_owner_name name;
    object record;
    grantee record;
BEGIN
    SELECT namespaces.oid, owners.oid, owners.rolname
      INTO STRICT catalog_schema_oid, legacy_owner, legacy_owner_name
      FROM pg_catalog.pg_namespace AS namespaces
      JOIN pg_catalog.pg_roles AS owners ON owners.oid = namespaces.nspowner
     WHERE namespaces.nspname = 'pgshard_catalog';
    IF legacy_owner_name = 'pgshard_catalog_owner' THEN
        RETURN;
    END IF;

    FOR object IN
        SELECT relations.relkind, relations.relname
         FROM pg_catalog.pg_class AS relations
         WHERE relations.relnamespace = catalog_schema_oid
           AND relations.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
           AND NOT (
               relations.relkind = 'S'
               AND EXISTS (
                   SELECT
                     FROM pg_catalog.pg_depend AS dependencies
                    WHERE dependencies.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                      AND dependencies.objid = relations.oid
                      AND dependencies.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                      AND dependencies.deptype IN ('a', 'i')
               )
           )
         ORDER BY relations.oid
    LOOP
        EXECUTE CASE object.relkind
            WHEN 'S' THEN pg_catalog.format(
                'ALTER SEQUENCE pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.relname
            )
            WHEN 'v' THEN pg_catalog.format(
                'ALTER VIEW pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.relname
            )
            WHEN 'm' THEN pg_catalog.format(
                'ALTER MATERIALIZED VIEW pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.relname
            )
            WHEN 'f' THEN pg_catalog.format(
                'ALTER FOREIGN TABLE pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.relname
            )
            ELSE pg_catalog.format(
                'ALTER TABLE pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.relname
            )
        END;
    END LOOP;

    FOR object IN
        SELECT routines.prokind,
               routines.proname,
               pg_catalog.pg_get_function_identity_arguments(routines.oid) AS identity_arguments
          FROM pg_catalog.pg_proc AS routines
         WHERE routines.pronamespace = catalog_schema_oid
         ORDER BY routines.oid
    LOOP
        EXECUTE CASE object.prokind
            WHEN 'p' THEN pg_catalog.format(
                'ALTER PROCEDURE pgshard_catalog.%I(%s) OWNER TO pgshard_catalog_owner',
                object.proname,
                object.identity_arguments
            )
            WHEN 'a' THEN pg_catalog.format(
                'ALTER AGGREGATE pgshard_catalog.%I(%s) OWNER TO pgshard_catalog_owner',
                object.proname,
                object.identity_arguments
            )
            ELSE pg_catalog.format(
                'ALTER FUNCTION pgshard_catalog.%I(%s) OWNER TO pgshard_catalog_owner',
                object.proname,
                object.identity_arguments
            )
        END;
    END LOOP;

    FOR object IN
        SELECT types.typtype, types.typname
          FROM pg_catalog.pg_type AS types
         WHERE types.typnamespace = catalog_schema_oid
           AND (
               (
                   types.typrelid = 0
                   AND NOT EXISTS (
                       SELECT
                         FROM pg_catalog.pg_type AS element_types
                        WHERE element_types.typarray = types.oid
                   )
               )
               OR EXISTS (
                   SELECT
                     FROM pg_catalog.pg_class AS composite_relations
                    WHERE composite_relations.oid = types.typrelid
                      AND composite_relations.relkind = 'c'
               )
           )
         ORDER BY types.oid
    LOOP
        EXECUTE CASE object.typtype
            WHEN 'd' THEN pg_catalog.format(
                'ALTER DOMAIN pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.typname
            )
            ELSE pg_catalog.format(
                'ALTER TYPE pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
                object.typname
            )
        END;
    END LOOP;

    FOR object IN
        SELECT collations.collname
          FROM pg_catalog.pg_collation AS collations
         WHERE collations.collnamespace = catalog_schema_oid
         ORDER BY collations.oid
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER COLLATION pgshard_catalog.%I OWNER TO pgshard_catalog_owner',
            object.collname
        );
    END LOOP;

    ALTER SCHEMA pgshard_catalog OWNER TO pgshard_catalog_owner;

    -- Ownership changes do not remove explicit grants. Strip every direct
    -- grant except the dedicated owner, including the two fixed runtime groups;
    -- the migration recreates their exact boundary below.
    FOR grantee IN
        SELECT DISTINCT roles.rolname
          FROM (
              SELECT acl.grantee
                FROM pg_catalog.pg_namespace AS namespaces
                CROSS JOIN LATERAL pg_catalog.aclexplode(namespaces.nspacl) AS acl
               WHERE namespaces.oid = catalog_schema_oid
              UNION
              SELECT acl.grantee
                FROM pg_catalog.pg_class AS relations
                CROSS JOIN LATERAL pg_catalog.aclexplode(relations.relacl) AS acl
               WHERE relations.relnamespace = catalog_schema_oid
              UNION
              SELECT acl.grantee
                FROM pg_catalog.pg_proc AS routines
                CROSS JOIN LATERAL pg_catalog.aclexplode(routines.proacl) AS acl
               WHERE routines.pronamespace = catalog_schema_oid
              UNION
              SELECT acl.grantee
                FROM pg_catalog.pg_type AS types
                CROSS JOIN LATERAL pg_catalog.aclexplode(types.typacl) AS acl
               WHERE types.typnamespace = catalog_schema_oid
              UNION
              SELECT acl.grantee
                FROM pg_catalog.pg_attribute AS attributes
                JOIN pg_catalog.pg_class AS relations
                  ON relations.oid = attributes.attrelid
                CROSS JOIN LATERAL pg_catalog.aclexplode(attributes.attacl) AS acl
               WHERE relations.relnamespace = catalog_schema_oid
          ) AS grants
          JOIN pg_catalog.pg_roles AS roles ON roles.oid = grants.grantee
         WHERE roles.rolname <> 'pgshard_catalog_owner'
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON SCHEMA pgshard_catalog FROM %I CASCADE',
            grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA pgshard_catalog FROM %I CASCADE',
            grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA pgshard_catalog FROM %I CASCADE',
            grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA pgshard_catalog FROM %I CASCADE',
            grantee.rolname
        );
        FOR object IN
            SELECT types.typname
              FROM pg_catalog.pg_type AS types
             WHERE types.typnamespace = catalog_schema_oid
               AND (
                   (
                       types.typrelid = 0
                       AND NOT EXISTS (
                           SELECT
                             FROM pg_catalog.pg_type AS element_types
                            WHERE element_types.typarray = types.oid
                       )
                   )
                   OR EXISTS (
                       SELECT
                         FROM pg_catalog.pg_class AS composite_relations
                        WHERE composite_relations.oid = types.typrelid
                          AND composite_relations.relkind = 'c'
                   )
               )
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE ALL PRIVILEGES ON TYPE pgshard_catalog.%I FROM %I CASCADE',
                object.typname,
                grantee.rolname
            );
        END LOOP;
        FOR object IN
            SELECT relations.relname, attributes.attname
              FROM pg_catalog.pg_attribute AS attributes
              JOIN pg_catalog.pg_class AS relations
                ON relations.oid = attributes.attrelid
             WHERE relations.relnamespace = catalog_schema_oid
               AND attributes.attnum > 0
               AND NOT attributes.attisdropped
               AND attributes.attacl IS NOT NULL
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE ALL PRIVILEGES (%I) ON TABLE pgshard_catalog.%I FROM %I CASCADE',
                object.attname,
                object.relname,
                grantee.rolname
            );
        END LOOP;
    END LOOP;

    -- Reset the released owner's schema-local default privileges so the old
    -- bootstrap role has no remaining catalog dependency and can be dropped.
    EXECUTE pg_catalog.format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA pgshard_catalog '
        'REVOKE ALL PRIVILEGES ON TABLES FROM pgshard_catalog_reader',
        legacy_owner_name
    );
    EXECUTE pg_catalog.format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA pgshard_catalog '
        'REVOKE ALL PRIVILEGES ON TABLES FROM pgshard_catalog_admin',
        legacy_owner_name
    );
    EXECUTE pg_catalog.format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA pgshard_catalog '
        'REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC',
        legacy_owner_name
    );
END
$pgshard_owner_takeover$;

SET LOCAL ROLE pgshard_catalog_owner;

-- The supported input shape fixes every trigger identity and attribute, but
-- function bodies are deliberately replaceable by this migration. Remove the
-- validated user triggers before any seed DML so no pre-existing body can run
-- with the bootstrap superuser's privileges. Foreign-key triggers remain and
-- were validated above as enabled built-in RI triggers.
DO $pgshard_drop_existing_user_triggers$
DECLARE
    attached_trigger record;
BEGIN
    FOR attached_trigger IN
        SELECT relations.relname, triggers.tgname
          FROM pg_catalog.pg_trigger AS triggers
          JOIN pg_catalog.pg_class AS relations ON relations.oid = triggers.tgrelid
         WHERE relations.relnamespace = 'pgshard_catalog'::pg_catalog.regnamespace
           AND NOT triggers.tgisinternal
         ORDER BY relations.oid, triggers.oid
    LOOP
        EXECUTE pg_catalog.format(
            'DROP TRIGGER %I ON pgshard_catalog.%I',
            attached_trigger.tgname,
            attached_trigger.relname
        );
    END LOOP;
END
$pgshard_drop_existing_user_triggers$;

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

CREATE TABLE IF NOT EXISTS pgshard_catalog.database_shards (
    database_shard_id uuid PRIMARY KEY DEFAULT gen_random_uuid()
        CHECK (database_shard_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    logical_database_id uuid NOT NULL
        REFERENCES pgshard_catalog.logical_databases(logical_database_id) ON DELETE RESTRICT,
    shard_ordinal bigint NOT NULL CHECK (shard_ordinal BETWEEN 0 AND 4294967295),
    state text NOT NULL DEFAULT 'provisioning'
        CHECK (state IN ('provisioning', 'active', 'draining', 'retired')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    draining_at timestamptz,
    retired_at timestamptz,
    CONSTRAINT database_shards_database_ordinal_key
        UNIQUE (logical_database_id, shard_ordinal),
    CONSTRAINT database_shards_database_identity_key
        UNIQUE (logical_database_id, database_shard_id),
    CONSTRAINT database_shards_lifecycle_check CHECK (
        (
            state = 'provisioning'
            AND activated_at IS NULL
            AND draining_at IS NULL
            AND retired_at IS NULL
        )
        OR (
            state = 'active'
            AND activated_at IS NOT NULL
            AND draining_at IS NULL
            AND retired_at IS NULL
        )
        OR (
            state = 'draining'
            AND activated_at IS NOT NULL
            AND draining_at IS NOT NULL
            AND retired_at IS NULL
        )
        OR (
            state = 'retired'
            AND activated_at IS NOT NULL
            AND draining_at IS NOT NULL
            AND retired_at IS NOT NULL
        )
    )
);

COMMENT ON TABLE pgshard_catalog.database_shards IS
    'Permanent logical shard identities scoped to one logical database; placement is versioned separately.';

CREATE TABLE IF NOT EXISTS pgshard_catalog.database_shard_placements (
    placement_id uuid PRIMARY KEY DEFAULT gen_random_uuid()
        CHECK (placement_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    logical_database_id uuid NOT NULL,
    database_shard_id uuid NOT NULL,
    placement_generation bigint NOT NULL CHECK (placement_generation > 0),
    shard_id pgshard_catalog.resource_name NOT NULL
        REFERENCES pgshard_catalog.shards(shard_id) ON DELETE RESTRICT,
    state text NOT NULL DEFAULT 'staged'
        CHECK (state IN ('staged', 'active', 'superseded')),
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    activated_at timestamptz,
    superseded_at timestamptz,
    CONSTRAINT database_shard_placements_database_shard_fkey
        FOREIGN KEY (logical_database_id, database_shard_id)
        REFERENCES pgshard_catalog.database_shards(
            logical_database_id,
            database_shard_id
        ) ON DELETE RESTRICT,
    CONSTRAINT database_shard_placements_generation_key
        UNIQUE (logical_database_id, database_shard_id, placement_generation),
    CONSTRAINT database_shard_placements_lifecycle_check CHECK (
        (
            state = 'staged'
            AND activated_at IS NULL
            AND superseded_at IS NULL
        )
        OR (
            state = 'active'
            AND activated_at IS NOT NULL
            AND superseded_at IS NULL
        )
        OR (
            state = 'superseded'
            AND activated_at IS NOT NULL
            AND superseded_at IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS database_shard_placements_one_active
    ON pgshard_catalog.database_shard_placements(
        logical_database_id,
        database_shard_id
    )
    WHERE state = 'active';

COMMENT ON TABLE pgshard_catalog.database_shard_placements IS
    'Generationed physical placement history for each permanent database-shard identity.';

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
    logical_database_id uuid NOT NULL,
    routing_epoch bigint NOT NULL,
    range_start pgshard_catalog.uint64_boundary NOT NULL,
    range_end pgshard_catalog.uint64_boundary NOT NULL,
    database_shard_id uuid NOT NULL,
    PRIMARY KEY (routing_epoch, range_start),
    CONSTRAINT routing_ranges_database_epoch_fkey
        FOREIGN KEY (logical_database_id, routing_epoch)
        REFERENCES pgshard_catalog.routing_epochs(logical_database_id, routing_epoch)
        ON DELETE RESTRICT,
    CONSTRAINT routing_ranges_database_shard_fkey
        FOREIGN KEY (logical_database_id, database_shard_id)
        REFERENCES pgshard_catalog.database_shards(
            logical_database_id,
            database_shard_id
        ) ON DELETE RESTRICT,
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

CREATE INDEX IF NOT EXISTS managed_replication_slots_live_by_attachment
    ON pgshard_catalog.managed_replication_slots(attachment_generation, slot_name)
    WHERE state IN ('allocated', 'active', 'retiring');

CREATE INDEX IF NOT EXISTS managed_replication_slots_live_by_shard
    ON pgshard_catalog.managed_replication_slots(shard_id, slot_name)
    WHERE state IN ('allocated', 'active', 'retiring');

CREATE INDEX IF NOT EXISTS managed_replication_slots_live_by_logical_consumer
    ON pgshard_catalog.managed_replication_slots(
        logical_database_id,
        consumer_id,
        shard_id,
        slot_name
    )
    WHERE state IN ('allocated', 'active', 'retiring');

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
    'Permanent create-attempt ledger. A pending row is a durable barrier against owner retirement after the orchestration or catalog-fence session is lost.';

-- The create-attempt ledger is capability authority, not reconstructable
-- bookkeeping. A catalog from before the ledger existed can retain allocated
-- rows, because no physical effect has yet been acknowledged. Active or
-- retiring consumer slots cannot be assigned an honest receipt after the fact.
DO $pgshard_managed_slot_creation_attempt_upgrade$
BEGIN
    IF EXISTS (
        SELECT
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.state IN ('active', 'retiring')
           AND NOT EXISTS (
               SELECT
                 FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
                WHERE attempts.slot_generation = slots.slot_generation
                  AND attempts.slot_name = slots.slot_name
                  AND attempts.allocation_kind = 'consumer'
                  AND attempts.slot_role = slots.slot_role
           )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'receiptless live managed replication slots block catalog upgrade',
            DETAIL = 'pre-ledger active or retiring consumer slots have no exact create-attempt receipt',
            HINT = 'finish retiring every live consumer slot with the previous release, then retry the migration';
    END IF;
END
$pgshard_managed_slot_creation_attempt_upgrade$;

CREATE TABLE IF NOT EXISTS pgshard_catalog.managed_slot_target_fences (
    target_name pgshard_catalog.replication_slot_name PRIMARY KEY,
    fence_id uuid UNIQUE,
    owner_pid integer CHECK (owner_pid IS NULL OR owner_pid > 0),
    owner_backend_start timestamptz,
    owner_postmaster_start timestamptz,
    acquired_at timestamptz,
    CHECK (
        (
            fence_id IS NULL
            AND owner_pid IS NULL
            AND owner_backend_start IS NULL
            AND owner_postmaster_start IS NULL
            AND acquired_at IS NULL
        )
        OR
        (
            fence_id IS NOT NULL
            AND owner_pid IS NOT NULL
            AND owner_backend_start IS NOT NULL
            AND owner_postmaster_start IS NOT NULL
            AND acquired_at IS NOT NULL
        )
    ),
    CHECK (fence_id IS NULL OR fence_id <> '00000000-0000-0000-0000-000000000000'::uuid)
);

COMMENT ON TABLE pgshard_catalog.managed_slot_target_fences IS
    'Hidden per-target session fences. A random capability is bound to the exact live PostgreSQL backend and postmaster generation without acquiring an advisory lock.';

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

INSERT INTO pgshard_catalog.cluster_configuration(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO pgshard_catalog.cluster_state(singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

-- Routes used to name a physical cell directly. Upgrade that released shape
-- to a permanent database-shard identity plus a generationed placement. The
-- legacy catalog cannot distinguish identities across routing generations, so
-- one identity is recovered for each database/cell pair. Active targets keep
-- the ordinal implied by active range order, staged-only targets follow them,
-- and historical-only targets are appended deterministically. Reusing one
-- physical target within a live epoch is ambiguous and fails closed. A target
-- used only by superseded epochs is retained as retired history; it must not
-- become a new live placement that blocks physical-cell retirement.
-- A partially hand-written conversion is ambiguous and therefore fails
-- closed instead of guessing which identity owns an existing range.
ALTER TABLE pgshard_catalog.routing_ranges
    ADD COLUMN IF NOT EXISTS logical_database_id uuid;
ALTER TABLE pgshard_catalog.routing_ranges
    ADD COLUMN IF NOT EXISTS database_shard_id uuid;

DO $pgshard_database_shard_upgrade$
DECLARE
    has_legacy_physical_target boolean;
    changed bigint;
BEGIN
    SELECT EXISTS (
        SELECT
          FROM pg_catalog.pg_attribute AS attributes
         WHERE attributes.attrelid =
                   'pgshard_catalog.routing_ranges'::pg_catalog.regclass
           AND attributes.attname = 'shard_id'
           AND attributes.attnum > 0
           AND NOT attributes.attisdropped
    ) INTO has_legacy_physical_target;

    IF has_legacy_physical_target THEN
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.routing_ranges AS ranges
             WHERE ranges.logical_database_id IS NOT NULL
                OR ranges.database_shard_id IS NOT NULL
        ) OR EXISTS (
            SELECT FROM pgshard_catalog.database_shards
        ) OR EXISTS (
            SELECT FROM pgshard_catalog.database_shard_placements
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'partially converted database-shard routing blocks catalog upgrade',
                HINT = 'restore the last complete shardschema backup, then retry the migration';
        END IF;

        IF EXISTS (
            SELECT
              FROM pgshard_catalog.routing_ranges AS ranges
              JOIN pgshard_catalog.routing_epochs AS epochs
                ON epochs.routing_epoch = ranges.routing_epoch
              LEFT JOIN pgshard_catalog.shards AS shards
                ON shards.shard_id = ranges.shard_id
             WHERE epochs.state IN ('staged', 'active')
               AND (shards.shard_id IS NULL OR shards.state NOT IN ('active', 'draining'))
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'legacy live routing references an unavailable physical shard',
                HINT = 'repair or retire the unavailable staged routing with the previous release, then retry the migration';
        END IF;

        IF EXISTS (
            SELECT
              FROM pgshard_catalog.routing_ranges AS ranges
              JOIN pgshard_catalog.routing_epochs AS epochs
                ON epochs.routing_epoch = ranges.routing_epoch
             WHERE epochs.state IN ('staged', 'active')
             GROUP BY epochs.logical_database_id,
                      epochs.routing_epoch,
                      ranges.shard_id
            HAVING pg_catalog.count(*) <> 1
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'ambiguous legacy live routing reuses a physical target within one epoch',
                HINT = 'remove or complete the ambiguous staged routing with the previous release, then retry the migration';
        END IF;

        DROP TABLE IF EXISTS pg_temp.pgshard_database_shard_upgrade_targets;
        CREATE TEMPORARY TABLE pg_temp.pgshard_database_shard_upgrade_targets (
            logical_database_id uuid NOT NULL,
            shard_id pgshard_catalog.resource_name NOT NULL,
            is_live boolean NOT NULL,
            shard_ordinal bigint NOT NULL,
            PRIMARY KEY (logical_database_id, shard_id),
            UNIQUE (logical_database_id, shard_ordinal)
        ) ON COMMIT DROP;

        INSERT INTO pg_temp.pgshard_database_shard_upgrade_targets(
            logical_database_id,
            shard_id,
            is_live,
            shard_ordinal
        )
        WITH legacy_targets AS (
            SELECT epochs.logical_database_id,
                   ranges.shard_id,
                   shards.shard_number,
                   pg_catalog.min(ranges.range_start)
                       FILTER (WHERE epochs.state = 'active')
                       AS active_range_start,
                   pg_catalog.min(epochs.routing_epoch)
                       FILTER (WHERE epochs.state = 'staged')
                       AS staged_routing_epoch,
                   pg_catalog.min(ranges.range_start)
                       FILTER (WHERE epochs.state = 'staged')
                       AS staged_range_start,
                   pg_catalog.min(epochs.routing_epoch)
                       FILTER (WHERE epochs.state = 'superseded')
                       AS historical_routing_epoch,
                   pg_catalog.min(ranges.range_start)
                       FILTER (WHERE epochs.state = 'superseded')
                       AS historical_range_start
              FROM pgshard_catalog.routing_ranges AS ranges
              JOIN pgshard_catalog.routing_epochs AS epochs
                ON epochs.routing_epoch = ranges.routing_epoch
              JOIN pgshard_catalog.shards AS shards
                ON shards.shard_id = ranges.shard_id
             GROUP BY epochs.logical_database_id,
                      ranges.shard_id,
                      shards.shard_number
        )
        SELECT targets.logical_database_id,
               targets.shard_id,
               targets.active_range_start IS NOT NULL
                   OR targets.staged_routing_epoch IS NOT NULL,
               pg_catalog.row_number() OVER (
                   PARTITION BY targets.logical_database_id
                   ORDER BY
                       CASE
                           WHEN targets.active_range_start IS NOT NULL THEN 0
                           WHEN targets.staged_routing_epoch IS NOT NULL THEN 1
                           ELSE 2
                       END,
                       targets.active_range_start NULLS LAST,
                       targets.staged_routing_epoch NULLS LAST,
                       targets.staged_range_start NULLS LAST,
                       targets.historical_routing_epoch NULLS LAST,
                       targets.historical_range_start NULLS LAST,
                       targets.shard_number,
                       targets.shard_id
               ) - 1
          FROM legacy_targets AS targets
         ORDER BY targets.logical_database_id,
                  targets.active_range_start NULLS LAST,
                  targets.shard_number,
                  targets.shard_id;

        INSERT INTO pgshard_catalog.database_shards(
            logical_database_id,
            shard_ordinal,
            state,
            activated_at,
            draining_at,
            retired_at
        )
        SELECT legacy.logical_database_id,
               legacy.shard_ordinal,
               CASE WHEN legacy.is_live THEN 'active' ELSE 'retired' END,
               statement_timestamp(),
               CASE WHEN legacy.is_live THEN NULL ELSE statement_timestamp() END,
               CASE WHEN legacy.is_live THEN NULL ELSE statement_timestamp() END
          FROM pg_temp.pgshard_database_shard_upgrade_targets AS legacy
         ORDER BY legacy.logical_database_id, legacy.shard_ordinal;

        INSERT INTO pgshard_catalog.database_shard_placements(
            logical_database_id,
            database_shard_id,
            placement_generation,
            shard_id,
            state,
            activated_at,
            superseded_at
        )
        SELECT database_shards.logical_database_id,
               database_shards.database_shard_id,
               1,
               legacy.shard_id,
               CASE WHEN legacy.is_live THEN 'active' ELSE 'superseded' END,
               statement_timestamp(),
               CASE WHEN legacy.is_live THEN NULL ELSE statement_timestamp() END
          FROM pg_temp.pgshard_database_shard_upgrade_targets AS legacy
          JOIN pgshard_catalog.database_shards AS database_shards
            ON database_shards.logical_database_id = legacy.logical_database_id
           AND database_shards.shard_ordinal = legacy.shard_ordinal
         ORDER BY database_shards.logical_database_id,
                  database_shards.shard_ordinal;

        UPDATE pgshard_catalog.routing_ranges AS ranges
           SET logical_database_id = epochs.logical_database_id,
               database_shard_id = database_shards.database_shard_id
          FROM pgshard_catalog.routing_epochs AS epochs,
               pg_temp.pgshard_database_shard_upgrade_targets AS legacy,
               pgshard_catalog.database_shards AS database_shards
         WHERE epochs.routing_epoch = ranges.routing_epoch
           AND legacy.logical_database_id = epochs.logical_database_id
           AND legacy.shard_id = ranges.shard_id
           AND database_shards.logical_database_id = legacy.logical_database_id
           AND database_shards.shard_ordinal = legacy.shard_ordinal;

        IF EXISTS (
            SELECT
              FROM pgshard_catalog.routing_ranges AS ranges
             WHERE ranges.logical_database_id IS NULL
                OR ranges.database_shard_id IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'legacy routing could not be mapped to database-shard identities';
        END IF;

        ALTER TABLE pgshard_catalog.routing_ranges
            DROP CONSTRAINT IF EXISTS routing_ranges_shard_id_fkey;
        ALTER TABLE pgshard_catalog.routing_ranges
            DROP CONSTRAINT IF EXISTS routing_ranges_routing_epoch_fkey;
        ALTER TABLE pgshard_catalog.routing_ranges
            DROP COLUMN shard_id;

        IF EXISTS (
            SELECT
              FROM pgshard_catalog.cluster_state AS state
             WHERE state.singleton
               AND state.catalog_epoch = 9223372036854775806
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'catalog epoch exhausted during database-shard upgrade';
        END IF;
        UPDATE pgshard_catalog.cluster_state
           SET catalog_epoch = catalog_epoch + 1,
               changed_at = statement_timestamp()
         WHERE singleton;
        GET DIAGNOSTICS changed = ROW_COUNT;
        IF changed <> 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'database-shard upgrade requires the cluster-state singleton';
        END IF;
    END IF;
END
$pgshard_database_shard_upgrade$;

ALTER TABLE pgshard_catalog.routing_ranges
    ALTER COLUMN logical_database_id SET NOT NULL;
ALTER TABLE pgshard_catalog.routing_ranges
    ALTER COLUMN database_shard_id SET NOT NULL;

DO $pgshard_database_shard_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT
          FROM pg_catalog.pg_constraint AS constraints
         WHERE constraints.conrelid =
                   'pgshard_catalog.routing_ranges'::pg_catalog.regclass
           AND constraints.conname = 'routing_ranges_database_epoch_fkey'
    ) THEN
        ALTER TABLE pgshard_catalog.routing_ranges
            ADD CONSTRAINT routing_ranges_database_epoch_fkey
            FOREIGN KEY (logical_database_id, routing_epoch)
            REFERENCES pgshard_catalog.routing_epochs(
                logical_database_id,
                routing_epoch
            ) ON DELETE RESTRICT NOT VALID;
    END IF;
    ALTER TABLE pgshard_catalog.routing_ranges
        VALIDATE CONSTRAINT routing_ranges_database_epoch_fkey;

    IF NOT EXISTS (
        SELECT
          FROM pg_catalog.pg_constraint AS constraints
         WHERE constraints.conrelid =
                   'pgshard_catalog.routing_ranges'::pg_catalog.regclass
           AND constraints.conname = 'routing_ranges_database_shard_fkey'
    ) THEN
        ALTER TABLE pgshard_catalog.routing_ranges
            ADD CONSTRAINT routing_ranges_database_shard_fkey
            FOREIGN KEY (logical_database_id, database_shard_id)
            REFERENCES pgshard_catalog.database_shards(
                logical_database_id,
                database_shard_id
            ) ON DELETE RESTRICT NOT VALID;
    END IF;
    ALTER TABLE pgshard_catalog.routing_ranges
        VALIDATE CONSTRAINT routing_ranges_database_shard_fkey;
END
$pgshard_database_shard_constraints$;

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

CREATE OR REPLACE FUNCTION pgshard_catalog.managed_slot_backend_identity_live(
    expected_owner_pid integer,
    expected_backend_start timestamptz,
    expected_postmaster_start timestamptz
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
    SELECT expected_owner_pid IS NOT NULL
       AND expected_backend_start IS NOT NULL
       AND expected_postmaster_start = pg_catalog.pg_postmaster_start_time()
       AND EXISTS (
        SELECT
          FROM pg_catalog.pg_stat_activity AS activity
         WHERE activity.pid = expected_owner_pid
           AND activity.datid = (
               SELECT databases.oid
                 FROM pg_catalog.pg_database AS databases
                WHERE databases.datname = pg_catalog.current_database()
           )
           AND activity.backend_start = expected_backend_start
           AND activity.backend_type = 'client backend'
    )
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.lock_managed_slot_target_row(target_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    PERFORM 1
      FROM pgshard_catalog.managed_slot_target_fences AS fences
     WHERE fences.target_name::text = $1
     FOR UPDATE NOWAIT;
    IF FOUND THEN
        RETURN;
    END IF;

    -- A missing unique-key row cannot be protected with FOR UPDATE. SHARE
    -- UPDATE EXCLUSIVE serializes first insertion with itself but remains
    -- compatible with ROW EXCLUSIVE updates and ROW SHARE row locking on
    -- unrelated targets. NOWAIT prevents same-name speculative insertion from
    -- becoming a hidden wait edge.
    LOCK TABLE pgshard_catalog.managed_slot_target_fences
        IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;
    INSERT INTO pgshard_catalog.managed_slot_target_fences(target_name)
    VALUES ($1)
    ON CONFLICT ON CONSTRAINT managed_slot_target_fences_pkey DO NOTHING;
    PERFORM 1
      FROM pgshard_catalog.managed_slot_target_fences AS fences
     WHERE fences.target_name::text = $1
     FOR UPDATE NOWAIT;
EXCEPTION
    WHEN lock_not_available THEN
        RAISE EXCEPTION USING
            ERRCODE = '55P03',
            MESSAGE = 'managed slot target fence is busy';
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.lock_managed_slot_target(target_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    existing_fence_id uuid;
    existing_owner_pid integer;
    existing_backend_start timestamptz;
    existing_postmaster_start timestamptz;
BEGIN
    IF target_name IS NULL OR target_name = '' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'managed slot target is required';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_target_row($1);

    SELECT fences.fence_id,
           fences.owner_pid,
           fences.owner_backend_start,
           fences.owner_postmaster_start
      INTO existing_fence_id,
           existing_owner_pid,
           existing_backend_start,
           existing_postmaster_start
      FROM pgshard_catalog.managed_slot_target_fences AS fences
     WHERE fences.target_name::text = $1;

    IF existing_fence_id IS NOT NULL THEN
        IF pgshard_catalog.managed_slot_backend_identity_live(
            existing_owner_pid,
            existing_backend_start,
            existing_postmaster_start
        ) THEN
            IF existing_owner_pid <> pg_catalog.pg_backend_pid() THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55P03',
                    MESSAGE = 'managed slot target fence is busy';
            END IF;
            RETURN;
        END IF;

        UPDATE pgshard_catalog.managed_slot_target_fences AS fences
           SET fence_id = NULL,
               owner_pid = NULL,
               owner_backend_start = NULL,
               owner_postmaster_start = NULL,
               acquired_at = NULL
         WHERE fences.target_name::text = $1;
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

CREATE OR REPLACE FUNCTION pgshard_catalog.acquire_managed_slot_target_fence(target_name text)
RETURNS TABLE(acquired_fence_id uuid, acquired_backend_pid integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    existing_fence_id uuid;
    existing_owner_pid integer;
    existing_backend_start timestamptz;
    existing_postmaster_start timestamptz;
    new_fence_id uuid;
    new_backend_start timestamptz;
    new_postmaster_start timestamptz;
BEGIN
    IF target_name IS NULL OR target_name = '' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'managed slot target is required';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_target_row($1);

    SELECT fences.fence_id,
           fences.owner_pid,
           fences.owner_backend_start,
           fences.owner_postmaster_start
      INTO existing_fence_id,
           existing_owner_pid,
           existing_backend_start,
           existing_postmaster_start
      FROM pgshard_catalog.managed_slot_target_fences AS fences
     WHERE fences.target_name::text = $1;

    IF existing_fence_id IS NOT NULL
       AND pgshard_catalog.managed_slot_backend_identity_live(
           existing_owner_pid,
           existing_backend_start,
           existing_postmaster_start
       ) THEN
        IF existing_owner_pid = pg_catalog.pg_backend_pid() THEN
            RETURN QUERY SELECT existing_fence_id, existing_owner_pid;
            RETURN;
        END IF;
        RAISE EXCEPTION USING
            ERRCODE = '55P03',
            MESSAGE = 'managed slot target fence is busy';
    END IF;

    UPDATE pgshard_catalog.managed_slot_target_fences AS fences
       SET fence_id = NULL,
           owner_pid = NULL,
           owner_backend_start = NULL,
           owner_postmaster_start = NULL,
           acquired_at = NULL
     WHERE fences.target_name::text = $1;

    SELECT activity.backend_start,
           pg_catalog.pg_postmaster_start_time()
      INTO new_backend_start,
           new_postmaster_start
      FROM pg_catalog.pg_stat_activity AS activity
     WHERE activity.pid = pg_catalog.pg_backend_pid()
       AND activity.datid = (
           SELECT databases.oid
             FROM pg_catalog.pg_database AS databases
            WHERE databases.datname = pg_catalog.current_database()
       )
       AND activity.backend_type = 'client backend';
    IF new_backend_start IS NULL OR new_postmaster_start IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'could not identify the managed slot target fence backend';
    END IF;
    new_fence_id := pg_catalog.gen_random_uuid();

    UPDATE pgshard_catalog.managed_slot_target_fences AS fences
       SET fence_id = new_fence_id,
           owner_pid = pg_catalog.pg_backend_pid(),
           owner_backend_start = new_backend_start,
           owner_postmaster_start = new_postmaster_start,
           acquired_at = statement_timestamp()
     WHERE fences.target_name::text = $1;

    RETURN QUERY SELECT new_fence_id, pg_catalog.pg_backend_pid();
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.managed_slot_target_fence_held(target_name text)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
    SELECT EXISTS (
        SELECT
          FROM pgshard_catalog.managed_slot_target_fences AS fences
         WHERE fences.target_name::text = $1
           AND fences.fence_id IS NOT NULL
           AND fences.owner_pid = pg_catalog.pg_backend_pid()
           AND pgshard_catalog.managed_slot_backend_identity_live(
               fences.owner_pid,
               fences.owner_backend_start,
               fences.owner_postmaster_start
           )
    )
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.managed_slot_target_fence_matches(
    target_name text,
    expected_fence_id uuid
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
    SELECT expected_fence_id IS NOT NULL
       AND expected_fence_id <> '00000000-0000-0000-0000-000000000000'::uuid
       AND EXISTS (
           SELECT
             FROM pgshard_catalog.managed_slot_target_fences AS fences
            WHERE fences.target_name::text = $1
              AND fences.fence_id = expected_fence_id
              AND fences.owner_pid = pg_catalog.pg_backend_pid()
              AND pgshard_catalog.managed_slot_backend_identity_live(
                  fences.owner_pid,
                  fences.owner_backend_start,
                  fences.owner_postmaster_start
              )
       )
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.verify_managed_slot_target_fence(
    target_name text,
    expected_fence_id uuid
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF NOT pgshard_catalog.managed_slot_target_fence_matches(
        target_name,
        expected_fence_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot target fence is not held by this backend';
    END IF;
    RETURN pg_catalog.pg_backend_pid();
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.release_managed_slot_target_fence(
    target_name text,
    expected_fence_id uuid DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    existing_fence_id uuid;
    existing_owner_pid integer;
    existing_backend_start timestamptz;
    existing_postmaster_start timestamptz;
BEGIN
    SELECT fences.fence_id,
           fences.owner_pid,
           fences.owner_backend_start,
           fences.owner_postmaster_start
      INTO existing_fence_id,
           existing_owner_pid,
           existing_backend_start,
           existing_postmaster_start
      FROM pgshard_catalog.managed_slot_target_fences AS fences
     WHERE fences.target_name::text = $1
     FOR UPDATE NOWAIT;
    IF NOT FOUND OR existing_fence_id IS NULL THEN
        RETURN false;
    END IF;
    IF existing_owner_pid <> pg_catalog.pg_backend_pid()
       OR NOT pgshard_catalog.managed_slot_backend_identity_live(
           existing_owner_pid,
           existing_backend_start,
           existing_postmaster_start
       ) THEN
        IF expected_fence_id IS NULL THEN
            RETURN false;
        END IF;
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot target fence is owned by another capability';
    END IF;
    IF expected_fence_id IS NOT NULL AND existing_fence_id <> expected_fence_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot target fence is owned by another capability';
    END IF;

    UPDATE pgshard_catalog.managed_slot_target_fences AS fences
       SET fence_id = NULL,
           owner_pid = NULL,
           owner_backend_start = NULL,
           owner_postmaster_start = NULL,
           acquired_at = NULL
     WHERE fences.target_name::text = $1;
    RETURN true;
EXCEPTION
    WHEN lock_not_available THEN
        RAISE EXCEPTION USING
            ERRCODE = '55P03',
            MESSAGE = 'managed slot target fence is busy';
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
    -- function again while owning the same target fence.
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

    -- Version the shared catalog fence in the same transaction as the new
    -- phantom. A lifecycle transaction that already took an older
    -- REPEATABLE READ snapshot must then fail when its statement trigger locks
    -- cluster_state instead of missing this pending attempt.
    UPDATE pgshard_catalog.cluster_state
       SET changed_at = statement_timestamp()
     WHERE singleton;
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

CREATE OR REPLACE FUNCTION pgshard_catalog.managed_slot_creation_attempt_state(
    expected_slot_generation uuid,
    expected_slot_name text,
    expected_creation_receipt_id uuid
)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
    SELECT attempts.state
      FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
     WHERE attempts.slot_generation = expected_slot_generation
       AND attempts.slot_name::text = expected_slot_name
       AND attempts.creation_receipt_id = expected_creation_receipt_id
     LIMIT 1
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.activate_managed_replication_slot(
    expected_slot_generation uuid,
    expected_creation_receipt_id uuid,
    expected_consistent_point pg_lsn,
    expected_two_phase_at pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    target_name text;
    target_role text;
    target_state text;
    target_consistent_point pg_lsn;
    target_two_phase_at pg_lsn;
    changed bigint;
BEGIN
    IF expected_slot_generation IS NULL
       OR expected_slot_generation = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_creation_receipt_id IS NULL
       OR expected_creation_receipt_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_consistent_point IS NULL
       OR expected_consistent_point <= '0/0'
       OR expected_two_phase_at IS NULL
       OR expected_two_phase_at <= '0/0' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'managed slot activation authority is incomplete';
    END IF;

    PERFORM 1
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'catalog state is missing';
    END IF;

    SELECT slots.slot_name::text,
           slots.slot_role,
           slots.state,
           slots.consistent_point,
           slots.two_phase_at
      INTO target_name,
           target_role,
           target_state,
           target_consistent_point,
           target_two_phase_at
      FROM pgshard_catalog.managed_replication_slots AS slots
     WHERE slots.slot_generation = expected_slot_generation
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot allocation is missing';
    END IF;

    PERFORM pgshard_catalog.lock_managed_slot_target(target_name);

    IF target_state = 'active' THEN
        IF target_consistent_point = expected_consistent_point
           AND target_two_phase_at = expected_two_phase_at
           AND EXISTS (
               SELECT
                 FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
                WHERE attempts.creation_receipt_id = expected_creation_receipt_id
                  AND attempts.slot_generation = expected_slot_generation
                  AND attempts.slot_name::text = target_name
                  AND attempts.allocation_kind = 'consumer'
                  AND attempts.slot_role = target_role
                  AND attempts.state = 'activated'
           ) THEN
            RETURN;
        END IF;
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot activation authority changed';
    END IF;
    IF target_state <> 'allocated' THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'managed slot allocation is not eligible for activation';
    END IF;

    UPDATE pgshard_catalog.managed_slot_creation_attempts
       SET state = 'activated', resolved_at = statement_timestamp()
     WHERE creation_receipt_id = expected_creation_receipt_id
       AND slot_generation = expected_slot_generation
       AND slot_name::text = target_name
       AND allocation_kind = 'consumer'
       AND slot_role = target_role
       AND state = 'pending';
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot activation requires its exact pending creation attempt';
    END IF;

    UPDATE pgshard_catalog.managed_replication_slots
       SET state = 'active',
           consistent_point = expected_consistent_point,
           two_phase_at = expected_two_phase_at,
           activated_at = statement_timestamp()
     WHERE slot_generation = expected_slot_generation
       AND state = 'allocated';
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'managed slot allocation changed during activation';
    END IF;
END
$function$;

DROP FUNCTION IF EXISTS pgshard_catalog.complete_managed_replication_slot_retirement(
    uuid,
    text,
    uuid
);

CREATE OR REPLACE FUNCTION pgshard_catalog.complete_managed_replication_slot_retirement(
    expected_slot_generation uuid,
    expected_slot_name text,
    expected_creation_receipt_id uuid,
    expected_fence_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    target_name text;
    target_role text;
    target_state text;
    attachment_state text;
    consumer_shard_state text;
    attempt_state text;
    changed bigint;
BEGIN
    IF expected_slot_generation IS NULL
       OR expected_slot_generation = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_slot_name IS NULL
       OR expected_slot_name = ''
       OR expected_creation_receipt_id IS NULL
       OR expected_creation_receipt_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_fence_id IS NULL
       OR expected_fence_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'managed slot retirement authority is incomplete';
    END IF;

    PERFORM 1
      FROM pgshard_catalog.cluster_state
     WHERE singleton
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'catalog state is missing';
    END IF;

    SELECT slots.slot_name::text,
           slots.slot_role,
           slots.state,
           attachments.state,
           consumer_shards.state
      INTO target_name,
           target_role,
           target_state,
           attachment_state,
           consumer_shard_state
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
     WHERE slots.slot_generation = expected_slot_generation
     FOR UPDATE OF slots;
    IF NOT FOUND OR target_name IS DISTINCT FROM expected_slot_name THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot retirement authority changed';
    END IF;

    IF NOT pgshard_catalog.managed_slot_target_fence_matches(
        target_name,
        expected_fence_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot final retirement requires its live target fence';
    END IF;

    SELECT attempts.state
      INTO attempt_state
      FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
     WHERE attempts.creation_receipt_id = expected_creation_receipt_id
       AND attempts.slot_generation = expected_slot_generation
       AND attempts.slot_name::text = target_name
       AND attempts.allocation_kind = 'consumer'
       AND attempts.slot_role = target_role
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot retirement requires its exact creation attempt';
    END IF;

    IF target_state = 'retired' THEN
        IF attempt_state = 'retired' THEN
            RETURN;
        END IF;
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot retirement authority changed';
    END IF;

    IF target_state = 'allocated' THEN
        IF attachment_state <> 'staged' OR attempt_state <> 'pending' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'allocated managed slot is not eligible for absence reconciliation';
        END IF;
    ELSIF target_state = 'active' THEN
        IF attempt_state <> 'activated'
           OR NOT (
               attachment_state = 'staged'
               OR (
                   attachment_state = 'retiring'
                   AND consumer_shard_state = 'fenced'
               )
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'active managed slot is not eligible for absence reconciliation';
        END IF;
    ELSIF target_state = 'retiring' THEN
        IF attempt_state <> 'activated'
           OR attachment_state <> 'retiring'
           OR consumer_shard_state <> 'fenced' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'active managed slot is not eligible for absence reconciliation';
        END IF;
    ELSE
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'managed slot is not eligible for absence reconciliation';
    END IF;

    UPDATE pgshard_catalog.managed_slot_creation_attempts
       SET state = 'retired', resolved_at = statement_timestamp()
     WHERE creation_receipt_id = expected_creation_receipt_id
       AND slot_generation = expected_slot_generation
       AND slot_name::text = target_name
       AND allocation_kind = 'consumer'
       AND slot_role = target_role
       AND state = attempt_state;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'managed slot creation attempt changed during retirement';
    END IF;

    UPDATE pgshard_catalog.managed_replication_slots
       SET state = 'retired', retired_at = statement_timestamp()
     WHERE slot_generation = expected_slot_generation
       AND state = target_state;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'managed slot allocation changed during retirement';
    END IF;
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

CREATE OR REPLACE FUNCTION pgshard_catalog.slot_sync_probe_receipt_state(
    expected_probe_generation uuid,
    candidate_receipt_id uuid
)
RETURNS TABLE(
    creation_receipt_present boolean,
    cleanup_receipt_present boolean,
    creation_receipt_matches boolean,
    cleanup_receipt_matches boolean
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
    SELECT probes.creation_receipt_id IS NOT NULL,
           probes.cleanup_receipt_id IS NOT NULL,
           candidate_receipt_id IS NOT NULL
               AND probes.creation_receipt_id = candidate_receipt_id,
           candidate_receipt_id IS NOT NULL
               AND probes.cleanup_receipt_id = candidate_receipt_id
      FROM pgshard_catalog.slot_sync_probes AS probes
     WHERE probes.probe_generation = expected_probe_generation
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.activate_slot_sync_probe(
    expected_probe_generation uuid,
    expected_creation_receipt_id uuid,
    expected_consistent_point pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    changed bigint;
BEGIN
    IF expected_probe_generation IS NULL
       OR expected_probe_generation = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_creation_receipt_id IS NULL
       OR expected_creation_receipt_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_consistent_point IS NULL
       OR expected_consistent_point = '0/0'::pg_lsn THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'slot-sync probe activation authority is incomplete';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.probe_generation = expected_probe_generation
           AND probes.state = 'active'
           AND probes.creation_receipt_id = expected_creation_receipt_id
           AND probes.consistent_point = expected_consistent_point
    ) THEN
        RETURN;
    END IF;

    UPDATE pgshard_catalog.slot_sync_probes
       SET consistent_point = expected_consistent_point,
           creation_receipt_id = expected_creation_receipt_id,
           state = 'active',
           activated_at = statement_timestamp()
     WHERE probe_generation = expected_probe_generation
       AND state = 'allocated';
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed = 1 THEN
        RETURN;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe activation authority changed';
END
$function$;

DROP FUNCTION IF EXISTS pgshard_catalog.begin_slot_sync_probe_retirement(uuid, uuid);

CREATE OR REPLACE FUNCTION pgshard_catalog.begin_slot_sync_probe_retirement(
    expected_probe_generation uuid,
    expected_creation_receipt_id uuid,
    expected_consistent_point pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    changed bigint;
BEGIN
    IF expected_probe_generation IS NULL
       OR expected_probe_generation = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_creation_receipt_id IS NULL
       OR expected_creation_receipt_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_consistent_point IS NULL
       OR expected_consistent_point = '0/0'::pg_lsn THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'slot-sync probe retirement authority is incomplete';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.probe_generation = expected_probe_generation
           AND probes.state IN ('retiring', 'retired')
           AND probes.cleanup_receipt_id = expected_creation_receipt_id
           AND (
               (
                   probes.creation_receipt_id IS NULL
                   AND probes.consistent_point IS NULL
               )
               OR (
                   probes.creation_receipt_id = expected_creation_receipt_id
                   AND probes.consistent_point = expected_consistent_point
               )
           )
    ) THEN
        RETURN;
    END IF;

    UPDATE pgshard_catalog.slot_sync_probes
       SET state = 'retiring',
           cleanup_receipt_id = expected_creation_receipt_id,
           retiring_at = statement_timestamp()
     WHERE probe_generation = expected_probe_generation
       AND state IN ('allocated', 'active')
       AND (
           (
               state = 'allocated'
               AND consistent_point IS NULL
               AND creation_receipt_id IS NULL
           )
           OR (
               state = 'active'
               AND consistent_point = expected_consistent_point
               AND creation_receipt_id = expected_creation_receipt_id
           )
       );
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed = 1 THEN
        RETURN;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe retirement authority changed';
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.complete_slot_sync_probe_retirement(
    expected_probe_generation uuid,
    expected_slot_name text,
    expected_creation_receipt_id uuid,
    expected_fence_id uuid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    changed bigint;
BEGIN
    IF expected_probe_generation IS NULL
       OR expected_probe_generation = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_slot_name IS NULL
       OR expected_slot_name = ''
       OR expected_creation_receipt_id IS NULL
       OR expected_creation_receipt_id = '00000000-0000-0000-0000-000000000000'::uuid
       OR expected_fence_id IS NULL
       OR expected_fence_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'slot-sync probe final retirement authority is incomplete';
    END IF;
    IF NOT pgshard_catalog.managed_slot_target_fence_matches(
        expected_slot_name,
        expected_fence_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'slot-sync probe final retirement requires its exact live target fence';
    END IF;

    IF EXISTS (
        SELECT
          FROM pgshard_catalog.slot_sync_probes AS probes
         WHERE probes.probe_generation = expected_probe_generation
           AND probes.slot_name::text = expected_slot_name
           AND probes.cleanup_receipt_id = expected_creation_receipt_id
           AND probes.state = 'retired'
    ) THEN
        RETURN;
    END IF;

    UPDATE pgshard_catalog.slot_sync_probes
       SET state = 'retired', retired_at = statement_timestamp()
     WHERE probe_generation = expected_probe_generation
       AND slot_name::text = expected_slot_name
       AND cleanup_receipt_id = expected_creation_receipt_id
       AND state = 'retiring';
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed = 1 THEN
        RETURN;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'slot-sync probe final retirement authority changed';
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
           AND probes.state IN ('allocated', 'active', 'retiring')
        UNION
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
         WHERE slots.shard_id = OLD.shard_id
           AND slots.state IN ('allocated', 'active', 'retiring')
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
           AND probes.state IN ('allocated', 'active', 'retiring')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('shard %s still has a non-retired slot-sync probe', OLD.shard_id);
    END IF;

    IF becoming_unavailable AND EXISTS (
        SELECT
          FROM pgshard_catalog.database_shard_placements AS placements
         WHERE placements.shard_id = OLD.shard_id
           AND placements.state IN ('staged', 'active')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = format('shard %s has a live database-shard placement', OLD.shard_id);
    END IF;

    RETURN NEW;
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_database_shard_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'database-shard identities are permanent';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NOT (
            (
                NEW.state = 'provisioning'
                AND NEW.activated_at IS NULL
                AND NEW.draining_at IS NULL
                AND NEW.retired_at IS NULL
            )
            OR (
                NEW.state = 'active'
                AND NEW.activated_at IS NOT NULL
                AND NEW.draining_at IS NULL
                AND NEW.retired_at IS NULL
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'a database shard must start provisioning or active';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.database_shard_id IS DISTINCT FROM OLD.database_shard_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.shard_ordinal IS DISTINCT FROM OLD.shard_ordinal
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'database-shard identity is immutable';
    END IF;

    IF NEW.state = OLD.state
       AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
       AND NEW.draining_at IS NOT DISTINCT FROM OLD.draining_at
       AND NEW.retired_at IS NOT DISTINCT FROM OLD.retired_at THEN
        RETURN NEW;
    ELSIF OLD.state = 'provisioning'
          AND NEW.state = 'active'
          AND OLD.activated_at IS NULL
          AND NEW.activated_at IS NOT NULL
          AND NEW.draining_at IS NULL
          AND NEW.retired_at IS NULL THEN
        RETURN NEW;
    ELSIF OLD.state = 'active'
          AND NEW.state = 'draining'
          AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
          AND OLD.draining_at IS NULL
          AND NEW.draining_at IS NOT NULL
          AND NEW.retired_at IS NULL THEN
        RETURN NEW;
    ELSIF OLD.state = 'draining'
          AND NEW.state = 'active'
          AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
          AND NEW.draining_at IS NULL
          AND NEW.retired_at IS NULL THEN
        RETURN NEW;
    ELSIF OLD.state = 'draining'
          AND NEW.state = 'retired'
          AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
          AND NEW.draining_at IS NOT DISTINCT FROM OLD.draining_at
          AND OLD.retired_at IS NULL
          AND NEW.retired_at IS NOT NULL THEN
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.routing_ranges AS ranges
              JOIN pgshard_catalog.routing_epochs AS epochs
                ON epochs.logical_database_id = ranges.logical_database_id
               AND epochs.routing_epoch = ranges.routing_epoch
             WHERE ranges.logical_database_id = OLD.logical_database_id
               AND ranges.database_shard_id = OLD.database_shard_id
               AND epochs.state IN ('staged', 'active')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = format(
                    'database shard %s is referenced by live routing',
                    OLD.database_shard_id
                );
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.database_shard_placements AS placements
             WHERE placements.logical_database_id = OLD.logical_database_id
               AND placements.database_shard_id = OLD.database_shard_id
               AND placements.state IN ('staged', 'active')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = format(
                    'database shard %s still has a live placement',
                    OLD.database_shard_id
                );
        END IF;
        RETURN NEW;
    END IF;

    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = 'invalid database-shard lifecycle transition';
END
$function$;

CREATE OR REPLACE FUNCTION pgshard_catalog.protect_database_shard_placement()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    next_generation bigint;
    database_shard_state text;
    physical_shard_state text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'database-shard placement identities are permanent';
    END IF;

    SELECT database_shards.state
      INTO database_shard_state
      FROM pgshard_catalog.database_shards AS database_shards
     WHERE database_shards.logical_database_id = NEW.logical_database_id
       AND database_shards.database_shard_id = NEW.database_shard_id
     FOR KEY SHARE;
    IF database_shard_state IS NULL
       OR database_shard_state NOT IN ('active', 'draining') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'a live placement requires an active or draining database shard';
    END IF;

    SELECT shards.state
      INTO physical_shard_state
      FROM pgshard_catalog.shards AS shards
     WHERE shards.shard_id = NEW.shard_id
     FOR KEY SHARE;
    IF physical_shard_state IS NULL
       OR physical_shard_state NOT IN ('active', 'draining') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'a live placement requires an available physical shard';
    END IF;

    IF TG_OP = 'INSERT' THEN
        SELECT COALESCE(pg_catalog.max(placements.placement_generation), 0) + 1
          INTO next_generation
          FROM pgshard_catalog.database_shard_placements AS placements
         WHERE placements.logical_database_id = NEW.logical_database_id
           AND placements.database_shard_id = NEW.database_shard_id;
        IF NEW.placement_generation <> next_generation THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = format(
                    'database-shard placement generation must be %s',
                    next_generation
                );
        END IF;
        IF NEW.state = 'active' THEN
            IF NEW.placement_generation <> 1
               OR NEW.activated_at IS NULL
               OR NEW.superseded_at IS NOT NULL THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = 'only a genesis placement may start active';
            END IF;
        ELSIF NEW.state <> 'staged'
              OR NEW.activated_at IS NOT NULL
              OR NEW.superseded_at IS NOT NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'a replacement placement must start staged';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.placement_id IS DISTINCT FROM OLD.placement_id
       OR NEW.logical_database_id IS DISTINCT FROM OLD.logical_database_id
       OR NEW.database_shard_id IS DISTINCT FROM OLD.database_shard_id
       OR NEW.placement_generation IS DISTINCT FROM OLD.placement_generation
       OR NEW.shard_id IS DISTINCT FROM OLD.shard_id
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'database-shard placement identity is immutable';
    END IF;

    IF NEW.state = OLD.state
       AND NEW.activated_at IS NOT DISTINCT FROM OLD.activated_at
       AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = 'database-shard placement transitions require an atomic target-fenced cutover';
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
           AND probes.state IN ('allocated', 'active', 'retiring')
        UNION
        SELECT slots.slot_name::text
          FROM pgshard_catalog.managed_replication_slots AS slots
          JOIN pgshard_catalog.logical_consumer_attachments AS attachments
            ON attachments.attachment_generation = slots.attachment_generation
         WHERE attachments.restore_incarnation = OLD.restore_incarnation
           AND attachments.shard_id = OLD.shard_id
           AND slots.state IN ('allocated', 'active', 'retiring')
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
           AND probes.state IN ('allocated', 'active', 'retiring')
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

    -- Final retirement must run on the same catalog backend that still owns
    -- the connection-bound absence fence. Every earlier transition takes the
    -- database-enforced target lock in cluster-state-before-target order.
    IF (
        TG_OP = 'UPDATE'
        AND OLD.state = 'retiring'
        AND NEW.state = 'retired'
    ) THEN
        IF NOT pgshard_catalog.managed_slot_target_fence_held(OLD.slot_name::text) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'slot-sync probe final retirement requires its live target fence';
        END IF;
    ELSE
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
           AND slots.state IN ('allocated', 'active', 'retiring')
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
           AND slots.state IN ('allocated', 'active', 'retiring')
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
           AND slots.state IN ('allocated', 'active', 'retiring')
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
           AND slots.state IN ('allocated', 'active', 'retiring')
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
               AND slots.state IN ('allocated', 'active', 'retiring')
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
        OR (
            OLD.state = 'active'
            AND NEW.state = 'retired'
            AND attachment_state IN ('staged', 'retiring')
        )
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
        IF NOT EXISTS (
            SELECT
              FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
             WHERE attempts.slot_generation = NEW.slot_generation
               AND attempts.slot_name = NEW.slot_name
               AND attempts.allocation_kind = 'consumer'
               AND attempts.slot_role = NEW.slot_role
               AND attempts.state = 'activated'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'managed slot activation requires its receipt-authorized creation attempt';
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
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
             WHERE attempts.slot_generation = NEW.slot_generation
               AND attempts.slot_name = NEW.slot_name
               AND attempts.allocation_kind = 'consumer'
               AND attempts.slot_role = NEW.slot_role
               AND attempts.state IN ('pending', 'activated')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'managed slot retirement requires receipt-authorized absence reconciliation';
        END IF;
        IF EXISTS (
            SELECT
              FROM pgshard_catalog.managed_slot_creation_attempts AS attempts
             WHERE attempts.slot_generation = NEW.slot_generation
               AND attempts.slot_name = NEW.slot_name
               AND attempts.allocation_kind = 'consumer'
               AND attempts.slot_role = NEW.slot_role
               AND attempts.state = 'retired'
        ) AND NOT pgshard_catalog.managed_slot_target_fence_held(OLD.slot_name::text) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'managed slot final retirement requires its live target fence';
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
    target_logical_database_id uuid;
    active_placement_count bigint;
    physical_shard_id pgshard_catalog.resource_name;
    physical_shard_state text;
BEGIN
    SELECT epochs.logical_database_id
      INTO target_logical_database_id
      FROM pgshard_catalog.routing_epochs AS epochs
     WHERE epochs.routing_epoch = target_routing_epoch
       AND epochs.state = 'staged';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'routing epoch is not staged';
    END IF;

    IF EXISTS (
        SELECT placements.shard_id
          FROM pgshard_catalog.routing_ranges AS ranges
          JOIN pgshard_catalog.database_shard_placements AS placements
            ON placements.logical_database_id = ranges.logical_database_id
           AND placements.database_shard_id = ranges.database_shard_id
           AND placements.state = 'active'
         WHERE ranges.logical_database_id = target_logical_database_id
           AND ranges.routing_epoch = target_routing_epoch
         GROUP BY placements.shard_id
        HAVING pg_catalog.count(DISTINCT ranges.database_shard_id) <> 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'routing epoch maps multiple database shards to one physical shard';
    END IF;

    FOR current_range IN
        SELECT ranges.range_start,
               ranges.range_end,
               ranges.logical_database_id,
               ranges.database_shard_id
          FROM pgshard_catalog.routing_ranges AS ranges
         WHERE ranges.routing_epoch = target_routing_epoch
         ORDER BY ranges.range_start, ranges.range_end
    LOOP
        IF current_range.logical_database_id <> target_logical_database_id THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = 'routing range belongs to a different logical database';
        END IF;

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
            SELECT
              FROM pgshard_catalog.database_shards AS database_shards
             WHERE database_shards.logical_database_id = target_logical_database_id
               AND database_shards.database_shard_id = current_range.database_shard_id
               AND database_shards.state IN ('active', 'draining')
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format(
                    'routing epoch references unavailable database shard %s',
                    current_range.database_shard_id
                );
        END IF;

        SELECT pg_catalog.count(*),
               pg_catalog.min(placements.shard_id::text)::pgshard_catalog.resource_name
          INTO active_placement_count, physical_shard_id
          FROM pgshard_catalog.database_shard_placements AS placements
         WHERE placements.logical_database_id = target_logical_database_id
           AND placements.database_shard_id = current_range.database_shard_id
           AND placements.state = 'active';
        IF active_placement_count <> 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format(
                    'database shard %s has %s active placements',
                    current_range.database_shard_id,
                    active_placement_count
                );
        END IF;

        SELECT shards.state
          INTO physical_shard_state
          FROM pgshard_catalog.shards AS shards
         WHERE shards.shard_id = physical_shard_id;
        IF physical_shard_state IS NULL
           OR physical_shard_state NOT IN ('active', 'draining') THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = format(
                    'database shard %s is placed on unavailable shard %s',
                    current_range.database_shard_id,
                    physical_shard_id
                );
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

CREATE OR REPLACE FUNCTION pgshard_catalog.install_database_genesis(
    target_database_name pgshard_catalog.sql_identifier,
    target_cell_ordinals bigint[]
)
RETURNS TABLE(
    logical_database_id uuid,
    routing_epoch bigint,
    installed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pgshard_catalog, pg_temp
AS $function$
DECLARE
    cell_count bigint;
    observed_database_id uuid;
    observed_database_state text;
    observed_routing_epoch bigint;
    observed_catalog_epoch bigint;
BEGIN
    IF target_database_name::text IN ('postgres', 'shardschema', 'template0', 'template1') THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'logical database name is reserved';
    END IF;

    cell_count := pg_catalog.cardinality(target_cell_ordinals);
    IF target_cell_ordinals IS NULL OR cell_count NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'logical database genesis requires between 1 and 128 cells';
    END IF;
    IF EXISTS (
        SELECT
          FROM pg_catalog.unnest(target_cell_ordinals) AS cells(cell_ordinal)
         WHERE cells.cell_ordinal IS NULL
            OR cells.cell_ordinal NOT BETWEEN 0 AND 4294967295
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'logical database genesis contains an invalid cell ordinal';
    END IF;
    IF EXISTS (
        SELECT cells.cell_ordinal
          FROM pg_catalog.unnest(target_cell_ordinals) AS cells(cell_ordinal)
         GROUP BY cells.cell_ordinal
        HAVING pg_catalog.count(*) <> 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'logical database genesis contains a duplicate cell ordinal';
    END IF;

    -- Serialize with every catalog mutation before reading either the physical
    -- cell inventory or the database name. This gives retries one exact view
    -- and follows the catalog-state-before-target lock order used elsewhere.
    SELECT state.catalog_epoch
      INTO STRICT observed_catalog_epoch
      FROM pgshard_catalog.cluster_state AS state
     WHERE state.singleton
     FOR UPDATE;

    IF EXISTS (
        SELECT
          FROM pg_catalog.unnest(target_cell_ordinals) AS cells(cell_ordinal)
          LEFT JOIN pgshard_catalog.shards AS shards
            ON shards.shard_number = cells.cell_ordinal
           AND shards.state = 'active'
         WHERE shards.shard_id IS NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            MESSAGE = 'logical database genesis references an unavailable cell';
    END IF;

    SELECT databases.logical_database_id, databases.state
      INTO observed_database_id, observed_database_state
      FROM pgshard_catalog.logical_databases AS databases
     WHERE databases.database_name = target_database_name
     FOR NO KEY UPDATE;

    IF observed_database_id IS NOT NULL THEN
        IF observed_database_state <> 'active' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'logical database genesis identity is not active';
        END IF;

        SELECT active.routing_epoch
          INTO observed_routing_epoch
          FROM pgshard_catalog.active_routing_epochs AS active
          JOIN pgshard_catalog.routing_epochs AS epochs
            ON epochs.routing_epoch = active.routing_epoch
           AND epochs.logical_database_id = active.logical_database_id
           AND epochs.state = 'active'
         WHERE active.logical_database_id = observed_database_id
           AND NOT EXISTS (
               SELECT
                 FROM pgshard_catalog.routing_epochs AS competing_epochs
                WHERE competing_epochs.logical_database_id = observed_database_id
                  AND competing_epochs.state = 'active'
                  AND competing_epochs.routing_epoch <> active.routing_epoch
           )
         FOR KEY SHARE OF active, epochs;
        IF observed_routing_epoch IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'logical database genesis does not reference exactly one owned active routing epoch';
        END IF;

        IF EXISTS (
            WITH expected AS (
                SELECT cells.ordinality::bigint AS range_ordinal,
                       (cells.ordinality - 1)::bigint AS shard_ordinal,
                       pg_catalog.floor(
                           ((cells.ordinality - 1)::numeric * 18446744073709551616)
                           / cell_count
                       ) AS range_start,
                       pg_catalog.floor(
                           (cells.ordinality::numeric * 18446744073709551616)
                           / cell_count
                       ) AS range_end,
                       shards.shard_id
                  FROM pg_catalog.unnest(target_cell_ordinals)
                           WITH ORDINALITY AS cells(cell_ordinal, ordinality)
                  JOIN pgshard_catalog.shards AS shards
                    ON shards.shard_number = cells.cell_ordinal
            ),
            actual AS (
                SELECT pg_catalog.row_number() OVER (
                           ORDER BY ranges.range_start, ranges.range_end
                       )::bigint AS range_ordinal,
                       database_shards.shard_ordinal,
                       ranges.range_start,
                       ranges.range_end,
                       placements.shard_id
                  FROM pgshard_catalog.routing_ranges AS ranges
                  JOIN pgshard_catalog.database_shards AS database_shards
                    ON database_shards.logical_database_id = ranges.logical_database_id
                   AND database_shards.database_shard_id = ranges.database_shard_id
                  JOIN pgshard_catalog.database_shard_placements AS placements
                    ON placements.logical_database_id = ranges.logical_database_id
                   AND placements.database_shard_id = ranges.database_shard_id
                   AND placements.state = 'active'
                 WHERE ranges.routing_epoch = observed_routing_epoch
            )
            SELECT
              FROM expected
              FULL JOIN actual USING (range_ordinal)
             WHERE expected.shard_ordinal IS DISTINCT FROM actual.shard_ordinal
                OR expected.range_start IS DISTINCT FROM actual.range_start
                OR expected.range_end IS DISTINCT FROM actual.range_end
                OR expected.shard_id IS DISTINCT FROM actual.shard_id
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                MESSAGE = 'logical database genesis topology does not match active routing';
        END IF;

        logical_database_id := observed_database_id;
        routing_epoch := observed_routing_epoch;
        installed := false;
        RETURN NEXT;
        RETURN;
    END IF;

    INSERT INTO pgshard_catalog.logical_databases(database_name)
    VALUES (target_database_name)
    RETURNING logical_databases.logical_database_id
         INTO observed_database_id;

    INSERT INTO pgshard_catalog.routing_epochs(logical_database_id)
    VALUES (observed_database_id)
    RETURNING routing_epochs.routing_epoch
         INTO observed_routing_epoch;

    INSERT INTO pgshard_catalog.database_shards(
        logical_database_id,
        shard_ordinal,
        state,
        activated_at
    )
    SELECT observed_database_id,
           (cells.ordinality - 1)::bigint,
           'active',
           statement_timestamp()
      FROM pg_catalog.unnest(target_cell_ordinals)
               WITH ORDINALITY AS cells(cell_ordinal, ordinality)
     ORDER BY cells.ordinality;

    INSERT INTO pgshard_catalog.database_shard_placements(
        logical_database_id,
        database_shard_id,
        placement_generation,
        shard_id,
        state,
        activated_at
    )
    SELECT observed_database_id,
           database_shards.database_shard_id,
           1,
           shards.shard_id,
           'active',
           statement_timestamp()
      FROM pg_catalog.unnest(target_cell_ordinals)
               WITH ORDINALITY AS cells(cell_ordinal, ordinality)
      JOIN pgshard_catalog.database_shards AS database_shards
        ON database_shards.logical_database_id = observed_database_id
       AND database_shards.shard_ordinal = cells.ordinality - 1
      JOIN pgshard_catalog.shards AS shards
        ON shards.shard_number = cells.cell_ordinal
     ORDER BY cells.ordinality;

    INSERT INTO pgshard_catalog.routing_ranges(
        logical_database_id,
        routing_epoch,
        range_start,
        range_end,
        database_shard_id
    )
    SELECT observed_database_id,
           observed_routing_epoch,
           pg_catalog.floor(
               ((cells.ordinality - 1)::numeric * 18446744073709551616)
               / cell_count
           ),
           pg_catalog.floor(
               (cells.ordinality::numeric * 18446744073709551616)
               / cell_count
           ),
           database_shards.database_shard_id
      FROM pg_catalog.unnest(target_cell_ordinals)
               WITH ORDINALITY AS cells(cell_ordinal, ordinality)
      JOIN pgshard_catalog.database_shards AS database_shards
        ON database_shards.logical_database_id = observed_database_id
       AND database_shards.shard_ordinal = cells.ordinality - 1
     ORDER BY cells.ordinality;

    SELECT state.catalog_epoch
      INTO STRICT observed_catalog_epoch
      FROM pgshard_catalog.cluster_state AS state
     WHERE state.singleton;
    PERFORM pgshard_catalog.activate_routing_epoch(
        observed_database_id,
        observed_routing_epoch,
        NULL,
        observed_catalog_epoch
    );

    logical_database_id := observed_database_id;
    routing_epoch := observed_routing_epoch;
    installed := true;
    RETURN NEXT;
END
$function$;

COMMENT ON FUNCTION pgshard_catalog.install_database_genesis(
    pgshard_catalog.sql_identifier,
    bigint[]
) IS
    'Idempotently installs one immutable genesis database topology or rejects a conflicting retry.';

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

DROP TRIGGER IF EXISTS database_shards_touch_catalog
    ON pgshard_catalog.database_shards;
CREATE TRIGGER database_shards_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.database_shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS database_shards_lock_catalog
    ON pgshard_catalog.database_shards;
CREATE TRIGGER database_shards_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.database_shards
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS database_shards_protect_lifecycle
    ON pgshard_catalog.database_shards;
CREATE TRIGGER database_shards_protect_lifecycle
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.database_shards
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_database_shard_lifecycle();

DROP TRIGGER IF EXISTS database_shard_placements_touch_catalog
    ON pgshard_catalog.database_shard_placements;
CREATE TRIGGER database_shard_placements_touch_catalog
AFTER INSERT OR UPDATE OR DELETE ON pgshard_catalog.database_shard_placements
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.touch_catalog_state();

DROP TRIGGER IF EXISTS database_shard_placements_lock_catalog
    ON pgshard_catalog.database_shard_placements;
CREATE TRIGGER database_shard_placements_lock_catalog
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.database_shard_placements
FOR EACH STATEMENT EXECUTE FUNCTION pgshard_catalog.lock_catalog_state();

DROP TRIGGER IF EXISTS database_shard_placements_protect_history
    ON pgshard_catalog.database_shard_placements;
CREATE TRIGGER database_shard_placements_protect_history
BEFORE INSERT OR UPDATE OR DELETE ON pgshard_catalog.database_shard_placements
FOR EACH ROW EXECUTE FUNCTION pgshard_catalog.protect_database_shard_placement();

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
REVOKE ALL ON ALL ROUTINES IN SCHEMA pgshard_catalog FROM PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA pgshard_catalog TO pgshard_catalog_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgshard_catalog TO pgshard_catalog_reader;
REVOKE SELECT ON pgshard_catalog.managed_slot_creation_attempts
    FROM pgshard_catalog_reader;
REVOKE SELECT ON pgshard_catalog.managed_slot_target_fences
    FROM pgshard_catalog_reader;
REVOKE SELECT ON pgshard_catalog.slot_sync_probes
    FROM pgshard_catalog_reader;
GRANT SELECT (
    probe_generation,
    shard_id,
    restore_incarnation,
    system_identifier,
    database_oid,
    database_name,
    source_timeline,
    slot_name,
    consistent_point,
    state,
    created_at,
    activated_at,
    retiring_at,
    retired_at
) ON pgshard_catalog.slot_sync_probes TO pgshard_catalog_reader;

GRANT EXECUTE ON FUNCTION pgshard_catalog.acquire_managed_slot_target_fence(text)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.verify_managed_slot_target_fence(text, uuid)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.release_managed_slot_target_fence(text, uuid)
    TO pgshard_catalog_admin;

GRANT EXECUTE ON FUNCTION pgshard_catalog.begin_managed_slot_creation_attempt(
    uuid, text, text, numeric, bigint, bigint, uuid, bigint, uuid
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.abandon_managed_slot_creation_attempt(
    uuid, text, uuid
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.managed_slot_creation_attempt_state(
    uuid, text, uuid
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.activate_managed_replication_slot(
    uuid, uuid, pg_lsn, pg_lsn
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.complete_managed_replication_slot_retirement(
    uuid, text, uuid, uuid
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.slot_sync_probe_receipt_state(uuid, uuid)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.activate_slot_sync_probe(uuid, uuid, pg_lsn)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.begin_slot_sync_probe_retirement(uuid, uuid, pg_lsn)
    TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.complete_slot_sync_probe_retirement(
    uuid, text, uuid, uuid
) TO pgshard_catalog_admin;
GRANT EXECUTE ON FUNCTION pgshard_catalog.install_database_genesis(
    pgshard_catalog.sql_identifier,
    bigint[]
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
)
    ON pgshard_catalog.slot_sync_probes TO pgshard_catalog_admin;
REVOKE UPDATE ON pgshard_catalog.slot_sync_probes FROM pgshard_catalog_admin;
REVOKE UPDATE (
    consistent_point,
    creation_receipt_id,
    cleanup_receipt_id,
    state,
    activated_at,
    retiring_at,
    retired_at
) ON pgshard_catalog.slot_sync_probes FROM pgshard_catalog_admin;
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
), UPDATE (state, retired_at)
    ON pgshard_catalog.managed_replication_slots TO pgshard_catalog_admin;
REVOKE UPDATE (consistent_point, two_phase_at, activated_at)
    ON pgshard_catalog.managed_replication_slots FROM pgshard_catalog_admin;
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
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgshard_catalog GRANT SELECT ON TABLES TO pgshard_catalog_reader;

-- Trigger bodies are deliberately absent while an older physical-target
-- catalog is converted. Publish the final epoch explicitly at commit; a replay
-- notification at an unchanged epoch is harmless because readers de-duplicate
-- monotonically.
SELECT pg_catalog.pg_notify(
           'pgshard_catalog_changed',
           state.catalog_epoch::text
       )
  FROM pgshard_catalog.cluster_state AS state
 WHERE state.singleton;

COMMIT;
