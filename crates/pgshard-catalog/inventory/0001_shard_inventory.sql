-- Idempotent physical-shard inventory template for the shardschema catalog.
-- Caller-framed: the executor owns the transaction and supplies the expected
-- shard count out of band via the pgshard.expected_shard_count setting.
SET LOCAL session_replication_role = origin;
INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state)
SELECT (
         'shard-' || pg_catalog.lpad(expected.shard_number::text, 4, '0')
       )::pgshard_catalog.resource_name,
       expected.shard_number,
       'active'
  FROM pg_catalog.generate_series(0, pg_catalog.current_setting('pgshard.expected_shard_count')::bigint - 1) AS expected(shard_number)
  LEFT JOIN pgshard_catalog.shards AS shards
    ON shards.shard_id::text = 'shard-' || pg_catalog.lpad(expected.shard_number::text, 4, '0')
   AND shards.shard_number = expected.shard_number
 WHERE shards.shard_id IS NULL;
DO $pgshard_inventory_postcondition$
BEGIN
  IF EXISTS (
      SELECT
        FROM pg_catalog.generate_series(
               0,
               pg_catalog.current_setting('pgshard.expected_shard_count')::bigint - 1
             ) AS expected(shard_number)
        LEFT JOIN pgshard_catalog.shards AS shards
          ON shards.shard_id::text = 'shard-' || pg_catalog.lpad(
               expected.shard_number::text,
               4,
               '0'
             )
         AND shards.shard_number = expected.shard_number
       WHERE shards.shard_id IS NULL
          OR shards.state <> 'active'
          OR NOT EXISTS (
               SELECT
                 FROM pgshard_catalog.shard_restore_incarnations AS incarnations
                WHERE incarnations.shard_id = shards.shard_id
                  AND incarnations.state = 'active'
             )
  ) THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'initial shardschema inventory failed its transactional postcondition';
  END IF;
END
$pgshard_inventory_postcondition$;
