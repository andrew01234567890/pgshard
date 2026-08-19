# Automatic PostgreSQL tuning

`internal/pgtune` derives a PostgreSQL configuration from the resources a shard
member is scheduled with and the workload profile declared on the cluster. The
operator writes the result to `pgshard.override.conf`, which the agent's
`postgresql.conf` pulls in through `include_if_exists`, and records every
derived value with its reason under `status.tuning.derived` (`DerivedSetting`).

Preview what a given shape produces:

```
pgshard-operator tune --cpu 4 --memory 16Gi --profile oltp --storage ssd
pgshard-operator tune --cpu 16 --memory 64Gi --disk 1Ti --profile analytics --major 19 --json
```

## Inputs

| Input | Meaning |
|---|---|
| `--cpu` | CPU limit (millicores are floored to whole cores, minimum 1) |
| `--memory` | memory limit; the whole budget below is measured against it |
| `--disk` | PGDATA volume size; sizes WAL retention when known |
| `--storage` | `ssd`, `hdd` or `unknown` (treated as ssd) |
| `--profile` | `oltp` (default), `mixed`, `analytics` |
| `--max-backends` | the pooler's total server-connection budget across roles |
| `--logical-slots` | expected logical replication slots (resharding streams) |
| `--replicas` | streaming replicas per shard |
| `--major` | PostgreSQL major, 18 or 19 |

## Memory budget

The invariant every derivation must satisfy, checked again after overrides:

```
shared_buffers
+ max_backends × work_mem × 4
+ maintenance_work_mem × autovacuum_max_workers
+ logical_decoding_work_mem × logical_slots
+ 256MiB overhead
<= memory limit
```

`Derive` returns `ErrOverCommitted` rather than emit a configuration that
breaks it, whether the pressure comes from the inputs (a tiny limit with a
large backend budget) or from an override that raises `work_mem` or
`shared_buffers`. Values inside the budget:

| Setting | Rule |
|---|---|
| `shared_buffers` | 25% of memory, capped at 16GiB |
| `effective_cache_size` | 75% of memory |
| `work_mem` | what remains after the fixed terms, divided by `max_backends × 4`; capped at 64MiB (256MiB for analytics); below 1MiB is an error |
| `maintenance_work_mem`, `autovacuum_work_mem` | min(2GiB, memory/16) |
| `logical_decoding_work_mem` | 64MiB, shrunk so slots × budget stays within memory/8 |
| `wal_buffers` | 16MiB |
| `huge_pages` | `try` |
| `max_connections` | `max_backends` + 8 reserved for the superuser and the agent |

## Other groups

* I/O (PostgreSQL 18 AIO): `io_method=worker`; `io_workers` (18) or
  `io_max_workers` (19) = cpu/2 clamped to [3,32]; `effective_io_concurrency`
  200 on ssd, 2 on hdd; `random_page_cost` 1.1 on ssd, 4 on hdd.
* WAL: `max_wal_size` 10% of the disk clamped to [1GiB,64GiB] (4GiB when the
  disk is unknown), `min_wal_size` 1GiB, `checkpoint_completion_target` 0.9,
  `wal_compression=zstd`.
* Autovacuum: workers cpu/2 in [3,10], cost limit 1000 (oltp) or 2000,
  naptime 15s.
* Parallelism: `max_worker_processes` max(8, cpu×2), `max_parallel_workers`
  cpu, per-gather 2 (cpu/2 for analytics), maintenance workers cpu/4.
* Planner: `jit` off for oltp/mixed and on for analytics,
  `default_toast_compression=lz4`.
* Logging: checkpoints and lock waits on, `log_min_duration_statement`
  1000ms (oltp) or 5000ms, `shared_preload_libraries=pg_stat_statements`.
* `idle_in_transaction_session_timeout=10min`.

## Mandatory settings

Always emitted and never overridable: `wal_level=logical`,
`max_replication_slots` (replicas + logical slots + 8), `max_wal_senders`
(that + 2), `max_prepared_transactions` (= `max_connections`),
`synchronous_commit=on`, `track_commit_timestamp=on`, `max_slot_wal_keep_size`
(20% of the disk clamped to [4GiB,200GiB], 20GiB when unknown),
`idle_replication_slot_timeout=24h`, `password_encryption=scram-sha-256`,
`ssl=on`.

## Overrides

`spec.postgresql.parameters` are applied last, replacing a derived value or
adding a new setting with the reason `operator override`. Keys on the unsafe
list are rejected with `ErrUnsafeOverride` naming the key: `fsync`,
`full_page_writes`, `wal_level`, `max_prepared_transactions`, `ssl`,
`data_checksums`, `password_encryption`, `track_commit_timestamp`, the
replication-slot and WAL-sender limits, everything the agent owns
(`listen_addresses`, `hba_file`, `primary_conninfo`,
`synchronous_standby_names`, ...) and the `include*` directives.
`synchronous_commit` accepts only `remote_apply`, the one level stronger than
the `on` floor. `UnsafeKeys()` returns the full list.

## Rendering

`Render()` produces `postgresql.conf` text sorted by name; numbers and
identifier-like words are bare, everything else is single-quoted with embedded
quotes doubled, matching PostgreSQL's parser. `Derived()` returns the
`[]DerivedSetting` form used in the cluster status.

## Verification

Golden files under `internal/pgtune/testdata` cover 2c/4Gi, 4c/16Gi and
16c/64Gi for every profile. `go test -tags integration ./internal/pgtune` starts
the PostgreSQL 18 and 19 images with the rendered override included from
`postgresql.conf` and asserts that the server starts, that every emitted GUC is
sourced from that file, and that `SHOW` returns the expected values; the 18/19
split of `io_workers` versus `io_max_workers` came out of that check.
