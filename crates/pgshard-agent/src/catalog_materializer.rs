//! Applies a verified materialization program to the catalog database.
//!
//! The program is already proven to be pure SQL by the input compiler; this
//! module owns *when* it may run. Every mutation is applied on a writable
//! session bound to the accepted request, under a generation fence held on the
//! database where the writable generation lives, and with an exact authority
//! observation immediately before each commit.
//!
//! What the fence does and does not guarantee. While it is held, a competing
//! generation publication blocks on its locks, so it cannot advance past a
//! catalog commit that is still to come. It cannot make the two atomic: they
//! are separate databases and therefore separate transactions, and a commit is
//! dispatched before its reply is known. If the fence is lost with a commit
//! already in flight, whether that commit landed is genuinely unknown here, and
//! the step reports `AmbiguousCatalogCommit` rather than a clean failure. The
//! stage that drives this executor has to reconcile that state before treating
//! the catalog as materialized — this module deliberately does not pretend to
//! resolve it.
//!
//! The materialization stage drives this executor from one exact
//! writable-runtime capability; nothing else may apply a program.
use std::path::Path;
use std::time::Duration;

use crate::catalog_materialization_program::CatalogMaterializationProgram;
use crate::postgres_generation::{
    CatalogWriterSession, GenerationFence, OBSERVE_STANDARD_CONFORMING_STRINGS,
    PostgresGenerationError,
};
use crate::writable::DurableWritableGeneration;

/// Binds the sealed shard count into the inventory's expectation. The value is
/// a bound parameter, never text substituted into SQL.
const SET_EXPECTED_SHARD_COUNT: &str =
    "SELECT pg_catalog.set_config('pgshard.expected_shard_count', $1, true)";
/// Binds the allow-empty policy the preflight reads. Also a bound parameter.
const SET_ALLOW_EMPTY_DATABASE_TOPOLOGY: &str =
    "SELECT pg_catalog.set_config('pgshard.bootstrap_allow_empty_database_topology', $1, true)";

/// Proves the catalog holds exactly the configured shard inventory, not merely
/// that the configured shards are present.
///
/// The inventory fragment's own postcondition checks that every expected shard
/// exists, is active, and has an active restore incarnation. It cannot see an
/// *unexpected* active shard, so a catalog carrying extra active shards would
/// satisfy it while disagreeing with the sealed shard count.
const INVENTORY_IS_EXACT: &str = "\
    SELECT (SELECT pg_catalog.count(*) FROM pgshard_catalog.shards \
              WHERE state = 'active') = $1::bigint \
       AND NOT EXISTS ( \
             SELECT FROM pgshard_catalog.shards \
              WHERE state = 'active' \
                AND (shards.shard_number < 0 \
                     OR shards.shard_number > $1 - 1 \
                     OR shards.shard_id::text <> 'shard-' || pg_catalog.lpad( \
                          shards.shard_number::text, 4, '0')))";

/// Serializes verification against catalog writers, and proves the row every
/// writer serializes on exists at all.
///
/// Normal catalog DML takes its target relation before this row, so taking it
/// first here cannot deadlock against that order: verification only reads, and
/// a reader never blocks a writer's relation lock. Holding it makes the
/// predicates below one observation rather than several `READ COMMITTED`
/// snapshots a writer can commit between.
const LOCK_CLUSTER_STATE: &str =
    "SELECT FROM pgshard_catalog.cluster_state WHERE singleton FOR UPDATE";

/// Proves the catalog carries the one cluster identity the capability will be
/// bound to. The singleton primary key already forbids a second row; what is
/// checked here is that the row exists, names a real cluster, and homes the
/// shard the topology is written against.
const CLUSTER_IDENTITY_IS_CANONICAL: &str = "\
    SELECT pg_catalog.count(*) = 1 FROM pgshard_catalog.cluster_configuration \
     WHERE singleton \
       AND cluster_id <> '00000000-0000-0000-0000-000000000000'::uuid \
       AND home_shard_id::text = 'shard-0000'";

/// Proves the restore lineage agrees with the shard states.
///
/// Retired incarnations and non-active shards are legitimate history, so this
/// asserts a relationship rather than an absence: every shard has lineage, and
/// a shard is active exactly when it has an active incarnation. The partial
/// unique index makes that active incarnation unique, so counting is not needed.
const SHARD_LINEAGE_IS_COMPLETE: &str = "\
    SELECT NOT EXISTS ( \
      SELECT FROM pgshard_catalog.shards \
       WHERE NOT EXISTS ( \
               SELECT FROM pgshard_catalog.shard_restore_incarnations AS lineage \
                WHERE lineage.shard_id = shards.shard_id) \
          OR (shards.state = 'active') <> EXISTS ( \
               SELECT FROM pgshard_catalog.shard_restore_incarnations AS active_lineage \
                WHERE active_lineage.shard_id = shards.shard_id \
                  AND active_lineage.state = 'active'))";

/// The applying transaction's own framing. The program's fragments carry none:
/// that is what the input compiler enforces, and it is why the executor can put
/// its authority check immediately before `COMMIT`.
const APPLY_TRANSACTION_SETTINGS: &str = "\
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\
    SET LOCAL search_path = pg_catalog;\
    SET LOCAL standard_conforming_strings = on;\
    SET LOCAL lock_timeout = '5s';\
    SET LOCAL statement_timeout = '60s';\
    SET LOCAL transaction_timeout = '120s';\
    SET LOCAL idle_in_transaction_session_timeout = '30s';";

/// Applies the program and proves the catalog holds what it declares, retrying
/// while this attempt remains the authoritative one.
///
/// An apply can end ambiguous — the fence lost with a commit in flight — and no
/// observation from here can settle whether that commit landed. The program is
/// idempotent by construction, so the resolution is to run it again and verify
/// against the catalog, which is the authority on what actually happened.
///
/// Returns only when the catalog provably holds the declared state. A caller
/// must publish no materialization capability on any error: the attempt is
/// over, not merely delayed.
pub(crate) async fn materialize_and_verify<F>(
    socket_dir: &Path,
    program: &CatalogMaterializationProgram,
    shard_count: u32,
    allow_empty_database_topology: bool,
    expected_generation: &DurableWritableGeneration,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        // Verification is a second fenced round trip, so it runs only once the
        // apply has actually succeeded. A failed apply is classified below on
        // its own terms: replayed if retryable, returned if terminal.
        let outcome = match materialize(
            socket_dir,
            program,
            shard_count,
            allow_empty_database_topology,
            expected_generation,
            authority_exact,
        )
        .await
        {
            Ok(()) => {
                verify_materialized(
                    socket_dir,
                    program,
                    shard_count,
                    expected_generation,
                    authority_exact,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => return Ok(()),
            // Retryable only while this attempt is still the authoritative
            // one: an ambiguous commit, a lost fence, or a catalog that does
            // not yet match are all resolved by running the idempotent program
            // again and checking the catalog.
            Err(
                error @ (PostgresGenerationError::AmbiguousCatalogCommit
                | PostgresGenerationError::GenerationFenceLost
                | PostgresGenerationError::CatalogNotMaterialized),
            ) => {
                if attempts >= MATERIALIZATION_ATTEMPTS || !authority_exact() {
                    return Err(error);
                }
                tracing::warn!(
                    reason = %error,
                    attempt = attempts,
                    "catalog materialization did not settle; reconciling"
                );
                tokio::time::sleep(RECONCILE_BACKOFF).await;
            }
            // Everything else — authority moved on, the runtime changed, the
            // inputs were rejected — ends the attempt rather than retrying it.
            Err(error) => return Err(error),
        }
    }
}

/// Bounded so a persistent conflict ends the attempt instead of holding the
/// fence against generation publication indefinitely.
const MATERIALIZATION_ATTEMPTS: u32 = 3;
const RECONCILE_BACKOFF: Duration = Duration::from_millis(250);

/// Applies one verified program to the catalog database, exactly once per call.
///
/// `authority_exact` reports whether the attempt-private writable authority
/// still matches the accepted request. It is consulted immediately before every
/// commit, with nothing awaited in between, mirroring the invariant the
/// generation publisher already holds.
pub(crate) async fn materialize<F>(
    socket_dir: &Path,
    program: &CatalogMaterializationProgram,
    shard_count: u32,
    allow_empty_database_topology: bool,
    expected_generation: &DurableWritableGeneration,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    let mut writer = CatalogWriterSession::connect(socket_dir).await?;
    let shard_count = shard_count.to_string();
    let allow_empty = if allow_empty_database_topology {
        "true"
    } else {
        "false"
    };

    // Applied caller-framed from the verified body, not as the self-framed
    // file: an input that commits on its own leaves no point at which the
    // executor can observe authority, so the migration could commit after the
    // attempt's authority had been revoked. The body is the same
    // digest-verified bytes with its own framing statements excluded.
    apply_fenced(
        socket_dir,
        &mut writer,
        expected_generation,
        authority_exact,
        Step::CallerFramed {
            scalar: None,
            fragments: &[&program.migration_body],
        },
    )
    .await?;

    apply_fenced(
        socket_dir,
        &mut writer,
        expected_generation,
        authority_exact,
        Step::CallerFramed {
            scalar: Some((SET_EXPECTED_SHARD_COUNT, shard_count.as_str())),
            fragments: &[&program.inventory],
        },
    )
    .await?;

    // Preflight and genesis share one transaction, preflight first: its guard
    // raises inside the transaction that would install genesis, so a topology
    // conflict aborts both and its row locks are held through the install.
    apply_fenced(
        socket_dir,
        &mut writer,
        expected_generation,
        authority_exact,
        Step::CallerFramed {
            scalar: Some((SET_ALLOW_EMPTY_DATABASE_TOPOLOGY, allow_empty)),
            fragments: &[&program.preflight, &program.genesis],
        },
    )
    .await
}

/// Re-reads the catalog and requires it to equal what the program declares.
///
/// A successful apply is not proof: an ambiguous commit may have landed, an
/// earlier attempt may have left a prefix, and the fragments' own postconditions
/// do not establish full equality. This runs under its own fence, so the
/// generation is proven current across the verification as well.
pub(crate) async fn verify_materialized<F>(
    socket_dir: &Path,
    program: &CatalogMaterializationProgram,
    shard_count: u32,
    expected_generation: &DurableWritableGeneration,
    authority_exact: &F,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    if !authority_exact() {
        return Err(PostgresGenerationError::AuthorityChanged);
    }
    let mut writer = CatalogWriterSession::connect(socket_dir).await?;
    let fence = GenerationFence::hold(socket_dir, expected_generation).await?;
    let outcome = read_back(&mut writer, program, shard_count).await;
    let released = fence.release().await;
    outcome?;
    released?;
    // The catalog matched, but only an attempt that still holds its authority
    // may claim it materialized this state.
    if !authority_exact() {
        return Err(PostgresGenerationError::AuthorityChanged);
    }
    Ok(())
}

async fn read_back(
    writer: &mut CatalogWriterSession,
    program: &CatalogMaterializationProgram,
    shard_count: u32,
) -> Result<(), PostgresGenerationError> {
    // Bound as an integer: the comparison is numeric, unlike the GUC the
    // fragments read, which is text by definition.
    let shard_count = i64::from(shard_count);
    let transaction = writer.client().transaction().await?;
    transaction
        .batch_execute(APPLY_TRANSACTION_SETTINGS)
        .await?;
    // Every predicate below observes the catalog under this row lock.
    if transaction.query(LOCK_CLUSTER_STATE, &[]).await?.len() != 1 {
        tracing::warn!("catalog verification: the cluster-state singleton is absent");
        return Err(PostgresGenerationError::CatalogNotMaterialized);
    }
    require(
        &transaction,
        INVENTORY_IS_EXACT,
        &[&shard_count],
        "inventory",
    )
    .await?;
    require(
        &transaction,
        CLUSTER_IDENTITY_IS_CANONICAL,
        &[],
        "cluster identity",
    )
    .await?;
    require(
        &transaction,
        SHARD_LINEAGE_IS_COMPLETE,
        &[],
        "restore lineage",
    )
    .await?;
    // Re-run the declared topology guard with the allow-empty policy off, so an
    // empty or divergent topology raises here rather than being tolerated as it
    // is during first materialization.
    transaction
        .execute(SET_ALLOW_EMPTY_DATABASE_TOPOLOGY, &[&"false"])
        .await?;
    transaction.batch_execute(&program.preflight).await?;
    // Read-only: nothing to commit.
    transaction.rollback().await?;
    Ok(())
}

/// Runs one verification predicate, naming it if the catalog disagrees so the
/// reconcile log says which invariant failed rather than only that one did.
async fn require(
    transaction: &tokio_postgres::Transaction<'_>,
    predicate: &str,
    parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    invariant: &'static str,
) -> Result<(), PostgresGenerationError> {
    let holds: bool = transaction
        .query_one(predicate, parameters)
        .await?
        .try_get(0)?;
    if !holds {
        tracing::warn!(invariant, "catalog verification: the catalog disagrees");
        return Err(PostgresGenerationError::CatalogNotMaterialized);
    }
    Ok(())
}

enum Step<'a> {
    /// The executor owns the transaction and binds the scalar the fragments read.
    CallerFramed {
        scalar: Option<(&'static str, &'a str)>,
        fragments: &'a [&'a str],
    },
}

async fn apply_fenced<F>(
    socket_dir: &Path,
    writer: &mut CatalogWriterSession,
    expected_generation: &DurableWritableGeneration,
    authority_exact: &F,
    step: Step<'_>,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    if !authority_exact() {
        return Err(PostgresGenerationError::AuthorityChanged);
    }
    // Held across the commit below, then released. Proving the generation and
    // committing are on different databases and so cannot share a transaction;
    // the fence is what stops a new generation being published in between.
    let fence = GenerationFence::hold(socket_dir, expected_generation).await?;
    // A fence that dies has released its locks, so the guarded work is no
    // longer fenced and must not be allowed to reach its commit.
    // Deliberately not raced against `fence.lost()`. Cancelling the work after
    // COMMIT was queued cannot un-queue it, and classifying that as a plain
    // lost fence hid the one state the caller most needs to tell apart. The
    // writer's own timeouts bound the step instead.
    let mut dispatched = CommitDispatch::NotReached;
    let outcome = apply(writer, authority_exact, &fence, &mut dispatched, step).await;
    let released = fence.release().await;
    classify_step(outcome, released, dispatched)
}

/// Decides what a step's outcome means once the fence has been released.
///
/// A fence lost while a `COMMIT` was already in flight leaves that commit's
/// fate unknown, and that outranks every other classification: the caller must
/// be able to tell "did not happen" from "may have happened".
fn classify_step(
    outcome: Result<(), PostgresGenerationError>,
    released: Result<(), PostgresGenerationError>,
    dispatched: CommitDispatch,
) -> Result<(), PostgresGenerationError> {
    match (outcome, released) {
        (_, Err(_)) | (Err(PostgresGenerationError::GenerationFenceLost), _)
            if dispatched == CommitDispatch::Dispatched =>
        {
            Err(PostgresGenerationError::AmbiguousCatalogCommit)
        }
        (Err(error), _) => Err(error),
        (Ok(()), Err(lost)) => Err(lost),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Whether a `COMMIT` has been put on the wire for the current step.
///
/// Once it has, no later observation can prove it did not land, so a fence lost
/// from that point on is ambiguous rather than failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitDispatch {
    NotReached,
    Dispatched,
}

async fn apply<F>(
    writer: &mut CatalogWriterSession,
    authority_exact: &F,
    fence: &GenerationFence,
    dispatched: &mut CommitDispatch,
    step: Step<'_>,
) -> Result<(), PostgresGenerationError>
where
    F: Fn() -> bool,
{
    let client = writer.client();
    match step {
        Step::CallerFramed { scalar, fragments } => {
            let transaction = client.transaction().await?;
            transaction
                .batch_execute(APPLY_TRANSACTION_SETTINGS)
                .await?;
            if let Some((statement, value)) = scalar {
                transaction.execute(statement, &[&value]).await?;
            }
            for (index, fragment) in fragments.iter().enumerate() {
                // Re-observed BETWEEN fragments: the string mode decides where a
                // quoted literal ends and the compiler's proof assumed one
                // answer, so a fragment that changed it would move the boundary
                // for the next one. Not before the first, because issuing a
                // query there would forbid a fragment's own SET TRANSACTION —
                // the applying settings have already pinned the mode.
                if index > 0 {
                    let observed: String = transaction
                        .query_one(OBSERVE_STANDARD_CONFORMING_STRINGS, &[])
                        .await?
                        .try_get(0)?;
                    if observed != "on" {
                        return Err(PostgresGenerationError::InvalidCatalogWriterSettings);
                    }
                }
                transaction.batch_execute(fragment).await?;
            }
            #[cfg(test)]
            crate::postgres_generation::pre_commit_checkpoint().await;
            // No await or state-changing operation may be inserted between these
            // exact observations and dispatching COMMIT. The fence check
            // narrows, but cannot close, the window: COMMIT is queued before
            // its reply is known, so a fence lost from here on leaves the
            // commit's fate unknown rather than failed.
            if !fence.holds() {
                return Err(PostgresGenerationError::GenerationFenceLost);
            }
            if !authority_exact() {
                return Err(PostgresGenerationError::AuthorityChanged);
            }
            *dispatched = CommitDispatch::Dispatched;
            match transaction.commit().await {
                Ok(()) => Ok(()),
                Err(error) if !fence.holds() => {
                    tracing::warn!(reason = %error, "catalog commit outcome is unknown: the generation fence was lost in flight");
                    Err(PostgresGenerationError::AmbiguousCatalogCommit)
                }
                Err(error) => Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The committed goldens are rendered for a four-shard fixture cluster, so
    /// the sealed shard count has to match or genesis references a cell the
    /// inventory never created.
    const FIXTURE_SHARDS: u32 = 4;

    /// Drives the real compiled program against a live server and revokes
    /// authority in the window before the migration's commit checks.
    ///
    /// Everything this asserts is a server behaviour: that the fence blocks a
    /// competing publication, that a revoked attempt does not commit, and that
    /// nothing it created survives. The unit tests cannot reach any of it.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    #[allow(clippy::too_many_lines)]
    async fn live_postgres18_revoked_authority_does_not_materialize() {
        use crate::catalog_materialization_program::compile_catalog_materialization_program;
        use crate::postgres_generation::{
            CATALOG_DATABASE, gate_next_pre_commit, publish_writable_generation,
        };
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required");
        // Terms above the fence test's, which uses the same disposable server;
        // a lower term is rejected as a regression before anything is tested.
        let first = crate::postgres_generation::tests::generation("cluster-1", "holder-a", 3);
        let second = crate::postgres_generation::tests::generation("cluster-1", "holder-a", 4);

        create_catalog_database(&socket_dir).await;
        publish_writable_generation(&socket_dir, &first, &|| true)
            .await
            .expect("publish the first generation");

        let program = compile_catalog_materialization_program(
            include_bytes!("../../pgshard-catalog/migrations/0001_shardschema.sql"),
            include_bytes!("../../pgshard-catalog/inventory/0001_shard_inventory.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/genesis.golden.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/preflight.golden.sql"),
        )
        .expect("the real inputs compile");

        // Revoked while the migration's fragments have run but before the
        // executor observes the fence and the attempt's authority.
        let authorized = Arc::new(AtomicBool::new(true));
        let (entered, release) = gate_next_pre_commit();
        let attempt_authorized = Arc::clone(&authorized);
        let attempt_socket = socket_dir.clone();
        let attempt_generation = first.clone();
        let attempt = tokio::spawn(async move {
            materialize(
                &attempt_socket,
                &program,
                FIXTURE_SHARDS,
                true,
                &attempt_generation,
                &move || attempt_authorized.load(Ordering::Acquire),
            )
            .await
        });

        entered
            .await
            .expect("the executor reached its commit checks");

        // Started while the attempt still holds its fence, so it has to wait
        // rather than move the durable generation out from under the checks the
        // attempt is about to make.
        let publisher_socket = socket_dir.clone();
        let publisher_generation = second.clone();
        let publisher = tokio::spawn(async move {
            publish_writable_generation(&publisher_socket, &publisher_generation, &|| true).await
        });
        assert_generation_publication_waits(&socket_dir, &first).await;

        authorized.store(false, Ordering::Release);
        release.send(()).expect("release the executor");

        let outcome = attempt.await.expect("materializer task");
        publisher
            .await
            .expect("publication task")
            .expect("publication completes once the attempt releases its fence");
        assert!(
            matches!(outcome, Err(PostgresGenerationError::AuthorityChanged)),
            "a revoked attempt reported {outcome:?}"
        );

        // Nothing the revoked attempt ran may survive.
        let catalog = crate::postgres_generation::tests::connect_to(&socket_dir, CATALOG_DATABASE)
            .await
            .expect("inspect the catalog database");
        let installed: bool = catalog
            .client()
            .query_one(
                "SELECT pg_catalog.to_regnamespace('pgshard_catalog') IS NOT NULL",
                &[],
            )
            .await
            .expect("observe the catalog schema")
            .get(0);
        assert!(!installed, "a revoked attempt left pgshard_catalog behind");

        // The generation was already proven unchanged while the attempt held
        // its fence; by now the waiting publication has legitimately landed.

        // A retry under a new generation must then materialize completely,
        // which also proves the revoked attempt left nothing that would block
        // it and that the executor's ordered apply actually works end to end.
        let program = compile_catalog_materialization_program(
            include_bytes!("../../pgshard-catalog/migrations/0001_shardschema.sql"),
            include_bytes!("../../pgshard-catalog/inventory/0001_shard_inventory.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/genesis.golden.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/preflight.golden.sql"),
        )
        .expect("the real inputs compile");
        materialize(
            &socket_dir,
            &program,
            FIXTURE_SHARDS,
            true,
            &second,
            &|| true,
        )
        .await
        .expect("materialize under the replacement generation");

        let catalog = crate::postgres_generation::tests::connect_to(&socket_dir, CATALOG_DATABASE)
            .await
            .expect("inspect the materialized catalog");
        let shards: i64 = catalog
            .client()
            .query_one(
                "SELECT pg_catalog.count(*) FROM pgshard_catalog.shards",
                &[],
            )
            .await
            .expect("count materialized shards")
            .get(0);
        assert_eq!(
            shards,
            i64::from(FIXTURE_SHARDS),
            "the inventory did not materialize every configured shard"
        );
        let databases: i64 = catalog
            .client()
            .query_one(
                "SELECT pg_catalog.count(*) FROM pgshard_catalog.logical_databases",
                &[],
            )
            .await
            .expect("count genesis databases")
            .get(0);
        assert!(databases > 0, "genesis installed no logical database");
    }

    /// Reconciliation must settle a catalog left in a committed prefix, which
    /// is the state an ambiguous commit or an interrupted attempt leaves.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    async fn live_postgres18_reconciles_a_partially_materialized_catalog() {
        use crate::catalog_materialization_program::compile_catalog_materialization_program;
        use crate::postgres_generation::publish_writable_generation;

        let socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required");
        let generation = crate::postgres_generation::tests::generation("cluster-1", "holder-a", 9);

        create_catalog_database(&socket_dir).await;
        publish_writable_generation(&socket_dir, &generation, &|| true)
            .await
            .expect("publish the generation");
        let program = compile_catalog_materialization_program(
            include_bytes!("../../pgshard-catalog/migrations/0001_shardschema.sql"),
            include_bytes!("../../pgshard-catalog/inventory/0001_shard_inventory.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/genesis.golden.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/preflight.golden.sql"),
        )
        .expect("the real inputs compile");

        // The prefix an interrupted attempt leaves: the migration committed,
        // nothing after it. Applying it alone is exactly what the executor's
        // first step does.
        let mut writer = CatalogWriterSession::connect(&socket_dir)
            .await
            .expect("connect the writer");
        let migration = writer
            .client()
            .transaction()
            .await
            .expect("begin the migration");
        migration
            .batch_execute(&program.migration_body)
            .await
            .expect("apply the migration body");
        migration.commit().await.expect("commit the prefix");
        drop(writer);

        // Verification must refuse that prefix, and reconciliation must settle it.
        assert!(
            matches!(
                verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true)
                    .await,
                Err(PostgresGenerationError::CatalogNotMaterialized)
            ),
            "verification accepted a migration-only prefix"
        );
        materialize_and_verify(
            &socket_dir,
            &program,
            FIXTURE_SHARDS,
            true,
            &generation,
            &|| true,
        )
        .await
        .expect("reconcile the partially materialized catalog");
        assert_catalog_is_canonical(&socket_dir).await;
    }

    /// Verification must reject a catalog that satisfies the fragments' own
    /// postconditions but disagrees with the declared inventory.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    async fn live_postgres18_verification_rejects_an_unexpected_active_shard() {
        use crate::catalog_materialization_program::compile_catalog_materialization_program;
        use crate::postgres_generation::{CATALOG_DATABASE, publish_writable_generation};

        let socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required");
        let generation = crate::postgres_generation::tests::generation("cluster-1", "holder-a", 8);

        create_catalog_database(&socket_dir).await;
        publish_writable_generation(&socket_dir, &generation, &|| true)
            .await
            .expect("publish the generation");
        let program = compile_catalog_materialization_program(
            include_bytes!("../../pgshard-catalog/migrations/0001_shardschema.sql"),
            include_bytes!("../../pgshard-catalog/inventory/0001_shard_inventory.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/genesis.golden.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/preflight.golden.sql"),
        )
        .expect("the real inputs compile");

        materialize(
            &socket_dir,
            &program,
            FIXTURE_SHARDS,
            true,
            &generation,
            &|| true,
        )
        .await
        .expect("materialize the declared state");
        verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true)
            .await
            .expect("the declared state verifies");

        // An active shard the sealed count does not declare. The inventory's own
        // postcondition still passes — it only looks for missing shards — so
        // this is exactly the divergence verification has to catch.
        let catalog = crate::postgres_generation::tests::connect_to(&socket_dir, CATALOG_DATABASE)
            .await
            .expect("connect to the catalog");
        catalog
            .client()
            .batch_execute(
                "INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) \
                 VALUES ('shard-0009', 9, 'active')",
            )
            .await
            .expect("introduce an undeclared active shard");

        let outcome =
            verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true).await;
        assert!(
            matches!(
                outcome,
                Err(PostgresGenerationError::CatalogNotMaterialized)
            ),
            "verification accepted an undeclared active shard: {outcome:?}"
        );
    }

    /// Materializes once, then proves each remaining invariant is load-bearing:
    /// breaking it is rejected and repairing it is accepted again.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL 18 server"]
    async fn live_postgres18_verification_rejects_a_catalog_that_breaks_an_invariant() {
        use crate::postgres_generation::CATALOG_DATABASE;

        let (socket_dir, generation, program) = materialized_fixture(10).await;
        let catalog = crate::postgres_generation::tests::connect_to(&socket_dir, CATALOG_DATABASE)
            .await
            .expect("connect to the catalog");

        // `cluster_configuration_immutable` rejects ordinary DML, so reaching a
        // divergent identity at all takes disabling that guard. The question
        // verification has to answer is what happens to a catalog that already
        // holds such a state, however it got there.
        let unguarded = |statement: &str| {
            format!(
                "ALTER TABLE pgshard_catalog.cluster_configuration \
                    DISABLE TRIGGER cluster_configuration_immutable; \
                 {statement}; \
                 ALTER TABLE pgshard_catalog.cluster_configuration \
                    ENABLE TRIGGER cluster_configuration_immutable"
            )
        };
        for (invariant, break_it, repair_it) in [
            (
                "the home shard the topology is written against",
                unguarded(
                    "UPDATE pgshard_catalog.cluster_configuration \
                        SET home_shard_id = 'shard-0001' WHERE singleton",
                ),
                unguarded(
                    "UPDATE pgshard_catalog.cluster_configuration \
                        SET home_shard_id = 'shard-0000' WHERE singleton",
                ),
            ),
            (
                "a cluster identity the capability can bind",
                unguarded(
                    "UPDATE pgshard_catalog.cluster_configuration \
                        SET cluster_id = '00000000-0000-0000-0000-000000000000'::uuid \
                      WHERE singleton",
                ),
                unguarded(
                    "UPDATE pgshard_catalog.cluster_configuration \
                        SET cluster_id = pg_catalog.gen_random_uuid() WHERE singleton",
                ),
            ),
            (
                "an active shard has an active restore incarnation",
                "UPDATE pgshard_catalog.shard_restore_incarnations \
                    SET state = 'retired', retired_at = pg_catalog.statement_timestamp() \
                  WHERE shard_id = 'shard-0000' AND state = 'active'"
                    .to_owned(),
                // Retirement is one-way, so lineage is repaired the way a real
                // restore repairs it: by allocating a fresh active incarnation.
                "INSERT INTO pgshard_catalog.shard_restore_incarnations\
                     (restore_incarnation, shard_id, state) \
                 VALUES (pg_catalog.gen_random_uuid(), 'shard-0000', 'active')"
                    .to_owned(),
            ),
            (
                "the row every catalog writer serializes on",
                "DELETE FROM pgshard_catalog.cluster_state WHERE singleton".to_owned(),
                "INSERT INTO pgshard_catalog.cluster_state(singleton) VALUES (true) \
                 ON CONFLICT (singleton) DO NOTHING"
                    .to_owned(),
            ),
        ] {
            catalog
                .client()
                .batch_execute(&break_it)
                .await
                .unwrap_or_else(|error| panic!("break {invariant}: {error:?}"));
            let outcome =
                verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true)
                    .await;
            assert!(
                matches!(
                    outcome,
                    Err(PostgresGenerationError::CatalogNotMaterialized)
                ),
                "verification accepted a catalog that does not hold {invariant}: {outcome:?}"
            );
            catalog
                .client()
                .batch_execute(&repair_it)
                .await
                .unwrap_or_else(|error| panic!("repair {invariant}: {error:?}"));
            verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true)
                .await
                .unwrap_or_else(|error| {
                    panic!("verification rejected a repaired catalog holding {invariant}: {error}")
                });
        }
    }

    /// The predicates must be one observation, not several `READ COMMITTED`
    /// snapshots a writer can commit an undeclared shard between. Proven by
    /// holding the row a writer serializes on: verification has to block on it.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL 18 server"]
    async fn live_postgres18_verification_serializes_on_the_cluster_state_row() {
        use crate::postgres_generation::CATALOG_DATABASE;

        let (socket_dir, generation, program) = materialized_fixture(11).await;
        let writer = crate::postgres_generation::tests::connect_to(&socket_dir, CATALOG_DATABASE)
            .await
            .expect("connect as a competing catalog writer");
        writer
            .client()
            .batch_execute(&format!("BEGIN; {LOCK_CLUSTER_STATE};"))
            .await
            .expect("hold the row a catalog writer serializes on");

        let outcome =
            verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true).await;
        let Err(PostgresGenerationError::Database(error)) = outcome else {
            panic!("verification observed the catalog without taking the row lock: {outcome:?}");
        };
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
            "verification failed for some reason other than the held row lock: {error}"
        );

        writer
            .client()
            .batch_execute("ROLLBACK")
            .await
            .expect("release the row");
        verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true)
            .await
            .expect("verification succeeds once the row is free");
    }

    /// The retry is bounded, so a divergence the idempotent program cannot undo
    /// ends the attempt instead of holding the fence against publication.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL 18 server"]
    async fn live_postgres18_bounded_retry_gives_up_on_a_catalog_it_cannot_reconcile() {
        use crate::postgres_generation::CATALOG_DATABASE;

        let (socket_dir, generation, program) = materialized_fixture(12).await;
        let catalog = crate::postgres_generation::tests::connect_to(&socket_dir, CATALOG_DATABASE)
            .await
            .expect("connect to the catalog");
        // Replaying the program cannot remove this, so every attempt verifies
        // false and the loop has to stop rather than spin.
        catalog
            .client()
            .batch_execute(
                "INSERT INTO pgshard_catalog.shards(shard_id, shard_number, state) \
                 VALUES ('shard-0009', 9, 'active')",
            )
            .await
            .expect("introduce an undeclared active shard");

        let outcome = materialize_and_verify(
            &socket_dir,
            &program,
            FIXTURE_SHARDS,
            true,
            &generation,
            &|| true,
        )
        .await;
        assert!(
            matches!(
                outcome,
                Err(PostgresGenerationError::CatalogNotMaterialized)
            ),
            "the bounded retry did not end the attempt: {outcome:?}"
        );
    }

    /// A freshly materialized catalog plus everything needed to re-verify it.
    async fn materialized_fixture(
        term: u64,
    ) -> (
        std::path::PathBuf,
        DurableWritableGeneration,
        CatalogMaterializationProgram,
    ) {
        use crate::catalog_materialization_program::compile_catalog_materialization_program;
        use crate::postgres_generation::publish_writable_generation;

        let socket_dir = std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required");
        let generation =
            crate::postgres_generation::tests::generation("cluster-1", "holder-a", term);

        create_catalog_database(&socket_dir).await;
        publish_writable_generation(&socket_dir, &generation, &|| true)
            .await
            .expect("publish the generation");
        let program = compile_catalog_materialization_program(
            include_bytes!("../../pgshard-catalog/migrations/0001_shardschema.sql"),
            include_bytes!("../../pgshard-catalog/inventory/0001_shard_inventory.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/genesis.golden.sql"),
            include_bytes!("../../pgshard-catalog/testdata/materialization/preflight.golden.sql"),
        )
        .expect("the real inputs compile");

        materialize(
            &socket_dir,
            &program,
            FIXTURE_SHARDS,
            true,
            &generation,
            &|| true,
        )
        .await
        .expect("materialize the declared state");
        verify_materialized(&socket_dir, &program, FIXTURE_SHARDS, &generation, &|| true)
            .await
            .expect("the declared state verifies");
        (socket_dir, generation, program)
    }

    /// Requires a competing publication to be waiting on the fence the paused
    /// attempt holds, with the durable generation still the one it acts for.
    async fn assert_generation_publication_waits(
        socket_dir: &std::path::Path,
        acting_for: &DurableWritableGeneration,
    ) {
        let observer = crate::postgres_generation::tests::connect_to(
            socket_dir,
            crate::postgres_generation::GENERATION_DATABASE,
        )
        .await
        .expect("observe generation locks");
        let mut waiting = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let blocked: i64 = observer
                .client()
                .query_one(
                    "SELECT pg_catalog.count(*) FROM pg_catalog.pg_locks \
                     WHERE NOT granted \
                       AND relation = 'pgshard_internal.writable_generation'::regclass",
                    &[],
                )
                .await
                .expect("read pg_locks")
                .get(0);
            if blocked > 0 {
                waiting = true;
                break;
            }
        }
        assert!(waiting, "publication did not wait on the attempt's fence");
        let durable: Vec<u8> = observer
            .client()
            .query_one(
                "SELECT generation FROM pgshard_internal.writable_generation",
                &[],
            )
            .await
            .expect("read the durable generation")
            .get(0);
        assert_eq!(
            durable,
            acting_for.canonical_bytes(),
            "a waiting publication changed the durable generation"
        );
    }

    /// Requires the catalog to hold the fixture's full declared state.
    async fn assert_catalog_is_canonical(socket_dir: &std::path::Path) {
        let catalog = crate::postgres_generation::tests::connect_to(
            socket_dir,
            crate::postgres_generation::CATALOG_DATABASE,
        )
        .await
        .expect("inspect the materialized catalog");
        let shards: i64 = catalog
            .client()
            .query_one(
                "SELECT pg_catalog.count(*) FROM pgshard_catalog.shards",
                &[],
            )
            .await
            .expect("count materialized shards")
            .get(0);
        assert_eq!(
            shards,
            i64::from(FIXTURE_SHARDS),
            "the inventory did not materialize every configured shard"
        );
        let databases: i64 = catalog
            .client()
            .query_one(
                "SELECT pg_catalog.count(*) FROM pgshard_catalog.logical_databases",
                &[],
            )
            .await
            .expect("count genesis databases")
            .get(0);
        assert!(databases > 0, "genesis installed no logical database");
    }

    /// Gives the test a catalog database with no prior state.
    ///
    /// These tests share one server, and several of them deliberately leave the
    /// catalog diverged — an undeclared shard, a committed migration-only
    /// prefix. Dropping and recreating removes the ordering coupling that
    /// otherwise makes a later test's premise depend on an earlier test's
    /// leftovers.
    async fn create_catalog_database(socket_dir: &std::path::Path) {
        let admin = crate::postgres_generation::tests::connect_to(
            socket_dir,
            crate::postgres_generation::GENERATION_DATABASE,
        )
        .await
        .expect("connect to reset the catalog database");
        admin
            .client()
            .batch_execute(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                crate::postgres_generation::CATALOG_DATABASE
            ))
            .await
            .expect("drop any prior catalog database");
        // The migration refuses to bootstrap onto a server that already carries
        // pgshard roles, and roles outlive the database that scoped their grants.
        admin
            .client()
            .batch_execute(
                "DO $reset$ \
                 DECLARE role_name text; \
                 BEGIN \
                   FOREACH role_name IN ARRAY ARRAY[ \
                     'pgshard_catalog_admin', 'pgshard_catalog_owner', \
                     'pgshard_catalog_reader', 'pgshard_operation_writer'] LOOP \
                     IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN \
                       EXECUTE pg_catalog.format('DROP OWNED BY %I', role_name); \
                       EXECUTE pg_catalog.format('DROP ROLE %I', role_name); \
                     END IF; \
                   END LOOP; \
                 END $reset$",
            )
            .await
            .expect("drop any prior catalog roles");
        admin
            .client()
            .batch_execute(&format!(
                "CREATE DATABASE {}",
                crate::postgres_generation::CATALOG_DATABASE
            ))
            .await
            .expect("create the catalog database");
    }

    /// The classification the enabling stage has to act on, so it must be
    /// reachable rather than shadowed by the fence's own loss report.
    #[test]
    fn a_fence_lost_after_commit_was_dispatched_is_ambiguous_not_merely_lost() {
        let classify = classify_step;

        // Lost while the commit was in flight: unknown, not failed.
        assert!(matches!(
            classify(
                Err(PostgresGenerationError::GenerationFenceLost),
                Ok(()),
                CommitDispatch::Dispatched,
            ),
            Err(PostgresGenerationError::AmbiguousCatalogCommit)
        ));
        assert!(matches!(
            classify(
                Ok(()),
                Err(PostgresGenerationError::GenerationFenceLost),
                CommitDispatch::Dispatched,
            ),
            Err(PostgresGenerationError::AmbiguousCatalogCommit)
        ));
        // Lost before the commit was ever dispatched: nothing landed.
        assert!(matches!(
            classify(
                Err(PostgresGenerationError::GenerationFenceLost),
                Ok(()),
                CommitDispatch::NotReached,
            ),
            Err(PostgresGenerationError::GenerationFenceLost)
        ));
        // An ordinary failure keeps its own cause.
        assert!(matches!(
            classify(
                Err(PostgresGenerationError::AuthorityChanged),
                Ok(()),
                CommitDispatch::NotReached,
            ),
            Err(PostgresGenerationError::AuthorityChanged)
        ));
        assert!(classify(Ok(()), Ok(()), CommitDispatch::Dispatched).is_ok());
    }

    #[test]
    fn the_two_scalars_are_the_only_parameterized_statements() {
        // Both bind a single parameter and set a fixed, namespaced GUC locally.
        // A value reaching the database any other way would not be covered by
        // the input compiler's purity proof.
        for statement in [SET_EXPECTED_SHARD_COUNT, SET_ALLOW_EMPTY_DATABASE_TOPOLOGY] {
            assert_eq!(statement.matches("$1").count(), 1);
            assert!(statement.contains("set_config"));
            assert!(statement.ends_with(", true)"));
        }
        assert!(SET_EXPECTED_SHARD_COUNT.contains("'pgshard.expected_shard_count'"));
        assert!(
            SET_ALLOW_EMPTY_DATABASE_TOPOLOGY
                .contains("'pgshard.bootstrap_allow_empty_database_topology'")
        );
    }

    #[test]
    fn the_applying_transaction_carries_no_value_and_pins_its_own_settings() {
        assert!(!APPLY_TRANSACTION_SETTINGS.contains('$'));
        assert!(APPLY_TRANSACTION_SETTINGS.contains("ISOLATION LEVEL READ COMMITTED"));
        assert!(APPLY_TRANSACTION_SETTINGS.contains("SET LOCAL search_path = pg_catalog"));
    }
}
