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
//! Nothing here is driven yet: the materializer is constructed and unit-tested,
//! but no caller applies a program.
#![allow(
    dead_code,
    reason = "dormant materializer; the enabling stage drives it"
)]

use std::path::Path;

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

    async fn create_catalog_database(socket_dir: &std::path::Path) {
        let admin = crate::postgres_generation::tests::connect_to(
            socket_dir,
            crate::postgres_generation::GENERATION_DATABASE,
        )
        .await
        .expect("connect to create the catalog database");
        let _ = admin
            .client()
            .batch_execute("CREATE DATABASE shardschema")
            .await;
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
