//! Applies a verified materialization program to the catalog database.
//!
//! The program is already proven to be pure SQL by the input compiler; this
//! module owns *when* it may run. Every mutation is applied on a writable
//! session bound to the accepted request, under a generation fence held on the
//! database where the writable generation lives, and with an exact authority
//! observation immediately before each commit.
//!
//! Nothing here is driven yet: the materializer is constructed and unit-tested,
//! but no caller applies a program.
#![allow(
    dead_code,
    reason = "dormant materializer; the enabling stage drives it"
)]

use std::path::Path;

use crate::catalog_materialization_program::CatalogMaterializationProgram;
use crate::postgres_generation::{CatalogWriterSession, GenerationFence, PostgresGenerationError};
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
    let outcome = tokio::select! {
        outcome = apply(writer, authority_exact, step) => outcome,
        () = fence.lost() => Err(PostgresGenerationError::GenerationFenceLost),
    };
    // Reported even when the work failed: a lost fence means the guarantee was
    // absent, which the caller must not confuse with a clean failure.
    let released = fence.release().await;
    outcome.and(released)
}

async fn apply<F>(
    writer: &mut CatalogWriterSession,
    authority_exact: &F,
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
            for fragment in fragments {
                transaction.batch_execute(fragment).await?;
            }
            // No await or state-changing operation may be inserted between this
            // exact authority observation and dispatching COMMIT.
            if !authority_exact() {
                return Err(PostgresGenerationError::AuthorityChanged);
            }
            transaction.commit().await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
