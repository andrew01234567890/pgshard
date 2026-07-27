//! Fail-closed checks over the three fixed `PostgreSQL` login identities.
//!
//! The compatibility bootstrap shell decides three things about each of
//! `pgshard_replication`, `pgshard_pooler_catalog` and
//! `pgshard_orchestrator_catalog` before it publishes a serving policy: that
//! the role has exactly the shape the operator owns, that its role-wide
//! defaults are exactly the canonical set, and that its credential really
//! authenticates. This module is that decision in the agent, over the wire
//! protocol rather than through `psql`.
//!
//! # The credential never becomes text
//!
//! The shell keeps the plaintext out of SQL, argv and the server log by
//! deriving a SCRAM verifier client-side and handing it to `psql`'s `\bind` as
//! a parameter. This module preserves both halves of that property and
//! strengthens the first: the verifier is derived in this process, so no child
//! process is spawned and there is no argv to leak into; and the verifier is
//! sent as a bound parameter of an otherwise constant statement, so the only
//! statement text `PostgreSQL` can log is the constant. Proving a credential
//! sends it through the SCRAM exchange, which is not SQL at all.
//!
//! # A connection that succeeded is not a credential that was checked
//!
//! Under a `trust` or `peer` record `PostgreSQL` admits the identity without
//! ever consulting the password, and the agent's own pre-serving policy uses
//! `peer` for `local postgres postgres`. A proof that concluded from a
//! successful connection alone would therefore conclude from nothing.
//! [`prove_catalog_credential`] settles the question two ways that do not
//! share an assumption: it first requires a connection carrying a deliberately
//! wrong credential to be refused *as a password*, and it then requires the
//! session it does open to name `scram-sha-256` in
//! [`system_user`](https://www.postgresql.org/docs/18/functions-info.html),
//! which is where `PostgreSQL` records the method that authenticated it.
//! `tokio-postgres` reports neither on its own.
//!
//! # The negative control carries no byte of the credential
//!
//! The wrong credential the first half offers is generated from the operating
//! system's entropy and shares no derivation with the real one. That is a
//! security property, not a stylistic one: the method the negative control
//! exists to detect is precisely a method that has not yet been ruled out, and
//! `password`, PAM, LDAP and RADIUS all demand a cleartext `PasswordMessage`
//! before they answer. A control built by extending the real credential would
//! therefore hand the whole secret to the server, in the clear, on the one
//! probe whose purpose is to be refused — and a refusal afterwards does not
//! recall bytes already in the server's memory and possibly its log.
//!
//! # What this module cannot do on its own
//!
//! Proving a credential requires an authentication path, and an authentication
//! path is an HBA record. Before serving activation the postmaster runs under
//! the agent's pre-serving policy, which carries no `scram-sha-256` record and
//! no record for the catalog database at all, so both catalog identities are
//! refused before they are ever asked for a password. The shell solves this by
//! reloading a third, check-time policy onto its private bootstrap postmaster
//! for the duration of the two probes. Until the agent owns an equivalent
//! policy, [`prove_catalog_credential`] reports
//! [`CatalogIdentityError::NoProvenAuthenticationPath`] and refuses. It is
//! deliberately not weakened into a comparison against the stored verifier:
//! that would prove the catalog holds a hash derived from the Secret, which is
//! already established by [`install_login_credential`], and would not prove
//! that the identity can log in.

use std::path::Path;
use std::time::Duration;

use thiserror::Error;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, GenericClient, NoTls, Transaction};

/// The database that owns the catalog logins and their role-wide defaults.
const CATALOG_DATABASE: &str = "shardschema";
/// The database the replication role's shape is read from. It owns no catalog
/// objects, so its state is readable before the catalog database exists.
const GENERATION_DATABASE: &str = "postgres";

/// The exact verifier prefix the pinned server settings must produce.
/// `password_encryption` and `scram_iterations` are both pinned by the
/// configuration, so any other prefix means the client and the server disagree
/// about the credential format.
const SCRAM_VERIFIER_PREFIX: &str = "SCRAM-SHA-256$4096:";

/// Closes every channel that copies a bound parameter into the server log, for
/// the duration of one transaction.
///
/// Observing these would not be enough. All four are reloadable, so a
/// `pg_reload_conf()` between the observation and the statement that carries
/// the verifier re-opens the channel the observation just reported closed. A
/// session assignment outranks the configuration file and cannot be reloaded
/// away, so this is what makes the observation below true for the rest of the
/// transaction rather than true at the moment it was taken.
///
/// The `auto_explain` assignments are issued whether or not the module is
/// loaded: `PostgreSQL` accepts an assignment to an unclaimed `prefix.name`
/// parameter as a placeholder and applies it if the module is loaded later.
const PIN_PARAMETER_CAPTURE: &str = "\
SET LOCAL log_parameter_max_length = 0; \
SET LOCAL log_parameter_max_length_on_error = 0; \
SET LOCAL auto_explain.log_min_duration = -1; \
SET LOCAL auto_explain.log_parameter_max_length = 0";

/// Every setting that decides whether `PostgreSQL` copies a bound parameter
/// into the log beneath the statement that used it.
///
/// The first two are the core log; the last two are `auto_explain`, which
/// writes parameters into a line of its own that the core settings do not
/// reach. `auto_explain`'s parameters do not exist until the module is loaded
/// into the backend, so their absence is read as the closed state and their
/// presence is read as the module being there to be closed.
///
/// Read from `pg_settings` rather than through `current_setting`, which
/// renders a byte-unit parameter with its unit — `1024` reads back as `1kB` —
/// and would make this a comparison against a rendering.
const OBSERVE_PARAMETER_CAPTURE: &str = "\
SELECT COALESCE((SELECT settings.setting FROM pg_catalog.pg_settings AS settings \
                  WHERE settings.name OPERATOR(pg_catalog.=) 'log_parameter_max_length'), '-1'), \
       COALESCE((SELECT settings.setting FROM pg_catalog.pg_settings AS settings \
                  WHERE settings.name \
                        OPERATOR(pg_catalog.=) 'log_parameter_max_length_on_error'), '-1'), \
       COALESCE((SELECT settings.setting FROM pg_catalog.pg_settings AS settings \
                  WHERE settings.name \
                        OPERATOR(pg_catalog.=) 'auto_explain.log_min_duration'), '-1'), \
       COALESCE((SELECT settings.setting FROM pg_catalog.pg_settings AS settings \
                  WHERE settings.name \
                        OPERATOR(pg_catalog.=) 'auto_explain.log_parameter_max_length'), '0')";

/// The method `PostgreSQL` must name as the one that authenticated the proof
/// session. Any other value means the session was admitted without the
/// credential being checked, or checked by something this module does not
/// accept as a proof.
const PROVEN_AUTHENTICATION_METHOD: &str = "scram-sha-256";

/// `PostgreSQL`'s own record of how the session in front of it authenticated.
/// It is `NULL` under `trust`, which is exactly the case a successful
/// connection cannot distinguish on its own.
const OBSERVE_AUTHENTICATED_IDENTITY: &str = "SELECT COALESCE(pg_catalog.system_user(), '')";

/// The application names the two proof connections carry, so an operator
/// reading the server log can tell the deliberate failure apart from a real
/// one.
const PROOF_APPLICATION_NAME: &str = "pgshard-catalog-credential-proof";
const NEGATIVE_CONTROL_APPLICATION_NAME: &str = "pgshard-catalog-credential-negative-control";

/// How long one connection attempt may take, and how long the whole proof may
/// take. The shell bounds the same two probes with `connect_timeout=5` inside
/// a `timeout 10s`; a proof that hangs is a proof that never fails closed.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PROOF_DEADLINE: Duration = Duration::from_secs(10);

/// The negative control's shape, which is the shape the controller generates:
/// 32 random bytes rendered as 64 lowercase hexadecimal characters.
///
/// Matching it is what keeps the two probes indistinguishable to anything that
/// branches on length or alphabet. A control of a different shape could be
/// refused by a length check, a `CHECK` constraint or an authentication module
/// that never looked at the value, and the refusal would be read as proof that
/// the path verifies credentials when nothing verified anything.
const PROBE_ENTROPY_BYTES: usize = 32;
const PROBE_CREDENTIAL_BYTES: usize = PROBE_ENTROPY_BYTES * 2;

/// The alphabet the controller renders a credential in.
const LOWERCASE_HEX: [u8; 16] = *b"0123456789abcdef";

/// One of the three login identities the operator owns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogLogin {
    /// The pooler's read-only catalog identity.
    PoolerCatalog,
    /// The orchestrator's operation-writer identity.
    OrchestratorCatalog,
    /// The per-shard physical replication identity.
    Replication,
}

impl CatalogLogin {
    /// The fixed role name. Never interpolated: it appears only inside the
    /// sealed statements below.
    pub(crate) const fn role(self) -> &'static str {
        match self {
            Self::PoolerCatalog => "pgshard_pooler_catalog",
            Self::OrchestratorCatalog => "pgshard_orchestrator_catalog",
            Self::Replication => "pgshard_replication",
        }
    }

    /// The database whose session the state query must be read on.
    pub(crate) const fn database(self) -> &'static str {
        match self {
            Self::PoolerCatalog | Self::OrchestratorCatalog => CATALOG_DATABASE,
            Self::Replication => GENERATION_DATABASE,
        }
    }

    const fn state_query(self) -> &'static str {
        match self {
            Self::PoolerCatalog => POOLER_CATALOG_STATE,
            Self::OrchestratorCatalog => ORCHESTRATOR_CATALOG_STATE,
            Self::Replication => REPLICATION_STATE,
        }
    }

    const fn install_credential(self) -> &'static str {
        match self {
            Self::PoolerCatalog => INSTALL_POOLER_CATALOG_CREDENTIAL,
            Self::OrchestratorCatalog => INSTALL_ORCHESTRATOR_CATALOG_CREDENTIAL,
            Self::Replication => INSTALL_REPLICATION_CREDENTIAL,
        }
    }

    const fn session_predicate(self) -> Option<&'static str> {
        match self {
            Self::PoolerCatalog => Some(POOLER_CATALOG_SESSION),
            Self::OrchestratorCatalog => Some(ORCHESTRATOR_CATALOG_SESSION),
            // The replication identity is proved by opening a physical
            // replication connection, which is a different protocol shape and
            // a different HBA keyword. It is not a SQL session predicate.
            Self::Replication => None,
        }
    }
}

/// What the catalog says about one login identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoginState {
    /// The role does not exist. It may be created in its staging shape.
    Absent,
    /// The role exists with the exact operator-owned shape and no credential.
    /// Only this state may have a credential installed.
    Staging,
    /// The role exists with the exact operator-owned shape and a credential.
    Safe,
    /// The role exists but is not a shape the operator owns. Never adopted,
    /// never repaired, never deleted.
    Unsafe,
}

impl LoginState {
    fn parse(observed: &str) -> Option<Self> {
        match observed {
            "absent" => Some(Self::Absent),
            "staging" => Some(Self::Staging),
            "safe" => Some(Self::Safe),
            "unsafe" => Some(Self::Unsafe),
            _ => None,
        }
    }
}

/// One role-wide default, as the `ALTER ROLE` assignment that installs it and
/// the `pg_db_role_setting` entry `PostgreSQL` stores for it.
///
/// Both halves are stated once so the sealed statements and the sealed
/// comparison array cannot drift apart silently. The shell keeps four copies
/// of this list and nothing relates them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RoleDefault {
    /// The right-hand side of `ALTER ROLE … IN DATABASE shardschema SET`.
    pub(crate) assignment: &'static str,
    /// The `name=value` entry `PostgreSQL` stores in `setconfig`.
    pub(crate) stored: &'static str,
}

/// The canonical role-wide defaults, in the exact order `PostgreSQL` stores
/// them when the assignments are applied in this order.
///
/// Order is load-bearing: the state query compares `setconfig` with an array
/// literal, and array equality is ordered.
pub(crate) const CANONICAL_ROLE_DEFAULTS: [RoleDefault; 11] = [
    RoleDefault {
        assignment: "search_path = pg_catalog",
        stored: "search_path=pg_catalog",
    },
    RoleDefault {
        assignment: "statement_timeout = '30s'",
        stored: "statement_timeout=30s",
    },
    RoleDefault {
        assignment: "lock_timeout = '5s'",
        stored: "lock_timeout=5s",
    },
    RoleDefault {
        assignment: "transaction_timeout = '120s'",
        stored: "transaction_timeout=120s",
    },
    RoleDefault {
        assignment: "idle_in_transaction_session_timeout = '30s'",
        stored: "idle_in_transaction_session_timeout=30s",
    },
    RoleDefault {
        assignment: "default_transaction_read_only = off",
        stored: "default_transaction_read_only=off",
    },
    RoleDefault {
        assignment: "row_security = off",
        stored: "row_security=off",
    },
    RoleDefault {
        assignment: "synchronous_commit = on",
        stored: "synchronous_commit=on",
    },
    RoleDefault {
        assignment: "zero_damaged_pages = off",
        stored: "zero_damaged_pages=off",
    },
    RoleDefault {
        assignment: "ignore_checksum_failure = off",
        stored: "ignore_checksum_failure=off",
    },
    RoleDefault {
        assignment: "jit = off",
        stored: "jit=off",
    },
];

/// Reads the exact operator-owned state of one login identity.
///
/// # Errors
///
/// Returns an error when the query fails or the catalog answers with anything
/// other than the four states the query can produce.
pub(crate) async fn read_login_state(
    client: &impl GenericClient,
    login: CatalogLogin,
) -> Result<LoginState, CatalogIdentityError> {
    let observed: String = client
        .query_one(login.state_query(), &[])
        .await?
        .try_get(0)?;
    LoginState::parse(&observed)
        .ok_or(CatalogIdentityError::UnexpectedLoginState { role: login.role() })
}

/// Installs one credential onto a role that is in its staging shape.
///
/// The plaintext is turned into a SCRAM verifier in this process and the
/// verifier is bound as a parameter, so neither value is ever part of a
/// statement, an argument vector or an environment variable. The update
/// re-states the whole staging shape in its `WHERE` clause, so a role that
/// changed between the state read and this write is refused rather than
/// silently adopted.
///
/// # Errors
///
/// Returns an error when the session cannot be made to keep bound parameters
/// out of the server log, when the derived verifier is not the pinned format,
/// when the role changed during installation, or when the role that results is
/// not one the operator owns.
pub(crate) async fn install_login_credential(
    client: &mut Client,
    login: CatalogLogin,
    password: &[u8],
) -> Result<(), CatalogIdentityError> {
    let verifier = ScramVerifier::derive(password, login)?;
    // A real transaction rather than a `BEGIN` on a shared handle. A `Client`
    // permits concurrent requests and tracks no transaction state of its own,
    // so a `COMMIT` another task issued on the same handle could land between
    // the update and its post-condition and leave the rollback below with
    // nothing to undo. Taking `&mut Client` is what excludes that, and it is
    // also what lets the post-condition be believed: the update holds the
    // `pg_authid` row lock until this transaction ends, so the row the state
    // query reads back is the row this transaction wrote and no other writer
    // can have replaced it in between. That lock is the whole reason the
    // shell's second `WHERE rolpassword = $1` read-back can be dropped here —
    // without it, `rolpassword LIKE 'SCRAM-SHA-256$4096:%'` would be the only
    // value check, and any verifier at all satisfies that.
    //
    // Framed here rather than left to the caller for the same reason: a
    // refused installation that had already published a login would hand out
    // exactly the credential it just refused to establish.
    let transaction = client.transaction().await?;
    require_no_parameter_capture(&transaction).await?;
    write_credential(&transaction, login, &verifier).await?;
    transaction.commit().await?;
    Ok(())
}

async fn write_credential(
    transaction: &Transaction<'_>,
    login: CatalogLogin,
    verifier: &ScramVerifier,
) -> Result<(), CatalogIdentityError> {
    let verifier = verifier.as_str()?;
    let installed = transaction
        .query(login.install_credential(), &[&verifier])
        .await?;
    if installed.len() != 1 {
        return Err(CatalogIdentityError::RoleChangedDuringInstallation { role: login.role() });
    }
    // Re-read the whole role rather than the one column just written. The
    // shell reads its verifier back through the same bound parameter, which
    // proves byte equality and nothing else, and pays for it by sending the
    // secret a second time. Under the row lock the update is holding, the
    // state query proves strictly more — the stored verifier carries the
    // pinned SCRAM format, the login bit is set, and the rest of the role is
    // still the shape the operator owns — with the secret not on the wire at
    // all.
    if read_login_state(transaction, login).await? != LoginState::Safe {
        return Err(CatalogIdentityError::RoleChangedDuringInstallation { role: login.role() });
    }
    Ok(())
}

/// Closes every channel that would copy the bound verifier into the server
/// log, then proves it is closed.
///
/// Binding the verifier keeps it out of the statement text, and that is the
/// half the statement itself can guarantee. It is not the whole property:
/// `PostgreSQL` writes bound parameters into a `DETAIL` line beneath the
/// statement it logs, and `auto_explain` writes them into a `Query Parameters`
/// line the core settings do not reach. Every one of those settings is
/// reloadable, so an observation says only what was true when it was taken.
/// The assignment is what makes it true for the statement that follows, and
/// the observation is what refuses a session where the assignment did not
/// take.
async fn require_no_parameter_capture(
    transaction: &Transaction<'_>,
) -> Result<(), CatalogIdentityError> {
    transaction
        .batch_execute(PIN_PARAMETER_CAPTURE)
        .await
        .map_err(|source| CatalogIdentityError::ParameterCaptureCannotBeClosed { source })?;
    let observed = transaction
        .query_one(OBSERVE_PARAMETER_CAPTURE, &[])
        .await?;
    let statement: String = observed.try_get(0)?;
    let on_error: String = observed.try_get(1)?;
    if statement != "0" || on_error != "0" {
        return Err(CatalogIdentityError::ParameterLoggingIsEnabled);
    }
    let explain_duration: String = observed.try_get(2)?;
    let explain_parameters: String = observed.try_get(3)?;
    if explain_duration != "-1" || explain_parameters != "0" {
        return Err(CatalogIdentityError::ParameterCaptureModuleIsEnabled);
    }
    Ok(())
}

/// Proves one catalog credential by authenticating with it and observing the
/// exact session a production client will get.
///
/// The connection carries no startup options on purpose. The catalog logins
/// are not superusers and so cannot inherit the agent's own session settings;
/// asking for none is what makes the observed values the role-and-database
/// defaults rather than this call's own arguments.
///
/// # Errors
///
/// Returns [`CatalogIdentityError::NoProvenAuthenticationPath`] when no
/// password-verifying path could be established for the identity — which is
/// the state before serving activation —
/// [`CatalogIdentityError::AuthenticationPathDoesNotVerifyCredentials`] when
/// the path in force admits the identity without checking the credential,
/// [`CatalogIdentityError::CredentialRejected`] when the credential itself is
/// refused, [`CatalogIdentityError::AuthenticationMethodIsNotProven`] when the
/// server does not name the pinned method as the one that authenticated the
/// session, and [`CatalogIdentityError::SessionIsNotCanonical`] when the
/// identity authenticates into a session that is not the canonical one.
pub(crate) async fn prove_catalog_credential(
    socket_dir: &Path,
    login: CatalogLogin,
    password: &[u8],
) -> Result<(), CatalogIdentityError> {
    match tokio::time::timeout(PROOF_DEADLINE, prove(socket_dir, login, password)).await {
        Ok(proved) => proved,
        Err(_) => Err(CatalogIdentityError::ProofTimedOut { role: login.role() }),
    }
}

async fn prove(
    socket_dir: &Path,
    login: CatalogLogin,
    password: &[u8],
) -> Result<(), CatalogIdentityError> {
    let Some(predicate) = login.session_predicate() else {
        return Err(CatalogIdentityError::NotASessionIdentity { role: login.role() });
    };
    let password = std::str::from_utf8(password)
        .map_err(|_| CatalogIdentityError::NonCanonicalCredential { role: login.role() })?;

    require_the_path_verifies_credentials(socket_dir, login, password).await?;

    let (client, driver) = connect_as(
        socket_dir,
        login,
        password.as_bytes(),
        PROOF_APPLICATION_NAME,
    )
    .await
    .map_err(classify_connect)?;
    let observed = async {
        let authenticated: String = client
            .query_one(OBSERVE_AUTHENTICATED_IDENTITY, &[])
            .await?
            .try_get(0)?;
        let canonical: i32 = client.query_one(predicate, &[]).await?.try_get(0)?;
        Ok::<_, CatalogIdentityError>((authenticated, canonical))
    }
    .await;
    drop(client);
    driver.abort();

    let (authenticated, canonical) = observed?;
    if authenticated != format!("{PROVEN_AUTHENTICATION_METHOD}:{}", login.role()) {
        return Err(CatalogIdentityError::AuthenticationMethodIsNotProven { role: login.role() });
    }
    if canonical != 1 {
        return Err(CatalogIdentityError::SessionIsNotCanonical { role: login.role() });
    }
    Ok(())
}

/// Proves the path in force actually verifies credentials, by giving it one it
/// must refuse.
///
/// A `trust` or `peer` record admits the identity without consulting the
/// password at all, so the connection that follows would succeed whatever the
/// credential was and prove nothing about it. `PostgreSQL` reports a refusal
/// as [`SqlState::INVALID_PASSWORD`] only for the methods that actually
/// checked a password, so a refused wrong credential is the evidence that the
/// path is one; an accepted one is the evidence that it is not.
///
/// The credential offered here is generated by
/// [`negative_control_credential`] and carries no byte of the real one. This
/// probe runs against a path that is by definition unproven, and several of
/// the methods it must rule out — `password`, PAM, LDAP, RADIUS — read a
/// cleartext `PasswordMessage` before they decide anything.
async fn require_the_path_verifies_credentials(
    socket_dir: &Path,
    login: CatalogLogin,
    password: &str,
) -> Result<(), CatalogIdentityError> {
    let probe = negative_control_credential(password)?;
    match connect_as(
        socket_dir,
        login,
        probe.expose(),
        NEGATIVE_CONTROL_APPLICATION_NAME,
    )
    .await
    {
        Ok((client, driver)) => {
            drop(client);
            driver.abort();
            Err(
                CatalogIdentityError::AuthenticationPathDoesNotVerifyCredentials {
                    role: login.role(),
                },
            )
        }
        Err(error) => match classify_sql_state(error.code()) {
            ConnectVerdict::CredentialRefused => Ok(()),
            ConnectVerdict::NoProvenPath => Err(CatalogIdentityError::NoProvenAuthenticationPath),
            ConnectVerdict::NotAnAuthenticationDecision => {
                Err(CatalogIdentityError::Database(error))
            }
        },
    }
}

/// The credential the negative control offers, overwritten on drop.
///
/// Bytes rather than a `String` because a `String` cannot be overwritten in
/// place without `unsafe`, and this workspace forbids it. Deliberately not
/// `Debug`, `Display`, `Clone` or serializable, for the same reason
/// [`ScramVerifier`] is not: a value that can be formatted is a value that
/// reaches a log.
struct ProbeCredential(Box<[u8]>);

impl ProbeCredential {
    fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for ProbeCredential {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// A credential generated from nothing but the operating system's entropy.
///
/// Takes no argument on purpose, and that is the whole guarantee: a function
/// that is never given the credential under test cannot return a value derived
/// from it, and the compiler enforces that rather than a reviewer. The bytes
/// come from the same CSPRNG rustls uses for its own key material — ring's
/// `SystemRandom`, which is the `getrandom` syscall — so this is the
/// controller's own generation procedure repeated, not an imitation of it.
fn fresh_probe_credential() -> Result<ProbeCredential, CatalogIdentityError> {
    let mut entropy = [0_u8; PROBE_ENTROPY_BYTES];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut entropy)
        .map_err(|_| CatalogIdentityError::NoNegativeControlCredential)?;
    let mut rendered = Vec::with_capacity(PROBE_CREDENTIAL_BYTES);
    for byte in entropy {
        rendered.push(LOWERCASE_HEX[usize::from(byte >> 4)]);
        rendered.push(LOWERCASE_HEX[usize::from(byte & 0x0f)]);
    }
    entropy.fill(0);
    Ok(ProbeCredential(rendered.into_boxed_slice()))
}

/// A credential that is guaranteed not to be the one under test, and that
/// carries none of it.
///
/// Two properties, and they are not the same property. Being *wrong* is what
/// makes the probe a control at all. Being *underived* is what makes offering
/// it safe: the negative control's whole premise is that the method in force
/// is not yet known, and `password`, PAM, LDAP and RADIUS each demand a
/// cleartext `PasswordMessage` before they refuse anything. Whatever this
/// returns is handed to the server in the clear on those paths, so it must
/// carry no byte the real credential contributed.
///
/// The inequality is checked rather than reasoned about. 256 bits of CSPRNG
/// output collides with one specific value with probability `2^-256`, which is
/// far below the rate at which the machine running this check would instead
/// mis-execute the comparison — but "cannot happen" and "is refused if it
/// happens" differ, and only the second is a guarantee. The comparison is
/// against a value this process already holds and leaks nothing: it decides
/// only whether a value independent of the credential is emitted or an error
/// is.
fn negative_control_credential(password: &str) -> Result<ProbeCredential, CatalogIdentityError> {
    let probe = fresh_probe_credential()?;
    if probe.expose() == password.as_bytes() {
        return Err(CatalogIdentityError::NoNegativeControlCredential);
    }
    Ok(probe)
}

/// Opens one short-lived proof session. The caller owns the driver task and is
/// responsible for ending it.
///
/// The connection carries no startup options on purpose. The catalog logins
/// are not superusers and so cannot inherit the agent's own session settings;
/// asking for none is what makes the observed values the role-and-database
/// defaults rather than this call's own arguments.
///
/// # The copies this module cannot reach
///
/// `tokio_postgres::Config::password` stores `password.as_ref().to_vec()`: an
/// ordinary `Vec<u8>` on a type that derives `Clone` and implements no `Drop`.
/// The driver copies it again to authenticate — into the `PasswordMessage`
/// send buffer under a cleartext record, and into `ScramSha256`'s own
/// `Vec<u8>` under a SCRAM one. Every one of those is released to the
/// allocator without being overwritten, and nothing in this module can reach
/// any of them.
///
/// So the guarantee the wrappers here provide is narrow, and is stated
/// narrowly rather than implied to be more: the buffers *this* module owns are
/// overwritten when they are dropped. The credential is not "fully zeroed" —
/// at least two copies per connection attempt outlive the attempt inside the
/// dependencies.
async fn connect_as(
    socket_dir: &Path,
    login: CatalogLogin,
    password: &[u8],
    application_name: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
    let mut config = Config::new();
    config
        .host_path(socket_dir)
        .port(5432)
        .user(login.role())
        .dbname(login.database())
        .password(password)
        .connect_timeout(CONNECT_TIMEOUT)
        .application_name(application_name);
    let (client, connection) = config.connect(NoTls).await?;
    let driver = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(reason = %error, "catalog credential proof connection ended");
        }
    });
    Ok((client, driver))
}

/// A derived SCRAM verifier, overwritten on drop the way the shell `unset`s
/// its own. Deliberately not `Debug`, `Display`, `Clone` or serializable.
struct ScramVerifier {
    verifier: Box<[u8]>,
    role: &'static str,
}

impl ScramVerifier {
    fn derive(password: &[u8], login: CatalogLogin) -> Result<Self, CatalogIdentityError> {
        let verifier = postgres_protocol::password::scram_sha_256(password);
        if !verifier.starts_with(SCRAM_VERIFIER_PREFIX) {
            return Err(CatalogIdentityError::InvalidVerifier { role: login.role() });
        }
        Ok(Self {
            verifier: verifier.into_bytes().into_boxed_slice(),
            role: login.role(),
        })
    }

    /// The verifier, for use as a bound parameter and nothing else.
    ///
    /// Fallible rather than lossy. The bytes come from an ASCII-only encoder
    /// in this process, so this cannot fail without the dependency being
    /// broken — but the substitute a lossy conversion would reach for is the
    /// empty string, and binding that would install an empty verifier onto a
    /// role the update is about to give a login to.
    fn as_str(&self) -> Result<&str, CatalogIdentityError> {
        std::str::from_utf8(&self.verifier)
            .map_err(|_| CatalogIdentityError::InvalidVerifier { role: self.role })
    }
}

impl Drop for ScramVerifier {
    fn drop(&mut self) {
        self.verifier.fill(0);
    }
}

/// What one connection failure says about whether the check could run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectVerdict {
    /// A method that checks passwords ran and refused the credential.
    CredentialRefused,
    /// No password-verifying path was established.
    NoProvenPath,
    /// The failure was not an authentication decision at all.
    NotAnAuthenticationDecision,
}

/// Separates "no password-verifying path could be established" from "a
/// password-verifying path ran and refused the credential".
///
/// Conflating them is how a check that cannot run at all gets mistaken for a
/// check that ran and failed, and that distinction is the whole reason this
/// stage cannot yet succeed before serving activation.
///
/// `PostgreSQL` reports [`SqlState::INVALID_PASSWORD`] only from
/// `auth_failed` for the `password`, `md5` and `scram-sha-256` methods, so it
/// alone means a password was checked. Everything else authentication can
/// refuse arrives as [`SqlState::INVALID_AUTHORIZATION_SPECIFICATION`] —
/// including a policy with no matching record, an explicit `reject`, a `peer`
/// or `trust` method that failed for its own reasons, a role that does not
/// exist and a role that is not permitted to log in. That class is therefore
/// read as "the proof could not run" and never as a verdict on the credential.
const fn classify_sql_state(code: Option<&SqlState>) -> ConnectVerdict {
    match code {
        Some(&SqlState::INVALID_PASSWORD) => ConnectVerdict::CredentialRefused,
        Some(&SqlState::INVALID_AUTHORIZATION_SPECIFICATION) => ConnectVerdict::NoProvenPath,
        _ => ConnectVerdict::NotAnAuthenticationDecision,
    }
}

fn classify_connect(error: tokio_postgres::Error) -> CatalogIdentityError {
    match classify_sql_state(error.code()) {
        ConnectVerdict::CredentialRefused => CatalogIdentityError::CredentialRejected,
        ConnectVerdict::NoProvenPath => CatalogIdentityError::NoProvenAuthenticationPath,
        ConnectVerdict::NotAnAuthenticationDecision => CatalogIdentityError::Database(error),
    }
}

/// Redacted login-identity failure. No variant carries a credential.
#[derive(Debug, Error)]
pub(crate) enum CatalogIdentityError {
    /// The server reported an error.
    #[error("PostgreSQL login identity check failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    /// The state query answered with something other than its four states.
    #[error("PostgreSQL role {role} reported an unexpected state")]
    UnexpectedLoginState {
        /// Role whose state could not be classified.
        role: &'static str,
    },
    /// The credential was not the canonical generated shape.
    #[error("PostgreSQL role {role} credential is not the canonical generated shape")]
    NonCanonicalCredential {
        /// Role whose credential was rejected.
        role: &'static str,
    },
    /// The session would not let the parameter-capture settings be closed.
    #[error("PostgreSQL session cannot close its bound-parameter log channels: {source}")]
    ParameterCaptureCannotBeClosed {
        /// Underlying failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// The session still logs bound parameters after being told not to.
    #[error(
        "PostgreSQL session logs bound parameters: a credential must not be sent until \
         log_parameter_max_length and log_parameter_max_length_on_error are both zero"
    )]
    ParameterLoggingIsEnabled,
    /// A loaded module captures bound parameters on a channel of its own.
    #[error(
        "PostgreSQL session loads a module that logs bound parameters: a credential must not be \
         sent until auto_explain.log_min_duration is -1 and \
         auto_explain.log_parameter_max_length is zero"
    )]
    ParameterCaptureModuleIsEnabled,
    /// The derived verifier was not the pinned SCRAM format.
    #[error("PostgreSQL role {role} verifier is not the pinned client-generated format")]
    InvalidVerifier {
        /// Role whose verifier was rejected.
        role: &'static str,
    },
    /// The role no longer had its staging shape when the credential landed.
    #[error("PostgreSQL role {role} changed during credential installation")]
    RoleChangedDuringInstallation {
        /// Role that changed.
        role: &'static str,
    },
    /// No password-verifying path could be established, so nothing about the
    /// credential was decided. `PostgreSQL` answers the same way for a policy
    /// with no matching record, an explicit `reject`, a non-password method
    /// that refused, a role that does not exist and a role that is not
    /// permitted to log in, so this names none of them in particular.
    #[error(
        "PostgreSQL established no password-verifying authentication path for the catalog \
         identity, so the credential could not be proved either way"
    )]
    NoProvenAuthenticationPath,
    /// No credential could be generated that is independent of the one under
    /// test and guaranteed to differ from it, so the negative control could
    /// not be run without offering something derived from the real
    /// credential. Refusing is the only remaining option: the alternative is
    /// to send the secret to a path that has not been shown to protect it.
    #[error(
        "PostgreSQL credential proof could not generate an independent negative-control \
         credential, so the authentication path was left unproven"
    )]
    NoNegativeControlCredential,
    /// The path in force admitted a credential it must have refused, so it
    /// does not check credentials and no connection over it proves one.
    #[error(
        "PostgreSQL accepted a deliberately wrong credential for {role}: the authentication path \
         in force does not verify credentials"
    )]
    AuthenticationPathDoesNotVerifyCredentials {
        /// Role whose authentication path was rejected.
        role: &'static str,
    },
    /// The server did not name the pinned method as the one that
    /// authenticated the session.
    #[error("PostgreSQL did not authenticate {role} with the pinned scram-sha-256 method")]
    AuthenticationMethodIsNotProven {
        /// Role whose session was rejected.
        role: &'static str,
    },
    /// The proof did not finish inside its deadline.
    #[error("PostgreSQL credential proof for {role} did not finish inside its deadline")]
    ProofTimedOut {
        /// Role whose proof was abandoned.
        role: &'static str,
    },
    /// An HBA record admitted the identity and the credential was refused.
    #[error("PostgreSQL refused the catalog credential")]
    CredentialRejected,
    /// The identity authenticated into a session that is not the canonical one.
    #[error("PostgreSQL role {role} authenticated into a non-canonical session")]
    SessionIsNotCanonical {
        /// Role whose session was rejected.
        role: &'static str,
    },
    /// The identity is not proved by a SQL session.
    #[error("PostgreSQL role {role} is not proved by a SQL session predicate")]
    NotASessionIdentity {
        /// Role that has no session predicate.
        role: &'static str,
    },
}

const POOLER_CATALOG_STATE: &str = "\
SELECT COALESCE((
  SELECT CASE WHEN
  NOT roles.rolsuper
  AND roles.rolinherit
  AND NOT roles.rolcreaterole
  AND NOT roles.rolcreatedb
  AND NOT roles.rolreplication
  AND NOT roles.rolbypassrls
  AND roles.rolconnlimit = -1
  AND roles.rolvaliduntil IS NULL
  AND (
    NOT EXISTS (
      SELECT
        FROM pg_catalog.pg_db_role_setting AS settings
       WHERE settings.setrole = roles.oid
    )
    OR EXISTS (
      SELECT
        FROM pg_catalog.pg_db_role_setting AS settings
        JOIN pg_catalog.pg_database AS databases
          ON databases.oid = settings.setdatabase
       WHERE settings.setrole = roles.oid
         AND databases.datname = 'shardschema'
         AND settings.setconfig = ARRAY[
               'search_path=pg_catalog',
               'statement_timeout=30s',
               'lock_timeout=5s',
               'transaction_timeout=120s',
               'idle_in_transaction_session_timeout=30s',
               'default_transaction_read_only=off',
               'row_security=off',
               'synchronous_commit=on',
               'zero_damaged_pages=off',
               'ignore_checksum_failure=off',
               'jit=off'
             ]::text[]
         AND NOT EXISTS (
               SELECT
                 FROM pg_catalog.pg_db_role_setting AS other_settings
                WHERE other_settings.setrole = roles.oid
                  AND (other_settings.setdatabase, other_settings.setrole)
                      IS DISTINCT FROM (settings.setdatabase, settings.setrole)
             )
    )
  )
  AND NOT EXISTS (
    SELECT
      FROM pg_catalog.pg_db_role_setting AS database_settings
      JOIN pg_catalog.pg_database AS scoped_database
        ON scoped_database.oid = database_settings.setdatabase
     WHERE database_settings.setrole = 0
       AND scoped_database.datname = 'shardschema'
  )
  AND (
    SELECT pg_catalog.count(*)
      FROM pg_catalog.pg_auth_members AS memberships
     WHERE memberships.member = roles.oid
  ) = 1
  AND EXISTS (
    SELECT
      FROM pg_catalog.pg_auth_members AS memberships
     WHERE memberships.member = roles.oid
       AND memberships.roleid = 'pgshard_catalog_reader'::pg_catalog.regrole
       AND memberships.grantor = (
         SELECT principals.oid
           FROM pg_catalog.pg_roles AS principals
          WHERE principals.rolname = current_user
       )
       AND NOT memberships.admin_option
       AND memberships.inherit_option
       AND NOT memberships.set_option
  )
  AND NOT EXISTS (
    SELECT
      FROM pg_catalog.pg_auth_members AS memberships
     WHERE memberships.roleid = roles.oid
  )
  AND NOT EXISTS (
    SELECT
      FROM pg_catalog.pg_database AS databases
     WHERE databases.datdba = roles.oid
  )
  AND NOT EXISTS (
    SELECT
      FROM pg_catalog.pg_tablespace AS tablespaces
     WHERE tablespaces.spcowner = roles.oid
  )
  THEN CASE
    WHEN roles.rolcanlogin
      AND roles.rolpassword LIKE 'SCRAM-SHA-256$4096:%'
      THEN 'safe'
    WHEN NOT roles.rolcanlogin
      AND roles.rolpassword IS NULL
      THEN 'staging'
    ELSE 'unsafe'
  END
  ELSE 'unsafe'
END
  FROM pg_catalog.pg_authid AS roles
 WHERE roles.rolname = 'pgshard_pooler_catalog'
), 'absent')";

const ORCHESTRATOR_CATALOG_STATE: &str = "\
SELECT COALESCE((
  SELECT CASE WHEN
  NOT roles.rolsuper
  AND roles.rolinherit
  AND NOT roles.rolcreaterole
  AND NOT roles.rolcreatedb
  AND NOT roles.rolreplication
  AND NOT roles.rolbypassrls
  AND roles.rolconnlimit = -1
  AND roles.rolvaliduntil IS NULL
  AND EXISTS (
    SELECT
      FROM pg_catalog.pg_db_role_setting AS settings
      JOIN pg_catalog.pg_database AS databases
        ON databases.oid = settings.setdatabase
     WHERE settings.setrole = roles.oid
       AND databases.datname = 'shardschema'
       AND settings.setconfig = ARRAY[
             'search_path=pg_catalog',
             'statement_timeout=30s',
             'lock_timeout=5s',
             'transaction_timeout=120s',
             'idle_in_transaction_session_timeout=30s',
             'default_transaction_read_only=off',
             'row_security=off',
             'synchronous_commit=on',
             'zero_damaged_pages=off',
             'ignore_checksum_failure=off',
             'jit=off'
           ]::text[]
       AND NOT EXISTS (
             SELECT
               FROM pg_catalog.pg_db_role_setting AS other_settings
              WHERE other_settings.setrole = roles.oid
                AND (other_settings.setdatabase, other_settings.setrole)
                    IS DISTINCT FROM (settings.setdatabase, settings.setrole)
           )
  )
  AND NOT EXISTS (
    SELECT
      FROM pg_catalog.pg_db_role_setting AS database_settings
      JOIN pg_catalog.pg_database AS scoped_database
        ON scoped_database.oid = database_settings.setdatabase
     WHERE database_settings.setrole = 0
       AND scoped_database.datname = 'shardschema'
  )
  AND (
    SELECT pg_catalog.count(*)
      FROM pg_catalog.pg_auth_members AS memberships
     WHERE memberships.member = roles.oid
  ) = 1
  AND EXISTS (
    SELECT
      FROM pg_catalog.pg_auth_members AS memberships
     WHERE memberships.member = roles.oid
       AND memberships.roleid = 'pgshard_operation_writer'::pg_catalog.regrole
       AND memberships.grantor = (
         SELECT principals.oid
           FROM pg_catalog.pg_roles AS principals
          WHERE principals.rolname = current_user
       )
       AND NOT memberships.admin_option
       AND memberships.inherit_option
       AND NOT memberships.set_option
  )
  AND NOT EXISTS (
    SELECT
      FROM pg_catalog.pg_auth_members AS memberships
     WHERE memberships.roleid = roles.oid
  )
  THEN CASE
    WHEN roles.rolcanlogin
      AND roles.rolpassword LIKE 'SCRAM-SHA-256$4096:%'
      THEN 'safe'
    WHEN NOT roles.rolcanlogin
      AND roles.rolpassword IS NULL
      THEN 'staging'
    ELSE 'unsafe'
  END
  ELSE 'unsafe'
END
  FROM pg_catalog.pg_authid AS roles
 WHERE roles.rolname = 'pgshard_orchestrator_catalog'
), 'absent')";

const REPLICATION_STATE: &str = "\
SELECT COALESCE((
  SELECT CASE WHEN
    NOT roles.rolsuper
    AND NOT roles.rolinherit
    AND NOT roles.rolcreaterole
    AND NOT roles.rolcreatedb
    AND NOT roles.rolbypassrls
    AND roles.rolconnlimit = -1
    AND roles.rolvaliduntil IS NULL
    AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_auth_members AS memberships
       WHERE memberships.member = roles.oid OR memberships.roleid = roles.oid
    )
    AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_database AS databases
       WHERE databases.datdba = roles.oid
    )
    AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_tablespace AS tablespaces
       WHERE tablespaces.spcowner = roles.oid
    )
    AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_db_role_setting AS settings
       WHERE settings.setrole = roles.oid
    )
    AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_shdepend AS dependencies
       WHERE dependencies.refclassid = 'pg_catalog.pg_authid'::pg_catalog.regclass
         AND dependencies.refobjid = roles.oid
    )
  THEN CASE
    WHEN roles.rolcanlogin
      AND roles.rolreplication
      AND roles.rolpassword LIKE 'SCRAM-SHA-256$4096:%'
      THEN 'safe'
    WHEN NOT roles.rolcanlogin
      AND NOT roles.rolreplication
      AND roles.rolpassword IS NULL
      THEN 'staging'
    ELSE 'unsafe'
  END
  ELSE 'unsafe'
  END
    FROM pg_catalog.pg_authid AS roles
   WHERE roles.rolname = 'pgshard_replication'
), 'absent')";

const INSTALL_POOLER_CATALOG_CREDENTIAL: &str = "\
UPDATE pg_catalog.pg_authid SET rolpassword = $1, rolcanlogin = true \
 WHERE rolname = 'pgshard_pooler_catalog' AND NOT rolcanlogin AND rolpassword IS NULL \
   AND NOT rolsuper AND rolinherit AND NOT rolcreaterole AND NOT rolcreatedb \
   AND NOT rolreplication AND NOT rolbypassrls AND rolconnlimit = -1 \
   AND rolvaliduntil IS NULL RETURNING 1";

const INSTALL_ORCHESTRATOR_CATALOG_CREDENTIAL: &str = "\
UPDATE pg_catalog.pg_authid SET rolpassword = $1, rolcanlogin = true \
 WHERE rolname = 'pgshard_orchestrator_catalog' AND NOT rolcanlogin AND rolpassword IS NULL \
   AND NOT rolsuper AND rolinherit AND NOT rolcreaterole AND NOT rolcreatedb \
   AND NOT rolreplication AND NOT rolbypassrls AND rolconnlimit = -1 \
   AND rolvaliduntil IS NULL RETURNING 1";

const INSTALL_REPLICATION_CREDENTIAL: &str = "\
UPDATE pg_catalog.pg_authid SET rolpassword = $1, rolcanlogin = true, rolreplication = true \
 WHERE rolname = 'pgshard_replication' AND NOT rolcanlogin AND rolpassword IS NULL \
   AND NOT rolsuper AND NOT rolinherit AND NOT rolcreaterole AND NOT rolcreatedb \
   AND NOT rolreplication AND NOT rolbypassrls AND rolconnlimit = -1 \
   AND rolvaliduntil IS NULL RETURNING 1";

/// The reader's `pg_has_role` conjunct states the intent rather than carrying
/// it alone: revoking that membership also revokes the schema privileges the
/// count below reads through, so such a session is refused by the server
/// before the predicate returns. The operation writer's negative membership
/// check has no such backstop and is load-bearing on its own.
const POOLER_CATALOG_SESSION: &str = "\
SELECT CASE WHEN current_user = 'pgshard_pooler_catalog'
              AND current_setting('search_path') = 'pg_catalog'
              AND current_setting('statement_timeout')::interval = interval '30 seconds'
              AND current_setting('lock_timeout')::interval = interval '5 seconds'
              AND current_setting('transaction_timeout')::interval = interval '120 seconds'
              AND current_setting('idle_in_transaction_session_timeout')::interval \
                    = interval '30 seconds'
              AND current_setting('default_transaction_read_only') = 'off'
              AND current_setting('row_security') = 'off'
              AND current_setting('synchronous_commit') = 'on'
              AND current_setting('jit') = 'off'
              AND pg_catalog.pg_has_role(
                    current_user,
                    'pgshard_catalog_reader',
                    'USAGE'
                  )
              AND (SELECT pg_catalog.count(*) FROM pgshard_catalog.shards) >= 1
            THEN 1 ELSE 0 END";

const ORCHESTRATOR_CATALOG_SESSION: &str = "\
SELECT CASE WHEN current_user = 'pgshard_orchestrator_catalog'
              AND current_setting('search_path') = 'pg_catalog'
              AND current_setting('statement_timeout')::interval = interval '30 seconds'
              AND current_setting('lock_timeout')::interval = interval '5 seconds'
              AND current_setting('transaction_timeout')::interval = interval '120 seconds'
              AND current_setting('idle_in_transaction_session_timeout')::interval \
                    = interval '30 seconds'
              AND current_setting('default_transaction_read_only') = 'off'
              AND current_setting('row_security') = 'off'
              AND current_setting('synchronous_commit') = 'on'
              AND current_setting('zero_damaged_pages') = 'off'
              AND current_setting('ignore_checksum_failure') = 'off'
              AND current_setting('jit') = 'off'
              AND pg_catalog.pg_has_role(
                    current_user,
                    'pgshard_operation_writer',
                    'USAGE'
                  )
              AND NOT pg_catalog.pg_has_role(
                    current_user,
                    'pgshard_catalog_reader',
                    'USAGE'
                  )
              AND pg_catalog.has_function_privilege(
                    current_user,
                    'pgshard_catalog.accept_operation(uuid,uuid,text,text,bigint,text,bytea,\
bigint,bigint,bigint)',
                    'EXECUTE'
                  )
              AND pg_catalog.has_function_privilege(
                    current_user,
                    'pgshard_catalog.get_operation(uuid)',
                    'EXECUTE'
                  )
              AND NOT pg_catalog.has_table_privilege(
                    current_user,
                    'pgshard_catalog.operation_requests',
                    'SELECT'
                  )
              AND NOT pg_catalog.has_table_privilege(
                    current_user,
                    'pgshard_catalog.operation_tombstones',
                    'SELECT'
                  )
              AND (
                    SELECT pg_catalog.count(*)
                      FROM pgshard_catalog.get_operation(
                        '00000000-0000-0000-0000-000000000001'::uuid
                      )
                  ) = 0
            THEN 1 ELSE 0 END";

/// The catalog database's own settings, reset.
///
/// `ALTER DATABASE shardschema SET …` stores a `pg_db_role_setting` row with
/// `setrole = 0`, which no predicate keyed on `setrole = roles.oid` can see —
/// and every ported role-defaults predicate is keyed that way. `PostgreSQL`
/// applies a database-scoped setting at `PGC_SUSET` to every session that
/// opens on the database, so a role whose own defaults are exactly canonical
/// still inherits it, and `ALTER DATABASE shardschema SET zero_damaged_pages
/// = on` would reach every pooler session. Resetting it is half the answer;
/// the other half is the conjunct in each catalog state query that refuses a
/// catalog still carrying one.
pub(crate) const RESET_CATALOG_DATABASE_DEFAULTS: &str = "ALTER DATABASE shardschema RESET ALL;";

/// The sealed role-wide default assignments, in canonical order.
///
/// Applied by a later producer step; stated here because the state query above
/// is only meaningful against the exact assignments that produce it.
pub(crate) const INSTALL_POOLER_CATALOG_DEFAULTS: &str = "\
ALTER ROLE pgshard_pooler_catalog RESET ALL;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema RESET ALL;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET search_path = pg_catalog;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET statement_timeout = '30s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET lock_timeout = '5s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET transaction_timeout = '120s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema \
SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema \
SET default_transaction_read_only = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET row_security = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET synchronous_commit = on;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET zero_damaged_pages = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET ignore_checksum_failure = off;
ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET jit = off;";

/// The same sealed assignments for the operation-writer identity.
pub(crate) const INSTALL_ORCHESTRATOR_CATALOG_DEFAULTS: &str = "\
ALTER ROLE pgshard_orchestrator_catalog RESET ALL;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema RESET ALL;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET search_path = pg_catalog;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET statement_timeout = '30s';
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET lock_timeout = '5s';
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET transaction_timeout = '120s';
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema \
SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema \
SET default_transaction_read_only = off;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET row_security = off;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET synchronous_commit = on;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET zero_damaged_pages = off;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET ignore_checksum_failure = off;
ALTER ROLE pgshard_orchestrator_catalog IN DATABASE shardschema SET jit = off;";

#[cfg(test)]
mod tests {
    use super::*;

    const LOGINS: [CatalogLogin; 3] = [
        CatalogLogin::PoolerCatalog,
        CatalogLogin::OrchestratorCatalog,
        CatalogLogin::Replication,
    ];

    /// The comparison array in each state query has to be exactly the settings
    /// the sealed assignments install, in the order `PostgreSQL` stores them.
    /// Nothing else relates the two, and array equality is ordered.
    #[test]
    fn the_state_queries_compare_against_exactly_the_defaults_that_are_installed() {
        let mut expected = String::new();
        for (index, default) in CANONICAL_ROLE_DEFAULTS.iter().enumerate() {
            if index > 0 {
                expected.push(',');
            }
            expected.push('\'');
            expected.push_str(default.stored);
            expected.push('\'');
        }
        for query in [POOLER_CATALOG_STATE, ORCHESTRATOR_CATALOG_STATE] {
            let compacted: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
            let array = compacted
                .split_once("ARRAY[ ")
                .and_then(|(_, rest)| rest.split_once(" ]::text[]"))
                .map(|(array, _)| array.replace(", ", ","))
                .expect("the state query compares setconfig with an array literal");
            assert_eq!(
                array, expected,
                "a state query compares against settings the sealed assignments do not install"
            );
        }
    }

    /// The sealed assignments have to install every canonical default, in the
    /// canonical order, for both catalog identities.
    #[test]
    fn the_sealed_assignments_install_every_canonical_default_in_order() {
        for (statements, role) in [
            (INSTALL_POOLER_CATALOG_DEFAULTS, "pgshard_pooler_catalog"),
            (
                INSTALL_ORCHESTRATOR_CATALOG_DEFAULTS,
                "pgshard_orchestrator_catalog",
            ),
        ] {
            let compacted: String = statements.split_whitespace().collect::<Vec<_>>().join(" ");
            // The two RESET ALL statements come first: a role whose defaults
            // are being re-established must not keep an old one.
            assert!(compacted.starts_with(&format!(
                "ALTER ROLE {role} RESET ALL; ALTER ROLE {role} IN DATABASE shardschema RESET ALL;"
            )));
            let mut cursor = 0;
            for default in CANONICAL_ROLE_DEFAULTS {
                let expected = format!(
                    "ALTER ROLE {role} IN DATABASE shardschema SET {};",
                    default.assignment
                );
                let found = compacted[cursor..]
                    .find(&expected)
                    .unwrap_or_else(|| panic!("{role} never installs {expected}"));
                cursor += found + expected.len();
            }
            assert_eq!(
                compacted.matches("ALTER ROLE").count(),
                CANONICAL_ROLE_DEFAULTS.len() + 2,
                "{role} installs a default the canonical list does not name"
            );
        }
    }

    /// The one property the whole module exists to preserve: no credential and
    /// no derived verifier is ever part of a statement. Every statement that
    /// carries one carries it as a parameter, and none of them is built at
    /// run time.
    #[test]
    fn every_credential_carrying_statement_binds_it_as_a_parameter() {
        for login in LOGINS {
            let statement = login.install_credential();
            assert_eq!(
                statement.matches("$1").count(),
                1,
                "the credential-carrying statement does not bind exactly one parameter"
            );
            assert!(
                statement.contains("SET rolpassword = $1"),
                "the credential-carrying statement does not assign the bound parameter"
            );
            // The state query reads a role's shape and must never carry one.
            assert!(!login.state_query().contains("$1"));
        }
    }

    /// The update has to re-state the whole staging shape, so a role that
    /// changed between the state read and the write is refused rather than
    /// adopted. Dropping any one of these predicates would let a tampered role
    /// receive the operator's credential.
    #[test]
    fn installing_a_credential_re_states_the_entire_staging_shape() {
        for login in LOGINS {
            let statement = login.install_credential();
            for predicate in [
                "NOT rolcanlogin",
                "rolpassword IS NULL",
                "NOT rolsuper",
                "NOT rolcreaterole",
                "NOT rolcreatedb",
                "NOT rolreplication",
                "NOT rolbypassrls",
                "rolconnlimit = -1",
                "rolvaliduntil IS NULL",
                "RETURNING 1",
            ] {
                assert!(
                    statement.contains(predicate),
                    "{} does not re-state {predicate}",
                    login.role()
                );
            }
            assert!(statement.contains(login.role()));
        }
        // Inheritance is the one flag whose canonical value differs, so it is
        // asserted per identity rather than in the shared list above.
        assert!(INSTALL_POOLER_CATALOG_CREDENTIAL.contains(" AND rolinherit AND"));
        assert!(INSTALL_ORCHESTRATOR_CATALOG_CREDENTIAL.contains(" AND rolinherit AND"));
        assert!(INSTALL_REPLICATION_CREDENTIAL.contains(" AND NOT rolinherit AND"));
    }

    /// Each state query has to answer about exactly one role, and each of the
    /// four states has to be reachable from its text.
    #[test]
    fn each_state_query_classifies_exactly_one_role_into_the_four_states() {
        for login in LOGINS {
            let query = login.state_query();
            assert_eq!(
                query.matches("roles.rolname = ").count(),
                1,
                "{} is classified by more than one role predicate",
                login.role()
            );
            assert!(query.contains(&format!("roles.rolname = '{}'", login.role())));
            for state in ["'safe'", "'staging'", "'unsafe'", "'absent'"] {
                assert!(
                    query.contains(state),
                    "{} cannot report {state}",
                    login.role()
                );
            }
            assert!(query.contains("LIKE 'SCRAM-SHA-256$4096:%'"));
        }
    }

    #[test]
    fn the_four_states_round_trip_and_nothing_else_is_accepted() {
        assert_eq!(LoginState::parse("absent"), Some(LoginState::Absent));
        assert_eq!(LoginState::parse("staging"), Some(LoginState::Staging));
        assert_eq!(LoginState::parse("safe"), Some(LoginState::Safe));
        assert_eq!(LoginState::parse("unsafe"), Some(LoginState::Unsafe));
        for rejected in ["", "SAFE", "safe ", "ok", "absent\n"] {
            assert_eq!(
                LoginState::parse(rejected),
                None,
                "{rejected:?} was accepted"
            );
        }
    }

    /// A verifier is derived, never the password, and the derivation is the
    /// pinned format. The password must not be recoverable from it.
    #[test]
    fn the_derived_verifier_is_the_pinned_format_and_hides_the_password() {
        let password = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let verifier =
            ScramVerifier::derive(password, CatalogLogin::PoolerCatalog).expect("derive verifier");
        let bound = verifier.as_str().expect("a bindable verifier");
        assert!(bound.starts_with(SCRAM_VERIFIER_PREFIX));
        assert!(!bound.contains(std::str::from_utf8(password).expect("ASCII password")));
        // Salted, so two derivations of the same password differ. A constant
        // verifier would mean the salt was not being drawn.
        let again =
            ScramVerifier::derive(password, CatalogLogin::PoolerCatalog).expect("derive verifier");
        assert_ne!(bound, again.as_str().expect("a bindable verifier"));
    }

    /// The session predicate has to observe the defaults, not ask for them.
    /// A predicate carrying its own settings would prove nothing about what a
    /// production client inherits.
    #[test]
    fn the_session_predicates_only_observe_settings() {
        for login in [
            CatalogLogin::PoolerCatalog,
            CatalogLogin::OrchestratorCatalog,
        ] {
            let predicate = login.session_predicate().expect("a session identity");
            assert!(
                !predicate.contains("SET "),
                "{} sets a setting",
                login.role()
            );
            assert!(!predicate.contains("set_config"));
            assert!(predicate.contains(&format!("current_user = '{}'", login.role())));
            for setting in CANONICAL_ROLE_DEFAULTS
                .iter()
                .map(|default| default.stored.split('=').next().unwrap_or_default())
            {
                // zero_damaged_pages and ignore_checksum_failure are observed
                // by the writer only; the reader cannot change them and the
                // shell does not observe them there either.
                if login == CatalogLogin::PoolerCatalog
                    && matches!(setting, "zero_damaged_pages" | "ignore_checksum_failure")
                {
                    continue;
                }
                assert!(
                    predicate.contains(&format!("current_setting('{setting}')")),
                    "{} does not observe {setting}",
                    login.role()
                );
            }
        }
        assert!(CatalogLogin::Replication.session_predicate().is_none());
    }

    #[test]
    fn errors_never_render_a_credential() {
        let error = CatalogIdentityError::NonCanonicalCredential {
            role: CatalogLogin::PoolerCatalog.role(),
        };
        let rendered = format!("{error}: {error:?}");
        assert!(rendered.contains("pgshard_pooler_catalog"));
        assert!(!rendered.contains("SCRAM-SHA-256"));
    }

    /// The one class `PostgreSQL` reserves for a method that actually checked
    /// a password is the only one that may be read as a verdict on the
    /// credential. Reading the other one that way is how "could not run"
    /// becomes "ran and failed", and it is the same class the server returns
    /// for a missing record, an explicit reject, a role that does not exist
    /// and a role that is not permitted to log in.
    #[test]
    fn only_a_password_failure_is_read_as_a_verdict_on_the_credential() {
        assert_eq!(
            classify_sql_state(Some(&SqlState::INVALID_PASSWORD)),
            ConnectVerdict::CredentialRefused
        );
        assert_eq!(
            classify_sql_state(Some(&SqlState::INVALID_AUTHORIZATION_SPECIFICATION)),
            ConnectVerdict::NoProvenPath
        );
        for unrelated in [
            SqlState::CONNECTION_FAILURE,
            SqlState::TOO_MANY_CONNECTIONS,
            SqlState::UNDEFINED_OBJECT,
            SqlState::INSUFFICIENT_PRIVILEGE,
        ] {
            assert_eq!(
                classify_sql_state(Some(&unrelated)),
                ConnectVerdict::NotAnAuthenticationDecision,
                "{unrelated:?} was read as an authentication decision"
            );
        }
        assert_eq!(
            classify_sql_state(None),
            ConnectVerdict::NotAnAuthenticationDecision
        );
    }

    /// A credential shaped exactly the way the controller generates one. The
    /// live fixture's own, so the two cannot drift apart.
    const A_REAL_CREDENTIAL: &str = match std::str::from_utf8(CATALOG_PASSWORD) {
        Ok(credential) => credential,
        Err(_) => panic!("the fixture credential is ASCII"),
    };

    /// Enough draws that a generator which is secretly a function of its input
    /// repeats, and few enough that the test costs nothing. Two independent
    /// 256-bit values colliding across this many draws has probability below
    /// `2^-240`.
    const NEGATIVE_CONTROL_SAMPLES: usize = 64;

    /// The negative control is only a control if the credential it offers
    /// cannot be the one under test, for every input the caller can hand it.
    #[test]
    fn the_negative_control_credential_can_never_be_the_one_under_test() {
        for password in ["", A_REAL_CREDENTIAL, "!", "!!", "not canonical at all"] {
            let probe = negative_control_credential(password)
                .expect("the negative control has an entropy source");
            assert_ne!(
                probe.expose(),
                password.as_bytes(),
                "{password:?} was offered to itself"
            );
        }
    }

    /// The negative control must not be a function of the credential under
    /// test.
    ///
    /// This is the regression guard for a control built by extending the real
    /// credential. Such a control is *wrong*, which is all the old assertion
    /// asked for — and it puts the entire real secret on the wire in the
    /// clear, because the record it is probing may be `password`, PAM, LDAP or
    /// RADIUS, every one of which reads a cleartext `PasswordMessage` before
    /// it refuses the method. Any derivation from the input, whether it
    /// appends, truncates, substitutes, reverses or hashes, is a pure function
    /// of that input and therefore repeats. Fresh entropy does not.
    #[test]
    fn the_negative_control_credential_is_not_a_function_of_the_one_under_test() {
        // The generator the control is built on takes no argument, so it
        // cannot derive anything from the credential even in principle. This
        // coercion is what fails to compile if that ever stops being true.
        let _: fn() -> Result<ProbeCredential, CatalogIdentityError> = fresh_probe_credential;

        let mut offered = std::collections::BTreeSet::new();
        for _ in 0..NEGATIVE_CONTROL_SAMPLES {
            let probe = negative_control_credential(A_REAL_CREDENTIAL)
                .expect("the negative control has an entropy source");
            assert!(
                offered.insert(probe.expose().to_vec()),
                "the negative control repeated itself, so it is computed from the credential \
                 under test rather than from fresh entropy"
            );
        }
    }

    /// The negative control must carry no byte the credential under test
    /// contributed, not merely differ from it somewhere.
    ///
    /// Every byte of the stand-in below is outside the credential alphabet, so
    /// a control that transported, permuted, reversed, truncated or padded any
    /// part of its input would show one of those markers in its output. A
    /// control that merely appended fresh entropy to the real credential
    /// passes the repetition test above and fails this one.
    #[test]
    fn the_negative_control_credential_carries_no_byte_of_the_one_under_test() {
        let marker_credential = "z".repeat(PROBE_CREDENTIAL_BYTES);
        for _ in 0..NEGATIVE_CONTROL_SAMPLES {
            let probe = negative_control_credential(&marker_credential)
                .expect("the negative control has an entropy source");
            assert!(
                !probe.expose().contains(&b'z'),
                "the negative control carries a byte of the credential under test"
            );
        }
    }

    /// The negative control must be the shape a real credential is.
    ///
    /// A control of a different length or alphabet can be refused by something
    /// that never checked a password — a length check, a constraint, an
    /// authentication module that rejects malformed input — and that refusal
    /// would be read as proof that the path verifies credentials.
    #[test]
    fn the_negative_control_credential_is_the_shape_a_real_credential_is() {
        assert_eq!(
            PROBE_CREDENTIAL_BYTES,
            A_REAL_CREDENTIAL.len(),
            "the probe is not the length the controller generates"
        );
        for _ in 0..NEGATIVE_CONTROL_SAMPLES {
            let probe = negative_control_credential(A_REAL_CREDENTIAL)
                .expect("the negative control has an entropy source");
            assert_eq!(
                probe.expose().len(),
                PROBE_CREDENTIAL_BYTES,
                "the probe is not the length the controller generates"
            );
            assert!(
                probe
                    .expose()
                    .iter()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
                "the probe is not the alphabet the controller generates"
            );
        }
    }

    /// Observing the parameter-capture settings says only what was true when
    /// the observation was taken: every one of them is reloadable. The
    /// assignment is what makes the observation hold for the statement that
    /// carries the verifier, and both halves have to name the same settings.
    #[test]
    fn every_parameter_capture_channel_is_both_closed_and_observed() {
        for channel in [
            "log_parameter_max_length",
            "log_parameter_max_length_on_error",
            "auto_explain.log_min_duration",
            "auto_explain.log_parameter_max_length",
        ] {
            assert!(
                PIN_PARAMETER_CAPTURE.contains(&format!("SET LOCAL {channel} =")),
                "{channel} is observed but never closed"
            );
            assert!(
                OBSERVE_PARAMETER_CAPTURE.contains(&format!("'{channel}'")),
                "{channel} is closed but never observed"
            );
        }
        // Transaction-scoped on purpose: the assignment has to cover the
        // statement that carries the verifier and give the caller's session
        // back unchanged afterwards.
        assert_eq!(
            PIN_PARAMETER_CAPTURE.matches("SET LOCAL ").count(),
            4,
            "a parameter-capture channel is closed for longer or shorter than the transaction"
        );
        assert!(!PIN_PARAMETER_CAPTURE.contains("SET log_"));
    }

    /// A database-scoped setting is stored with `setrole = 0`, which no
    /// predicate keyed on the role's own oid can see, and `PostgreSQL` still
    /// applies it to every session on the database. Both catalog identities
    /// have to refuse a catalog carrying one, and the sealed statements have
    /// to be able to clear it.
    #[test]
    fn a_database_scoped_default_is_refused_and_can_be_reset() {
        for query in [POOLER_CATALOG_STATE, ORCHESTRATOR_CATALOG_STATE] {
            let compacted: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
            assert!(
                compacted.contains("WHERE database_settings.setrole = 0"),
                "a state query cannot see a database-scoped default"
            );
            assert!(compacted.contains("AND scoped_database.datname = 'shardschema'"));
        }
        assert!(RESET_CATALOG_DATABASE_DEFAULTS.contains("ALTER DATABASE shardschema RESET ALL"));
        assert!(!RESET_CATALOG_DATABASE_DEFAULTS.contains("ALTER ROLE"));
    }

    // The live tests below need a real server for the same reason the
    // materializer's do: a role shape, a stored verifier, a role-wide default
    // and an authentication decision are all things only `PostgreSQL` can
    // answer, and every one of them is what these checks exist to observe.

    const CATALOG_PASSWORD: &[u8; 64] =
        b"5f6d7e8c9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f809";
    const OPERATION_WRITER_PASSWORD: &[u8; 64] =
        b"a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
    const REPLICATION_PASSWORD: &[u8; 64] =
        b"0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";

    const MIGRATION: &str = include_str!("../../pgshard-catalog/migrations/0001_shardschema.sql");

    const STAGE_POOLER_CATALOG: &str = "\
BEGIN;
CREATE ROLE pgshard_pooler_catalog
  NOLOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION
  NOBYPASSRLS CONNECTION LIMIT -1;
GRANT pgshard_catalog_reader TO pgshard_pooler_catalog
  WITH ADMIN FALSE, INHERIT TRUE, SET FALSE;
COMMIT;";

    const STAGE_ORCHESTRATOR_CATALOG: &str = "\
BEGIN;
CREATE ROLE pgshard_orchestrator_catalog
  NOLOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION
  NOBYPASSRLS CONNECTION LIMIT -1;
GRANT pgshard_operation_writer TO pgshard_orchestrator_catalog
  WITH ADMIN FALSE, INHERIT TRUE, SET FALSE;
COMMIT;";

    const STAGE_REPLICATION: &str = "\
BEGIN;
CREATE ROLE pgshard_replication
  NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION
  NOBYPASSRLS CONNECTION LIMIT -1;
COMMIT;";

    const READ_STORED_DEFAULTS: &str = "\
SELECT settings.setconfig \
  FROM pg_catalog.pg_db_role_setting AS settings \
  JOIN pg_catalog.pg_database AS databases ON databases.oid = settings.setdatabase \
 WHERE settings.setrole = $1::text::regrole AND databases.datname = 'shardschema'";

    fn socket_dir() -> std::path::PathBuf {
        std::env::var_os("PGSHARD_AGENT_TEST_SOCKET_DIR")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SOCKET_DIR is required")
    }

    /// A plain superuser session. Deliberately not the agent's own connection
    /// helper: that one pins `log_statement = none`, which would make the
    /// statement-log test pass without proving anything.
    async fn superuser(dbname: &str) -> Client {
        let mut config = Config::new();
        config
            .host_path(socket_dir())
            .port(5432)
            .user("postgres")
            .dbname(dbname)
            .application_name("pgshard-agent-identity-test");
        let (client, connection) = config.connect(NoTls).await.expect("connect as superuser");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// Gives a test the catalog database and the three login identities in
    /// their absent state. The migration refuses to bootstrap onto a server
    /// that already carries pgshard roles, and roles outlive the database that
    /// scoped their grants.
    async fn reset_catalog() -> Client {
        let admin = superuser("postgres").await;
        admin
            .batch_execute("DROP DATABASE IF EXISTS shardschema WITH (FORCE)")
            .await
            .expect("drop any prior catalog database");
        admin
            .batch_execute(
                "DO $reset$ \
                 DECLARE role_name text; \
                 BEGIN \
                   FOREACH role_name IN ARRAY ARRAY[ \
                     'pgshard_pooler_catalog', 'pgshard_orchestrator_catalog', \
                     'pgshard_replication', 'pgshard_catalog_admin', 'pgshard_catalog_owner', \
                     'pgshard_catalog_reader', 'pgshard_operation_writer', \
                     'pgshard_identity_test_unprivileged'] LOOP \
                     IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN \
                       EXECUTE pg_catalog.format('DROP OWNED BY %I', role_name); \
                       EXECUTE pg_catalog.format('DROP ROLE %I', role_name); \
                     END IF; \
                   END LOOP; \
                 END $reset$",
            )
            .await
            .expect("drop any prior pgshard roles");
        admin
            .batch_execute("CREATE DATABASE shardschema")
            .await
            .expect("create the catalog database");
        let catalog = superuser("shardschema").await;
        catalog
            .batch_execute(MIGRATION)
            .await
            .expect("apply the shardschema migration");
        open_parameter_logging(&catalog).await;
        catalog
    }

    /// Hands every live test a session whose parameter-capture channels are
    /// wide open.
    ///
    /// This is the hostile starting state on purpose. The installation closes
    /// them for its own transaction and proves it, so a fixture that closed
    /// them first would make every live installation pass without that
    /// mechanism existing at all.
    async fn open_parameter_logging(client: &Client) {
        client
            .batch_execute(
                "SET log_parameter_max_length = 1024; \
                 SET log_parameter_max_length_on_error = 1024",
            )
            .await
            .expect("open the parameter log");
    }

    /// The exact operator-owned role shapes, credentials and defaults, in the
    /// order the compatibility shell establishes them.
    async fn stage_and_install(catalog: &mut Client) {
        catalog
            .batch_execute(STAGE_POOLER_CATALOG)
            .await
            .expect("stage the pooler catalog role");
        catalog
            .batch_execute(STAGE_ORCHESTRATOR_CATALOG)
            .await
            .expect("stage the orchestrator catalog role");
        // The operation-writer identity's role-wide defaults are part of the
        // shape its state query requires, so they precede its credential. The
        // reader's are re-established after every login exists, which is where
        // the shell puts them.
        catalog
            .batch_execute(INSTALL_ORCHESTRATOR_CATALOG_DEFAULTS)
            .await
            .expect("install the orchestrator catalog defaults");
        install_login_credential(catalog, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD)
            .await
            .expect("install the pooler catalog credential");
        install_login_credential(
            catalog,
            CatalogLogin::OrchestratorCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await
        .expect("install the orchestrator catalog credential");
        catalog
            .batch_execute(RESET_CATALOG_DATABASE_DEFAULTS)
            .await
            .expect("reset the catalog database defaults");
        catalog
            .batch_execute(INSTALL_POOLER_CATALOG_DEFAULTS)
            .await
            .expect("install the pooler catalog defaults");
    }

    /// The three states a role passes through, and the one it must never be
    /// adopted from. Only a real catalog can answer any of them.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    async fn live_postgres18_classifies_the_operator_owned_role_states() {
        let mut catalog = reset_catalog().await;
        let mut generation = superuser("postgres").await;
        open_parameter_logging(&generation).await;

        for login in [
            CatalogLogin::PoolerCatalog,
            CatalogLogin::OrchestratorCatalog,
        ] {
            assert_eq!(
                read_login_state(&catalog, login).await.expect("read state"),
                LoginState::Absent,
                "{} was not absent on a fresh catalog",
                login.role()
            );
        }
        assert_eq!(
            read_login_state(&generation, CatalogLogin::Replication)
                .await
                .expect("read replication state"),
            LoginState::Absent
        );

        catalog
            .batch_execute(STAGE_POOLER_CATALOG)
            .await
            .expect("stage the pooler catalog role");
        catalog
            .batch_execute(STAGE_ORCHESTRATOR_CATALOG)
            .await
            .expect("stage the orchestrator catalog role");
        generation
            .batch_execute(STAGE_REPLICATION)
            .await
            .expect("stage the replication role");

        // The orchestrator identity carries its canonical defaults from the
        // moment it is created, so its staging shape already includes them.
        catalog
            .batch_execute(INSTALL_ORCHESTRATOR_CATALOG_DEFAULTS)
            .await
            .expect("install the orchestrator catalog defaults");

        for (client, login) in [
            (&catalog, CatalogLogin::PoolerCatalog),
            (&catalog, CatalogLogin::OrchestratorCatalog),
            (&generation, CatalogLogin::Replication),
        ] {
            assert_eq!(
                read_login_state(client, login).await.expect("read state"),
                LoginState::Staging,
                "{} was not staging after being created without a credential",
                login.role()
            );
        }

        install_login_credential(&mut catalog, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD)
            .await
            .expect("install the pooler catalog credential");
        install_login_credential(
            &mut catalog,
            CatalogLogin::OrchestratorCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await
        .expect("install the orchestrator catalog credential");
        install_login_credential(
            &mut generation,
            CatalogLogin::Replication,
            REPLICATION_PASSWORD,
        )
        .await
        .expect("install the replication credential");
        catalog
            .batch_execute(INSTALL_POOLER_CATALOG_DEFAULTS)
            .await
            .expect("install the pooler catalog defaults");

        for (client, login) in [
            (&catalog, CatalogLogin::PoolerCatalog),
            (&catalog, CatalogLogin::OrchestratorCatalog),
            (&generation, CatalogLogin::Replication),
        ] {
            assert_eq!(
                read_login_state(client, login).await.expect("read state"),
                LoginState::Safe,
                "{} was not safe after its credential was installed",
                login.role()
            );
        }

        // A staged role can receive a credential exactly once: the update
        // re-states the staging shape, so replaying it against a role that
        // already has one is refused rather than silently replacing it.
        let replayed =
            install_login_credential(&mut catalog, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD)
                .await;
        assert!(
            matches!(
                replayed,
                Err(CatalogIdentityError::RoleChangedDuringInstallation { .. })
            ),
            "a credential was reinstalled onto a role that already had one: {replayed:?}"
        );
    }

    /// Every divergence the shape check exists for, one at a time: break it,
    /// require unsafe, repair it, require safe again. A check that only ever
    /// answered "safe" would pass the state test above unnoticed.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    async fn live_postgres18_refuses_to_adopt_a_role_the_operator_does_not_own() {
        let mut catalog = reset_catalog().await;
        stage_and_install(&mut catalog).await;
        assert_eq!(
            read_login_state(&catalog, CatalogLogin::PoolerCatalog)
                .await
                .expect("read state"),
            LoginState::Safe
        );

        for (divergence, break_it, repair_it) in [
            (
                "a privilege the operator never grants",
                "ALTER ROLE pgshard_pooler_catalog CREATEDB",
                "ALTER ROLE pgshard_pooler_catalog NOCREATEDB",
            ),
            (
                "a connection limit the operator never sets",
                "ALTER ROLE pgshard_pooler_catalog CONNECTION LIMIT 4",
                "ALTER ROLE pgshard_pooler_catalog CONNECTION LIMIT -1",
            ),
            (
                // `ALTER ROLE … VALID UNTIL` takes only a string constant, so
                // an expiry can be set to 'infinity' but never back to NULL.
                // The check requires NULL, so the repair is catalog DML.
                "an expiry the operator never sets",
                "ALTER ROLE pgshard_pooler_catalog VALID UNTIL '2030-01-01'",
                "UPDATE pg_catalog.pg_authid SET rolvaliduntil = NULL \
                   WHERE rolname = 'pgshard_pooler_catalog'",
            ),
            (
                "an expiry that never arrives is still an expiry",
                "ALTER ROLE pgshard_pooler_catalog VALID UNTIL 'infinity'",
                "UPDATE pg_catalog.pg_authid SET rolvaliduntil = NULL \
                   WHERE rolname = 'pgshard_pooler_catalog'",
            ),
            (
                "a role-wide default the operator never installs",
                "ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET jit = on",
                "ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET jit = off",
            ),
            (
                // Stored with `setrole = 0`, so no predicate keyed on the
                // role's own oid can see it, and applied at PGC_SUSET to
                // every session that opens on the database — a role whose own
                // defaults are exactly canonical still inherits it.
                "a database-scoped default the operator never installs",
                "ALTER DATABASE shardschema SET zero_damaged_pages = on",
                RESET_CATALOG_DATABASE_DEFAULTS,
            ),
            (
                "a membership the operator never grants",
                "GRANT pgshard_operation_writer TO pgshard_pooler_catalog",
                "REVOKE pgshard_operation_writer FROM pgshard_pooler_catalog",
            ),
        ] {
            catalog
                .batch_execute(break_it)
                .await
                .unwrap_or_else(|error| panic!("break {divergence}: {error}"));
            assert_eq!(
                read_login_state(&catalog, CatalogLogin::PoolerCatalog)
                    .await
                    .expect("read state"),
                LoginState::Unsafe,
                "a role carrying {divergence} was not reported unsafe"
            );
            catalog
                .batch_execute(repair_it)
                .await
                .unwrap_or_else(|error| panic!("repair {divergence}: {error}"));
            assert_eq!(
                read_login_state(&catalog, CatalogLogin::PoolerCatalog)
                    .await
                    .expect("read state"),
                LoginState::Safe,
                "a repaired role was not reported safe again after {divergence}"
            );
        }
    }

    /// Installing a credential has to leave a role the operator owns, and the
    /// update's own `WHERE` clause cannot see everything that decides that.
    /// The operation-writer identity carries its role-wide defaults as part of
    /// its shape, and a staging role that never received them is not one the
    /// operator owns however correct its flags are.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    async fn live_postgres18_refuses_an_installation_that_leaves_a_role_it_does_not_own() {
        let mut catalog = reset_catalog().await;
        catalog
            .batch_execute(STAGE_ORCHESTRATOR_CATALOG)
            .await
            .expect("stage the orchestrator catalog role");

        let refused = install_login_credential(
            &mut catalog,
            CatalogLogin::OrchestratorCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await;
        assert!(
            matches!(
                refused,
                Err(CatalogIdentityError::RoleChangedDuringInstallation { .. })
            ),
            "a credential was installed onto a role missing its role-wide defaults: {refused:?}"
        );
        // The role-wide defaults are part of the shape, so this role reads as
        // unsafe either way. What the refusal has to have prevented is the
        // write itself: a refused installation that had already published a
        // login would have handed out the credential it refused to establish.
        let published: bool = catalog
            .query_one(
                "SELECT rolcanlogin OR rolpassword IS NOT NULL FROM pg_catalog.pg_authid \
                  WHERE rolname = 'pgshard_orchestrator_catalog'",
                &[],
            )
            .await
            .expect("observe the refused role")
            .try_get(0)
            .expect("a boolean");
        assert!(!published, "a refused installation left a login behind");

        // The same role, once it carries them, accepts the same credential.
        // The `WHERE` clause still matches, so only the post-condition can
        // have made the difference above.
        catalog
            .batch_execute(INSTALL_ORCHESTRATOR_CATALOG_DEFAULTS)
            .await
            .expect("install the orchestrator catalog defaults");
        install_login_credential(
            &mut catalog,
            CatalogLogin::OrchestratorCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await
        .expect("install the orchestrator catalog credential");
    }

    /// `PostgreSQL` stores role-wide defaults in the order they were applied,
    /// and the state query compares them with an ordered array literal. Only
    /// the server can settle what that order actually is.
    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL 18 Unix socket"]
    async fn live_postgres18_role_defaults_store_exactly_the_array_the_state_query_compares() {
        let mut catalog = reset_catalog().await;
        stage_and_install(&mut catalog).await;

        let expected: Vec<String> = CANONICAL_ROLE_DEFAULTS
            .iter()
            .map(|default| default.stored.to_owned())
            .collect();
        for login in [
            CatalogLogin::PoolerCatalog,
            CatalogLogin::OrchestratorCatalog,
        ] {
            let stored: Vec<String> = catalog
                .query_one(READ_STORED_DEFAULTS, &[&login.role()])
                .await
                .expect("read the stored role-wide defaults")
                .try_get(0)
                .expect("setconfig is a text array");
            assert_eq!(
                stored,
                expected,
                "{} stores defaults the state query does not compare against",
                login.role()
            );
        }
    }

    /// The property the whole credential path exists to preserve, and the
    /// reason binding alone is not it.
    ///
    /// With `log_statement = all` the server logs the statement text, and
    /// binding the verifier is what keeps the verifier out of that text. It
    /// does not keep it out of the `DETAIL` line the server writes beneath it:
    /// that is decided by `log_parameter_max_length`, which the fixture leaves
    /// deliberately wide open. The installation closes it for its own
    /// transaction and hands the session back exactly as it found it, so this
    /// test is run on a session that would otherwise publish the verifier
    /// verbatim — and it proves that session really would, by binding a canary
    /// through it and finding the canary in the log.
    ///
    /// An implementation that stopped closing the channel is caught by the
    /// verifier appearing; one that stopped binding is caught by the verifier
    /// appearing in the statement text; one that closed the channel for the
    /// whole session rather than the transaction is caught by the canary
    /// afterwards.
    ///
    /// Requires `PGSHARD_AGENT_TEST_SERVER_LOG` to name the server's log file
    /// and the server to run with `log_statement = all`; the test proves the
    /// log was really recording before it concludes anything from an absence.
    #[tokio::test]
    #[ignore = "requires a PostgreSQL 18 server logging every statement to a readable file"]
    async fn live_postgres18_a_credential_never_reaches_the_statement_log() {
        const CANARY: &str = "pgshard-bound-parameter-canary";
        let log_path = std::env::var_os("PGSHARD_AGENT_TEST_SERVER_LOG")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_SERVER_LOG is required");
        let mut catalog = reset_catalog().await;
        catalog
            .batch_execute(STAGE_POOLER_CATALOG)
            .await
            .expect("stage the pooler catalog role");
        let read_log = |from: u64| {
            let log = std::fs::read_to_string(&log_path).expect("read the server log");
            log.get(usize::try_from(from).unwrap_or(0)..)
                .expect("the server log only grows")
                .to_owned()
        };
        let log_end = || {
            std::fs::metadata(&log_path)
                .expect("stat the server log")
                .len()
        };

        // The channel is open on this session, and this is what open looks
        // like. Without this the absence below would be the absence of a
        // channel rather than the closing of one.
        let before = log_end();
        catalog
            .query_one("SELECT $1::pg_catalog.text", &[&CANARY])
            .await
            .expect("bind a canary");
        assert!(
            read_log(before).contains(CANARY),
            "the fixture session does not log bound parameters, so this test proves nothing"
        );

        let before = log_end();
        install_login_credential(&mut catalog, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD)
            .await
            .expect("install the pooler catalog credential");
        let written = read_log(before);

        // The exact verifier the server now stores. Searching the log for the
        // format's prefix would find the state query's own `LIKE` pattern and
        // report a leak that is not one, so the search is for this value.
        let stored: String = catalog
            .query_one(
                "SELECT rolpassword FROM pg_catalog.pg_authid \
                  WHERE rolname = 'pgshard_pooler_catalog'",
                &[],
            )
            .await
            .expect("read the stored verifier")
            .try_get(0)
            .expect("a stored verifier");
        assert!(stored.starts_with(SCRAM_VERIFIER_PREFIX));

        // An absence proves nothing unless the log was really recording. The
        // constant statement text has to be there.
        assert!(
            written.contains("UPDATE pg_catalog.pg_authid SET rolpassword = $1"),
            "the server did not log the installing statement, so this test proves nothing"
        );
        assert!(
            !written.contains(&stored),
            "the derived SCRAM verifier reached the statement log"
        );
        assert!(
            !written.contains(std::str::from_utf8(CATALOG_PASSWORD).expect("ASCII credential")),
            "a credential reached the statement log"
        );

        // Closed for the transaction, not for the session. A caller's session
        // is handed back exactly as it was found.
        let before = log_end();
        catalog
            .query_one("SELECT $1::pg_catalog.text", &[&CANARY])
            .await
            .expect("bind a canary");
        assert!(
            read_log(before).contains(CANARY),
            "the installation left the caller's session permanently altered"
        );

        // A session that cannot close the channel is never given a verifier.
        // `SET ROLE` is enough to lose the privilege: the settings are
        // `PGC_SUSET`, and the check is against the current role.
        catalog
            .batch_execute(
                "CREATE ROLE pgshard_identity_test_unprivileged NOLOGIN; \
                 SET ROLE pgshard_identity_test_unprivileged",
            )
            .await
            .expect("drop to an unprivileged role");
        let refused = install_login_credential(
            &mut catalog,
            CatalogLogin::OrchestratorCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await;
        catalog.batch_execute("RESET ROLE").await.expect("reset");
        assert!(
            matches!(
                refused,
                Err(CatalogIdentityError::ParameterCaptureCannotBeClosed { .. })
            ),
            "a credential was sent on a session that cannot close its log channels: {refused:?}"
        );
    }

    /// Writes one policy into the server's HBA file and reloads it.
    async fn install_policy(admin: &Client, policy: &str) {
        let hba_file = std::env::var_os("PGSHARD_AGENT_TEST_HBA_FILE")
            .map(std::path::PathBuf::from)
            .expect("PGSHARD_AGENT_TEST_HBA_FILE is required");
        std::fs::write(&hba_file, policy).expect("write the policy under test");
        admin
            .batch_execute("SELECT pg_catalog.pg_reload_conf()")
            .await
            .expect("reload the policy under test");
    }

    /// Puts one HBA method in front of the pooler identity and nothing else,
    /// and reports what the proof makes of it. The publisher session keeps
    /// `trust` so a policy under test can never lock the fixture out.
    async fn prove_under_record(
        admin: &Client,
        socket_dir: &Path,
        method: &str,
    ) -> Result<(), CatalogIdentityError> {
        install_policy(
            admin,
            &format!(
                "local all postgres trust\n\
                 local shardschema pgshard_pooler_catalog {method}\n\
                 local all all reject\n"
            ),
        )
        .await;
        prove_catalog_credential(socket_dir, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD).await
    }

    /// The policy the compatibility bootstrap shell reloads onto its private
    /// postmaster for exactly the two credential probes, and nothing else. It
    /// is neither the pre-serving policy nor the serving policy.
    async fn install_check_time_policy(admin: &Client) {
        install_policy(
            admin,
            "local all postgres trust\n\
             local shardschema pgshard_pooler_catalog scram-sha-256\n\
             local shardschema pgshard_orchestrator_catalog scram-sha-256\n\
             local all all reject\n\
             host all all 0.0.0.0/0 reject\n\
             host all all ::0/0 reject\n",
        )
        .await;
    }

    /// Proving a credential needs an HBA record that admits the identity, and
    /// the agent's pre-serving policy carries none. This is the whole reason
    /// the check cannot yet succeed before serving activation, so it is stated
    /// as an executable fact rather than a comment.
    ///
    /// Requires `PGSHARD_AGENT_TEST_HBA_FILE` to name a reloadable HBA file the
    /// running server was started with. The test leaves its own policy in
    /// force, so the server must be a disposable one.
    #[tokio::test]
    #[ignore = "requires a PostgreSQL 18 server with a writable, reloadable HBA file"]
    async fn live_postgres18_a_credential_proof_needs_an_hba_record_that_admits_it() {
        let socket_dir = socket_dir();
        let mut catalog = reset_catalog().await;
        stage_and_install(&mut catalog).await;
        let admin = superuser("postgres").await;

        // The agent's own pre-serving policy, verbatim. It admits the local
        // publisher session and nothing else.
        install_policy(
            &admin,
            "local postgres postgres peer\nlocal all all reject\nlocal replication all reject\n",
        )
        .await;
        for login in [
            CatalogLogin::PoolerCatalog,
            CatalogLogin::OrchestratorCatalog,
        ] {
            let refused = prove_catalog_credential(&socket_dir, login, CATALOG_PASSWORD).await;
            assert!(
                matches!(
                    refused,
                    Err(CatalogIdentityError::NoProvenAuthenticationPath)
                ),
                "{} was not refused for want of a record under the pre-serving policy: {refused:?}",
                login.role()
            );
        }

        // `trust` admits the identity without ever consulting the credential,
        // so a proof that concluded from the connection alone would call this
        // a success — and it has to be named as a path that does not verify
        // rather than reported as an absent one. `peer` is the same keyword
        // the pre-serving policy uses for the publisher session, so neither
        // shape is hypothetical.
        let peered = prove_under_record(&admin, &socket_dir, "peer").await;
        assert!(
            matches!(
                peered,
                Err(CatalogIdentityError::NoProvenAuthenticationPath
                    | CatalogIdentityError::AuthenticationPathDoesNotVerifyCredentials { .. })
            ),
            "a peer record was accepted as a proof of the credential: {peered:?}"
        );
        let trusted = prove_under_record(&admin, &socket_dir, "trust").await;
        assert!(
            matches!(
                trusted,
                Err(CatalogIdentityError::AuthenticationPathDoesNotVerifyCredentials { .. })
            ),
            "a trust record was not reported as a path that does not verify credentials: \
             {trusted:?}"
        );

        // `password` does verify the credential, so the negative control alone
        // is satisfied by it — and it verifies by having the client send the
        // credential in the clear. Only the method the server itself names can
        // tell that apart from the pinned one.
        let cleartext = prove_under_record(&admin, &socket_dir, "password").await;
        assert!(
            matches!(
                cleartext,
                Err(CatalogIdentityError::AuthenticationMethodIsNotProven { .. })
            ),
            "a cleartext password record was accepted as the pinned method: {cleartext:?}"
        );

        install_check_time_policy(&admin).await;
        prove_catalog_credential(&socket_dir, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD)
            .await
            .expect("the pooler credential authenticates into its canonical session");
        prove_catalog_credential(
            &socket_dir,
            CatalogLogin::OrchestratorCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await
        .expect("the operation-writer credential authenticates into its canonical session");

        // The same admitted path with the wrong credential has to be refused
        // as a credential, not as a missing record: it is that distinction the
        // classification exists to make.
        let wrong = prove_catalog_credential(
            &socket_dir,
            CatalogLogin::PoolerCatalog,
            OPERATION_WRITER_PASSWORD,
        )
        .await;
        assert!(
            matches!(wrong, Err(CatalogIdentityError::CredentialRejected)),
            "a wrong credential on an admitted path reported {wrong:?}"
        );
    }

    /// A session that authenticates is not yet a proof. The predicate has to
    /// observe the privilege boundary in both directions — the group role each
    /// identity must hold, and the one it must not — and the role-wide
    /// defaults a production client will inherit.
    ///
    /// Requires `PGSHARD_AGENT_TEST_HBA_FILE` to name a reloadable HBA file the
    /// running server was started with; the check-time policy is what makes the
    /// identities reachable at all.
    #[tokio::test]
    #[ignore = "requires a PostgreSQL 18 server with a writable, reloadable HBA file"]
    async fn live_postgres18_authenticating_is_not_the_same_as_a_canonical_session() {
        let socket_dir = socket_dir();
        let mut catalog = reset_catalog().await;
        stage_and_install(&mut catalog).await;
        install_check_time_policy(&superuser("postgres").await).await;
        prove_catalog_credential(&socket_dir, CatalogLogin::PoolerCatalog, CATALOG_PASSWORD)
            .await
            .expect("the fixture starts from a canonical session");

        // Every case is a refusal; the flag marks the ones whose refusal must
        // be the predicate's own verdict rather than a server privilege error.
        for (divergence, diverge, login, password, verdict_is_the_predicate) in [
            (
                "a role-wide default a production session would inherit",
                "ALTER ROLE pgshard_pooler_catalog IN DATABASE shardschema SET jit = on",
                CatalogLogin::PoolerCatalog,
                CATALOG_PASSWORD,
                true,
            ),
            (
                "the reader membership the operation writer must not hold",
                "GRANT pgshard_catalog_reader TO pgshard_orchestrator_catalog",
                CatalogLogin::OrchestratorCatalog,
                OPERATION_WRITER_PASSWORD,
                true,
            ),
            // Last: revoking this also takes away the schema privileges the
            // predicate reads through, so every later case would be refused
            // for that reason instead of its own.
            (
                "the reader membership the pooler must hold",
                "REVOKE pgshard_catalog_reader FROM pgshard_pooler_catalog",
                CatalogLogin::PoolerCatalog,
                CATALOG_PASSWORD,
                false,
            ),
        ] {
            catalog
                .batch_execute(diverge)
                .await
                .unwrap_or_else(|error| panic!("diverge {divergence}: {error}"));
            let diverged = prove_catalog_credential(&socket_dir, login, password).await;
            assert!(
                diverged.is_err(),
                "a session missing {divergence} was accepted as a proof"
            );
            assert!(
                !verdict_is_the_predicate
                    || matches!(
                        diverged,
                        Err(CatalogIdentityError::SessionIsNotCanonical { .. })
                    ),
                "a session missing {divergence} was refused for the wrong reason: {diverged:?}"
            );
        }
    }
}
