//! Bounded `PostgreSQL` client handshake boundary.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use pgshard_pgwire::{
    BACKEND_STARTUP_ERROR_MESSAGE_LENGTH, Decode, ErrorResponseSeverity, MAX_STARTUP_FRAME_LENGTH,
    StartupFrame, decode_startup, encode_error_response,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::server::{AcceptBackoff, accept_bounded, connection_task_result, drain_connections};
use crate::{config::BackendTarget, state::PoolerState};

const MAX_FRONTEND_CONNECTIONS: usize = 1_024;
const ACCEPT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const ACCEPT_MAX_RETRY_DELAY: Duration = Duration::from_secs(1);
const ACCEPT_MAX_FAILURE_DURATION: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const SESSION_READINESS_INTERVAL: Duration = Duration::from_millis(100);
const UNAVAILABLE_SQLSTATE: [u8; 5] = *b"57P03";
const UNAVAILABLE_MESSAGE: &str = "pgshard data plane is not available";
const BACKEND_FAILURE_SQLSTATE: [u8; 5] = *b"08006";
const BACKEND_FAILURE_MESSAGE: &str = "pgshard could not reach the shard-zero writer";
const CATALOG_DATABASE_SQLSTATE: [u8; 5] = *b"3D000";
const CATALOG_DATABASE_MESSAGE: &str =
    "shardschema is not available through the application pooler";
const REPLICATION_SQLSTATE: [u8; 5] = *b"0A000";
const REPLICATION_MESSAGE: &str =
    "replication connections are not available through the application pooler";
const ERROR_BUFFER_LENGTH: usize = 128;

struct FrontendServerPolicy {
    maximum_connections: usize,
    accept_initial_retry_delay: Duration,
    accept_max_retry_delay: Duration,
    accept_max_failure_duration: Duration,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    session_readiness_interval: Duration,
    #[cfg(test)]
    accepted_connections: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

const DEFAULT_FRONTEND_SERVER_POLICY: FrontendServerPolicy = FrontendServerPolicy {
    maximum_connections: MAX_FRONTEND_CONNECTIONS,
    accept_initial_retry_delay: ACCEPT_INITIAL_RETRY_DELAY,
    accept_max_retry_delay: ACCEPT_MAX_RETRY_DELAY,
    accept_max_failure_duration: ACCEPT_MAX_FAILURE_DURATION,
    startup_timeout: STARTUP_TIMEOUT,
    shutdown_timeout: SHUTDOWN_TIMEOUT,
    session_readiness_interval: SESSION_READINESS_INTERVAL,
    #[cfg(test)]
    accepted_connections: None,
};

/// Runs the bounded read-write `PostgreSQL` listener until shutdown.
///
/// This boundary understands startup framing and refuses SSL/GSS negotiation.
/// Without an explicit backend it rejects regular sessions. With the bounded
/// compatibility backend it relays one client socket to shard zero while
/// preserving `PostgreSQL` authentication and cancellation end to end. It is not
/// a connection pool, SQL router, or client-facing TLS implementation.
///
/// # Errors
///
/// Returns an I/O error if a connection task panics, the listener becomes
/// permanently unusable, or continuous non-connection accept failures exhaust
/// the bounded outage budget.
pub(crate) async fn serve_listener(
    listener: TcpListener,
    state: PoolerState,
    backend: Option<BackendTarget>,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    serve_listener_with_policy(
        listener,
        state,
        backend,
        shutdown,
        DEFAULT_FRONTEND_SERVER_POLICY,
    )
    .await
}

async fn serve_listener_with_policy(
    mut listener: TcpListener,
    state: PoolerState,
    backend: Option<BackendTarget>,
    shutdown: impl Future<Output = ()> + Send,
    policy: FrontendServerPolicy,
) -> io::Result<()> {
    let permits = Arc::new(Semaphore::new(policy.maximum_connections));
    let mut accept_backoff = AcceptBackoff::new(
        policy.accept_initial_retry_delay,
        policy.accept_max_retry_delay,
        policy.accept_max_failure_duration,
    );
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            () = shutdown.as_mut() => break,
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(result) = completed {
                    connection_task_result(result)?;
                }
            }
            accepted = accept_bounded(
                &mut listener,
                Arc::clone(&permits),
                &mut accept_backoff,
                "PostgreSQL",
            ) => {
                let (stream, permit) = accepted?;
                #[cfg(test)]
                if let Some(accepted) = &policy.accepted_connections {
                    accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                connections.spawn(serve_connection(
                    stream,
                    permit,
                    state.clone(),
                    backend.clone(),
                    policy.startup_timeout,
                    policy.session_readiness_interval,
                ));
            }
        }
    }

    drain_connections(&mut connections, policy.shutdown_timeout).await
}

async fn serve_connection(
    mut stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    state: PoolerState,
    backend: Option<BackendTarget>,
    startup_timeout: Duration,
    session_readiness_interval: Duration,
) {
    let mut input = [0_u8; MAX_STARTUP_FRAME_LENGTH];
    let action = tokio::time::timeout(startup_timeout, read_startup(&mut stream, &mut input)).await;
    let result = match action {
        Ok(Ok(StartupAction::Regular {
            length,
            startup_policy,
        })) => {
            relay_regular_startup(
                &mut stream,
                &input[..length],
                startup_policy,
                &state,
                backend.as_ref(),
                session_readiness_interval,
            )
            .await
        }
        Ok(Ok(StartupAction::Cancel { length })) => {
            forward_cancel(&input[..length], backend.as_ref()).await;
            Ok(())
        }
        Ok(Ok(StartupAction::Closed)) => Ok(()),
        Ok(Err(error)) => {
            tracing::debug!(%error, "PostgreSQL handshake closed with an I/O error");
            Ok(())
        }
        Err(_) => {
            tracing::debug!("PostgreSQL handshake exceeded its startup deadline");
            return;
        }
    };
    if let Err(error) = result {
        tracing::debug!(%error, "PostgreSQL compatibility session closed with an I/O error");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupAction {
    Regular {
        length: usize,
        startup_policy: StartupPolicy,
    },
    Cancel {
        length: usize,
    },
    Closed,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StartupPolicy {
    catalog_database: bool,
    replication: bool,
}

async fn read_startup(
    stream: &mut TcpStream,
    input: &mut [u8; MAX_STARTUP_FRAME_LENGTH],
) -> io::Result<StartupAction> {
    let mut filled = 0;
    let mut ssl_refused = false;
    let mut gss_refused = false;

    loop {
        match decode_startup(&input[..filled]) {
            Ok(Decode::Incomplete { .. }) => {
                if filled == input.len() {
                    return Ok(StartupAction::Closed);
                }
                let read = stream.read(&mut input[filled..]).await?;
                if read == 0 {
                    return Ok(StartupAction::Closed);
                }
                filled += read;
            }
            Ok(Decode::Complete { frame, consumed }) => match frame {
                StartupFrame::SslRequest => {
                    if ssl_refused || consumed != filled {
                        return Ok(StartupAction::Closed);
                    }
                    ssl_refused = true;
                    stream.write_all(b"N").await?;
                    filled = 0;
                }
                StartupFrame::GssEncryptionRequest => {
                    if gss_refused || consumed != filled {
                        return Ok(StartupAction::Closed);
                    }
                    gss_refused = true;
                    stream.write_all(b"N").await?;
                    filled = 0;
                }
                StartupFrame::CancelRequest { .. } => {
                    return Ok(if consumed == filled {
                        StartupAction::Cancel { length: consumed }
                    } else {
                        StartupAction::Closed
                    });
                }
                StartupFrame::Startup { parameters, .. } => {
                    if consumed != filled {
                        return Ok(StartupAction::Closed);
                    }
                    return Ok(StartupAction::Regular {
                        length: consumed,
                        startup_policy: startup_policy(parameters),
                    });
                }
            },
            Err(error) => {
                tracing::debug!(%error, "rejected malformed PostgreSQL startup frame");
                return Ok(StartupAction::Closed);
            }
        }
    }
}

fn startup_policy(parameters: pgshard_pgwire::StartupParameters<'_>) -> StartupPolicy {
    let mut explicit_database = false;
    let mut catalog_user = false;
    let mut policy = StartupPolicy::default();
    for parameter in parameters.iter() {
        let Ok((name, value)) = parameter else {
            return StartupPolicy {
                catalog_database: true,
                replication: true,
            };
        };
        if name == b"database" {
            explicit_database = true;
            if value == b"shardschema" {
                policy.catalog_database = true;
            }
        } else if name == b"user" && value == b"shardschema" {
            catalog_user = true;
        } else if name == b"replication" && !replication_disabled(value) {
            policy.replication = true;
        }
    }
    policy.catalog_database |= !explicit_database && catalog_user;
    policy
}

// PostgreSQL parse_bool false spellings; every other value stays gated (fail closed).
fn replication_disabled(value: &[u8]) -> bool {
    const FALSE_SPELLINGS: [&[u8]; 6] = [b"false", b"off", b"no", b"0", b"f", b"n"];
    FALSE_SPELLINGS
        .iter()
        .any(|spelling| value.eq_ignore_ascii_case(spelling))
}

async fn relay_regular_startup(
    frontend: &mut TcpStream,
    startup: &[u8],
    startup_policy: StartupPolicy,
    state: &PoolerState,
    target: Option<&BackendTarget>,
    readiness_interval: Duration,
) -> io::Result<()> {
    if startup_policy.catalog_database {
        return send_fatal(
            frontend,
            CATALOG_DATABASE_SQLSTATE,
            CATALOG_DATABASE_MESSAGE,
        )
        .await;
    }
    if startup_policy.replication {
        return send_fatal(frontend, REPLICATION_SQLSTATE, REPLICATION_MESSAGE).await;
    }
    let Some(target) = target else {
        return send_unavailable(frontend).await;
    };
    if !state.readiness().ready {
        return send_unavailable(frontend).await;
    }

    let backend = tokio::time::timeout(target.connect_timeout(), target.connect()).await;
    let Ok(Ok(mut backend)) = backend else {
        tracing::debug!("shard-zero backend connection attempt failed");
        return send_fatal(frontend, BACKEND_FAILURE_SQLSTATE, BACKEND_FAILURE_MESSAGE).await;
    };
    if !state.readiness().ready {
        return send_unavailable(frontend).await;
    }
    frontend.set_nodelay(true)?;
    backend.set_nodelay(true)?;
    if !matches!(
        tokio::time::timeout(target.connect_timeout(), backend.write_all(startup)).await,
        Ok(Ok(()))
    ) {
        tracing::debug!("shard-zero backend startup write failed");
        return send_fatal(frontend, BACKEND_FAILURE_SQLSTATE, BACKEND_FAILURE_MESSAGE).await;
    }

    tokio::select! {
        result = tokio::io::copy_bidirectional(frontend, &mut backend) => {
            result.map(|_| ())
        }
        () = wait_until_unready(state, readiness_interval) => {
            tracing::debug!("closing compatibility session after pooler readiness loss");
            Ok(())
        }
    }
}

async fn wait_until_unready(state: &PoolerState, interval: Duration) {
    loop {
        if !state.readiness().ready {
            return;
        }
        tokio::time::sleep(interval).await;
    }
}

async fn forward_cancel(startup: &[u8], target: Option<&BackendTarget>) {
    let Some(target) = target else {
        return;
    };
    let Ok(Ok(mut backend)) =
        tokio::time::timeout(target.connect_timeout(), target.connect()).await
    else {
        return;
    };
    let _ = tokio::time::timeout(target.connect_timeout(), backend.write_all(startup)).await;
}

async fn send_unavailable(stream: &mut TcpStream) -> io::Result<()> {
    send_fatal(stream, UNAVAILABLE_SQLSTATE, UNAVAILABLE_MESSAGE).await
}

async fn send_fatal(stream: &mut TcpStream, sqlstate: [u8; 5], message: &str) -> io::Result<()> {
    let mut response = [0_u8; ERROR_BUFFER_LENGTH];
    let response_length = encode_error_response(
        ErrorResponseSeverity::Fatal,
        sqlstate,
        message,
        BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
        &mut response,
    )
    .map_err(io::Error::other)?;
    stream.write_all(&response[..response_length]).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_pgwire::{
        BackendTag, ProtocolVersion, decode_backend, encode_gss_encryption_request,
        encode_ssl_request, encode_startup,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    async fn server(
        policy: FrontendServerPolicy,
        state: PoolerState,
        backend: Option<BackendTarget>,
    ) -> (
        std::net::SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind PostgreSQL test listener");
        let address = listener.local_addr().expect("PostgreSQL test address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let task = tokio::spawn(serve_listener_with_policy(
            listener,
            state,
            backend,
            async move {
                let _ = shutdown_receiver.await;
            },
            policy,
        ));
        (address, shutdown_sender, task)
    }

    fn catalog_snapshot(ready: bool) -> crate::state::PoolerCatalogSnapshot {
        crate::state::PoolerCatalogSnapshot {
            phase: if ready { "connected" } else { "backoff" },
            connection_up: ready,
            ready,
            readiness_reason: if ready { "ready" } else { "connection" },
            catalog_epoch: ready.then_some(1),
            cache_age: ready.then_some(Duration::ZERO),
            consecutive_failures: u64::from(!ready),
            total_failures: u64::from(!ready),
            connect_attempts: 1,
            successful_connections: u64::from(ready),
            last_failure: (!ready).then_some("connection"),
        }
    }

    fn ready_state() -> PoolerState {
        PoolerState::from_catalog(catalog_snapshot(true), true)
    }

    fn backend_target(address: std::net::SocketAddr) -> BackendTarget {
        BackendTarget::new(
            &address.ip().to_string(),
            address.port(),
            Duration::from_secs(1),
        )
    }

    fn test_policy() -> FrontendServerPolicy {
        FrontendServerPolicy {
            startup_timeout: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(100),
            ..DEFAULT_FRONTEND_SERVER_POLICY
        }
    }

    fn startup_packet(parameters: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut packet = [0_u8; 128];
        let length = encode_startup(ProtocolVersion::new(3, 2), parameters, &mut packet)
            .expect("test startup packet");
        packet[..length].to_vec()
    }

    fn policy_for(parameters: &[(&[u8], &[u8])]) -> StartupPolicy {
        let packet = startup_packet(parameters);
        let Ok(Decode::Complete {
            frame: StartupFrame::Startup { parameters, .. },
            ..
        }) = decode_startup(&packet)
        else {
            panic!("test startup packet did not decode");
        };
        startup_policy(parameters)
    }

    async fn regular_startup(stream: &mut TcpStream) {
        let packet = startup_packet(&[(b"user", b"postgres")]);
        stream
            .write_all(&packet)
            .await
            .expect("send startup packet");
    }

    async fn read_startup_packet(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; 4];
        stream
            .read_exact(&mut header)
            .await
            .expect("read startup length");
        let length = usize::try_from(u32::from_be_bytes(header)).expect("startup length usize");
        assert!((4..=MAX_STARTUP_FRAME_LENGTH).contains(&length));
        let mut packet = vec![0_u8; length];
        packet[..4].copy_from_slice(&header);
        stream
            .read_exact(&mut packet[4..])
            .await
            .expect("read startup body");
        packet
    }

    async fn read_fatal(stream: &mut TcpStream, sqlstate: &[u8; 6]) -> Vec<u8> {
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
            .await
            .expect("fatal response deadline")
            .expect("read fatal response");
        let Decode::Complete { frame, consumed } =
            decode_backend(&response, BACKEND_STARTUP_ERROR_MESSAGE_LENGTH)
                .expect("decode fatal response")
        else {
            panic!("fatal response was incomplete");
        };
        assert_eq!(consumed, response.len());
        assert_eq!(frame.tag(), BackendTag::ErrorResponse);
        assert!(
            response
                .windows(sqlstate.len())
                .any(|bytes| bytes == sqlstate)
        );
        assert!(response.windows(6).any(|bytes| bytes == b"VFATAL"));
        response
    }

    #[test]
    fn replication_parameter_blocks_only_replication_sessions() {
        for value in [
            b"false".as_slice(),
            b"off",
            b"no",
            b"0",
            b"f",
            b"n",
            b"FALSE",
            b"Off",
            b"No",
            b"F",
            b"N",
        ] {
            assert!(
                !policy_for(&[(b"user", b"postgres"), (b"replication", value)]).replication,
                "blocked non-replication startup for {value:?}"
            );
        }
        for value in [
            b"database".as_slice(),
            b"DATABASE",
            b"true",
            b"on",
            b"yes",
            b"1",
            b"t",
            b"y",
            b"TRUE",
            b"On",
            b"Yes",
            b"T",
            b"Y",
        ] {
            assert!(
                policy_for(&[(b"user", b"postgres"), (b"replication", value)]).replication,
                "allowed replication startup for {value:?}"
            );
        }
        for value in [b"".as_slice(), b"2", b"maybe", b"tru", b"fals", b"of"] {
            assert!(
                policy_for(&[(b"user", b"postgres"), (b"replication", value)]).replication,
                "allowed unrecognized replication value {value:?}"
            );
        }
    }

    #[tokio::test]
    async fn refuses_encryption_then_rejects_regular_startup() {
        let (address, shutdown, task) = server(test_policy(), ready_state(), None).await;
        let mut stream = TcpStream::connect(address)
            .await
            .expect("connect to PostgreSQL boundary");

        stream
            .write_all(&encode_gss_encryption_request())
            .await
            .expect("send GSS request");
        let mut refusal = [0];
        tokio::time::timeout(Duration::from_secs(1), stream.read_exact(&mut refusal))
            .await
            .expect("GSS refusal deadline")
            .expect("read GSS refusal");
        assert_eq!(refusal, *b"N");

        stream
            .write_all(&encode_ssl_request())
            .await
            .expect("send SSL request");
        tokio::time::timeout(Duration::from_secs(1), stream.read_exact(&mut refusal))
            .await
            .expect("SSL refusal deadline")
            .expect("read SSL refusal");
        assert_eq!(refusal, *b"N");

        regular_startup(&mut stream).await;
        read_fatal(&mut stream, b"C57P03").await;

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn cancel_and_incomplete_startup_close_without_a_response() {
        let (address, shutdown, task) = server(test_policy(), ready_state(), None).await;

        let mut cancel = TcpStream::connect(address)
            .await
            .expect("connect cancellation socket");
        cancel
            .write_all(&[0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 7, 1, 2, 3, 4])
            .await
            .expect("send cancellation request");
        let mut response = [0];
        let cancel_read = tokio::time::timeout(Duration::from_secs(1), cancel.read(&mut response))
            .await
            .expect("cancel close deadline")
            .expect("read cancel close");
        assert_eq!(cancel_read, 0);

        let mut incomplete = TcpStream::connect(address)
            .await
            .expect("connect incomplete startup socket");
        incomplete
            .write_all(&[0, 0, 0])
            .await
            .expect("send incomplete startup");
        let incomplete_read =
            tokio::time::timeout(Duration::from_secs(1), incomplete.read(&mut response))
                .await
                .expect("incomplete startup close deadline")
                .expect("read timeout close");
        assert_eq!(incomplete_read, 0);

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn shutdown_aborts_a_held_startup_after_bounded_drain() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let policy = FrontendServerPolicy {
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_millis(25),
            accepted_connections: Some(Arc::clone(&accepted)),
            ..DEFAULT_FRONTEND_SERVER_POLICY
        };
        let (address, shutdown, task) = server(policy, ready_state(), None).await;
        let mut stream = TcpStream::connect(address)
            .await
            .expect("connect partial startup socket");
        stream
            .write_all(&[0, 0, 0])
            .await
            .expect("send partial startup");
        tokio::time::timeout(Duration::from_secs(1), async {
            while accepted.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server accepts partial startup");

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("bounded drain");
        let mut response = [0];
        assert_eq!(
            stream
                .read(&mut response)
                .await
                .expect("read drained close"),
            0
        );
    }

    #[tokio::test]
    async fn shutdown_aborts_a_stalled_async_backend_resolution() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let policy = FrontendServerPolicy {
            shutdown_timeout: Duration::from_millis(25),
            accepted_connections: Some(Arc::clone(&accepted)),
            ..test_policy()
        };
        let (address, shutdown, task) = server(
            policy,
            ready_state(),
            Some(BackendTarget::pending_for_test(Duration::from_secs(30))),
        )
        .await;
        let mut stream = TcpStream::connect(address)
            .await
            .expect("connect stalled-resolution client");
        regular_startup(&mut stream).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while accepted.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server accepts stalled-resolution client");

        shutdown.send(()).expect("server retains shutdown receiver");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("stalled async resolution cannot block shutdown")
            .expect("server task")
            .expect("bounded drain");
        let mut response = [0];
        assert_eq!(
            stream
                .read(&mut response)
                .await
                .expect("read stalled-resolution close"),
            0
        );
    }

    #[tokio::test]
    async fn relays_startup_and_session_bytes_to_the_configured_backend() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake backend");
        let backend_address = backend_listener.local_addr().expect("fake backend address");
        let backend_task = tokio::spawn(async move {
            let (mut backend, _) = backend_listener.accept().await.expect("accept relay");
            let startup = read_startup_packet(&mut backend).await;
            backend
                .write_all(b"ready")
                .await
                .expect("write ready marker");
            let mut query = [0_u8; 5];
            backend
                .read_exact(&mut query)
                .await
                .expect("read query bytes");
            assert_eq!(&query, b"query");
            backend
                .write_all(b"result")
                .await
                .expect("write result bytes");
            startup
        });
        let (address, shutdown, task) = server(
            test_policy(),
            ready_state(),
            Some(backend_target(backend_address)),
        )
        .await;

        let startup = startup_packet(&[(b"user", b"postgres"), (b"database", b"postgres")]);
        let mut client = TcpStream::connect(address)
            .await
            .expect("connect relay client");
        client.write_all(&startup).await.expect("send startup");
        let mut ready = [0_u8; 5];
        client
            .read_exact(&mut ready)
            .await
            .expect("read ready marker");
        assert_eq!(&ready, b"ready");
        client.write_all(b"query").await.expect("send query bytes");
        let mut result = [0_u8; 6];
        client
            .read_exact(&mut result)
            .await
            .expect("read result bytes");
        assert_eq!(&result, b"result");
        assert_eq!(backend_task.await.expect("fake backend task"), startup);

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn relays_a_replication_false_startup_as_a_normal_session() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake backend");
        let backend_address = backend_listener.local_addr().expect("fake backend address");
        let backend_task = tokio::spawn(async move {
            let (mut backend, _) = backend_listener.accept().await.expect("accept relay");
            let startup = read_startup_packet(&mut backend).await;
            backend
                .write_all(b"ready")
                .await
                .expect("write ready marker");
            startup
        });
        let (address, shutdown, task) = server(
            test_policy(),
            ready_state(),
            Some(backend_target(backend_address)),
        )
        .await;

        let startup = startup_packet(&[(b"user", b"postgres"), (b"replication", b"off")]);
        let mut client = TcpStream::connect(address)
            .await
            .expect("connect replication-false client");
        client.write_all(&startup).await.expect("send startup");
        let mut ready = [0_u8; 5];
        client
            .read_exact(&mut ready)
            .await
            .expect("read ready marker");
        assert_eq!(&ready, b"ready");
        assert_eq!(backend_task.await.expect("fake backend task"), startup);

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn forwards_cancellation_to_the_same_configured_target() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind cancellation backend");
        let backend_address = backend_listener
            .local_addr()
            .expect("cancellation backend address");
        let backend_task = tokio::spawn(async move {
            let (mut backend, _) = backend_listener.accept().await.expect("accept cancel");
            let mut request = Vec::new();
            backend
                .read_to_end(&mut request)
                .await
                .expect("read cancellation request");
            request
        });
        let (address, shutdown, task) = server(
            test_policy(),
            ready_state(),
            Some(backend_target(backend_address)),
        )
        .await;
        let request = [0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 7, 1, 2, 3, 4];
        let mut client = TcpStream::connect(address)
            .await
            .expect("connect cancellation client");
        client
            .write_all(&request)
            .await
            .expect("send cancellation request");
        let mut response = [0_u8; 1];
        assert_eq!(
            client.read(&mut response).await.expect("read cancel close"),
            0
        );
        assert_eq!(
            backend_task.await.expect("cancellation backend task"),
            request
        );

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn blocks_catalog_and_replication_startups_without_contacting_backend() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind forbidden-startup backend");
        let backend_address = backend_listener
            .local_addr()
            .expect("forbidden-startup backend address");
        let (address, shutdown, task) = server(
            test_policy(),
            ready_state(),
            Some(backend_target(backend_address)),
        )
        .await;

        for (parameters, sqlstate) in [
            (
                vec![
                    (b"user".as_slice(), b"postgres".as_slice()),
                    (b"database".as_slice(), b"shardschema".as_slice()),
                ],
                b"C3D000".as_slice(),
            ),
            (
                vec![(b"user".as_slice(), b"shardschema".as_slice())],
                b"C3D000".as_slice(),
            ),
            (
                vec![
                    (b"user".as_slice(), b"postgres".as_slice()),
                    (b"replication".as_slice(), b"database".as_slice()),
                ],
                b"C0A000".as_slice(),
            ),
            (
                vec![
                    (b"user".as_slice(), b"postgres".as_slice()),
                    (b"replication".as_slice(), b"on".as_slice()),
                ],
                b"C0A000".as_slice(),
            ),
            (
                vec![
                    (b"user".as_slice(), b"postgres".as_slice()),
                    (b"replication".as_slice(), b"maybe".as_slice()),
                ],
                b"C0A000".as_slice(),
            ),
        ] {
            let mut client = TcpStream::connect(address)
                .await
                .expect("connect forbidden-startup client");
            client
                .write_all(&startup_packet(&parameters))
                .await
                .expect("send forbidden startup");
            read_fatal(
                &mut client,
                sqlstate.try_into().expect("six-byte SQLSTATE field"),
            )
            .await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(25), backend_listener.accept())
                .await
                .is_err(),
            "forbidden startup contacted the backend"
        );

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn rejects_new_sessions_when_the_catalog_is_unready() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unready backend");
        let backend_address = backend_listener
            .local_addr()
            .expect("unready backend address");
        let (address, shutdown, task) = server(
            test_policy(),
            PoolerState::from_catalog(catalog_snapshot(false), true),
            Some(backend_target(backend_address)),
        )
        .await;
        let mut client = TcpStream::connect(address)
            .await
            .expect("connect unready client");
        regular_startup(&mut client).await;
        read_fatal(&mut client, b"C57P03").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), backend_listener.accept())
                .await
                .is_err(),
            "unready catalog contacted the backend"
        );

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn closes_an_existing_session_after_catalog_readiness_loss() {
        let backend_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind readiness backend");
        let backend_address = backend_listener
            .local_addr()
            .expect("readiness backend address");
        let backend_task = tokio::spawn(async move {
            let (mut backend, _) = backend_listener.accept().await.expect("accept relay");
            let _ = read_startup_packet(&mut backend).await;
            backend
                .write_all(b"ready")
                .await
                .expect("write ready marker");
            let mut remaining = Vec::new();
            backend
                .read_to_end(&mut remaining)
                .await
                .expect("read relay close");
        });
        let ready = Arc::new(AtomicBool::new(true));
        let ready_source = Arc::clone(&ready);
        let state = PoolerState::from_catalog_source(
            move || catalog_snapshot(ready_source.load(Ordering::SeqCst)),
            true,
        );
        let policy = FrontendServerPolicy {
            session_readiness_interval: Duration::from_millis(5),
            ..test_policy()
        };
        let (address, shutdown, task) =
            server(policy, state, Some(backend_target(backend_address))).await;
        let mut client = TcpStream::connect(address)
            .await
            .expect("connect relay client");
        regular_startup(&mut client).await;
        let mut marker = [0_u8; 5];
        client
            .read_exact(&mut marker)
            .await
            .expect("read ready marker");
        assert_eq!(&marker, b"ready");

        ready.store(false, Ordering::SeqCst);
        let mut response = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut response))
            .await
            .expect("readiness-loss close deadline")
            .expect("read readiness-loss close");
        assert_eq!(read, 0);
        backend_task.await.expect("readiness backend task");

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    #[ignore = "requires a live PostgreSQL 18 server in PGSHARD_POOLER_TEST_ADDRESS"]
    async fn relays_a_live_postgres18_session() {
        let backend_address: std::net::SocketAddr = std::env::var("PGSHARD_POOLER_TEST_ADDRESS")
            .expect("PGSHARD_POOLER_TEST_ADDRESS")
            .parse()
            .expect("PostgreSQL test socket address");
        let (address, shutdown, task) = server(
            test_policy(),
            ready_state(),
            Some(backend_target(backend_address)),
        )
        .await;
        let mut config = tokio_postgres::Config::new();
        config
            .host(address.ip().to_string())
            .port(address.port())
            .user("postgres")
            .dbname("postgres");
        let (client, connection) = config
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect through compatibility relay");
        let connection_task = tokio::spawn(connection);
        let version: i32 = client
            .query_one(
                "SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4",
                &[],
            )
            .await
            .expect("query through compatibility relay")
            .get(0);
        assert!(version >= 180_000, "PostgreSQL {version} is older than 18");
        drop(client);
        connection_task
            .await
            .expect("PostgreSQL connection task")
            .expect("clean PostgreSQL connection");

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }
}
