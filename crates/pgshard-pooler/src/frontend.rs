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

const MAX_FRONTEND_CONNECTIONS: usize = 1_024;
const ACCEPT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const ACCEPT_MAX_RETRY_DELAY: Duration = Duration::from_secs(1);
const ACCEPT_MAX_FAILURE_DURATION: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const UNAVAILABLE_SQLSTATE: [u8; 5] = *b"57P03";
const UNAVAILABLE_MESSAGE: &str = "pgshard data plane is not available";
const ERROR_BUFFER_LENGTH: usize = 128;

struct FrontendServerPolicy {
    maximum_connections: usize,
    accept_initial_retry_delay: Duration,
    accept_max_retry_delay: Duration,
    accept_max_failure_duration: Duration,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
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
    #[cfg(test)]
    accepted_connections: None,
};

/// Runs the bounded read-write `PostgreSQL` listener until shutdown.
///
/// This boundary understands startup framing and refuses SSL/GSS negotiation,
/// but deliberately rejects regular sessions until authentication, backend
/// pooling, and query execution exist. Cancellation requests are closed without
/// a response because `PostgreSQL` cancellation is a one-way protocol.
///
/// # Errors
///
/// Returns an I/O error if a connection task panics, the listener becomes
/// permanently unusable, or continuous non-connection accept failures exhaust
/// the bounded outage budget.
pub(crate) async fn serve_listener(
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    serve_listener_with_policy(listener, shutdown, DEFAULT_FRONTEND_SERVER_POLICY).await
}

async fn serve_listener_with_policy(
    mut listener: TcpListener,
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
                connections.spawn(serve_connection(stream, permit, policy.startup_timeout));
            }
        }
    }

    drain_connections(&mut connections, policy.shutdown_timeout).await
}

async fn serve_connection(
    mut stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    startup_timeout: Duration,
) {
    match tokio::time::timeout(startup_timeout, reject_startup(&mut stream)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!(%error, "PostgreSQL handshake closed with an I/O error"),
        Err(_) => tracing::debug!("PostgreSQL handshake exceeded its startup deadline"),
    }
}

async fn reject_startup(stream: &mut TcpStream) -> io::Result<()> {
    let mut input = [0_u8; MAX_STARTUP_FRAME_LENGTH];
    let mut filled = 0;
    let mut ssl_refused = false;
    let mut gss_refused = false;

    loop {
        match decode_startup(&input[..filled]) {
            Ok(Decode::Incomplete { .. }) => {
                if filled == input.len() {
                    return Ok(());
                }
                let read = stream.read(&mut input[filled..]).await?;
                if read == 0 {
                    return Ok(());
                }
                filled += read;
            }
            Ok(Decode::Complete { frame, consumed }) => match frame {
                StartupFrame::SslRequest => {
                    if ssl_refused || consumed != filled {
                        return Ok(());
                    }
                    ssl_refused = true;
                    stream.write_all(b"N").await?;
                    filled = 0;
                }
                StartupFrame::GssEncryptionRequest => {
                    if gss_refused {
                        return Ok(());
                    }
                    gss_refused = true;
                    stream.write_all(b"N").await?;
                    filled = 0;
                }
                StartupFrame::CancelRequest { .. } => return Ok(()),
                StartupFrame::Startup { .. } => {
                    let mut response = [0_u8; ERROR_BUFFER_LENGTH];
                    let response_length = encode_error_response(
                        ErrorResponseSeverity::Fatal,
                        UNAVAILABLE_SQLSTATE,
                        UNAVAILABLE_MESSAGE,
                        BACKEND_STARTUP_ERROR_MESSAGE_LENGTH,
                        &mut response,
                    )
                    .map_err(io::Error::other)?;
                    stream.write_all(&response[..response_length]).await?;
                    return Ok(());
                }
            },
            Err(error) => {
                tracing::debug!(%error, "rejected malformed PostgreSQL startup frame");
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgshard_pgwire::{
        BackendTag, ProtocolVersion, decode_backend, encode_gss_encryption_request,
        encode_ssl_request, encode_startup,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    async fn server(
        policy: FrontendServerPolicy,
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
            async move {
                let _ = shutdown_receiver.await;
            },
            policy,
        ));
        (address, shutdown_sender, task)
    }

    fn test_policy() -> FrontendServerPolicy {
        FrontendServerPolicy {
            startup_timeout: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(100),
            ..DEFAULT_FRONTEND_SERVER_POLICY
        }
    }

    async fn regular_startup(stream: &mut TcpStream) {
        let mut packet = [0_u8; 128];
        let length = encode_startup(
            ProtocolVersion::new(3, 2),
            &[(b"user".as_slice(), b"postgres".as_slice())],
            &mut packet,
        )
        .expect("test startup packet");
        stream
            .write_all(&packet[..length])
            .await
            .expect("send startup packet");
    }

    #[tokio::test]
    async fn refuses_encryption_then_rejects_regular_startup() {
        let (address, shutdown, task) = server(test_policy()).await;
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
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
            .await
            .expect("startup rejection deadline")
            .expect("read startup rejection");
        let Decode::Complete { frame, consumed } =
            decode_backend(&response, BACKEND_STARTUP_ERROR_MESSAGE_LENGTH)
                .expect("decode startup rejection")
        else {
            panic!("startup rejection was incomplete");
        };
        assert_eq!(consumed, response.len());
        assert_eq!(frame.tag(), BackendTag::ErrorResponse);
        assert!(response.windows(6).any(|bytes| bytes == b"C57P03"));
        assert!(response.windows(6).any(|bytes| bytes == b"VFATAL"));

        shutdown.send(()).expect("server retains shutdown receiver");
        task.await.expect("server task").expect("clean shutdown");
    }

    #[tokio::test]
    async fn cancel_and_incomplete_startup_close_without_a_response() {
        let (address, shutdown, task) = server(test_policy()).await;

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
        let (address, shutdown, task) = server(policy).await;
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
}
