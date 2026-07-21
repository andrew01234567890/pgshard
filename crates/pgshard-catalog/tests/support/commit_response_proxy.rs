use std::error::Error;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, tcp};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_postgres::{Config, config::Host};

const PROXY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROXY_FRAME_BYTES: usize = 16 * 1024 * 1024;
const COMMIT_QUERY_FRAME: &[u8] = b"Q\0\0\0\x0bCOMMIT\0";
const COMMIT_COMPLETE_PAYLOAD: &[u8] = b"COMMIT\0";

type ProxyError = Box<dyn Error + Send + Sync>;
type ProxyResult<T = ()> = Result<T, ProxyError>;

/// A single-connection TCP proxy that loses exactly one `PostgreSQL` COMMIT response.
///
/// The proxy starts forwarding immediately so that a client can authenticate. Call
/// [`CommitResponseProxy::arm_commit_response_loss`] only after the client connection
/// is ready. Dropping an unfinished proxy aborts its background task.
pub(crate) struct CommitResponseProxy {
    database_url: String,
    arm: Option<oneshot::Sender<CommitResponseProxyArm>>,
    task: Option<JoinHandle<ProxyResult>>,
}

struct CommitResponseProxyArm {
    acknowledge: oneshot::Sender<()>,
}

impl CommitResponseProxy {
    /// Starts a one-connection proxy in front of `database_url`.
    pub(crate) async fn start(database_url: &str) -> ProxyResult<Self> {
        let config: Config = database_url.parse()?;
        let upstream_host = match config.get_hosts() {
            [Host::Tcp(host)] => host.clone(),
            _ => {
                return Err(io::Error::other(
                    "catalog fault proxy requires exactly one TCP database host",
                )
                .into());
            }
        };
        let upstream_port = match config.get_ports() {
            [] => 5432,
            [port] => *port,
            _ => {
                return Err(io::Error::other(
                    "catalog fault proxy requires exactly one database port",
                )
                .into());
            }
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let proxy_port = listener.local_addr()?.port();
        let proxy_database_url = proxy_database_url(database_url, proxy_port)?;
        let (arm, armed) = oneshot::channel();
        let task = tokio::spawn(run_commit_response_proxy(
            listener,
            upstream_host,
            upstream_port,
            armed,
        ));

        Ok(Self {
            database_url: proxy_database_url,
            arm: Some(arm),
            task: Some(task),
        })
    }

    /// Returns the URL that routes a `PostgreSQL` client through this proxy.
    pub(crate) fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Arms response loss for the next exact simple-query `COMMIT` frame.
    pub(crate) async fn arm_commit_response_loss(&mut self) -> ProxyResult {
        let arm = self
            .arm
            .take()
            .ok_or_else(|| io::Error::other("catalog fault proxy was already armed"))?;
        let (acknowledge, acknowledged) = oneshot::channel();
        arm.send(CommitResponseProxyArm { acknowledge })
            .map_err(|_| io::Error::other("catalog fault proxy exited before it was armed"))?;
        timeout(PROXY_TIMEOUT, acknowledged)
            .await
            .map_err(|_| {
                io::Error::other("catalog fault proxy arm acknowledgement exceeded the bound")
            })?
            .map_err(|_| {
                io::Error::other("catalog fault proxy exited before acknowledging its arm")
            })?;
        Ok(())
    }

    /// Waits until `PostgreSQL` has returned COMMIT Complete and `ReadyForQuery` upstream.
    pub(crate) async fn wait_for_commit(mut self) -> ProxyResult {
        let mut task = self
            .task
            .take()
            .ok_or_else(|| io::Error::other("catalog fault proxy task was already consumed"))?;
        if let Ok(result) = timeout(PROXY_TIMEOUT, &mut task).await {
            return result?;
        }
        task.abort();
        let aborted = timeout(PROXY_TIMEOUT, task).await.map_err(|_| {
            io::Error::other("catalog fault proxy did not stop after bounded abort")
        })?;
        drop(aborted);
        Err(io::Error::other(
            "catalog fault proxy did not confirm the dispatched COMMIT within the bound",
        )
        .into())
    }
}

impl Drop for CommitResponseProxy {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn proxy_database_url(database_url: &str, proxy_port: u16) -> ProxyResult<String> {
    let authority_start = ["postgres://", "postgresql://"]
        .into_iter()
        .find_map(|prefix| database_url.strip_prefix(prefix).map(|rest| (prefix, rest)))
        .ok_or_else(|| {
            io::Error::other("catalog fault proxy requires a PostgreSQL database URL")
        })?;
    let (scheme, remainder) = authority_start;
    let authority_end = remainder.find(['/', '?']).unwrap_or(remainder.len());
    let (authority, suffix) = remainder.split_at(authority_end);
    let credentials = authority
        .rsplit_once('@')
        .map_or("", |(credentials, _)| &authority[..=credentials.len()]);
    let separator = if suffix.contains('?') { '&' } else { '?' };
    Ok(format!(
        "{scheme}{credentials}127.0.0.1:{proxy_port}{suffix}{separator}sslmode=disable"
    ))
}

async fn run_commit_response_proxy(
    listener: TcpListener,
    upstream_host: String,
    upstream_port: u16,
    armed: oneshot::Receiver<CommitResponseProxyArm>,
) -> ProxyResult {
    let (downstream, _) = timeout(PROXY_TIMEOUT, listener.accept())
        .await
        .map_err(|_| io::Error::other("catalog fault proxy accept exceeded the bound"))??;
    let upstream = timeout(
        PROXY_TIMEOUT,
        TcpStream::connect((upstream_host.as_str(), upstream_port)),
    )
    .await
    .map_err(|_| io::Error::other("catalog fault proxy upstream connect exceeded the bound"))??;
    downstream.set_nodelay(true)?;
    upstream.set_nodelay(true)?;
    let (mut downstream_read, mut downstream_write) = downstream.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let arm = Box::pin(relay_until_proxy_armed(
        &mut downstream_read,
        &mut downstream_write,
        &mut upstream_read,
        &mut upstream_write,
        armed,
    ))
    .await?;
    arm.acknowledge
        .send(())
        .map_err(|()| io::Error::other("catalog fault proxy arm acknowledgement was dropped"))?;
    Box::pin(relay_until_commit(
        &mut downstream_read,
        &mut downstream_write,
        &mut upstream_read,
        &mut upstream_write,
    ))
    .await
}

async fn relay_until_proxy_armed(
    downstream_read: &mut tcp::OwnedReadHalf,
    downstream_write: &mut tcp::OwnedWriteHalf,
    upstream_read: &mut tcp::OwnedReadHalf,
    upstream_write: &mut tcp::OwnedWriteHalf,
    mut armed: oneshot::Receiver<CommitResponseProxyArm>,
) -> ProxyResult<CommitResponseProxyArm> {
    let mut frontend = [0_u8; 8192];
    let mut backend = [0_u8; 8192];
    loop {
        tokio::select! {
            arm_result = &mut armed => {
                return arm_result.map_err(|_| {
                    io::Error::other("catalog fault proxy arm sender was dropped").into()
                });
            }
            read = downstream_read.read(&mut frontend) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other(
                        "catalog client closed before the fault proxy was armed",
                    ).into());
                }
                upstream_write.write_all(&frontend[..read]).await?;
                upstream_write.flush().await?;
            }
            read = upstream_read.read(&mut backend) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other(
                        "catalog server closed before the fault proxy was armed",
                    ).into());
                }
                downstream_write.write_all(&backend[..read]).await?;
                downstream_write.flush().await?;
            }
        }
    }
}

async fn relay_until_commit(
    downstream_read: &mut tcp::OwnedReadHalf,
    downstream_write: &mut tcp::OwnedWriteHalf,
    upstream_read: &mut tcp::OwnedReadHalf,
    upstream_write: &mut tcp::OwnedWriteHalf,
) -> ProxyResult {
    let mut frontend_read = [0_u8; 8192];
    let mut backend_read = [0_u8; 8192];
    let mut frontend_frames = Vec::new();
    loop {
        tokio::select! {
            read = downstream_read.read(&mut frontend_read) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other(
                        "catalog client closed before COMMIT dispatch",
                    ).into());
                }
                frontend_frames.extend_from_slice(&frontend_read[..read]);
                while let Some(frame) = take_protocol_frame(&mut frontend_frames)? {
                    if frame == COMMIT_QUERY_FRAME {
                        downstream_write.shutdown().await?;
                        upstream_write.write_all(&frame).await?;
                        upstream_write.flush().await?;
                        read_committed_response(upstream_read).await?;
                        return Ok(());
                    }
                    upstream_write.write_all(&frame).await?;
                    upstream_write.flush().await?;
                }
            }
            read = upstream_read.read(&mut backend_read) => {
                let read = read?;
                if read == 0 {
                    return Err(io::Error::other(
                        "catalog server closed before COMMIT dispatch",
                    ).into());
                }
                downstream_write.write_all(&backend_read[..read]).await?;
                downstream_write.flush().await?;
            }
        }
    }
}

fn take_protocol_frame(buffer: &mut Vec<u8>) -> ProxyResult<Option<Vec<u8>>> {
    if buffer.len() < 5 {
        return Ok(None);
    }
    let body_length = u32::from_be_bytes(buffer[1..5].try_into()?) as usize;
    if !(4..=MAX_PROXY_FRAME_BYTES).contains(&body_length) {
        return Err(
            io::Error::other("catalog fault proxy observed an invalid protocol frame").into(),
        );
    }
    let frame_length = body_length
        .checked_add(1)
        .ok_or_else(|| io::Error::other("catalog fault proxy frame length overflowed"))?;
    if buffer.len() < frame_length {
        return Ok(None);
    }
    Ok(Some(buffer.drain(..frame_length).collect()))
}

async fn read_committed_response(upstream_read: &mut tcp::OwnedReadHalf) -> ProxyResult<Vec<u8>> {
    let deadline = Instant::now() + PROXY_TIMEOUT;
    let mut read_buffer = [0_u8; 8192];
    let mut backend_frames = Vec::new();
    let mut response = Vec::new();
    let mut commit_completed = false;
    loop {
        let read = timeout_at(deadline, upstream_read.read(&mut read_buffer))
            .await
            .map_err(|_| io::Error::other("catalog COMMIT response exceeded the proxy bound"))??;
        if read == 0 {
            return Err(io::Error::other("catalog server closed before confirming COMMIT").into());
        }
        response.extend_from_slice(&read_buffer[..read]);
        backend_frames.extend_from_slice(&read_buffer[..read]);
        while let Some(frame) = take_protocol_frame(&mut backend_frames)? {
            match frame[0] {
                b'C' if &frame[5..] == COMMIT_COMPLETE_PAYLOAD => commit_completed = true,
                b'E' => {
                    return Err(
                        io::Error::other("catalog server rejected the injected COMMIT").into(),
                    );
                }
                b'Z' if commit_completed => return Ok(response),
                _ => {}
            }
        }
    }
}
