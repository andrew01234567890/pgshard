//! Fault-isolated, challenge-bound catalog-activation TLS endpoint.
//!
//! This listener is deliberately separate from the plaintext diagnostics
//! listener. It advertises only the already-inert consumer capability and has
//! no path to `PostgreSQL`, serving, routing, fencing, or Lease authority.

use std::convert::Infallible;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::header::{
    ACCEPT, CACHE_CONTROL, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, EXPECT,
    HOST, TRANSFER_ENCODING, UPGRADE,
};
use axum::http::uri::Authority;
use axum::http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Version,
};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use pgshard_types::catalog_activation::{
    CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_RESPONSE_VERSION, CatalogActivationCapabilityChallenge,
    CatalogActivationCapabilityChallengeResponse,
};
use rustix::fs::{Mode, OFlags};
use rustls::ServerConfig;
use rustls::server::NoServerSessionStorage;
use rustls_pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer,
    pem::{PemObject, SectionKind},
};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::catalog_activation_consumer::{
    CatalogActivationCapabilityState, CatalogActivationEndpointState,
};

/// Exact path served only by the TLS listener.
pub const CATALOG_ACTIVATION_CAPABILITY_PATH: &str = "/capabilities/catalog-activation";

const MAXIMUM_CONNECTIONS: usize = 16;
const MAXIMUM_HEADERS: usize = 16;
const MAXIMUM_HEADER_BYTES: usize = 8 * 1_024;
const MAXIMUM_CHALLENGE_BYTES: usize = 4 * 1_024;
const MAXIMUM_TLS_MATERIAL_BYTES: usize = 64 * 1_024;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
const HTTP_HEADER_TIMEOUT: Duration = Duration::from_secs(1);
const HTTP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);
const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAXIMUM_RETRY: Duration = Duration::from_secs(5);

/// Validated optional endpoint configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogActivationTlsConfig {
    bind: SocketAddr,
    certificate_path: PathBuf,
    private_key_path: PathBuf,
}

impl CatalogActivationTlsConfig {
    /// Builds an endpoint on a port distinct from plaintext diagnostics.
    ///
    /// # Errors
    ///
    /// Returns an error for an ephemeral or shared port, unsafe paths, or the
    /// same path being used for public and private material.
    pub(crate) fn new(
        bind: SocketAddr,
        certificate_path: PathBuf,
        private_key_path: PathBuf,
        http_bind: SocketAddr,
    ) -> Result<Self, CatalogActivationTlsConfigError> {
        if bind.port() == 0 || bind.port() == http_bind.port() {
            return Err(CatalogActivationTlsConfigError);
        }
        if !absolute_normal_path(&certificate_path)
            || !absolute_normal_path(&private_key_path)
            || certificate_path == private_key_path
        {
            return Err(CatalogActivationTlsConfigError);
        }
        Ok(Self {
            bind,
            certificate_path,
            private_key_path,
        })
    }

    /// Returns the dedicated TLS socket address.
    #[must_use]
    pub const fn bind(&self) -> SocketAddr {
        self.bind
    }

    /// Returns the server certificate path.
    #[must_use]
    pub fn certificate_path(&self) -> &Path {
        &self.certificate_path
    }

    /// Returns the server private-key path.
    #[must_use]
    pub fn private_key_path(&self) -> &Path {
        &self.private_key_path
    }
}

fn absolute_normal_path(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .any(|component| matches!(component, Component::Normal(_)))
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

/// Syntactically unsafe TLS endpoint configuration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("invalid catalog-activation TLS endpoint configuration")]
pub struct CatalogActivationTlsConfigError;

#[derive(Clone, Copy)]
struct ServerPolicy {
    maximum_connections: usize,
    handshake_timeout: Duration,
    header_timeout: Duration,
    connection_timeout: Duration,
    drain_timeout: Duration,
}

const DEFAULT_SERVER_POLICY: ServerPolicy = ServerPolicy {
    maximum_connections: MAXIMUM_CONNECTIONS,
    handshake_timeout: TLS_HANDSHAKE_TIMEOUT,
    header_timeout: HTTP_HEADER_TIMEOUT,
    connection_timeout: HTTP_CONNECTION_TIMEOUT,
    drain_timeout: CONNECTION_DRAIN_TIMEOUT,
};

/// Starts the optional endpoint in an independently supervised task.
///
/// File, bind, accept, TLS, and HTTP failures withdraw only this network
/// endpoint. They never change diagnostics, readiness, `PostgreSQL`, or Lease
/// state. The task retries until global shutdown.
#[must_use]
pub fn spawn_catalog_activation_tls_server(
    config: Option<CatalogActivationTlsConfig>,
    capability: CatalogActivationCapabilityState,
    shutdown: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    config.map(|config| {
        tokio::spawn(async move {
            supervise(config, capability, shutdown).await;
        })
    })
}

async fn supervise(
    config: CatalogActivationTlsConfig,
    capability: CatalogActivationCapabilityState,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut retry = INITIAL_RETRY;
    while !*shutdown.borrow() {
        match run_attempt(&config, capability.clone(), shutdown.clone()).await {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(
                    reason = %error,
                    retry_after_ms = retry.as_millis(),
                    "catalog-activation TLS endpoint unavailable; retrying independently"
                );
            }
        }
        if wait_or_stop(&mut shutdown, retry).await {
            return;
        }
        retry = retry.saturating_mul(2).min(MAXIMUM_RETRY);
    }
}

async fn run_attempt(
    config: &CatalogActivationTlsConfig,
    capability: CatalogActivationCapabilityState,
    shutdown: watch::Receiver<bool>,
) -> Result<(), CatalogActivationTlsServerError> {
    let load_config = config.clone();
    let tls = tokio::task::spawn_blocking(move || load_server_config(&load_config))
        .await
        .map_err(CatalogActivationTlsServerError::MaterialTask)??;
    if *shutdown.borrow() {
        return Ok(());
    }
    let listener = TcpListener::bind(config.bind)
        .await
        .map_err(CatalogActivationTlsServerError::Bind)?;
    tracing::info!(bind = %config.bind, "catalog-activation TLS endpoint listening");
    serve_on_with_policy(
        listener,
        tls,
        capability,
        wait_for_shutdown(shutdown),
        DEFAULT_SERVER_POLICY,
    )
    .await
    .map_err(CatalogActivationTlsServerError::Serve)
}

async fn wait_or_stop(shutdown: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        biased;
        () = wait_for_shutdown(shutdown.clone()) => true,
        () = tokio::time::sleep(delay) => false,
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow_and_update() {
            return;
        }
    }
}

fn load_server_config(
    config: &CatalogActivationTlsConfig,
) -> Result<Arc<ServerConfig>, CatalogActivationTlsServerError> {
    let certificate = read_bounded_regular_file(
        &config.certificate_path,
        MAXIMUM_TLS_MATERIAL_BYTES,
        TlsMaterialKind::Certificate,
    )?;
    let private_key = read_bounded_regular_file(
        &config.private_key_path,
        MAXIMUM_TLS_MATERIAL_BYTES,
        TlsMaterialKind::PrivateKey,
    )?;
    server_config_from_material(&certificate, &private_key)
}

fn server_config_from_material(
    certificate: &[u8],
    private_key: &[u8],
) -> Result<Arc<ServerConfig>, CatalogActivationTlsServerError> {
    let certificate = parse_certificate(certificate)?;
    let private_key = parse_private_key(private_key)?;
    let mut config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(CatalogActivationTlsServerError::Configuration)?
            .with_no_client_auth()
            .with_single_cert(vec![certificate], private_key)
            .map_err(CatalogActivationTlsServerError::Configuration)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config.max_early_data_size = 0;
    config.send_half_rtt_data = false;
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(NoServerSessionStorage {});
    Ok(Arc::new(config))
}

#[derive(Clone, Copy)]
enum TlsMaterialKind {
    Certificate,
    PrivateKey,
}

fn read_bounded_regular_file(
    path: &Path,
    maximum: usize,
    kind: TlsMaterialKind,
) -> Result<Vec<u8>, CatalogActivationTlsServerError> {
    let metadata = fs::metadata(path).map_err(|source| material_file_error(kind, source))?;
    if !metadata.file_type().is_file() {
        return Err(CatalogActivationTlsServerError::MaterialNotRegular(kind));
    }
    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
        Mode::empty(),
    )
    .map_err(|source| material_file_error(kind, source.into()))?;
    let file = File::from(descriptor);
    if !file
        .metadata()
        .map_err(|source| material_file_error(kind, source))?
        .file_type()
        .is_file()
    {
        return Err(CatalogActivationTlsServerError::MaterialNotRegular(kind));
    }
    let mut contents = Vec::with_capacity(maximum.saturating_add(1));
    file.take(maximum.saturating_add(1) as u64)
        .read_to_end(&mut contents)
        .map_err(|source| material_file_error(kind, source))?;
    if contents.len() > maximum {
        return Err(CatalogActivationTlsServerError::MaterialTooLarge(kind));
    }
    Ok(contents)
}

fn material_file_error(
    kind: TlsMaterialKind,
    source: io::Error,
) -> CatalogActivationTlsServerError {
    CatalogActivationTlsServerError::MaterialFile { kind, source }
}

fn parse_certificate(
    contents: &[u8],
) -> Result<CertificateDer<'static>, CatalogActivationTlsServerError> {
    if !contents.starts_with(b"-----BEGIN CERTIFICATE-----\n") {
        return Err(CatalogActivationTlsServerError::InvalidCertificate);
    }
    let mut sections = <(SectionKind, Vec<u8>)>::pem_slice_iter(contents);
    let Some(section) = sections.next() else {
        return Err(CatalogActivationTlsServerError::InvalidCertificate);
    };
    let section = section.map_err(|_| CatalogActivationTlsServerError::InvalidCertificate)?;
    if !sections.remainder().is_empty() {
        return Err(CatalogActivationTlsServerError::InvalidCertificate);
    }
    match section {
        (SectionKind::Certificate, der) => Ok(CertificateDer::from(der)),
        _ => Err(CatalogActivationTlsServerError::InvalidCertificate),
    }
}

fn parse_private_key(
    contents: &[u8],
) -> Result<PrivateKeyDer<'static>, CatalogActivationTlsServerError> {
    if !contents.starts_with(b"-----BEGIN PRIVATE KEY-----\n") {
        return Err(CatalogActivationTlsServerError::InvalidPrivateKey);
    }
    let mut sections = <(SectionKind, Vec<u8>)>::pem_slice_iter(contents);
    let Some(section) = sections.next() else {
        return Err(CatalogActivationTlsServerError::InvalidPrivateKey);
    };
    let section = section.map_err(|_| CatalogActivationTlsServerError::InvalidPrivateKey)?;
    if !sections.remainder().is_empty() {
        return Err(CatalogActivationTlsServerError::InvalidPrivateKey);
    }
    match section {
        (SectionKind::PrivateKey, der) => Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der))),
        _ => Err(CatalogActivationTlsServerError::InvalidPrivateKey),
    }
}

async fn serve_on_with_policy(
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    capability: CatalogActivationCapabilityState,
    shutdown: impl Future<Output = ()> + Send + 'static,
    policy: ServerPolicy,
) -> io::Result<()> {
    let acceptor = TlsAcceptor::from(tls);
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
            accepted = listener.accept(), if connections.len() < policy.maximum_connections => {
                match accepted {
                    Ok((stream, _)) => {
                        connections.spawn(serve_connection(
                            stream,
                            acceptor.clone(),
                            capability.clone(),
                            policy,
                        ));
                    }
                    Err(error) if is_retryable_connection_accept_error(&error) => {
                        tracing::debug!("transient catalog-activation TLS accept error");
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    drain_connections(&mut connections, policy.drain_timeout).await
}

fn is_retryable_connection_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    ) || [
        rustix::io::Errno::NETDOWN,
        rustix::io::Errno::PROTO,
        rustix::io::Errno::NOPROTOOPT,
        rustix::io::Errno::HOSTDOWN,
        rustix::io::Errno::NONET,
        rustix::io::Errno::HOSTUNREACH,
        rustix::io::Errno::OPNOTSUPP,
        rustix::io::Errno::NETUNREACH,
    ]
    .into_iter()
    .any(|errno| error.raw_os_error() == Some(errno.raw_os_error()))
}

async fn serve_connection(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    capability: CatalogActivationCapabilityState,
    policy: ServerPolicy,
) {
    let tls = match timeout(policy.handshake_timeout, acceptor.accept(stream)).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(_)) => {
            tracing::debug!("catalog-activation TLS handshake rejected");
            return;
        }
        Err(_) => {
            tracing::debug!("catalog-activation TLS handshake timed out");
            return;
        }
    };
    if tls.get_ref().1.alpn_protocol() != Some(b"http/1.1") {
        tracing::debug!("catalog-activation TLS connection omitted HTTP/1.1 ALPN");
        return;
    }
    let service = service_fn(move |request: Request<Incoming>| {
        let capability = capability.clone();
        async move { Ok::<_, Infallible>(handle_request(request.map(Body::new), capability).await) }
    });
    let mut server = http1::Builder::new();
    server
        .keep_alive(false)
        .auto_date_header(false)
        .max_headers(MAXIMUM_HEADERS)
        .max_buf_size(MAXIMUM_HEADER_BYTES)
        .timer(TokioTimer::new())
        .header_read_timeout(policy.header_timeout);
    let connection = server.serve_connection(TokioIo::new(tls), service);
    match timeout(policy.connection_timeout, connection).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => tracing::debug!("catalog-activation HTTP/1.1 connection rejected"),
        Err(_) => tracing::debug!("catalog-activation HTTP/1.1 connection timed out"),
    }
}

async fn drain_connections(
    connections: &mut JoinSet<()>,
    drain_timeout: Duration,
) -> io::Result<()> {
    let drained = timeout(drain_timeout, async {
        while let Some(result) = connections.join_next().await {
            connection_task_result(result)?;
        }
        Ok(())
    })
    .await;
    if let Ok(result) = drained {
        return result;
    }
    tracing::warn!(
        timeout_ms = drain_timeout.as_millis(),
        remaining_connections = connections.len(),
        "forcing catalog-activation TLS shutdown after drain timeout"
    );
    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result
            && !error.is_cancelled()
        {
            return Err(io::Error::other(format!(
                "join aborted catalog-activation TLS connection: {error}"
            )));
        }
    }
    Ok(())
}

fn connection_task_result(result: Result<(), JoinError>) -> io::Result<()> {
    result.map_err(|error| {
        io::Error::other(format!("join catalog-activation TLS connection: {error}"))
    })
}

async fn handle_request(
    request: Request<Body>,
    capability: CatalogActivationCapabilityState,
) -> Response<Body> {
    if request.version() != Version::HTTP_11 {
        return empty_response(StatusCode::BAD_REQUEST);
    }
    if request
        .uri()
        .path_and_query()
        .map(axum::http::uri::PathAndQuery::as_str)
        != Some(CATALOG_ACTIVATION_CAPABILITY_PATH)
    {
        return empty_response(StatusCode::NOT_FOUND);
    }
    if request.method() != Method::POST {
        return empty_response(StatusCode::METHOD_NOT_ALLOWED);
    }
    let content_length = match validate_headers(request.headers()) {
        Ok(content_length) => content_length,
        Err(RequestHeaderError::UnsupportedMediaType) => {
            return empty_response(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        Err(RequestHeaderError::BodyTooLarge) => {
            return empty_response(StatusCode::PAYLOAD_TOO_LARGE);
        }
        Err(RequestHeaderError::Invalid) => return empty_response(StatusCode::BAD_REQUEST),
    };
    let capability = match capability.snapshot() {
        CatalogActivationEndpointState::Disabled => {
            return empty_response(StatusCode::NOT_FOUND);
        }
        CatalogActivationEndpointState::Unavailable => {
            return empty_response(StatusCode::SERVICE_UNAVAILABLE);
        }
        CatalogActivationEndpointState::Available(capability) => capability,
    };
    let body = match to_bytes(request.into_body(), MAXIMUM_CHALLENGE_BYTES).await {
        Ok(body) if body.len() == content_length => body,
        Ok(_) => return empty_response(StatusCode::BAD_REQUEST),
        Err(_) => return empty_response(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let challenge: CatalogActivationCapabilityChallenge = match serde_json::from_slice(&body) {
        Ok(challenge) => challenge,
        Err(_) => return empty_response(StatusCode::BAD_REQUEST),
    };
    if challenge.validate().is_err()
        || capability.validate().is_err()
        || capability.cluster != challenge.cluster
        || capability.carrier != challenge.carrier
        || capability.target != challenge.target
    {
        return empty_response(StatusCode::BAD_REQUEST);
    }
    let response = CatalogActivationCapabilityChallengeResponse {
        schema_version: CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_RESPONSE_VERSION.to_owned(),
        nonce: challenge.nonce.clone(),
        request_sha256: challenge.request_sha256.clone(),
        capability: *capability,
    };
    if response.validate_for(&challenge).is_err() {
        return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let body = match serde_json::to_vec(&response) {
        Ok(body) if body.len() <= MAXIMUM_CHALLENGE_BYTES => body,
        Ok(_) | Err(_) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    json_response(body)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestHeaderError {
    Invalid,
    UnsupportedMediaType,
    BodyTooLarge,
}

fn validate_headers(headers: &HeaderMap) -> Result<usize, RequestHeaderError> {
    if headers.len() != 5
        || !headers.keys().all(allowed_request_header)
        || singleton(headers, TRANSFER_ENCODING).is_some()
        || singleton(headers, CONTENT_ENCODING).is_some()
        || singleton(headers, EXPECT).is_some()
        || singleton(headers, UPGRADE).is_some()
    {
        return Err(RequestHeaderError::Invalid);
    }
    let host = singleton_required(headers, HOST)?
        .to_str()
        .map_err(|_| RequestHeaderError::Invalid)?;
    if host.len() > 260 || Authority::from_str(host).is_err() {
        return Err(RequestHeaderError::Invalid);
    }
    if singleton_required(headers, ACCEPT)? != HeaderValue::from_static("application/json") {
        return Err(RequestHeaderError::Invalid);
    }
    if singleton_required(headers, CONTENT_TYPE)? != HeaderValue::from_static("application/json") {
        return Err(RequestHeaderError::UnsupportedMediaType);
    }
    if singleton_required(headers, CONNECTION)? != HeaderValue::from_static("close") {
        return Err(RequestHeaderError::Invalid);
    }
    let length = singleton_required(headers, CONTENT_LENGTH)?
        .to_str()
        .map_err(|_| RequestHeaderError::Invalid)?;
    if length.is_empty() || !length.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RequestHeaderError::Invalid);
    }
    let parsed = length
        .parse::<usize>()
        .map_err(|_| RequestHeaderError::Invalid)?;
    if parsed == 0 || parsed.to_string() != length {
        return Err(RequestHeaderError::Invalid);
    }
    if parsed > MAXIMUM_CHALLENGE_BYTES {
        return Err(RequestHeaderError::BodyTooLarge);
    }
    Ok(parsed)
}

fn allowed_request_header(name: &HeaderName) -> bool {
    name == HOST
        || name == ACCEPT
        || name == CONTENT_TYPE
        || name == CONTENT_LENGTH
        || name == CONNECTION
}

fn singleton_required(
    headers: &HeaderMap,
    name: HeaderName,
) -> Result<&HeaderValue, RequestHeaderError> {
    singleton(headers, name).ok_or(RequestHeaderError::Invalid)
}

fn singleton(headers: &HeaderMap, name: HeaderName) -> Option<&HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?;
    values.next().is_none().then_some(first)
}

fn empty_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CACHE_CONTROL, "no-store")
        .header(CONNECTION, "close")
        .header(CONTENT_LENGTH, "0")
        .body(Body::empty())
        .expect("static catalog-activation HTTP response")
}

fn json_response(body: Vec<u8>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header(CACHE_CONTROL, "no-store")
        .header(CONNECTION, "close")
        .header(CONTENT_LENGTH, body.len().to_string())
        .body(Body::from(body))
        .expect("bounded catalog-activation JSON response")
}

#[derive(Debug, Error)]
enum CatalogActivationTlsServerError {
    #[error("read catalog-activation TLS {kind}")]
    MaterialFile {
        kind: TlsMaterialKind,
        #[source]
        source: io::Error,
    },
    #[error("catalog-activation TLS {0} is not a regular file")]
    MaterialNotRegular(TlsMaterialKind),
    #[error("catalog-activation TLS {0} exceeds 64 KiB")]
    MaterialTooLarge(TlsMaterialKind),
    #[error("catalog-activation TLS certificate is not one exact PEM certificate")]
    InvalidCertificate,
    #[error("catalog-activation TLS private key is not one exact PKCS#8 PEM key")]
    InvalidPrivateKey,
    #[error("catalog-activation TLS certificate and key configuration is invalid")]
    Configuration(#[source] rustls::Error),
    #[error("catalog-activation TLS material task failed")]
    MaterialTask(#[source] JoinError),
    #[error("bind catalog-activation TLS listener")]
    Bind(#[source] io::Error),
    #[error("serve catalog-activation TLS listener")]
    Serve(#[source] io::Error),
}

impl std::fmt::Display for TlsMaterialKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Certificate => "certificate",
            Self::PrivateKey => "private key",
        })
    }
}

impl std::fmt::Debug for TlsMaterialKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::header::{ACCEPT, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST};
    use axum::http::{HeaderName, HeaderValue, Method, Request, StatusCode, Version};
    use pgshard_types::catalog_activation::{
        CATALOG_ACTIVATION_ACCEPTANCE_VERSION,
        CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_REQUEST_VERSION,
        CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_RESPONSE_VERSION,
        CATALOG_ACTIVATION_CAPABILITY_VERSION, CATALOG_ACTIVATION_CONSUMER_VERSION,
        CATALOG_ACTIVATION_FSYNC_PERSISTENCE, CATALOG_ACTIVATION_REQUEST_VERSION,
        CatalogActivationCapability, CatalogActivationCapabilityCarrier,
        CatalogActivationCapabilityChallenge, CatalogActivationCapabilityChallengeResponse,
        CatalogActivationCapabilityCluster, CatalogActivationCapabilityTarget,
    };
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::{ClientConfig, RootCertStore};
    use rustls_pki_types::{CertificateDer, ServerName};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use tokio_rustls::TlsConnector;

    use super::{
        CATALOG_ACTIVATION_CAPABILITY_PATH, DEFAULT_SERVER_POLICY, MAXIMUM_CHALLENGE_BYTES,
        ServerPolicy, handle_request, serve_on_with_policy, server_config_from_material,
    };
    use crate::catalog_activation_consumer::CatalogActivationCapabilityState;

    fn capability() -> CatalogActivationCapability {
        CatalogActivationCapability {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_VERSION.to_owned(),
            capability: CATALOG_ACTIVATION_CONSUMER_VERSION.to_owned(),
            request_schema_version: CATALOG_ACTIVATION_REQUEST_VERSION.to_owned(),
            acceptance_schema_version: CATALOG_ACTIVATION_ACCEPTANCE_VERSION.to_owned(),
            persistence: CATALOG_ACTIVATION_FSYNC_PERSISTENCE.to_owned(),
            cluster: CatalogActivationCapabilityCluster {
                name: "cluster-a".to_owned(),
                uid: "cluster-uid".to_owned(),
            },
            carrier: CatalogActivationCapabilityCarrier {
                namespace: "database".to_owned(),
                name: "cluster-a-catalog-activation".to_owned(),
                uid: "carrier-uid".to_owned(),
            },
            target: CatalogActivationCapabilityTarget {
                shard: 0,
                member: 0,
                instance_id: "cluster-a-shard-0-member-0".to_owned(),
                pod_name: "cluster-a-shard-0-member-0".to_owned(),
                pod_uid: "pod-uid".to_owned(),
            },
        }
    }

    fn challenge() -> CatalogActivationCapabilityChallenge {
        let capability = capability();
        CatalogActivationCapabilityChallenge {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_REQUEST_VERSION.to_owned(),
            nonce: "1".repeat(64),
            request_sha256: "a".repeat(64),
            cluster: capability.cluster,
            carrier: capability.carrier,
            target: capability.target,
        }
    }

    fn available_state() -> CatalogActivationCapabilityState {
        let state = CatalogActivationCapabilityState::configured();
        state.available(capability());
        state
    }

    fn request_with_body(body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_11)
            .uri(CATALOG_ACTIVATION_CAPABILITY_PATH)
            .header(HOST, "localhost")
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, body.len())
            .header(CONNECTION, "close")
            .body(Body::from(body))
            .expect("valid test request")
    }

    fn challenge_request() -> Request<Body> {
        request_with_body(serde_json::to_vec(&challenge()).expect("serialize challenge"))
    }

    fn tls_material() -> (String, String, CertificateDer<'static>) {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".to_owned()])
                .expect("generate test certificate");
        let certificate_pem = cert.pem();
        let certificate_der = cert.der().clone();
        let private_key_pem = signing_key.serialize_pem();
        (certificate_pem, private_key_pem, certificate_der)
    }

    fn client_config(certificate: CertificateDer<'static>) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(certificate).expect("add test trust anchor");
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .expect("TLS 1.3 client policy")
                .with_root_certificates(roots)
                .with_no_client_auth();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(config)
    }

    fn strict_challenge_response_body(response: &[u8]) -> &[u8] {
        let delimiter = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("HTTP header terminator");
        let header = std::str::from_utf8(&response[..delimiter]).expect("ASCII HTTP headers");
        let mut lines = header.split("\r\n");
        assert_eq!(lines.next(), Some("HTTP/1.1 200 OK"));

        let mut headers = BTreeMap::new();
        for line in lines {
            let (name, value) = line.split_once(": ").expect("canonical HTTP header");
            assert!(
                headers.insert(name.to_ascii_lowercase(), value).is_none(),
                "duplicate response header {name}"
            );
        }
        assert_eq!(
            headers.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "cache-control",
                "connection",
                "content-length",
                "content-type"
            ]
        );
        assert_eq!(headers["cache-control"], "no-store");
        assert_eq!(headers["connection"], "close");
        assert_eq!(headers["content-type"], "application/json");

        let body = &response[delimiter + 4..];
        assert_eq!(
            headers["content-length"]
                .parse::<usize>()
                .expect("numeric content length"),
            body.len()
        );
        body
    }

    #[tokio::test]
    async fn capability_state_and_response_are_exact() {
        assert_eq!(
            handle_request(
                challenge_request(),
                CatalogActivationCapabilityState::disabled()
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            handle_request(
                challenge_request(),
                CatalogActivationCapabilityState::configured()
            )
            .await
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let response = handle_request(challenge_request(), available_state()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().len(), 4);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[CONNECTION], "close");
        let body = to_bytes(response.into_body(), MAXIMUM_CHALLENGE_BYTES)
            .await
            .expect("bounded response body");
        let expected = CatalogActivationCapabilityChallengeResponse {
            schema_version: CATALOG_ACTIVATION_CAPABILITY_CHALLENGE_RESPONSE_VERSION.to_owned(),
            nonce: challenge().nonce,
            request_sha256: challenge().request_sha256,
            capability: capability(),
        };
        assert_eq!(
            body,
            serde_json::to_vec(&expected).expect("serialize response")
        );
        let parsed: CatalogActivationCapabilityChallengeResponse =
            serde_json::from_slice(&body).expect("parse response");
        parsed
            .validate_for(&challenge())
            .expect("response is bound to the challenge");
    }

    #[tokio::test]
    async fn method_path_schema_and_identity_are_fail_closed() {
        let mut request = challenge_request();
        *request.method_mut() = Method::GET;
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::METHOD_NOT_ALLOWED
        );

        let mut request = challenge_request();
        *request.uri_mut() = format!("{CATALOG_ACTIVATION_CAPABILITY_PATH}?query=1")
            .parse()
            .expect("valid URI");
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::NOT_FOUND
        );

        let mut mismatched = challenge();
        mismatched.target.pod_uid = "different-pod-uid".to_owned();
        assert_eq!(
            handle_request(
                request_with_body(serde_json::to_vec(&mismatched).expect("serialize mismatch")),
                available_state()
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );

        let mut document = serde_json::to_value(challenge()).expect("serialize challenge value");
        document
            .as_object_mut()
            .expect("challenge object")
            .insert("unexpected".to_owned(), serde_json::Value::Bool(true));
        assert_eq!(
            handle_request(
                request_with_body(serde_json::to_vec(&document).expect("serialize unknown field")),
                available_state()
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn request_framing_is_exact_and_bounded() {
        let mut request = challenge_request();
        request.headers_mut().remove(ACCEPT);
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::BAD_REQUEST
        );

        let mut request = challenge_request();
        request.headers_mut().insert(
            HeaderName::from_static("x-extra"),
            HeaderValue::from_static("rejected"),
        );
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::BAD_REQUEST
        );

        let mut request = challenge_request();
        request
            .headers_mut()
            .append(CONTENT_LENGTH, HeaderValue::from_static("1"));
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::BAD_REQUEST
        );

        let mut request = challenge_request();
        request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );

        let mut request = challenge_request();
        let length = request.headers()[CONTENT_LENGTH]
            .to_str()
            .expect("ASCII content length")
            .to_owned();
        request.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&format!("0{length}")).expect("valid test header"),
        );
        assert_eq!(
            handle_request(request, available_state()).await.status(),
            StatusCode::BAD_REQUEST
        );

        let oversized = vec![b'x'; MAXIMUM_CHALLENGE_BYTES + 1];
        assert_eq!(
            handle_request(request_with_body(oversized), available_state())
                .await
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn tls_policy_forbids_early_data_tickets_and_resumption() {
        let (certificate, private_key, _) = tls_material();
        let config = server_config_from_material(certificate.as_bytes(), private_key.as_bytes())
            .expect("valid TLS material");
        assert_eq!(config.alpn_protocols, [b"http/1.1"]);
        assert_eq!(config.max_early_data_size, 0);
        assert!(!config.send_half_rtt_data);
        assert_eq!(config.send_tls13_tickets, 0);
        assert!(!config.session_storage.can_cache());
        assert!(!config.ticketer.enabled());

        let two_certificates = format!("{certificate}{certificate}");
        assert!(
            server_config_from_material(two_certificates.as_bytes(), private_key.as_bytes())
                .is_err()
        );
        let trailing_key = format!("{private_key}\nignored");
        assert!(
            server_config_from_material(certificate.as_bytes(), trailing_key.as_bytes()).is_err()
        );
    }

    #[tokio::test]
    async fn tls_listener_matches_strict_client_framing_and_rejects_plaintext() {
        let (certificate, private_key, certificate_der) = tls_material();
        let server_config =
            server_config_from_material(certificate.as_bytes(), private_key.as_bytes())
                .expect("valid TLS material");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_on_with_policy(
            listener,
            server_config,
            available_state(),
            async move {
                let _ = shutdown_rx.await;
            },
            DEFAULT_SERVER_POLICY,
        ));

        let connector = TlsConnector::from(client_config(certificate_der));
        let stream = TcpStream::connect(address)
            .await
            .expect("connect TLS client");
        let server_name = ServerName::try_from("localhost".to_owned()).expect("valid server name");
        let mut tls = connector
            .connect(server_name, stream)
            .await
            .expect("authenticated TLS 1.3 handshake");
        let body = serde_json::to_vec(&challenge()).expect("serialize challenge");
        let request = format!(
            "POST {CATALOG_ACTIVATION_CAPABILITY_PATH} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        tls.write_all(request.as_bytes())
            .await
            .expect("write HTTP headers");
        tls.write_all(&body).await.expect("write HTTP body");
        let mut response = Vec::new();
        tls.read_to_end(&mut response).await.expect("read response");
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        let response_body = strict_challenge_response_body(&response);
        let parsed: CatalogActivationCapabilityChallengeResponse =
            serde_json::from_slice(response_body).expect("parse TLS response");
        parsed
            .validate_for(&challenge())
            .expect("TLS response matches challenge");

        let mut plaintext = TcpStream::connect(address)
            .await
            .expect("connect plaintext client");
        plaintext
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write plaintext request");
        plaintext
            .shutdown()
            .await
            .expect("close plaintext write side");
        let mut plaintext_response = Vec::new();
        timeout(
            Duration::from_secs(2),
            plaintext.read_to_end(&mut plaintext_response),
        )
        .await
        .expect("plaintext connection closed")
        .expect("read plaintext close");
        assert!(!plaintext_response.starts_with(b"HTTP/"));

        shutdown_tx.send(()).expect("request test shutdown");
        server
            .await
            .expect("join test server")
            .expect("serve cleanly");
    }

    #[tokio::test]
    async fn shutdown_aborts_a_stalled_handshake_after_drain_deadline() {
        let (certificate, private_key, _) = tls_material();
        let server_config =
            server_config_from_material(certificate.as_bytes(), private_key.as_bytes())
                .expect("valid TLS material");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let policy = ServerPolicy {
            handshake_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_millis(10),
            ..DEFAULT_SERVER_POLICY
        };
        let server = tokio::spawn(serve_on_with_policy(
            listener,
            server_config,
            available_state(),
            async move {
                let _ = shutdown_rx.await;
            },
            policy,
        ));
        let stalled = TcpStream::connect(address)
            .await
            .expect("connect stalled client");
        tokio::task::yield_now().await;
        shutdown_tx.send(()).expect("request test shutdown");
        timeout(Duration::from_secs(1), server)
            .await
            .expect("bounded server shutdown")
            .expect("join test server")
            .expect("serve cleanly");
        drop(stalled);
    }
}
