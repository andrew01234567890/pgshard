//! Shared Kubernetes client transport policy for the agent.

use std::time::Duration;

use kube::Config;

/// Longest a pooled connection may go without the API server saying anything.
///
/// `hyper-timeout` starts this countdown whenever a socket read returns
/// `Pending`, which includes the read a keep-alive connection parks on between
/// requests. It bounds the connection's inactivity rather than any request, so
/// it has to stay well above the interval at which the agent issues them: the
/// activation consumer polls every 2 seconds and a writable Lease retries at
/// most every 30. Sized any tighter it retires healthy connections instead of
/// dead ones, and every request that follows pays a fresh TCP and TLS
/// handshake.
///
/// It is a backstop and not a load-bearing bound. `kube` is built here with
/// `rustls-tls` and no HTTP/2 feature, so its connector offers no ALPN protocol
/// at all and every connection is HTTP/1.1 — where cancelling a request that
/// exceeded its own budget already discards the connection it was issued on, so
/// the request after it opens a fresh one. `None` would be correct on this
/// build. A finite value is kept because it costs nothing at this distance from
/// the request interval and does not depend on the protocol staying HTTP/1.1.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_mins(1);

/// Applies the agent's transport policy for a per-request budget.
///
/// The budget itself is enforced by the `tokio::time::timeout` each caller
/// wraps its request in, and the writable-Lease timing validation is what makes
/// that budget load bearing. Only the phases that belong to one request are
/// held to it.
pub(crate) fn apply_request_budget(config: &mut Config, request_timeout: Duration) {
    config.connect_timeout = Some(request_timeout);
    config.read_timeout = Some(IDLE_CONNECTION_TIMEOUT);
    config.write_timeout = Some(request_timeout);
    // Retrying inside the client would spend an unknown multiple of the budget
    // the caller reserved for one request.
    config.default_retry = false;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shortest interval at which either agent client issues a request is
    /// the activation consumer's 2 second poll; the longest a writable Lease
    /// may be configured to wait between renewals is 30 seconds.
    const SLOWEST_CONFIGURED_REQUEST_INTERVAL: Duration = Duration::from_secs(30);

    fn configured(request_timeout: Duration) -> Config {
        let mut config = Config::new(
            "https://10.96.0.1:443"
                .parse()
                .expect("valid cluster address"),
        );
        apply_request_budget(&mut config, request_timeout);
        config
    }

    #[test]
    fn the_request_budget_bounds_one_request_and_never_the_idle_connection() {
        let budget = Duration::from_secs(2);
        let config = configured(budget);
        assert_eq!(config.connect_timeout, Some(budget));
        assert_eq!(config.write_timeout, Some(budget));
        assert!(!config.default_retry);
        assert!(
            config
                .read_timeout
                .is_some_and(|idle| idle > SLOWEST_CONFIGURED_REQUEST_INTERVAL),
            "the inactivity bound has to be set and has to sit well above the request \
             interval; at or under it, healthy pooled connections are retired and every \
             renewal pays a fresh handshake"
        );
    }
}
