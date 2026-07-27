//! Shared Kubernetes client transport policy for the orchestrator.

use std::time::Duration;

use kube::Config;

/// Longest a pooled connection may go without the API server saying anything.
///
/// `hyper-timeout` starts this countdown whenever a socket read returns
/// `Pending`, which includes the read a keep-alive connection parks on between
/// requests. It bounds a connection's inactivity rather than any request, so at
/// or under the interval at which this process issues them it retires healthy
/// connections instead of dead ones, and every request that follows pays a
/// fresh TCP and TLS handshake inside a budget that reserved nothing for one.
///
/// It is one of two bounds on an idle connection, and deliberately the looser.
/// `hyper-util` pools with its own 90 second idle timeout, which `kube` does not
/// override, and applies it at checkout rather than from a reaper task, so an
/// entry idle past 90 seconds is discarded whether or not this has fired. The
/// longest interval the validated configuration allows between two requests
/// from one client — a leader renewing against the 300 second maximum Lease, so
/// 100 seconds — already sits outside that window, and no value here restores
/// reuse there.
///
/// This is therefore sized to be comfortably above every configured cadence
/// rather than derived from the slowest one: it must never be the binding
/// eviction. Wherever reuse is reachable at all, the pool's window is what
/// decides, and no timing the configuration accepts can turn this into the
/// thing that retires connections. Shortening it, or lengthening
/// `pool_idle_timeout` past it, moves that decision here.
///
/// A finite value is still kept rather than `kube`'s own `None`, because the
/// pool expires an entry only when something tries to check it out. A peer that
/// stops answering without closing — a dropped conntrack or NAT entry leaves
/// the socket established and no FIN arrives — otherwise holds its socket for
/// as long as the process runs.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_mins(5);

/// Applies the orchestrator's transport policy for a per-request budget.
///
/// The budget itself is enforced by the `tokio::time::timeout` each caller
/// wraps its request in. Only the phases that belong to one request are held
/// to it.
///
/// Connections now survive between polling rounds instead of being retired a
/// request budget after use, so peak socket count is unchanged but the
/// steady-state idle count rises. The identity binder alone fans out to
/// `MAX_CONCURRENT_BINDINGS` requests at a time and `hyper-util` leaves
/// `max_idle_per_host` at `usize::MAX`, so what bounds those sockets is the
/// pool's 90 second window, not a cap.
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
    use std::cmp;
    use std::path::{Path, PathBuf};

    use crate::config::{
        MAXIMUM_KUBERNETES_LEASE_DURATION_SECONDS, MAXIMUM_KUBERNETES_LEASE_RETRY_MILLIS,
    };
    use crate::coordination::LEADER_RENEWAL_DIVISOR;

    use super::*;

    /// Every way this crate can reach a `kube::Config` or `Client` without
    /// building one from an existing `Config`. `incluster_env` and
    /// `incluster_dns` are covered by the `incluster` prefix.
    const CLIENT_CONSTRUCTORS: [&str; 5] = [
        "Config::incluster",
        "Config::infer",
        "Config::from_kubeconfig",
        "Config::from_custom_kubeconfig",
        "Client::try_default",
    ];

    /// The longest the validated configuration bounds allow one client to sit
    /// between two Kubernetes requests: a leader renewing against the longest
    /// accepted Lease, or an observer polling at the slowest accepted cadence.
    fn slowest_configured_request_interval() -> Duration {
        cmp::max(
            Duration::from_secs(MAXIMUM_KUBERNETES_LEASE_DURATION_SECONDS) / LEADER_RENEWAL_DIVISOR,
            Duration::from_millis(MAXIMUM_KUBERNETES_LEASE_RETRY_MILLIS),
        )
    }

    fn configured(request_timeout: Duration) -> Config {
        let mut config = Config::new(
            "https://10.96.0.1:443"
                .parse()
                .expect("valid cluster address"),
        );
        apply_request_budget(&mut config, request_timeout);
        config
    }

    fn rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("readable source directory") {
            let path = entry.expect("readable directory entry").path();
            if path.is_dir() {
                rust_sources(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }

    #[test]
    fn the_request_budget_bounds_one_request_and_never_the_idle_connection() {
        let budget = Duration::from_secs(1);
        let config = configured(budget);
        assert_eq!(config.connect_timeout, Some(budget));
        assert_eq!(config.write_timeout, Some(budget));
        assert!(!config.default_retry);
        assert!(
            config
                .read_timeout
                .is_some_and(|idle| idle > slowest_configured_request_interval()),
            "the inactivity bound has to be set and has to sit above every configured request \
             interval, so that it is never what retires a connection; at or under one, it \
             evicts ahead of the pool's own window and every request pays a fresh handshake"
        );
    }

    #[test]
    fn every_kubernetes_client_takes_its_transport_policy_from_here() {
        let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sources = Vec::new();
        rust_sources(&source_root, &mut sources);
        let policy = source_root.join("kube_transport.rs");
        assert!(sources.contains(&policy), "the policy module is a source");

        let mut divergent = Vec::new();
        let mut unpoliced = Vec::new();
        for source in sources {
            if source == policy {
                continue;
            }
            let text = std::fs::read_to_string(&source).expect("readable Rust source");
            if [
                "connect_timeout",
                "read_timeout",
                "write_timeout",
                "default_retry",
            ]
            .iter()
            .any(|field| text.contains(field))
            {
                divergent.push(source.clone());
            }
            // Matching the call rather than the name keeps a bare `use` of the
            // policy from standing in for applying it.
            let clients: usize = CLIENT_CONSTRUCTORS
                .iter()
                .map(|constructor| text.matches(constructor).count())
                .sum();
            if clients != text.matches("apply_request_budget(").count() {
                unpoliced.push(source);
            }
        }
        assert!(
            divergent.is_empty(),
            "transport timeouts belong to this module alone, because the read timeout is an \
             inactivity bound and not a request deadline: {divergent:?}"
        );
        assert!(
            unpoliced.is_empty(),
            "a Kubernetes client built without the shared policy inherits the kube defaults, \
             including client-side retries that spend an unknown multiple of one request \
             budget: {unpoliced:?}"
        );
    }
}
