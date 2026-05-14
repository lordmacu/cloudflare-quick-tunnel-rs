//! Top-level orchestrator. `QuickTunnelManager::start()` runs:
//!
//!   1. POST `/tunnel`               → `api::request_tunnel`
//!   2. Edge discovery               → `edge::discover`
//!   3. QUIC dial + register         → `connect_cycle` (helper)
//!   4. Spawn reactor task           → owns the QUIC connection,
//!      runs the supervisor, and reconnects on edge drop.
//!   5. Return handle holding `url` + shutdown channel.
//!
//! The reactor task is the long-lived owner: it cycles between
//! "supervise current connection" and "reconnect with backoff +
//! `replace_existing=true`" until the operator signals shutdown or
//! the reconnect attempt count exhausts.

use std::sync::Arc;
use std::time::Duration;

use quinn::Endpoint;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::api::{request_tunnel, DEFAULT_SERVICE_URL, DEFAULT_USER_AGENT};
use crate::edge::{discover, IpVersionFilter};
use crate::error::TunnelError;
use crate::pool::Pool;
use crate::quic_dial::{build_endpoint, dial_any};
use crate::rpc::{register_connection, ConnectionOptions, ControlSession, TunnelAuth};
use crate::supervisor::{self, SupervisorExit, SupervisorMetrics};

/// Default budget for POST + discovery + handshake + register.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default budget for the `unregisterConnection` RPC on shutdown.
pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(30);

/// Hard cap on consecutive reconnect failures before the reactor
/// gives up. Each failure widens the backoff (1s → 30s, exponential
/// with a cap), so 10 attempts spans roughly 90s of trying.
pub const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// How many parallel QUIC connections to register, each on a
/// distinct `conn_index`. Higher = more resilient to a single edge
/// POP dropping. cloudflared uses 4 for named tunnels and 1 for
/// quick tunnels; 2 is a happy middle that masks a single-POP
/// reconnect (~5s gap) without doubling traffic to localhost.
pub const DEFAULT_HA_CONNECTIONS: u8 = 2;

/// Hard ceiling — matches cloudflared. Going higher hits diminishing
/// returns and the edge may rate-limit registrations from one source.
pub const MAX_HA_CONNECTIONS: u8 = 4;

/// Crate version, baked into `ConnectionOptions.client.version`.
pub const CLIENT_VERSION: &str = concat!("cloudflare-quick-tunnel/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, Default)]
pub struct TunnelMetrics {
    pub streams_total: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub reconnects: u64,
}

pub struct QuickTunnelHandle {
    pub url: String,
    pub tunnel_id: Uuid,
    pub account_tag: String,
    /// Edge POP location of the first registered connection (e.g.
    /// `bog01`). Additional HA connections may land in different
    /// POPs; their locations are logged but not surfaced here.
    pub location: String,
    /// Fires the shutdown signal to every HA reactor at once.
    /// `Notify` is shared across N reactors via `Arc`; we don't
    /// use `oneshot` because we'd need one per reactor.
    shutdown: Arc<tokio::sync::Notify>,
    reactors: Vec<tokio::task::JoinHandle<()>>,
    metrics_view: SupervisorMetrics,
    reconnects: Arc<std::sync::atomic::AtomicU64>,
}

impl QuickTunnelHandle {
    pub fn metrics(&self) -> TunnelMetrics {
        let (s, i, o) = self.metrics_view.snapshot();
        TunnelMetrics {
            streams_total: s,
            bytes_in: i,
            bytes_out: o,
            reconnects: self.reconnects.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Signal every HA reactor to drain + unregister + close, then
    /// await them all.
    pub async fn shutdown_with(mut self, _grace: Duration) -> Result<(), TunnelError> {
        // `_grace` is honoured inside each reactor — they call
        // `ControlSession::shutdown_graceful(DEFAULT_GRACE_PERIOD)`
        // unconditionally on the way out. We keep the grace param
        // in the API for forward compatibility; once it matters,
        // pass it through via a richer shutdown command.
        self.shutdown.notify_waiters();
        for j in self.reactors.drain(..) {
            j.await
                .map_err(|e| TunnelError::Internal(format!("reactor join: {e}")))?;
        }
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), TunnelError> {
        self.shutdown_with(DEFAULT_GRACE_PERIOD).await
    }
}

impl Drop for QuickTunnelHandle {
    fn drop(&mut self) {
        // Notify all reactors; the detached tasks wind down on
        // their own. Drop is sync so we can't join them here.
        self.shutdown.notify_waiters();
    }
}

pub struct QuickTunnelManager {
    pub local_port: u16,
    pub discovery_timeout: Duration,
    pub service_url: String,
    pub user_agent: String,
    pub ha_connections: u8,
}

impl QuickTunnelManager {
    pub fn new(local_port: u16) -> Self {
        Self {
            local_port,
            discovery_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            service_url: DEFAULT_SERVICE_URL.into(),
            user_agent: DEFAULT_USER_AGENT.into(),
            ha_connections: DEFAULT_HA_CONNECTIONS,
        }
    }

    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.discovery_timeout = d;
        self
    }

    pub fn with_service_url(mut self, url: impl Into<String>) -> Self {
        self.service_url = url.into();
        self
    }

    pub fn with_user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = ua.into();
        self
    }

    /// Number of parallel QUIC connections to keep registered with
    /// the edge. Clamped to `1..=MAX_HA_CONNECTIONS`. Default is
    /// [`DEFAULT_HA_CONNECTIONS`] (2) — masks a single-POP drop
    /// without doubling tunnel cost.
    pub fn with_ha_connections(mut self, n: u8) -> Self {
        self.ha_connections = n.clamp(1, MAX_HA_CONNECTIONS);
        self
    }

    pub async fn start(self) -> Result<QuickTunnelHandle, TunnelError> {
        // Only the first connect cycle is bounded by `discovery_timeout`.
        // Subsequent reconnects from the reactor have their own backoff.
        tokio::time::timeout(self.discovery_timeout, self.start_inner())
            .await
            .map_err(|_| TunnelError::Internal("start() exceeded discovery_timeout".into()))?
    }

    async fn start_inner(self) -> Result<QuickTunnelHandle, TunnelError> {
        // 1. POST /tunnel — single call, the same credentials are
        //    reused on every connection + reconnect (the edge
        //    keys routing off `account_tag + tunnel_id`).
        let tunnel = request_tunnel(&self.service_url, &self.user_agent).await?;
        info!(hostname = %tunnel.hostname, id = %tunnel.id, ha = self.ha_connections, "got quick tunnel");
        let tunnel_id = Uuid::parse_str(&tunnel.id)
            .map_err(|e| TunnelError::Internal(format!("tunnel.id is not a uuid: {e}")))?;
        let url = if tunnel.hostname.starts_with("https://") {
            tunnel.hostname.clone()
        } else {
            format!("https://{}", tunnel.hostname)
        };

        let auth = TunnelAuth {
            account_tag: tunnel.account_tag.clone(),
            tunnel_secret: tunnel.secret.clone(),
        };

        // 2. Build the long-lived QUIC client endpoint. Endpoints
        //    are Clone — all HA reactors share the same UDP socket.
        let endpoint = build_endpoint()?;

        // 3. First connect cycle on `conn_index=0` —
        //    `replace_existing=false`. Synchronous so we have a
        //    URL + location to return immediately.
        let (conn0, control0, location0) =
            connect_cycle(&endpoint, &auth, tunnel_id, CLIENT_VERSION, 0, false).await?;
        info!(%location0, conn_index = 0, "first registration succeeded");

        let metrics = SupervisorMetrics::default();
        let reconnects = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let shutdown = Arc::new(tokio::sync::Notify::new());
        // One TCP keep-alive pool shared by every HA reactor —
        // they all proxy to the same `127.0.0.1:<local_port>` so a
        // single LIFO cache of idle sockets serves the whole tunnel.
        let pool = Arc::new(Pool::new(self.local_port));

        // Reactor for conn_index=0 (the one whose location we
        // already know).
        let mut reactors = Vec::with_capacity(self.ha_connections as usize);
        reactors.push(tokio::spawn(reactor_loop(
            self.local_port,
            endpoint.clone(),
            auth.clone(),
            tunnel_id,
            0,
            metrics.clone(),
            reconnects.clone(),
            pool.clone(),
            conn0,
            control0,
            shutdown.clone(),
        )));

        // 4. Spawn the remaining HA reactors in background — each
        //    does its own connect_cycle, on a distinct conn_index,
        //    with `replace_existing=false`. They keep retrying via
        //    their own reactor loop if the initial register fails.
        for idx in 1..self.ha_connections {
            let endpoint = endpoint.clone();
            let auth = auth.clone();
            let metrics = metrics.clone();
            let reconnects = reconnects.clone();
            let shutdown = shutdown.clone();
            let pool = pool.clone();
            let local_port = self.local_port;
            reactors.push(tokio::spawn(async move {
                match connect_cycle(&endpoint, &auth, tunnel_id, CLIENT_VERSION, idx, false).await {
                    Ok((conn, control, location)) => {
                        info!(%location, conn_index = idx, "HA registration succeeded");
                        reactor_loop(
                            local_port, endpoint, auth, tunnel_id, idx, metrics, reconnects, pool,
                            conn, control, shutdown,
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(error = %e, conn_index = idx, "HA registration failed; will retry");
                        reactor_loop_after_failure(
                            local_port, endpoint, auth, tunnel_id, idx, metrics, reconnects, pool,
                            shutdown,
                        )
                        .await;
                    }
                }
            }));
        }

        Ok(QuickTunnelHandle {
            url,
            tunnel_id,
            account_tag: tunnel.account_tag,
            location: location0,
            shutdown,
            reactors,
            metrics_view: metrics,
            reconnects,
        })
    }
}

/// Single attempt: dial the next edge, send `RegisterConnection`
/// on the supplied `conn_index`. `replace_existing=true` on
/// reconnects so the edge accepts the new conn for the same index
/// (the previous one was dropped); `false` on the very first
/// registration of each HA leg.
async fn connect_cycle(
    endpoint: &Endpoint,
    auth: &TunnelAuth,
    tunnel_id: Uuid,
    client_version: &str,
    conn_index: u8,
    replace_existing: bool,
) -> Result<(quinn::Connection, ControlSession, String), TunnelError> {
    let edges = discover(IpVersionFilter::Auto).await?;
    let cap = edges.len().min(5);
    let conn = dial_any(endpoint, &edges[..cap]).await?;

    let mut options = ConnectionOptions::default_for_quick_tunnel(client_version);
    options.replace_existing = replace_existing;

    let (details, control) =
        register_connection(&conn, auth, tunnel_id, conn_index, &options).await?;
    Ok((conn, control, details.location))
}

#[allow(clippy::too_many_arguments)]
async fn reactor_loop(
    local_port: u16,
    endpoint: Endpoint,
    auth: TunnelAuth,
    tunnel_id: Uuid,
    conn_index: u8,
    metrics: SupervisorMetrics,
    reconnects: Arc<std::sync::atomic::AtomicU64>,
    pool: Arc<Pool>,
    mut conn: quinn::Connection,
    mut control: ControlSession,
    shutdown: Arc<tokio::sync::Notify>,
) {
    debug!(conn_index, "reactor loop started");
    loop {
        // ── Supervise current connection ─────────────────────────────────────
        let (sup_tx, sup_rx) = oneshot::channel();
        let metrics_for_cycle = metrics.clone();
        let shutdown_wait = shutdown.notified();
        tokio::pin!(shutdown_wait);
        let exit = tokio::select! {
            biased;
            _ = &mut shutdown_wait => {
                // Caller wants us out. Forward to the supervisor
                // so its accept loop sees the shutdown branch and
                // closes the QUIC connection cleanly.
                let _ = sup_tx.send(());
                SupervisorExit::Shutdown
            }
            exit = supervisor::run(conn, local_port, metrics_for_cycle, pool.clone(), sup_rx) => exit,
        };

        match exit {
            SupervisorExit::Shutdown => {
                control.shutdown_graceful(DEFAULT_GRACE_PERIOD).await;
                debug!(conn_index, "reactor: clean shutdown");
                return;
            }
            SupervisorExit::ConnectionLost => {
                drop(control);

                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    if attempt > MAX_RECONNECT_ATTEMPTS {
                        warn!(
                            conn_index,
                            "reactor: giving up after {} reconnect attempts",
                            MAX_RECONNECT_ATTEMPTS
                        );
                        return;
                    }
                    let delay = backoff(attempt);
                    warn!(conn_index, attempt, ?delay, "reactor: scheduling reconnect");
                    let shutdown_wait = shutdown.notified();
                    tokio::pin!(shutdown_wait);
                    tokio::select! {
                        biased;
                        _ = shutdown_wait => {
                            debug!(conn_index, "reactor: shutdown during reconnect backoff");
                            return;
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                    match connect_cycle(
                        &endpoint,
                        &auth,
                        tunnel_id,
                        CLIENT_VERSION,
                        conn_index,
                        true,
                    )
                    .await
                    {
                        Ok((new_conn, new_control, new_loc)) => {
                            info!(conn_index, attempt, location = %new_loc, "reactor: reconnect succeeded");
                            reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            conn = new_conn;
                            control = new_control;
                            break;
                        }
                        Err(e) => {
                            warn!(attempt, error = %e, "reactor: reconnect failed");
                            // continue inner loop with bigger backoff
                        }
                    }
                }
            }
        }
    }
}

/// Entry point for an HA reactor whose initial register failed.
/// Loops with exp backoff trying to register; once one attempt
/// succeeds, hands off to the regular `reactor_loop`.
#[allow(clippy::too_many_arguments)]
async fn reactor_loop_after_failure(
    local_port: u16,
    endpoint: Endpoint,
    auth: TunnelAuth,
    tunnel_id: Uuid,
    conn_index: u8,
    metrics: SupervisorMetrics,
    reconnects: Arc<std::sync::atomic::AtomicU64>,
    pool: Arc<Pool>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        if attempt > MAX_RECONNECT_ATTEMPTS {
            warn!(
                conn_index,
                "HA reactor: giving up after {} initial-register attempts", MAX_RECONNECT_ATTEMPTS
            );
            return;
        }
        let delay = backoff(attempt);
        warn!(
            conn_index,
            attempt,
            ?delay,
            "HA reactor: scheduling initial register retry"
        );
        let shutdown_wait = shutdown.notified();
        tokio::pin!(shutdown_wait);
        tokio::select! {
            biased;
            _ = shutdown_wait => return,
            _ = tokio::time::sleep(delay) => {}
        }
        // First success of an HA leg's lifetime → replace_existing=false.
        let result = connect_cycle(
            &endpoint,
            &auth,
            tunnel_id,
            CLIENT_VERSION,
            conn_index,
            false,
        )
        .await;
        match result {
            Ok((conn, control, location)) => {
                info!(conn_index, %location, "HA leg eventually registered after {attempt} retries");
                // Hand off to the regular reactor for accept loop +
                // future reconnects.
                let shutdown = shutdown.clone();
                reactor_loop(
                    local_port, endpoint, auth, tunnel_id, conn_index, metrics, reconnects, pool,
                    conn, control, shutdown,
                )
                .await;
                return;
            }
            Err(e) => warn!(conn_index, attempt, error = %e, "HA register retry failed"),
        }
    }
}

/// Exponential backoff with a 30s ceiling: 1s, 2s, 4s, 8s, 16s, 30s, 30s, …
fn backoff(attempt: u32) -> Duration {
    let secs = 1u64.checked_shl(attempt.saturating_sub(1)).unwrap_or(30);
    Duration::from_secs(secs.min(30))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_curve() {
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(2), Duration::from_secs(2));
        assert_eq!(backoff(3), Duration::from_secs(4));
        assert_eq!(backoff(4), Duration::from_secs(8));
        assert_eq!(backoff(5), Duration::from_secs(16));
        assert_eq!(backoff(6), Duration::from_secs(30));
        assert_eq!(backoff(20), Duration::from_secs(30));
    }
}
