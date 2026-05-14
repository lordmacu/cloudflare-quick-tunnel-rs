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

use std::time::Duration;

use quinn::Endpoint;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::api::{request_tunnel, DEFAULT_SERVICE_URL, DEFAULT_USER_AGENT};
use crate::edge::{discover, IpVersionFilter};
use crate::error::TunnelError;
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
    pub location: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    reactor: Option<tokio::task::JoinHandle<()>>,
    metrics_view: SupervisorMetrics,
    reconnects: std::sync::Arc<std::sync::atomic::AtomicU64>,
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

    /// Signal the reactor to drain + unregister + close. Awaits
    /// the reactor task to fully finish.
    pub async fn shutdown_with(mut self, _grace: Duration) -> Result<(), TunnelError> {
        // `_grace` is honoured inside the reactor — it calls
        // `ControlSession::shutdown_graceful(DEFAULT_GRACE_PERIOD)`
        // unconditionally on the way out. We keep the grace param
        // in the API for forward compatibility; once it matters,
        // pass it through via a richer shutdown command.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.reactor.take() {
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
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // We can't await the reactor here — Drop is sync. The
        // detached task winds down on its own.
    }
}

pub struct QuickTunnelManager {
    pub local_port: u16,
    pub discovery_timeout: Duration,
    pub service_url: String,
    pub user_agent: String,
}

impl QuickTunnelManager {
    pub fn new(local_port: u16) -> Self {
        Self {
            local_port,
            discovery_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            service_url: DEFAULT_SERVICE_URL.into(),
            user_agent: DEFAULT_USER_AGENT.into(),
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

    pub async fn start(self) -> Result<QuickTunnelHandle, TunnelError> {
        // Only the first connect cycle is bounded by `discovery_timeout`.
        // Subsequent reconnects from the reactor have their own backoff.
        tokio::time::timeout(self.discovery_timeout, self.start_inner())
            .await
            .map_err(|_| TunnelError::Internal("start() exceeded discovery_timeout".into()))?
    }

    async fn start_inner(self) -> Result<QuickTunnelHandle, TunnelError> {
        // 1. POST /tunnel — single call, the same credentials are
        //    reused on every reconnect (the edge keys routing off
        //    `account_tag + tunnel_id`).
        let tunnel = request_tunnel(&self.service_url, &self.user_agent).await?;
        info!(hostname = %tunnel.hostname, id = %tunnel.id, "got quick tunnel");
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

        // 2. Build the long-lived QUIC client endpoint. Reused
        //    across reconnect cycles so the UDP socket stays stable.
        let endpoint = build_endpoint()?;

        // 3. First connect cycle — `replace_existing=false`.
        let (conn, control, location) =
            connect_cycle(&endpoint, &auth, tunnel_id, CLIENT_VERSION, false).await?;
        info!(%location, "first registration succeeded");

        let metrics = SupervisorMetrics::default();
        let reconnects = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let reactor = tokio::spawn(reactor_loop(
            self.local_port,
            endpoint,
            auth,
            tunnel_id,
            metrics.clone(),
            reconnects.clone(),
            conn,
            control,
            shutdown_rx,
        ));

        Ok(QuickTunnelHandle {
            url,
            tunnel_id,
            account_tag: tunnel.account_tag,
            location,
            shutdown_tx: Some(shutdown_tx),
            reactor: Some(reactor),
            metrics_view: metrics,
            reconnects,
        })
    }
}

/// Single attempt: dial the next edge, send `RegisterConnection`.
/// `replace_existing=true` on reconnects so the edge accepts the
/// new conn for `conn_index=0` (the previous one was dropped).
async fn connect_cycle(
    endpoint: &Endpoint,
    auth: &TunnelAuth,
    tunnel_id: Uuid,
    client_version: &str,
    replace_existing: bool,
) -> Result<(quinn::Connection, ControlSession, String), TunnelError> {
    let edges = discover(IpVersionFilter::Auto).await?;
    let cap = edges.len().min(5);
    let conn = dial_any(endpoint, &edges[..cap]).await?;

    let mut options = ConnectionOptions::default_for_quick_tunnel(client_version);
    options.replace_existing = replace_existing;

    let (details, control) = register_connection(&conn, auth, tunnel_id, 0, &options).await?;
    Ok((conn, control, details.location))
}

#[allow(clippy::too_many_arguments)]
async fn reactor_loop(
    local_port: u16,
    endpoint: Endpoint,
    auth: TunnelAuth,
    tunnel_id: Uuid,
    metrics: SupervisorMetrics,
    reconnects: std::sync::Arc<std::sync::atomic::AtomicU64>,
    mut conn: quinn::Connection,
    mut control: ControlSession,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    debug!("reactor loop started");
    loop {
        // ── Supervise current connection ─────────────────────────────────────
        let (sup_tx, sup_rx) = oneshot::channel();
        let metrics_for_cycle = metrics.clone();
        let exit = tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                // Caller wants us out. Forward to the supervisor
                // so its accept loop sees the shutdown branch and
                // closes the QUIC connection cleanly.
                let _ = sup_tx.send(());
                SupervisorExit::Shutdown
            }
            exit = supervisor::run(conn, local_port, metrics_for_cycle, sup_rx) => exit,
        };

        match exit {
            SupervisorExit::Shutdown => {
                // Graceful unregister on the way out. Best-effort
                // — the edge may have closed already.
                control.shutdown_graceful(DEFAULT_GRACE_PERIOD).await;
                debug!("reactor: clean shutdown");
                return;
            }
            SupervisorExit::ConnectionLost => {
                // Throw away the dead control session — its RPC
                // stream lives on a connection that's gone.
                drop(control);

                // ── Reconnect with exponential backoff ────────────────────
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    if attempt > MAX_RECONNECT_ATTEMPTS {
                        warn!(
                            "reactor: giving up after {} reconnect attempts",
                            MAX_RECONNECT_ATTEMPTS
                        );
                        return;
                    }
                    let delay = backoff(attempt);
                    warn!(attempt, ?delay, "reactor: scheduling reconnect");
                    tokio::select! {
                        biased;
                        _ = &mut shutdown_rx => {
                            debug!("reactor: shutdown signal during reconnect backoff");
                            return;
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                    match connect_cycle(&endpoint, &auth, tunnel_id, CLIENT_VERSION, true).await {
                        Ok((new_conn, new_control, new_loc)) => {
                            info!(attempt, location = %new_loc, "reactor: reconnect succeeded");
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
