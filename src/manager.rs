//! Top-level orchestrator. `QuickTunnelManager::start()` runs the
//! full flow:
//!
//!   1. POST `/tunnel`               → `api::request_tunnel`
//!   2. Edge discovery               → `edge::discover`
//!   3. QUIC dial                    → `quic_dial`
//!   4. capnp-RPC RegisterConnection → `rpc::register_connection`
//!   5. Spawn supervisor task        → `supervisor::start_supervisor`
//!   6. Return handle holding `url` + `shutdown` channel

use std::time::Duration;

use tracing::info;
use uuid::Uuid;

use crate::api::{request_tunnel, DEFAULT_SERVICE_URL, DEFAULT_USER_AGENT};
use crate::edge::{discover, IpVersionFilter};
use crate::error::TunnelError;
use crate::quic_dial::{build_endpoint, dial_any};
use crate::rpc::{register_connection, ConnectionOptions, TunnelAuth};
use crate::supervisor::{start_supervisor, SupervisorHandle, SupervisorMetrics};

/// Default budget for POST + discovery + handshake + register.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Crate version, baked into `ConnectionOptions.client.version`.
pub const CLIENT_VERSION: &str = concat!("cloudflare-quick-tunnel/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, Default)]
pub struct TunnelMetrics {
    pub streams_total: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

pub struct QuickTunnelHandle {
    pub url: String,
    pub tunnel_id: Uuid,
    pub account_tag: String,
    pub location: String,
    supervisor: Option<SupervisorHandle>,
    control: Option<crate::rpc::ControlSession>,
    metrics_view: SupervisorMetrics,
}

/// Default budget for the `unregisterConnection` RPC on shutdown.
/// Matches cloudflared's typical `GracePeriod` of 30s for clean
/// disconnects.
pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(30);

impl QuickTunnelHandle {
    pub fn metrics(&self) -> TunnelMetrics {
        let (s, i, o) = self.metrics_view.snapshot();
        TunnelMetrics {
            streams_total: s,
            bytes_in: i,
            bytes_out: o,
        }
    }

    /// Graceful shutdown: fires `unregisterConnection` on the
    /// control stream (giving the edge `grace` to stop routing),
    /// signals the supervisor, drains accepted streams, joins.
    pub async fn shutdown_with(mut self, grace: Duration) -> Result<(), TunnelError> {
        if let Some(control) = self.control.take() {
            control.shutdown_graceful(grace).await;
        }
        if let Some(sup) = self.supervisor.take() {
            let _ = sup.shutdown.send(());
            sup.join
                .await
                .map_err(|e| TunnelError::Internal(format!("supervisor join: {e}")))?;
        }
        Ok(())
    }

    /// Shorthand for `shutdown_with(DEFAULT_GRACE_PERIOD)`.
    pub async fn shutdown(self) -> Result<(), TunnelError> {
        self.shutdown_with(DEFAULT_GRACE_PERIOD).await
    }
}

impl Drop for QuickTunnelHandle {
    fn drop(&mut self) {
        // Fire-and-forget close if the caller dropped without
        // awaiting shutdown(). The QUIC connection's own Drop will
        // close the link, but signalling the supervisor lets it
        // exit its accept_bi loop cleanly. ControlSession's Drop
        // sends an Immediate shutdown — no async unregister.
        if let Some(sup) = self.supervisor.take() {
            let _ = sup.shutdown.send(());
        }
        // self.control drops next via field order, firing the
        // ShutdownCommand::Immediate path.
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
        tokio::time::timeout(self.discovery_timeout, self.start_inner())
            .await
            .map_err(|_| TunnelError::Internal("start() exceeded discovery_timeout".into()))?
    }

    async fn start_inner(self) -> Result<QuickTunnelHandle, TunnelError> {
        // 1. POST /tunnel
        let tunnel = request_tunnel(&self.service_url, &self.user_agent).await?;
        info!(hostname = %tunnel.hostname, id = %tunnel.id, "got quick tunnel");
        let tunnel_id = Uuid::parse_str(&tunnel.id)
            .map_err(|e| TunnelError::Internal(format!("tunnel.id is not a uuid: {e}")))?;
        let url = if tunnel.hostname.starts_with("https://") {
            tunnel.hostname.clone()
        } else {
            format!("https://{}", tunnel.hostname)
        };

        // 2. Edge discovery
        let edges = discover(IpVersionFilter::Auto).await?;

        // 3. QUIC dial — keep the Endpoint alive past start() by
        //    handing it to the supervisor (it owns the underlying
        //    UDP socket).
        let endpoint = build_endpoint()?;
        let cap = edges.len().min(5);
        let conn = dial_any(&endpoint, &edges[..cap]).await?;

        // 4. RegisterConnection on the first stream of `conn`. The
        //    capnp-RPC client lives only for this call (system
        //    drops afterwards). QUIC keepalive on the dial config
        //    keeps the link healthy past register.
        let auth = TunnelAuth {
            account_tag: tunnel.account_tag.clone(),
            tunnel_secret: tunnel.secret.clone(),
        };
        let options = ConnectionOptions::default_for_quick_tunnel(CLIENT_VERSION);
        let (details, control_session) =
            register_connection(&conn, &auth, tunnel_id, 0, &options).await?;
        info!(uuid = %details.uuid, location = %details.location, "registered with edge");

        // 5. Spawn the inbound-accept supervisor. We intentionally
        //    leak the Endpoint into the supervisor's closure by way
        //    of `conn`; quinn keeps the socket alive while a
        //    Connection on it lives.
        let sup = start_supervisor(conn, self.local_port);
        let metrics_view = sup.metrics.clone();
        // Park the endpoint inside a detached task so its UDP
        // socket survives past start(). Drop ties to handle
        // lifetime: when the supervisor exits, this task exits
        // (the conn close from supervisor cascades).
        tokio::spawn(async move {
            let _hold = endpoint;
            let () = std::future::pending().await;
        });

        Ok(QuickTunnelHandle {
            url,
            tunnel_id,
            account_tag: tunnel.account_tag,
            location: details.location,
            supervisor: Some(sup),
            control: Some(control_session),
            metrics_view,
        })
    }
}
