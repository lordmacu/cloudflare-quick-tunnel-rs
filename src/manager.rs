//! Top-level orchestrator. `QuickTunnelManager::start()` runs the
//! full flow:
//!
//!   1. POST `/tunnel`               → `api::request_tunnel`
//!   2. Edge discovery               → `edge::discover`
//!   3. QUIC dial                    → `quic_dial`
//!   4. capnp-RPC RegisterConnection → `rpc`
//!   5. Spawn supervisor task        → `supervisor`
//!   6. Return handle holding `url` + `shutdown` channel
//!
//! Wired end-to-end in Phase 92.8 once 92.2–92.7 land.

use std::time::Duration;

use uuid::Uuid;

use crate::api::{DEFAULT_SERVICE_URL, DEFAULT_USER_AGENT};
use crate::error::TunnelError;

/// Default budget for the URL-discovery + register handshake before
/// `start()` gives up. Matches the legacy `nexo-tunnel` default so
/// callers see the same behaviour after the drop-in swap.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

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
}

impl QuickTunnelHandle {
    pub async fn shutdown(self) -> Result<(), TunnelError> {
        Err(TunnelError::Internal(
            "QuickTunnelHandle::shutdown: not implemented until Phase 92.7".into(),
        ))
    }

    pub fn metrics(&self) -> TunnelMetrics {
        TunnelMetrics::default()
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
        Err(TunnelError::Internal(
            "QuickTunnelManager::start: not wired end-to-end until Phase 92.8".into(),
        ))
    }
}
