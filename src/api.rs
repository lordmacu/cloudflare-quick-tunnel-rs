//! POST `/tunnel` client for `api.trycloudflare.com`. Returns the
//! credentials the edge expects on the subsequent RegisterConnection
//! RPC. Implemented in Phase 92.2.

use serde::Deserialize;

use crate::error::{QuickTunnelApiError, TunnelError};

/// The full JSON envelope returned by `POST /tunnel`. Mirrors
/// `QuickTunnelResponse` from cloudflared/cmd/cloudflared/tunnel/quick_tunnel.go.
#[derive(Debug, Deserialize)]
pub struct QuickTunnelResponse {
    pub success: bool,
    pub result: QuickTunnel,
    #[serde(default)]
    pub errors: Vec<QuickTunnelApiError>,
}

/// The inner `result` body — the bits we actually need to drive the
/// QUIC handshake + RegisterConnection that follow.
#[derive(Debug, Deserialize)]
pub struct QuickTunnel {
    pub id: String,
    pub name: String,
    pub hostname: String,
    pub account_tag: String,
    /// 32 random bytes the edge pre-shares with the client for this
    /// quick tunnel. Used as the `TunnelSecret` in the
    /// `RegisterConnection` Cap'n Proto auth blob.
    #[serde(with = "serde_bytes")]
    pub secret: Vec<u8>,
}

// `serde_bytes` lifted inline to avoid a workspace-wide dep churn for
// the scaffold step.  Will be replaced with the `serde_bytes` crate in
// 92.2 if we end up needing the same behaviour anywhere else.
mod serde_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s: String = Deserialize::deserialize(d)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

/// Default service endpoint (the public trycloudflare API). Overridable
/// for tests via `QuickTunnelManager::with_service_url`.
pub const DEFAULT_SERVICE_URL: &str = "https://api.trycloudflare.com";

/// User-Agent the edge expects to see from a `cloudflared`-class
/// client. Pin a recent stable so we don't trip novelty filters.
/// Updated in lockstep with the schema commit recorded in
/// `THIRD_PARTY_NOTICES.md`.
pub const DEFAULT_USER_AGENT: &str = "cloudflared/2024.12.0";

/// Phase 92.2 will implement this against `reqwest` + the workspace's
/// `nexo-resilience::CircuitBreaker` (3× exponential backoff on 5xx,
/// no retry on 4xx).
pub async fn request_tunnel(
    _service_url: &str,
    _user_agent: &str,
) -> Result<QuickTunnel, TunnelError> {
    Err(TunnelError::Internal(
        "api::request_tunnel: not implemented until Phase 92.2".into(),
    ))
}
