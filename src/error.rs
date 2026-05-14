//! Typed error model for the quick-tunnel client. Each variant
//! corresponds to a distinct failure mode that callers can act on
//! independently (API rejection vs DNS gap vs handshake refused vs
//! permanent supervisor giveup).

use thiserror::Error;

/// Business-level error returned inside the API response body when
/// `success = false`. Mirrors cloudflared's `QuickTunnelError`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct QuickTunnelApiError {
    pub code: i32,
    pub message: String,
}

#[derive(Error, Debug)]
pub enum TunnelError {
    #[error("quick-tunnel API request failed: {0}")]
    Api(#[from] reqwest::Error),

    #[error("quick-tunnel API returned business errors: {0:?}")]
    ApiBusiness(Vec<QuickTunnelApiError>),

    #[error("quick-tunnel API responded non-JSON ({status}): {body_snippet}")]
    ApiNonJson {
        status: u16,
        body_snippet: String,
    },

    #[error("edge discovery failed: {0}")]
    Discovery(String),

    #[error("QUIC dial failed after {attempts} attempt(s); last: {last}")]
    QuicDial { attempts: usize, last: String },

    #[error("capnp-RPC RegisterConnection failed: {0}")]
    Register(String),

    #[error("connection lost; supervisor giving up after {0} attempts")]
    PermanentFailure(u32),

    #[error("shutdown requested")]
    Shutdown,

    #[error("internal invariant violated: {0}")]
    Internal(String),
}
