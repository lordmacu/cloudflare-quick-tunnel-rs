//! QUIC dial into the argotunnel edge using stock `quinn` + `rustls`.
//! The handshake recipe was nailed down in the Phase 92.0 spike (see
//! `docs/src/architecture/quick-tunnel-spike.md`):
//!
//!   - ALPN  `argotunnel`
//!   - SNI   `quic.cftunnel.com`  (NOT the ALPN)
//!   - trust system roots + three CF-internal CAs vendored under
//!     `crates/tunnel-quick/cf-edge-roots.pem`
//!
//! Implemented in Phase 92.4.

/// ALPN advertised in the TLS ClientHello to the edge.
pub const ALPN: &[u8] = b"argotunnel";

/// SNI server name used in the TLS ClientHello.
pub const EDGE_SNI: &str = "quic.cftunnel.com";

/// Embedded CF-internal CAs that sign `*.cftunnel.com`. Sourced from
/// `cloudflared/tlsconfig/cloudflare_ca.go` (Apache-2.0). Vendored
/// once here to keep the spike crate + the production crate on the
/// same trust anchors. Updated in lockstep with the schema commit
/// recorded in `THIRD_PARTY_NOTICES.md`.
pub const CF_EDGE_ROOTS_PEM: &[u8] = include_bytes!("../cf-edge-roots.pem");
