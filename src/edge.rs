//! Cloudflare edge discovery: DNS SRV (`_v2-origintunneld._tcp.argotunnel.com`)
//! with a DNS-over-TLS fallback through `1.1.1.1:853`. Mirrors the
//! semantics of `cloudflared/edgediscovery/allregions/discovery.go`.
//! Implemented in Phase 92.3.

use std::net::IpAddr;
use std::time::Duration;

/// SRV record we resolve to discover the v2 origintunneld pool.
pub const SRV_NAME: &str = "_v2-origintunneld._tcp.argotunnel.com";

/// Server name for the DoT fallback resolver.
pub const DOT_SERVER_NAME: &str = "cloudflare-dns.com";

/// Default in-memory cache TTL for resolved edges.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy)]
pub enum EdgeIpVersion {
    V4,
    V6,
}

#[derive(Debug, Clone, Copy)]
pub struct EdgeAddr {
    pub ip: IpAddr,
    pub port: u16,
    pub version: EdgeIpVersion,
}

/// Phase 92.3 fills this in (SRV + IP fanout + DoT fallback + shuffle).
pub async fn discover() -> Result<Vec<EdgeAddr>, crate::TunnelError> {
    Err(crate::TunnelError::Internal(
        "edge::discover: not implemented until Phase 92.3".into(),
    ))
}
