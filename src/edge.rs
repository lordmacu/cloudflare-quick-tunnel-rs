//! Cloudflare edge discovery: DNS SRV
//! (`_v2-origintunneld._tcp.argotunnel.com`) with a DNS-over-TLS
//! fallback through `1.1.1.1:853`. Mirrors the semantics of
//! `cloudflared/edgediscovery/allregions/discovery.go`.
//!
//! The result is a list of `EdgeAddr`s (resolved IPs + port 7844)
//! the caller can hand to `quic_dial::dial_any`. Order is shuffled
//! per-resolution so two adjacent processes don't pin the same
//! edge, and an in-memory cache with a 1h TTL keeps repeated
//! reconnects from hammering DNS.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::error::TunnelError;

/// SRV record we resolve to discover the v2 origintunneld pool.
pub const SRV_NAME: &str = "_v2-origintunneld._tcp.argotunnel.com";

/// Server name for the DoT fallback resolver.
pub const DOT_SERVER_NAME: &str = "cloudflare-dns.com";

/// DoT endpoint address (Cloudflare public resolver).
pub const DOT_SERVER_ADDR: &str = "1.1.1.1:853";

/// Default in-memory cache TTL for resolved edges.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IpVersionFilter {
    #[default]
    Auto,
    V4Only,
    V6Only,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl EdgeAddr {
    pub fn socket(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }

    fn from_ip(ip: IpAddr, port: u16) -> Self {
        let version = if ip.is_ipv4() {
            EdgeIpVersion::V4
        } else {
            EdgeIpVersion::V6
        };
        Self { ip, port, version }
    }

    fn matches(&self, filter: IpVersionFilter) -> bool {
        matches!(
            (filter, self.version),
            (IpVersionFilter::Auto, _)
                | (IpVersionFilter::V4Only, EdgeIpVersion::V4)
                | (IpVersionFilter::V6Only, EdgeIpVersion::V6)
        )
    }
}

/// One-shot discovery without caching. System resolver first; on
/// failure / empty answer, falls back to DoT via `1.1.1.1`.
pub async fn discover(filter: IpVersionFilter) -> Result<Vec<EdgeAddr>, TunnelError> {
    let primary = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
    match resolve_srv(&primary, filter).await {
        Ok(edges) if !edges.is_empty() => return Ok(edges),
        Ok(_) => warn!("system resolver returned zero edges; falling back to DoT"),
        Err(e) => warn!(error = %e, "system resolver SRV failed; falling back to DoT"),
    }

    let dot = build_dot_resolver()?;
    let edges = resolve_srv(&dot, filter).await?;
    if edges.is_empty() {
        return Err(TunnelError::Discovery(format!(
            "DoT fallback also returned no edges for {SRV_NAME}"
        )));
    }
    Ok(edges)
}

/// In-memory cache around `discover`. Re-resolves once the TTL
/// expires; otherwise hands out a fresh shuffle of the previous
/// result so callers see a different head edge across calls.
#[derive(Clone)]
pub struct EdgeRegistry {
    inner: Arc<RwLock<Option<Cached>>>,
    ttl: Duration,
}

struct Cached {
    edges: Vec<EdgeAddr>,
    expires_at: Instant,
    filter: IpVersionFilter,
}

impl EdgeRegistry {
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_CACHE_TTL)
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
            ttl,
        }
    }

    pub async fn get_or_refresh(
        &self,
        filter: IpVersionFilter,
    ) -> Result<Vec<EdgeAddr>, TunnelError> {
        {
            let guard = self.inner.read().await;
            if let Some(c) = guard.as_ref() {
                if c.filter == filter && c.expires_at > Instant::now() {
                    debug!(count = c.edges.len(), "edge cache hit");
                    return Ok(shuffled(&c.edges));
                }
            }
        }
        let edges = discover(filter).await?;
        let mut guard = self.inner.write().await;
        *guard = Some(Cached {
            edges: edges.clone(),
            expires_at: Instant::now() + self.ttl,
            filter,
        });
        Ok(shuffled(&edges))
    }
}

impl Default for EdgeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Internals ─────────────────────────────────────────────────────────────────

fn build_dot_resolver() -> Result<TokioAsyncResolver, TunnelError> {
    let addr: SocketAddr = DOT_SERVER_ADDR
        .parse()
        .map_err(|e| TunnelError::Discovery(format!("DoT addr parse: {e}")))?;
    let ns = NameServerConfigGroup::from_ips_tls(
        &[addr.ip()],
        addr.port(),
        DOT_SERVER_NAME.into(),
        true,
    );
    let cfg = ResolverConfig::from_parts(None, vec![], ns);
    let mut opts = ResolverOpts::default();
    opts.timeout = Duration::from_secs(15);
    Ok(TokioAsyncResolver::tokio(cfg, opts))
}

async fn resolve_srv(
    resolver: &TokioAsyncResolver,
    filter: IpVersionFilter,
) -> Result<Vec<EdgeAddr>, TunnelError> {
    let srv = resolver
        .srv_lookup(SRV_NAME)
        .await
        .map_err(|e| TunnelError::Discovery(format!("SRV {SRV_NAME}: {e}")))?;

    let mut edges = Vec::new();
    for rec in srv.iter() {
        let target = rec.target().to_utf8();
        let target = target.trim_end_matches('.');
        let port = rec.port();
        match resolver.lookup_ip(target).await {
            Ok(ips) => {
                for ip in ips.iter() {
                    let edge = EdgeAddr::from_ip(ip, port);
                    if edge.matches(filter) {
                        edges.push(edge);
                    }
                }
            }
            Err(e) => warn!(target, error = %e, "IP resolution failed for SRV target"),
        }
    }
    Ok(edges)
}

fn shuffled(input: &[EdgeAddr]) -> Vec<EdgeAddr> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    Instant::now().elapsed().as_nanos().hash(&mut h);
    let n = input.len().max(1);
    let offset = (h.finish() as usize) % n;
    let mut out = Vec::with_capacity(input.len());
    out.extend_from_slice(&input[offset..]);
    out.extend_from_slice(&input[..offset]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn fake(ip: u8) -> EdgeAddr {
        EdgeAddr {
            ip: IpAddr::V4(Ipv4Addr::new(198, 41, 192, ip)),
            port: 7844,
            version: EdgeIpVersion::V4,
        }
    }

    #[test]
    fn filter_matches_auto() {
        let e = fake(1);
        assert!(e.matches(IpVersionFilter::Auto));
        assert!(e.matches(IpVersionFilter::V4Only));
        assert!(!e.matches(IpVersionFilter::V6Only));
    }

    #[test]
    fn shuffle_preserves_set() {
        let input: Vec<_> = (0..8).map(fake).collect();
        let out = shuffled(&input);
        assert_eq!(out.len(), input.len());
        let mut in_ips: Vec<_> = input.iter().map(|e| e.ip).collect();
        let mut out_ips: Vec<_> = out.iter().map(|e| e.ip).collect();
        in_ips.sort();
        out_ips.sort();
        assert_eq!(in_ips, out_ips);
    }

    #[tokio::test]
    async fn registry_serves_cached_within_ttl() {
        let reg = EdgeRegistry::with_ttl(Duration::from_secs(60));
        {
            let mut g = reg.inner.write().await;
            *g = Some(Cached {
                edges: vec![fake(7), fake(8)],
                expires_at: Instant::now() + Duration::from_secs(60),
                filter: IpVersionFilter::Auto,
            });
        }
        let got = reg.get_or_refresh(IpVersionFilter::Auto).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    /// Real edge discovery. Gated so CI doesn't pound DNS on every
    /// PR; opt-in with `CFQT_LIVE_TESTS=1`.
    #[tokio::test]
    #[ignore]
    async fn live_discover_returns_edges() {
        if std::env::var_os("CFQT_LIVE_TESTS").is_none() {
            eprintln!("skip: set CFQT_LIVE_TESTS=1 to run");
            return;
        }
        let edges = discover(IpVersionFilter::Auto).await.unwrap();
        assert!(!edges.is_empty(), "should resolve at least one edge");
        for e in &edges {
            assert_eq!(e.port, 7844);
        }
    }
}
