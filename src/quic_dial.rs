//! QUIC dial into the argotunnel edge using stock `quinn` +
//! `rustls`. The handshake recipe was nailed down in the design
//! spike (see `docs/spike-verdict.md`):
//!
//!   - ALPN  `argotunnel`
//!   - SNI   `quic.cftunnel.com`  (NOT the ALPN)
//!   - trust system roots + three CF-internal CAs vendored at
//!     `cf-edge-roots.pem`.
//!
//! Public surface:
//!   - [`dial`]      — one shot against a single `EdgeAddr`.
//!   - [`dial_any`]  — try a slice of edges in order, return on
//!                     the first success or the last error.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use quinn::{ClientConfig, Endpoint, TransportConfig};
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::edge::EdgeAddr;
use crate::error::TunnelError;

/// ALPN advertised in the TLS ClientHello to the edge.
pub const ALPN: &[u8] = b"argotunnel";

/// SNI server name used in the TLS ClientHello.
pub const EDGE_SNI: &str = "quic.cftunnel.com";

/// Three CF-internal CAs that sign `*.cftunnel.com`. Sourced from
/// `cloudflared/tlsconfig/cloudflare_ca.go` (Apache-2.0). Without
/// them the edge cert chain fails to verify with `UnknownIssuer`.
pub const CF_EDGE_ROOTS_PEM: &[u8] = include_bytes!("../cf-edge-roots.pem");

/// Hard cap for the handshake. The edge itself uses 5s
/// (`cloudflared/quic/constants.go: HandshakeIdleTimeout`); 10s
/// gives a comfortable margin without making failures sluggish.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Dial a single edge. Caller owns the `Endpoint` so its UDP
/// socket stays alive across multiple dials.
pub async fn dial(endpoint: &Endpoint, edge: &EdgeAddr) -> Result<quinn::Connection, TunnelError> {
    let addr: SocketAddr = edge.socket();
    debug!(%addr, "QUIC connect");
    let connecting = endpoint
        .connect(addr, EDGE_SNI)
        .map_err(|e| TunnelError::QuicDial {
            attempts: 1,
            last: format!("connect builder: {e}"),
        })?;
    match timeout(DEFAULT_HANDSHAKE_TIMEOUT, connecting).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(TunnelError::QuicDial {
            attempts: 1,
            last: format!("handshake: {e}"),
        }),
        Err(_) => Err(TunnelError::QuicDial {
            attempts: 1,
            last: "handshake timed out".into(),
        }),
    }
}

/// Try a list of edges in order; stop on first successful handshake.
pub async fn dial_any(
    endpoint: &Endpoint,
    edges: &[EdgeAddr],
) -> Result<quinn::Connection, TunnelError> {
    if edges.is_empty() {
        return Err(TunnelError::QuicDial {
            attempts: 0,
            last: "no edges provided".into(),
        });
    }
    let mut last_err = String::new();
    for (i, edge) in edges.iter().enumerate() {
        match dial(endpoint, edge).await {
            Ok(c) => {
                info!(attempt = i, addr = %edge.socket(), "QUIC handshake OK");
                return Ok(c);
            }
            Err(e) => {
                warn!(attempt = i, addr = %edge.socket(), error = %e, "QUIC dial failed");
                last_err = e.to_string();
            }
        }
    }
    Err(TunnelError::QuicDial {
        attempts: edges.len(),
        last: last_err,
    })
}

/// Build a client `Endpoint` ready to dial the argotunnel edge.
/// Binds an ephemeral UDP socket on `0.0.0.0:0`; one Endpoint can
/// dial many edges concurrently, so callers typically build it
/// once at supervisor start.
pub fn build_endpoint() -> Result<Endpoint, TunnelError> {
    install_crypto_provider();
    let config = build_client_config()?;
    let local: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let mut endpoint = Endpoint::client(local)
        .map_err(|e| TunnelError::Internal(format!("Endpoint::client bind: {e}")))?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

/// Install rustls's ring-backed crypto provider exactly once
/// per-process. Other consumers in the same binary (reqwest,
/// hickory DoT) share it.
fn install_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn build_client_config() -> Result<ClientConfig, TunnelError> {
    let mut roots = RootCertStore::empty();

    match rustls_native_certs::load_native_certs() {
        Ok(certs) => {
            for c in certs {
                let der = CertificateDer::from(c.to_vec());
                let _ = roots.add(der);
            }
        }
        Err(e) => warn!(error = %e, "failed to load native CA roots; relying on embedded CF roots"),
    }
    let native_count = roots.len();

    let mut cf_added = 0usize;
    let mut reader = std::io::BufReader::new(CF_EDGE_ROOTS_PEM);
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert =
            cert.map_err(|e| TunnelError::Internal(format!("CF root PEM malformed: {e}")))?;
        if roots.add(cert).is_ok() {
            cf_added += 1;
        }
    }
    debug!(
        native = native_count,
        cf_added,
        total = roots.len(),
        "trust anchors built"
    );

    let mut tls = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| TunnelError::Internal(format!("rustls→quinn: {e}")))?;
    let mut cfg = ClientConfig::new(Arc::new(crypto));

    let mut transport = TransportConfig::default();
    transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    transport.keep_alive_interval(Some(Duration::from_secs(1)));
    cfg.transport_config(Arc::new(transport));

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::{EdgeIpVersion, IpVersionFilter};
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn endpoint_builds() {
        // quinn::Endpoint::client wires its UDP socket through the
        // tokio runtime, so this must run on the multi-thread
        // runner just like dial paths do.
        let endpoint = build_endpoint().expect("endpoint should build");
        drop(endpoint);
    }

    #[tokio::test]
    async fn dial_any_empty_short_circuits() {
        let endpoint = build_endpoint().unwrap();
        let err = dial_any(&endpoint, &[]).await.unwrap_err();
        match err {
            TunnelError::QuicDial { attempts, .. } => assert_eq!(attempts, 0),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn dial_unreachable_addr_times_out_fast() {
        let endpoint = build_endpoint().unwrap();
        let bogus = EdgeAddr {
            ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            port: 7844,
            version: EdgeIpVersion::V4,
        };
        let _ = IpVersionFilter::Auto;
        let result = tokio::time::timeout(
            Duration::from_secs(12),
            dial_any(&endpoint, std::slice::from_ref(&bogus)),
        )
        .await
        .expect("outer timeout shouldn't fire");
        assert!(result.is_err(), "TEST-NET should never connect");
    }

    /// Gated live smoke (same as the spike binary) — `CFQT_LIVE_TESTS=1`.
    #[tokio::test]
    #[ignore]
    async fn live_handshake_against_edge() {
        if std::env::var_os("CFQT_LIVE_TESTS").is_none() {
            eprintln!("skip: set CFQT_LIVE_TESTS=1 to run");
            return;
        }
        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
        let edges = crate::edge::discover(IpVersionFilter::Auto)
            .await
            .expect("discover");
        let endpoint = build_endpoint().expect("endpoint");
        let conn = dial_any(&endpoint, &edges[..3]).await.expect("handshake");
        drop(conn);
        endpoint.wait_idle().await;
    }
}
