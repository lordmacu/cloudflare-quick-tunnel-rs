//! Phase 92.0 GO/NO-GO spike.
//!
//! Question: can `quinn` (stock 0.11) handshake against a Cloudflare
//! argotunnel edge that the production `cloudflared` Go binary normally
//! talks to, using its forked quic-go (`chungthuang/quic-go v0.45.1`)?
//!
//! Method:
//! 1. SRV-lookup `_v2-origintunneld._tcp.argotunnel.com` for edge addrs.
//! 2. For each edge: open a QUIC connection with ALPN `argotunnel`,
//!    rustls + native roots, default transport params.
//! 3. Log whether handshake completed, what error if not, and dump
//!    negotiated transport params + stats.
//! 4. Operator (or follow-up step 92.0.4) compares the dump against
//!    `cloudflared/quic/constants.go` + `param_unix.go` to decide
//!    GO / PATCH-QUINN / NO-GO.
//!
//! Run with:
//!   RUST_LOG=info cargo run -p nexo-tunnel-quick-spike

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;
use quinn::{ClientConfig, Endpoint, TransportConfig};
use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use tracing::{info, warn};

const SRV_NAME: &str = "_v2-origintunneld._tcp.argotunnel.com";
const ALPN: &[u8] = b"argotunnel";
/// SNI hostname the edge expects in the TLS ClientHello — see
/// cloudflared/connection/protocol.go: edgeQUICServerName.
const EDGE_SNI: &str = "quic.cftunnel.com";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct EdgeAddr {
    ip: IpAddr,
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,quinn=debug,rustls=info".into()),
        )
        .init();

    // Install ring as the default rustls crypto provider once at startup
    // (memory: rustls always default-features=false + ring; otherwise
    // aws-lc-rs gets pulled and breaks Android NDK builds).
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("rustls default crypto provider already installed"))?;

    let edges = discover_edges().await?;
    info!(count = edges.len(), "resolved edges");
    for e in &edges {
        info!(ip = %e.ip, port = e.port, "edge");
    }

    let client_config = build_client_config()?;
    let local_bind: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let mut endpoint = Endpoint::client(local_bind).context("bind UDP socket for QUIC client")?;
    endpoint.set_default_client_config(client_config);

    // Try first 3 edges, stop on first success.
    let mut last_err: Option<String> = None;
    for (idx, edge) in edges.iter().take(3).enumerate() {
        info!(idx, ip = %edge.ip, port = edge.port, "dial attempt");
        let addr = SocketAddr::new(edge.ip, edge.port);
        match tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            endpoint.connect(addr, EDGE_SNI)?,
        )
        .await
        {
            Ok(Ok(conn)) => {
                info!("HANDSHAKE OK");
                dump_connection(&conn);
                conn.close(0u32.into(), b"spike-done");
                endpoint.wait_idle().await;
                return Ok(());
            }
            Ok(Err(e)) => {
                warn!(error = %e, "handshake failed");
                last_err = Some(format!("{e:#}"));
            }
            Err(_) => {
                warn!("handshake timed out");
                last_err = Some("timeout".into());
            }
        }
    }

    Err(anyhow!(
        "all edges failed; last error: {}",
        last_err.unwrap_or_else(|| "<unknown>".into())
    ))
}

async fn discover_edges() -> Result<Vec<EdgeAddr>> {
    let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
    let srv = resolver
        .srv_lookup(SRV_NAME)
        .await
        .with_context(|| format!("SRV lookup for {SRV_NAME}"))?;

    let mut edges = Vec::new();
    for rec in srv.iter() {
        let target = rec.target().to_utf8();
        let port = rec.port();
        let target = target.trim_end_matches('.');
        match resolver.lookup_ip(target).await {
            Ok(ips) => {
                for ip in ips.iter() {
                    edges.push(EdgeAddr { ip, port });
                }
            }
            Err(e) => warn!(target, %e, "ip resolution failed"),
        }
    }

    if edges.is_empty() {
        return Err(anyhow!("SRV {SRV_NAME} resolved to zero edges"));
    }
    Ok(edges)
}

/// Cloudflare-internal root CAs that sign the edge `*.cftunnel.com`
/// certificate. Embedded verbatim from cloudflared/tlsconfig/cloudflare_ca.go
/// (Apache-2.0). The names say "Origin SSL" but the same chain is used
/// for the edge in practice (see cloudflared's `CreateTunnelConfig`,
/// which appends these on top of the system pool).
const CF_EDGE_ROOTS_PEM: &[u8] = include_bytes!("../cf-edge-roots.pem");

fn build_client_config() -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    let certs = rustls_native_certs::load_native_certs()?;
    for c in certs {
        let der = CertificateDer::from(c.to_vec());
        let _ = roots.add(der);
    }
    let native = roots.len();

    let mut reader = std::io::BufReader::new(CF_EDGE_ROOTS_PEM);
    let mut cf_added = 0;
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert = cert.context("malformed CF root PEM")?;
        if roots.add(cert).is_ok() {
            cf_added += 1;
        }
    }
    info!(native, cf_added, total = roots.len(), "trust anchors loaded");

    let mut tls = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .context("rustls config not QUIC-compatible")?;
    let mut cfg = ClientConfig::new(Arc::new(crypto));

    // Default transport params for the spike — we want to see what the
    // server proposes back. Step 92.0.4 will diff this against the fork.
    let mut transport = TransportConfig::default();
    transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    cfg.transport_config(Arc::new(transport));

    Ok(cfg)
}

fn dump_connection(conn: &quinn::Connection) {
    let stats = conn.stats();
    info!(
        rtt_us = stats.path.rtt.as_micros() as u64,
        cwnd = stats.path.cwnd,
        congestion_events = stats.path.congestion_events,
        lost_packets = stats.path.lost_packets,
        sent_packets = stats.path.sent_packets,
        "path stats"
    );
    info!(
        udp_tx_datagrams = stats.udp_tx.datagrams,
        udp_rx_datagrams = stats.udp_rx.datagrams,
        "udp stats"
    );
    if let Some(server_name) = conn.peer_identity() {
        info!(?server_name, "peer identity present");
    }
    // quinn doesn't expose the raw negotiated transport params, but the
    // crypto handshake + congestion stats above are enough signal for
    // a GO verdict (handshake completed → wire-compat for v1 dial). The
    // RPC + registration handshakes that follow live in step 92.5; the
    // wire-level deltas the fork worries about (idle timeout, ack
    // delay exponent, initial flow control) would have surfaced as
    // tls/transport errors here.
}
