//! End-to-end live test: POST /tunnel → discover → dial → register.
//!
//! Gated on `CFQT_LIVE_TESTS=1` so CI doesn't hit the real CF edge
//! on every PR. Run manually:
//!
//!   CFQT_LIVE_TESTS=1 cargo test --test live_full_flow -- --ignored --nocapture
//!
//! This is the proving ground for `rpc::register_connection` —
//! the spike only confirmed the handshake; this confirms the
//! actual quick-tunnel registration succeeds end-to-end.

use cloudflare_quick_tunnel::api::{request_tunnel, DEFAULT_SERVICE_URL, DEFAULT_USER_AGENT};
use cloudflare_quick_tunnel::edge::{discover, IpVersionFilter};
use cloudflare_quick_tunnel::quic_dial::{build_endpoint, dial_any};
use cloudflare_quick_tunnel::rpc::{register_connection, ConnectionOptions, TunnelAuth};
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn live_full_register_flow() {
    if std::env::var_os("CFQT_LIVE_TESTS").is_none() {
        eprintln!("skip: set CFQT_LIVE_TESTS=1 to run");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,cloudflare_quick_tunnel=debug")
        .try_init();

    let tunnel = request_tunnel(DEFAULT_SERVICE_URL, DEFAULT_USER_AGENT)
        .await
        .expect("POST /tunnel");
    eprintln!("got tunnel hostname={} id={}", tunnel.hostname, tunnel.id);
    let tunnel_id = Uuid::parse_str(&tunnel.id).expect("tunnel.id is uuid");

    let edges = discover(IpVersionFilter::Auto).await.expect("discover");
    let endpoint = build_endpoint().expect("endpoint");
    let conn = dial_any(&endpoint, &edges[..edges.len().min(3)])
        .await
        .expect("handshake");

    let auth = TunnelAuth {
        account_tag: tunnel.account_tag.clone(),
        tunnel_secret: tunnel.secret.clone(),
    };
    let options = ConnectionOptions::default_for_quick_tunnel("cloudflare-quick-tunnel/0.1.0");

    let details = register_connection(&conn, &auth, tunnel_id, 0, &options)
        .await
        .expect("registerConnection");
    eprintln!(
        "registered uuid={} location={} remote_managed={}",
        details.uuid, details.location, details.tunnel_is_remotely_managed
    );
    assert!(!details.location.is_empty(), "edge should return a colo code");

    // Tear down cleanly so we don't hold the registration past the test.
    conn.close(0u32.into(), b"test-done");
    endpoint.wait_idle().await;
}
