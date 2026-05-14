//! Full end-to-end live test: start a local HTTP server, spin up a
//! quick tunnel pointed at it, curl the public URL from outside,
//! assert we get the local server's response back.
//!
//! Gated on `CFQT_LIVE_TESTS=1` because it provisions a real CF
//! quick tunnel and hits the public internet.
//!
//!   CFQT_LIVE_TESTS=1 cargo test --test live_serve_http -- --ignored --nocapture
//!
//! This proves 92.6 (per-stream HTTP proxy) and 92.7 (accept-loop
//! supervisor) work end-to-end against the real edge — the missing
//! step from the register-only flow validated in
//! `live_full_flow.rs`.

use std::time::Duration;

use cloudflare_quick_tunnel::QuickTunnelManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Tiny HTTP/1.1 server: replies "200 OK / hello <path>" to any
/// request, then closes. Connection: close so we don't have to
/// implement keep-alive. Returns the bound port.
async fn spawn_hello_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let mut req = Vec::new();
                // Read until we see the end of headers (\r\n\r\n).
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    req.extend_from_slice(&buf[..n]);
                    if req.len() > 16 * 1024 {
                        return;
                    }
                }
                // Pull the path out of the first line so we can
                // echo it back — handy for verifying routing.
                let first_line = std::str::from_utf8(&req)
                    .ok()
                    .and_then(|s| s.lines().next())
                    .unwrap_or("GET /unknown")
                    .to_string();
                let path = first_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("/")
                    .to_string();
                let body = format!("hello {path}");
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

#[tokio::test]
#[ignore]
async fn live_end_to_end_http_through_tunnel() {
    if std::env::var_os("CFQT_LIVE_TESTS").is_none() {
        eprintln!("skip: set CFQT_LIVE_TESTS=1 to run");
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,cloudflare_quick_tunnel=debug")
        .try_init();

    let port = spawn_hello_server().await;
    eprintln!("local hello server: 127.0.0.1:{port}");

    let handle = QuickTunnelManager::new(port).start().await.expect("start");
    eprintln!(
        "public URL: {} (location {}, tunnel {})",
        handle.url, handle.location, handle.tunnel_id
    );

    // The edge needs ~2-5 seconds to start routing for a fresh
    // quick tunnel even after RegisterConnection returns. Poll the
    // public URL until we either get a 200 or hit the deadline.
    //
    // We pin a manual DNS override to Cloudflare anycast — the
    // system stub resolver on dev boxes loves to negatively cache
    // a NXDOMAIN response taken 0.5s before the API issued the
    // hostname, and that NXDOMAIN can live in `nscd` / systemd-
    // resolved for minutes. Bypassing it makes the test reliable.
    let host = handle.url.trim_start_matches("https://").to_string();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .resolve(
            &host,
            std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(104, 16, 230, 132)),
                443,
            ),
        )
        .build()
        .unwrap();
    let probe_url = format!("{}/probe", handle.url);

    let mut attempt = 0u32;
    let mut body_text = String::new();
    // Edge routing for a fresh quick tunnel takes anywhere from
    // ~20s to ~90s in practice. Give it a generous window so a
    // slow POP doesn't fail the test for non-protocol reasons.
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while std::time::Instant::now() < deadline {
        attempt += 1;
        match client.get(&probe_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                body_text = resp.text().await.unwrap_or_default();
                eprintln!("attempt {attempt}: 200 OK body={body_text:?}");
                break;
            }
            Ok(resp) => {
                eprintln!("attempt {attempt}: status={} (waiting…)", resp.status());
            }
            Err(e) => {
                eprintln!("attempt {attempt}: transport error {e}");
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    assert!(
        body_text.contains("hello /probe"),
        "expected echoed path in body, got {body_text:?}"
    );

    let metrics = handle.metrics();
    eprintln!(
        "streams_total={} bytes_in={} bytes_out={}",
        metrics.streams_total, metrics.bytes_in, metrics.bytes_out
    );
    assert!(metrics.streams_total >= 1, "expected ≥1 stream");
    assert!(
        metrics.bytes_out > 0,
        "expected non-zero bytes_out (response body)"
    );

    handle.shutdown().await.expect("shutdown");
}
