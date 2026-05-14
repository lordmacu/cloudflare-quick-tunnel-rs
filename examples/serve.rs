//! Minimal usage example. Brings up a tunnel pointed at port 8080
//! (or the first arg), prints the public URL, and runs until SIGINT.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example serve -- 8080
//! ```
//!
//! In another terminal, start a local server on the same port and
//! curl the printed URL. The body comes from your local listener.

use std::env;
use std::time::Duration;

use cloudflare_quick_tunnel::QuickTunnelManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,cloudflare_quick_tunnel=info".into()),
        )
        .init();

    let port: u16 = env::args()
        .nth(1)
        .as_deref()
        .unwrap_or("8080")
        .parse()
        .map_err(|e| anyhow::anyhow!("bad port: {e}"))?;

    println!("provisioning tunnel for 127.0.0.1:{port}…");
    let handle = QuickTunnelManager::new(port).start().await?;

    println!();
    println!("  ┌─────────────────────────────────────────────────────────────");
    println!("  │  Public URL:  {}", handle.url);
    println!("  │  Tunnel ID:   {}", handle.tunnel_id);
    println!("  │  Edge POP:    {}", handle.location);
    println!("  └─────────────────────────────────────────────────────────────");
    println!();
    println!("Tunnel is live. Routing usually warms up over the next ~20–60s.");
    println!("Curl example (DNS cache override avoids local NXDOMAIN issues):");
    println!(
        "  curl --resolve {}:443:104.16.230.132 {}/",
        handle.url.trim_start_matches("https://"),
        handle.url
    );
    println!();
    println!("Press Ctrl+C to tear down the tunnel.");

    // Periodic metrics line so the operator can see traffic flowing.
    let metrics_view = handle.metrics();
    let _ = metrics_view; // baseline snapshot — printed at shutdown
    let metrics_loop = tokio::spawn({
        // Take a cheap pointer into the handle's metrics via a fresh
        // call each tick. The handle itself moves into the shutdown
        // path; we share the URL string and stop on the signal.
        let url = handle.url.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                println!("[{url}] tick — still up");
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    metrics_loop.abort();

    println!();
    let m = handle.metrics();
    println!(
        "shutting down: streams_total={} bytes_in={} bytes_out={} reconnects={}",
        m.streams_total, m.bytes_in, m.bytes_out, m.reconnects
    );
    handle.shutdown().await?;
    println!("tunnel closed cleanly.");
    Ok(())
}
