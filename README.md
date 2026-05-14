# cloudflare-quick-tunnel

> Pure-Rust client for **Cloudflare quick tunnels**
> (`https://*.trycloudflare.com`). Speaks QUIC + Cap'n Proto-RPC
> against the `argotunnel` edge directly — no `cloudflared`
> subprocess, no ~30 MB Go binary in your release tarball.

## Status

🚧 **Pre-alpha.** Scaffold landed; end-to-end `QuickTunnelManager::start()`
still being wired sub-phase by sub-phase. Module headers under `src/`
carry per-step status.

The throwaway crate under `spike/` proves viability — `quinn` stock
0.11 + `rustls` 0.23 (ring) completes a QUIC + TLS-1.3 handshake
against the production `argotunnel` edge. Run it:

```bash
cargo run -p cloudflare-quick-tunnel-spike
```

Findings + the three undocumented gotchas surfaced during the spike
are in [`docs/spike-verdict.md`](docs/spike-verdict.md).

## Why this crate exists

The legacy path for exposing a local HTTP service over
`https://*.trycloudflare.com` is to spawn `cloudflared` as a
subprocess and scrape its stderr. That works on every platform
`cloudflared` ships binaries for, but it:

- inflates packaged binaries by ~30 MB,
- adds an auto-download step on first run (supply-chain surface),
- blocks pure-Rust targets — Flutter/Android FFI, WASM, anywhere
  without a Go runtime,
- depends on Cloudflare's GitHub release URL layout staying stable.

`cloudflare-quick-tunnel` speaks the same protocol natively. The host
application stays a single self-contained Rust binary.

## Scope

- ✅ Quick tunnels (`https://*.trycloudflare.com`) — anonymous,
  rate-limited, perfect for dev + webhook receivers.
- ✅ HTTP/1.1 + HTTP/2 inbound proxy to a local TCP listener.
- ❌ Named tunnels (require CF account + API token).
- ❌ Argo Smart Routing.
- ❌ UDP/ICMP proxy (`datagram v2` / `v3`). Only `cloudflared access`
  and WARP use those.
- ❌ WARP integration.

## Quick start (when wired)

```rust
use cloudflare_quick_tunnel::QuickTunnelManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let handle = QuickTunnelManager::new(8080).start().await?;
    println!("Public URL: {}", handle.url);
    // ... keep handle alive while serving on 127.0.0.1:8080 ...
    handle.shutdown().await?;
    Ok(())
}
```

## License

MIT OR Apache-2.0 (standard Rust crate dual-license). See
[`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md) for the small
amount of Apache-2.0 material vendored from upstream `cloudflared`:
the Cap'n Proto schemas the edge expects and the three
Cloudflare-internal CA certificates that sign the edge's TLS cert.
