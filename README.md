# cloudflare-quick-tunnel

[![crates.io](https://img.shields.io/crates/v/cloudflare-quick-tunnel.svg)](https://crates.io/crates/cloudflare-quick-tunnel)
[![docs.rs](https://img.shields.io/docsrs/cloudflare-quick-tunnel)](https://docs.rs/cloudflare-quick-tunnel)
[![CI](https://github.com/lordmacu/cloudflare-quick-tunnel-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/lordmacu/cloudflare-quick-tunnel-rs/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/cloudflare-quick-tunnel.svg)](https://github.com/lordmacu/cloudflare-quick-tunnel-rs#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.86-blue)](https://blog.rust-lang.org/2025/04/03/Rust-1.86.0)

> Pure-Rust client for **Cloudflare quick tunnels**
> (`https://*.trycloudflare.com`). Speaks QUIC + Cap'n Proto-RPC
> against the `argotunnel` edge directly — no `cloudflared`
> subprocess, no ~30 MB Go binary in your release tarball.

Add to your project:

```toml
[dependencies]
cloudflare-quick-tunnel = "0.3"
```

> Built as part of **[Nexo](https://github.com/lordmacu/nexo-rs)** — a
> Rust multi-agent framework for building autonomous WhatsApp / Telegram /
> Email agents with a NATS event bus, pluggable LLMs (MiniMax, Claude,
> OpenAI, DeepSeek), per-agent memory, MCP support, and a SaaS-grade
> microapp framework. Nexo embeds this crate to expose its admin /
> webhook / pairing endpoints over a public HTTPS URL with zero ops
> overhead — and the result is a self-contained single binary that
> runs the same way on a VPS, in Docker, in Termux, and (next) as a
> Flutter Android FFI library. If that sounds useful, see the parent
> project.

## Status

✅ **End-to-end working.** The full chain — POST `/tunnel` →
edge discovery → QUIC dial → capnp-RPC `RegisterConnection` →
inbound stream acceptor → HTTP/1.1 proxy → graceful unregister
— is green under live tests. Reactor reconnect with exponential
backoff handles edge-side drops. `streams_total` / `bytes_in` /
`bytes_out` / `reconnects` are surfaced via `handle.metrics()`.

Try it locally:

```bash
# In one terminal: start any local HTTP server on 8080.
python3 -m http.server 8080 &

# In another: bring up the tunnel.
cargo run --example serve -- 8080
# →  Public URL: https://<sub>.trycloudflare.com
# →  Edge POP:   bog01
```

The throwaway crate under `spike/` was used for the design
spike — see [`docs/spike-verdict.md`](docs/spike-verdict.md) for
the three undocumented edge gotchas it surfaced (ALPN, SNI, CF
internal CAs).

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

## Quick start

```rust
use cloudflare_quick_tunnel::QuickTunnelManager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let handle = QuickTunnelManager::new(8080).start().await?;
    println!("Public URL: {}", handle.url);   // https://<sub>.trycloudflare.com
    println!("Edge POP:   {}", handle.location); // e.g. bog01, atl01
    // keep handle alive while serving on 127.0.0.1:8080 …
    handle.shutdown().await?;
    Ok(())
}
```

The `examples/serve.rs` binary is the same code with a SIGINT
shutdown loop + a periodic heartbeat.

## Build prerequisites

None — Cap'n Proto bindings are pre-generated under
`src/proto_gen/` and shipped with the crate, so `cargo build`
needs only the Rust toolchain. (Maintainers regenerating from
a bumped schema run `scripts/regen-schemas.sh`, which is the
only path that needs `capnp` installed.)

## License

MIT OR Apache-2.0 (standard Rust crate dual-license). See
[`THIRD_PARTY_NOTICES.md`](./THIRD_PARTY_NOTICES.md) for the small
amount of Apache-2.0 material vendored from upstream `cloudflared`:
the Cap'n Proto schemas the edge expects and the three
Cloudflare-internal CA certificates that sign the edge's TLS cert.
