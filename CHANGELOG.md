# Changelog

All notable changes to this project will be documented here. The
format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [0.1.0] — 2026-05-14

First public release. End-to-end working: provision a Cloudflare
quick tunnel, dial the `argotunnel` edge over QUIC + Cap'n Proto-RPC,
proxy HTTP/1.1 requests to a local TCP listener, reconnect on edge
drop, unregister cleanly on shutdown.

### Added

- `QuickTunnelManager::new(port).start() -> QuickTunnelHandle` —
  one call surface for spinning up a tunnel.
- POST `/tunnel` client with exponential-backoff retry on 5xx and
  network errors; 4xx + business errors fail fast.
- SRV-based edge discovery against `_v2-origintunneld._tcp.argotunnel.com`
  with a DNS-over-TLS fallback to `1.1.1.1:853`. In-memory cache
  (1h TTL) + per-call shuffle.
- QUIC dial with `quinn` 0.11 + `rustls` 0.23 (ring), pinned
  to the three Cloudflare-internal CAs that sign `*.cftunnel.com`
  + the system trust store. ALPN `argotunnel`, SNI
  `quic.cftunnel.com`.
- Cap'n Proto-RPC `RegisterConnection` over the control stream;
  schemas vendored verbatim from `cloudflared` at the pinned
  commit recorded in `THIRD_PARTY_NOTICES.md`.
- Stub `CloudflaredServer` bootstrap so the edge's liveness probe
  resolves (without it, fresh tunnels return HTTP 530 for the
  full warmup window).
- Per-request HTTP/1.1 stream framing: parse `ConnectRequest`,
  rebuild HTTP head from `HttpMethod` / `HttpHost` /
  `HttpHeader:<Name>` metadata, byte-pump the body bidi to the
  local TCP listener, write `ConnectResponse` back with
  `HttpStatus` + headers.
- Supervisor accept loop returning `SupervisorExit::{Shutdown,
  ConnectionLost}` so the manager's reactor can stitch multiple
  cycles together.
- Reactor with exponential backoff (1s → 2s → 4s → 8s → 16s →
  30s capped) and up to 10 consecutive reconnect attempts.
  Subsequent registers use `replace_existing=true`.
- Graceful shutdown: `handle.shutdown_with(grace)` sends
  `unregisterConnection` with the configured grace period before
  closing the QUIC connection.
- `handle.metrics()` surfacing `streams_total`, `bytes_in`,
  `bytes_out`, and `reconnects` as live atomics.
- `examples/serve.rs` — minimal SIGINT-driven demo binary.

### Tests

- 19 unit tests across `api`, `edge`, `manager`, `proxy`,
  `quic_dial`, `rpc`, `stream`.
- 4 live tests gated on `CFQT_LIVE_TESTS=1`: edge discovery,
  QUIC handshake, full register flow, end-to-end HTTP through
  the tunnel.

### Build prerequisites

`capnp` >= 0.5.2 on the host so `build.rs` can regenerate the
schema bindings. `apt install capnproto` / `brew install capnp` /
`choco install capnproto`.
