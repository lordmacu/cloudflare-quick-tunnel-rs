# Changelog

All notable changes to this project will be documented here. The
format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [SemVer](https://semver.org/).

## [0.3.1] — 2026-05-14

### Changed

- **MSRV bumped 1.85 → 1.86.** `icu_*` 2.2.0 (transitive under
  `url` → `idna` → `idna_adapter` 1.2.2) require Rust 1.86. We
  don't pin the transitive — `url` is pulled by both `reqwest`
  and `hickory-resolver` so the only realistic fix is to move
  MSRV. 1.86 has been stable since April 2025.

## [0.3.0] — 2026-05-14

### Added

- **WebSocket Upgrade support.** `proxy_http` now runs the two
  byte halves truly concurrently after the response head is
  parsed, so 101 Switching Protocols connections flow indefinitely
  in both directions until either peer closes. Previously the
  bidi `join!` shape blocked on the request-body pump finishing
  first, which never happens for WS.

- **HA pool.** `QuickTunnelManager::with_ha_connections(n)`
  (default 2, max 4). Spawns N independent reactor tasks, each
  registering on a distinct `conn_index`. The edge load-balances
  inbound requests across the pool, and a single POP outage
  no longer triggers a ~5s downtime window — surviving legs
  keep serving while the dropped one reconnects.

- **TCP keep-alive pool against `127.0.0.1:<port>`.** New
  `pool::Pool` (LIFO idle list with 30s TTL, 16-entry cap)
  reduces per-stream socket-connect overhead. `proxy_http`
  analyses request + response shape — only Content-Length-
  bounded HTTP/1.1 requests with no Upgrade / chunked /
  Connection: close go through the pooled framed-read path.
  WebSocket Upgrades, Transfer-Encoding: chunked, and close-
  bound responses fall back to the bidi-pump and the socket
  is dropped at the end.

### Changed

- **MSRV bumped 1.78 → 1.85.** Two transitive deps
  (`wiremock` 0.6.5, `idna_adapter` 1.2.2) require `edition2024`
  which stabilised in Rust 1.85. The previous `wiremock = "=0.6.4"`
  pin in dev-deps is removed; resolver picks the latest 0.6.x
  again.

- `QuickTunnelHandle::shutdown` now waits for **every** HA reactor
  to drain + unregister, not just one.

- `Drop` for `QuickTunnelHandle` uses `Notify::notify_waiters`
  (one signal → N reactors) instead of a single oneshot.

## [0.2.0] — 2026-05-14

### Changed

- **No more `capnp` build-time dependency.** Cap'n Proto bindings
  are now pre-generated under `src/proto_gen/` and shipped with
  the crate. End users no longer need `capnproto` installed on
  the host to `cargo build` — only the Rust toolchain.

  - `build.rs` removed.
  - `capnpc` dropped from build-dependencies.
  - Maintainers regenerate via `scripts/regen-schemas.sh` after
    bumping the vendored schemas.

  This is a **build-system change**, not a behavioural one: the
  generated bindings are byte-identical to what `0.1.0`'s
  `build.rs` produced. Upgrade is a drop-in `cargo update`.

### Removed

- `0.1.0` was yanked from crates.io with reason
  "build-time `capnp` dependency unnecessary; upgrade to 0.2.0".

## [0.1.0] — 2026-05-14 (yanked)

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
