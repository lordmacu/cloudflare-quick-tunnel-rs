# Implementation status

## Working end-to-end

The full chain — POST `/tunnel` → edge discovery → QUIC dial →
capnp-RPC `RegisterConnection` (with stub `CloudflaredServer`
bootstrap to satisfy the edge's liveness probe) → reactor loop
owning the inbound stream acceptor and reconnect-with-backoff →
HTTP/1.1 proxy to a local TCP listener → graceful
`unregisterConnection` on shutdown — is green under the gated
live test.

```bash
CFQT_LIVE_TESTS=1 cargo test --test live_serve_http -- --ignored --nocapture
# Last green run:
#   attempt 21: 200 OK body="hello /probe"
#   streams_total=1 bytes_in=0 bytes_out=12
#   reactor: clean shutdown
```

## Module status

| Module           | Status | Notes |
|------------------|--------|-------|
| `api.rs`         | ✅     | POST /tunnel, exp-backoff retry, wiremock tests. |
| `edge.rs`        | ✅     | SRV + DoT fallback, 1h cache, shuffle. |
| `quic_dial.rs`   | ✅     | rustls + 3 CF CAs, ALPN argotunnel, SNI quic.cftunnel.com. |
| `rpc.rs`         | ✅     | RegisterConnection + stub server bootstrap + graceful unregister. ControlSession lives on its own thread (capnp-rpc is !Send). |
| `stream.rs`      | ✅     | Per-stream signature + version + capnp ConnectRequest/Response codecs. |
| `proxy.rs`       | ✅     | HTTP rebuild + httparse response + counted bidirectional body pump. |
| `supervisor.rs`  | ✅     | accept_bi loop returning `SupervisorExit::{Shutdown, ConnectionLost}` so the reactor can stitch cycles. |
| `manager.rs`     | ✅     | Wires POST + first connect + reactor; QuickTunnelHandle with metrics + shutdown / shutdown_with. |

## Resilience

- **Reconnect on edge close:** reactor exp backoff 1s → 2s → 4s →
  8s → 16s → 30s (capped). Up to 10 consecutive failures before
  the reactor exits. Reconnect uses `replace_existing=true` so the
  edge accepts the new register for `conn_index=0`. `reconnects`
  metric counts successful re-registers.
- **Graceful shutdown:** `shutdown()` fires `unregisterConnection`
  with a 30s grace, then closes the QUIC connection. Drop falls
  back to fire-and-forget signal (the supervisor still gets a
  clean ApplicationClose error frame).
- **Keepalive:** QUIC `keep_alive_interval = 1s`. Edge's
  `MaxIdleTimeout = 5s`; we stay well inside it.

## Known gaps (none blocking)

1. **HA pool (multi conn_index 0..3).** Quick tunnels run with HA=1
   so a single edge POP outage forces a reconnect rather than
   silently failing over. Worth flagging for production-grade use,
   not for quick-tunnel scope.

2. **No per-stream timing / status histogram.** Only
   `streams_total + bytes_in + bytes_out + reconnects` surface
   today.

3. **Status code 530 warmup (≈ 20-90s).** CF-side anycast routing
   propagation latency on a fresh quick tunnel. Reconnect doesn't
   change it — even named tunnels have this. Documented in the
   live test's poll deadline (120s).

4. **No dedicated reconnect simulation test.** The reactor's
   reconnect arm is exercised mechanically (supervisor exit
   matching + backoff curve unit test) but we don't have a test
   that forces a mid-flight connection drop and asserts the
   reactor re-acquires.

## Verified live runs

- `live_full_flow`: register, edge replies with a `bog01`
  connection UUID. Always green.
- `live_serve_http`: spawns local origin, brings up tunnel, polls
  public URL until echoed body returns. Green; ~40–90s end-to-end
  depending on edge warmup.
- Manual curl through a sustained tunnel: ✅.
