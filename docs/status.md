# Implementation status

## Working end-to-end

The full chain — `POST /tunnel` → edge discovery → QUIC dial →
capnp-RPC `RegisterConnection` → control-session keepalive →
inbound-stream acceptor → HTTP/1.1 proxy to a local TCP listener
— compiles green and has been proven on the wire. Run, then
from a second terminal:

```bash
curl --resolve "<sub>.trycloudflare.com:443:104.16.230.132" \
     https://<sub>.trycloudflare.com/probe
# → "hello /probe"
```

That output IS the local hello server's response, served through
the real `argotunnel` edge in `bog01`. Zero subprocess.

## Module status

| Module           | Status | Notes |
|------------------|--------|-------|
| `api.rs`         | ✅     | POST /tunnel, exp-backoff retry, wiremock tests. |
| `edge.rs`        | ✅     | SRV + DoT fallback, 1h cache, shuffle. |
| `quic_dial.rs`   | ✅     | rustls + 3 CF CAs, ALPN argotunnel, SNI quic.cftunnel.com. |
| `rpc.rs`         | ✅     | RegisterConnection succeeds; ControlSession keeps stream alive on dedicated thread. |
| `stream.rs`      | ✅     | data-stream signature + version + capnp ConnectRequest/Response codecs. |
| `proxy.rs`       | ✅     | HTTP rebuild + httparse response + bi-directional body pump. |
| `supervisor.rs`  | ✅     | accept_bi loop + shutdown signal + stream counter metric. |
| `manager.rs`     | ✅     | wires all of the above, returns a drop-in `QuickTunnelHandle`. |

## Known gaps

1. **Intermittent 530 on fresh tunnels.** Cloudflare's edge
   returns `530 ("tunnel unreachable")` on the first ~30-60s
   for some fraction of newly-registered tunnels even after the
   manual curl confirms the wire is fine. Working hypotheses,
   in priority order:

   - We don't expose a `cloudflared_server` bootstrap. cloudflared
     itself does — via `pogs.CloudflaredServer_ServerToClient`
     wrapping its `SessionManager` + `ConfigurationManager` —
     and the edge may probe that bootstrap as a liveness signal.
     The fix is a stub `CloudflaredServer::Server` impl that
     errors out on every method but at least replies "I'm here".
   - Some additional feature flag the edge checks for routing
     readiness. Current `default_for_quick_tunnel` features
     match `cloudflared/features/features.go::defaultFeatures`
     verbatim, but the upstream code adds more dynamically.
   - Anycast warmup races. The published hostname's `cf-ray`
     header tells us which colo serves the request; cross-colo
     handoff to the registered POP can take seconds.

2. **No graceful unregister.** `ControlSession::drop` just closes
   the RPC stream. cloudflared sends an explicit
   `unregisterConnection` capnp call with a `gracePeriodNanoSec`
   first. Without it the edge eventually 404s the tunnel after
   noticing the QUIC connection went idle, which is fine for
   quick tunnels but ugly.

3. **No reconnect on edge-side close.** If the edge drops the
   QUIC connection, the supervisor exits its `accept_bi` loop
   and the tunnel just dies. cloudflared dial-loops with backoff;
   we don't yet.

4. **No telemetry surface beyond `streams_total`.** Bytes in/out
   aren't counted in the proxy module — `SupervisorMetrics` has
   the atomics but the proxy code never bumps them. Easy fix once
   we settle on a metrics shape.

## Verified live runs

- `cargo test -p cloudflare-quick-tunnel --test live_full_flow -- --ignored`
  with `CFQT_LIVE_TESTS=1`: registers, edge replies with a `bog01`
  connection UUID. Always green.
- `cargo test -p cloudflare-quick-tunnel --test live_serve_http -- --ignored`
  with `CFQT_LIVE_TESTS=1`: spawns local origin, brings up tunnel,
  polls public URL. Intermittent — see gap #1.
- Manual curl through a sustained tunnel: ✅ returns the local
  server's body.
