# Implementation status

## Working end-to-end

The full chain — `POST /tunnel` → edge discovery → QUIC dial →
capnp-RPC `RegisterConnection` → stub-bootstrap-backed liveness →
inbound-stream acceptor → HTTP/1.1 proxy to a local TCP listener
→ graceful `unregisterConnection` on shutdown — is green under
the gated live test.

```bash
CFQT_LIVE_TESTS=1 cargo test --test live_serve_http -- --ignored --nocapture
# Last green run:
#   attempt 13: 200 OK body="hello /probe"
#   streams_total=1 bytes_in=0 bytes_out=12
```

`bytes_out == strlen("hello /probe")` confirms the counter pumps
the proxy actually rides through `count`.

## Module status

| Module           | Status | Notes |
|------------------|--------|-------|
| `api.rs`         | ✅     | POST /tunnel, exp-backoff retry, wiremock tests. |
| `edge.rs`        | ✅     | SRV + DoT fallback, 1h cache, shuffle. |
| `quic_dial.rs`   | ✅     | rustls + 3 CF CAs, ALPN argotunnel, SNI quic.cftunnel.com. |
| `rpc.rs`         | ✅     | RegisterConnection + stub server bootstrap + graceful unregister. ControlSession lives on its own thread (capnp-rpc is !Send). |
| `stream.rs`      | ✅     | Per-stream signature + version + capnp ConnectRequest/Response codecs. |
| `proxy.rs`       | ✅     | HTTP rebuild + httparse response + counted bi-directional body pump. |
| `supervisor.rs`  | ✅     | accept_bi loop + shutdown signal + streams_total + bytes_in/out via stream counters. |
| `manager.rs`     | ✅     | Wires the full chain, returns a drop-in QuickTunnelHandle with shutdown / shutdown_with. |

## Known gaps

1. **No reconnect on edge-side close.** If the edge drops the
   QUIC connection (POP restart, network blip), the supervisor
   exits its `accept_bi` loop and the tunnel just dies. Fix
   sketch: outer loop in `manager.rs` that re-runs the
   dial → register → supervise chain with exp backoff, using
   `replace_existing=true` on subsequent register calls so the
   edge accepts the new connection for the same conn_index.

2. **Single connection only.** cloudflared HA pools open multiple
   conn_indexes (0..3) so a single edge POP outage doesn't break
   the tunnel. Quick tunnels run with HA=1 by default, so this is
   acceptable for the v0.1 scope but worth flagging.

3. **No path observability beyond `bytes_in/out + streams_total`.**
   No per-stream timing, no histogram of statuses, no logged
   request lines. Easy to bolt on once a tracing schema settles.

4. **Status code 530 warmup.** Even with the stub bootstrap fix,
   the edge can still return 530 for ~20–60s after a fresh
   register while routing propagates across the POP. Reconnect
   alone won't fix this; it's a CF-side timing characteristic of
   anonymous quick tunnels.

## Verified live runs

- `live_full_flow`: registers, edge replies with a `bog01`
  connection UUID. Always green.
- `live_serve_http`: spawns local origin, brings up tunnel, polls
  public URL until echoed body returns. Green; takes ~40-75s
  end-to-end depending on edge warmup.
- Manual curl through a sustained tunnel: ✅.
