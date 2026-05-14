# Spike verdict — quinn vs the argotunnel edge

**Date:** 2026-05-14
**Question:** Can `quinn` stock (0.11) complete a QUIC + TLS-1.3 handshake
against Cloudflare's `argotunnel` edge — the entry point the production
`cloudflared` Go binary uses with its forked `chungthuang/quic-go v0.45.1`?

**Verdict: GO.** No fork patches needed. The production crate
proceeds with `quinn` upstream.

## What was tested

A throwaway binary under `spike/` that:

1. SRV-looks up `_v2-origintunneld._tcp.argotunnel.com` (20 edges, port 7844).
2. Picks the first edge and opens a QUIC connection with `quinn` 0.11
   + `rustls` 0.23 (ring backend) + `tokio` runtime.
3. Logs handshake completion + path stats.

## Findings that mattered

Three undocumented requirements surfaced — none are wire-protocol
deltas, all are configuration-level. Documenting them here so the
production crate doesn't have to re-discover.

### 1. ALPN: `argotunnel`

From `cloudflared/connection/protocol.go:24` —
`quicProtos = "argotunnel"`. Set in `rustls::ClientConfig.alpn_protocols`.

### 2. SNI: `quic.cftunnel.com` (NOT the ALPN string)

From `cloudflared/connection/protocol.go` —
`edgeQUICServerName = "quic.cftunnel.com"`. This is the
hostname passed to `quinn::Endpoint::connect(addr, &server_name)`,
and it's what rustls puts in the TLS ClientHello `server_name`
extension.

Initial spike attempt passed `"argotunnel"` here (conflated with ALPN).
The edge responded to TLS handshake but never sent its Certificate
chain — likely because it routes Server name → backend, and the
mismatch dropped the connection without a TLS alert. Symptom: handshake
keys derived, one tiny Handshake packet received, then 10-second
silence → client-side timeout. Confusing because there's no explicit
TLS alert.

### 3. Trust anchors: system roots are NOT enough

The edge cert `*.cftunnel.com` chains to one of three Cloudflare-internal
CAs that ship inside `cloudflared`, embedded as a string in
`cloudflared/tlsconfig/cloudflare_ca.go`. They are NOT in any
Mozilla / system root pool:

- `CloudFlare Origin SSL ECC Certificate Authority`
- `CloudFlare Origin SSL Certificate Authority`
- `origin-pull.cloudflare.net`

Despite the "Origin" naming, the same chain signs edge certs. The
production crate must embed those three PEMs and add them on top of
the system pool (matching cloudflared `CreateTunnelConfig` semantics
in `tlsconfig/certreloader.go`).

Without them: handshake fails fast with `UnknownIssuer`.

## Transport params

Default `quinn::TransportConfig` is sufficient for handshake. The fork
delta exists (`InitialPacketSize: 1232` for v4 / `1252` for v6 — see
`cloudflared/supervisor/tunnel.go`) but it's a WARP-MTU workaround, not
a wire requirement. `quinn`'s default 1200-byte Initial fits well within
the edge's expectations.

Items still to verify in the capnp-RPC RegisterConnection step:

- `HandshakeIdleTimeout = 5s` / `MaxIdleTimeout = 5s` /
  `KeepAlivePeriod = 1s` — tight. Default quinn idle of 30s is
  client-side; whether the edge would close us early is unknown
  until we hold a connection long enough to test.
- `MaxIncomingStreams = 2^60` — fork default, may need bumping
  on quinn side once we register and start handling inbound HTTP
  streams.
- `EnableDatagrams: true` — only matters for v2/v3 datagram
  (UDP/ICMP proxy). Out of scope — the crate is HTTP-only.

## Wire log (success path)

```
trust anchors loaded native=147 cf_added=3 total=150
dial attempt idx=0 ip=198.41.x.x port=7844
HANDSHAKE OK
path stats rtt_us=57142 cwnd=12000 congestion_events=0 lost_packets=0 sent_packets=4
udp stats udp_tx_datagrams=3 udp_rx_datagrams=2
```

Three datagrams out (Initial / Handshake ACK / 1-RTT setup), two
datagrams in (Initial+ServerHello / Handshake+Cert chain). Clean.

## Out of scope for the spike

- capnp-RPC `RegisterConnection` over the established connection
  (binding ALPN + RPC is the next non-trivial deliverable; the
  handshake-only spike does not exercise it).
- Per-stream HTTP proxy framing (`quic_metadata_protocol.capnp`).
- Cross-target build (Android NDK rustls pin, WASM gate).

## Throwaway crate

`spike/` exists only to anchor this verdict. It is `publish = false`
and will be deleted once `src/quic_dial.rs` lands a real
graduated-from-spike implementation.
