# Third-party notices — `cloudflare-quick-tunnel`

This crate vendors a small amount of material from upstream
`cloudflared` (the Cloudflare-authored tunnel client) to talk to
the same edge endpoint. Each item below stays unmodified; what
follows is wiring around it.

## cloudflared

**Upstream:** <https://github.com/cloudflare/cloudflared>
**License:** Apache License, Version 2.0
**Pinned commit:** `0c9014870a06` (snapshot taken 2026-05-14)

### Vendored files

| Path in this crate | Source in cloudflared | License |
|---|---|---|
| `schemas/tunnelrpc.capnp` | `tunnelrpc/proto/tunnelrpc.capnp` | Apache-2.0 |
| `schemas/quic_metadata_protocol.capnp` | `tunnelrpc/proto/quic_metadata_protocol.capnp` | Apache-2.0 |
| `schemas/go.capnp` | `tunnelrpc/proto/go.capnp` | Apache-2.0 |
| `cf-edge-roots.pem` | extracted from `tlsconfig/cloudflare_ca.go` (`cloudflareRootCA` block) | Apache-2.0 |

### Why we keep them in-tree

- The schemas are the wire format. Re-implementing them is the
  guaranteed-to-drift path. Keeping them verbatim makes the
  upstream commit pin auditable.
- The three CF-internal CAs (`CloudFlare Origin SSL ECC CA`,
  `CloudFlare Origin SSL CA`, `origin-pull.cloudflare.net`) are
  the trust anchors for the `*.cftunnel.com` edge cert chain.
  They are not in any public root store. Without them the QUIC +
  TLS-1.3 handshake fails with `UnknownIssuer`. cloudflared bundles
  the same blob.

When bumping the upstream pin:

1. Update `Pinned commit` above to the new sha.
2. Re-copy the four files. Reject the bump if any of them require
   non-mechanical edits to keep the crate green — that signals a
   wire-format break worth investigating before shipping.
3. Update the CHANGELOG with the bump.
