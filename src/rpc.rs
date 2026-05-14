//! Cap'n Proto-RPC client for the `TunnelServer` interface (see
//! `schemas/tunnelrpc.capnp`). Speaks over a bidi QUIC stream that
//! starts with a `quic_metadata_protocol` header tagging the stream
//! as `rpc`. Implemented in Phase 92.5.
//!
//! The single call the quick-tunnel client actually needs is
//! `RegisterConnection(auth, tunnel_id, options, conn_index, edge_addr)`
//! returning `RegistrationDetails { uuid, location, tunnel_is_remotely_managed }`.
