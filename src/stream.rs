//! Per-request stream framing: each inbound HTTP request from the
//! edge arrives as a new bidi QUIC stream prefixed with a
//! `quic_metadata_protocol` header (see `schemas/`). After the
//! header we just byte-pump to the local TCP listener. Implemented
//! in Phase 92.6.
