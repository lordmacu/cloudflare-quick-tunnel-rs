//! Bidirectional byte pump between an inbound QUIC stream and the
//! local TCP listener the caller wants to expose. Wraps
//! `tokio::io::copy_bidirectional`. Implemented in Phase 92.6.
