//! Long-running task that owns the QUIC connection, handles
//! reconnect-with-backoff on edge-side close, sends keepalives, and
//! gracefully unregisters on shutdown. Implemented in Phase 92.7.
