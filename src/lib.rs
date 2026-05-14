//! `cloudflare-quick-tunnel` — pure-Rust client for Cloudflare's
//! `https://*.trycloudflare.com` "quick tunnel" service.
//!
//! Drop-in replacement for the common pattern of spawning the
//! `cloudflared` Go binary as a subprocess and scraping its stderr
//! for the public URL. Speaks QUIC + Cap'n Proto-RPC to the
//! `argotunnel` edge natively, so the host application stays a
//! single self-contained Rust binary.
//!
//! See `docs/spike-verdict.md` for the design decision record and
//! the three undocumented edge gotchas (ALPN / SNI / trust roots)
//! that the spike crate proved out against the production edge.

// Cap'n Proto-generated bindings live at the crate root because
// the generated code emits absolute `crate::<schema>_capnp::…`
// paths between schemas (e.g. `tunnelrpc` references `metadata`
// from `quic_metadata_protocol`). Hoisting them keeps the
// generator output usable verbatim.
#[allow(clippy::all, unused, non_camel_case_types, non_upper_case_globals, non_snake_case)]
pub mod tunnelrpc_capnp {
    include!(concat!(env!("OUT_DIR"), "/tunnelrpc_capnp.rs"));
}
#[allow(clippy::all, unused, non_camel_case_types, non_upper_case_globals, non_snake_case)]
pub mod quic_metadata_protocol_capnp {
    include!(concat!(env!("OUT_DIR"), "/quic_metadata_protocol_capnp.rs"));
}

pub mod api;
pub mod edge;
pub mod error;
pub mod manager;
pub mod proxy;
pub mod quic_dial;
pub mod rpc;
pub mod stream;
pub mod supervisor;

pub use error::TunnelError;
pub use manager::{QuickTunnelHandle, QuickTunnelManager, TunnelMetrics};
