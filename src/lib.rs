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

// Cap'n Proto-generated bindings. Shipped pre-generated under
// `src/proto_gen/` so consumers don't need the `capnp` toolchain
// on their host to build this crate. Regenerate with
// `scripts/regen-schemas.sh` after bumping the vendored schemas
// in `schemas/`.
//
// They live at the crate root because the generator emits absolute
// `crate::<schema>_capnp::…` paths between schemas — hoisting them
// keeps the output usable verbatim.
#[allow(
    clippy::all,
    unused,
    non_camel_case_types,
    non_upper_case_globals,
    non_snake_case
)]
pub mod tunnelrpc_capnp {
    include!("proto_gen/tunnelrpc_capnp.rs");
}
#[allow(
    clippy::all,
    unused,
    non_camel_case_types,
    non_upper_case_globals,
    non_snake_case
)]
pub mod quic_metadata_protocol_capnp {
    include!("proto_gen/quic_metadata_protocol_capnp.rs");
}

pub mod api;
pub mod edge;
pub mod error;
pub mod manager;
pub mod pool;
pub mod proxy;
pub mod quic_dial;
pub mod rpc;
pub mod stream;
pub mod supervisor;

pub use error::TunnelError;
pub use manager::{QuickTunnelHandle, QuickTunnelManager, TunnelMetrics};
