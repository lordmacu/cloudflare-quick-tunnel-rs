//! Generate Rust bindings for the Cap'n Proto schemas the Cloudflare
//! tunnel edge speaks (RegisterConnection + per-stream metadata
//! framing). The schemas live under `schemas/` and are vendored
//! verbatim from cloudflared at the pinned commit recorded in
//! `THIRD_PARTY_NOTICES.md`.
//!
//! Output lands in `$OUT_DIR/proto/` and is included from `src/proto.rs`.

fn main() {
    let mut cmd = capnpc::CompilerCommand::new();
    cmd.src_prefix("schemas")
        .file("schemas/tunnelrpc.capnp")
        .file("schemas/quic_metadata_protocol.capnp");
    if let Err(e) = cmd.run() {
        // Don't try to be clever — capnpc surfaces real wire-format
        // errors here. Fail loud so the build doesn't ship a half-
        // generated proto module.
        panic!("capnpc failed to compile schemas: {e}");
    }
    println!("cargo:rerun-if-changed=schemas/tunnelrpc.capnp");
    println!("cargo:rerun-if-changed=schemas/quic_metadata_protocol.capnp");
    println!("cargo:rerun-if-changed=schemas/go.capnp");
    println!("cargo:rerun-if-changed=build.rs");
}
