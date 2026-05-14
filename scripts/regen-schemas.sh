#!/usr/bin/env bash
# Regenerate `src/proto_gen/*.rs` from the vendored `schemas/*.capnp`.
#
# Run this whenever the vendored schemas change (i.e. when bumping
# the cloudflared commit pinned in THIRD_PARTY_NOTICES.md). The
# generated files are committed so end users don't need the `capnp`
# toolchain on their host to build the crate.
#
# Prereqs (maintainer-only):
#   - capnproto >= 0.5.2 on $PATH    (apt install capnproto / brew install capnp / choco install capnproto)
#   - rust toolchain                  (any stable)
#   - capnpc-rust binary              (cargo install capnpc)
#
# Output: src/proto_gen/{tunnelrpc,quic_metadata_protocol}_capnp.rs

set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if ! command -v capnp >/dev/null 2>&1; then
    echo "error: capnp not on PATH. Install capnproto first." >&2
    exit 1
fi

if ! command -v capnpc-rust >/dev/null 2>&1; then
    echo "error: capnpc-rust not on PATH. Run 'cargo install capnpc' (note: binary is capnpc-rust)." >&2
    exit 1
fi

OUT="$ROOT/src/proto_gen"
mkdir -p "$OUT"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# capnp compile drops files next to the input by default; redirect via
# -o to the output dir. The `capnpc-rust` binary backs `--rust`.
capnp compile \
    -orust:"$TMP" \
    --src-prefix="$ROOT/schemas" \
    "$ROOT/schemas/tunnelrpc.capnp" \
    "$ROOT/schemas/quic_metadata_protocol.capnp"

mv "$TMP/tunnelrpc_capnp.rs" "$OUT/tunnelrpc_capnp.rs"
mv "$TMP/quic_metadata_protocol_capnp.rs" "$OUT/quic_metadata_protocol_capnp.rs"

echo "regen ok:"
wc -l "$OUT"/*.rs
