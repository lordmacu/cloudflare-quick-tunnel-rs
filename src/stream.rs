//! Per-request stream framing on the edge ↔ tunnel side.
//!
//! When the edge receives a request bound for our quick-tunnel
//! hostname it opens a new bidi QUIC stream to us and writes:
//!
//! ```text
//!   [ 6-byte protocolSignature ][ 2-byte protocolVersion ][ capnp ConnectRequest ]
//! ```
//!
//! We respond with the analogous frame carrying a `ConnectResponse`
//! (status + headers as `metadata`), and then the stream becomes a
//! byte-pumped channel for the HTTP body (or arbitrary TCP, for
//! `ConnectionType::Tcp`).
//!
//! Constants + helpers here mirror
//! `cloudflared/tunnelrpc/quic/protocol.go` +
//! `request_server_stream.go`. We are the **server** side of this
//! framing because the edge is the one opening the stream — even
//! though "server" is confusing given we initiated the QUIC
//! connection. cloudflared calls it `RequestServerStream` for
//! exactly the same reason.

use capnp::message::ReaderOptions;
use capnp_futures::serialize;
use futures::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::debug;

use crate::error::TunnelError;
use crate::quic_metadata_protocol_capnp;

/// 6-byte tag the edge writes first to disambiguate stream kinds.
/// We only ever see `DATA_STREAM_SIGNATURE` on per-request streams.
/// `RPC_STREAM_SIGNATURE` is reserved for the cloudflared-server
/// RPC (session manager / config), which the edge does NOT open
/// against quick tunnels.
pub const DATA_STREAM_SIGNATURE: [u8; 6] = [0x0A, 0x36, 0xCD, 0x12, 0xA1, 0x3E];
pub const RPC_STREAM_SIGNATURE: [u8; 6] = [0x52, 0xBB, 0x82, 0x5C, 0xDB, 0x65];

/// Two ASCII bytes (`"01"`). cloudflared treats `readVersion` as a
/// NO-OP for now — kept here verbatim so a future bump shows up
/// loudly.
pub const PROTOCOL_V1: [u8; 2] = [b'0', b'1'];

/// What kind of payload the edge is asking us to serve on this
/// stream. Mirror of `quic_metadata_protocol.ConnectionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    Http,
    Websocket,
    Tcp,
}

/// Parsed `ConnectRequest`. `dest` is a full URL on HTTP/Websocket
/// streams (e.g. `https://abc.trycloudflare.com/path?q=1`) and a
/// `host:port` on TCP streams.
#[derive(Debug, Clone)]
pub struct ConnectRequest {
    pub dest: String,
    pub conn_type: ConnectionType,
    /// Free-form key/value map the edge attaches to the stream.
    /// For HTTP requests this carries `HttpMethod`, `HttpHost`,
    /// and one `HttpHeader:<Name>` entry per request header.
    pub metadata: Vec<(String, String)>,
}

impl ConnectRequest {
    /// O(n) lookup — usually fewer than a dozen entries, fine.
    pub fn meta(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

// ── Well-known metadata keys (mirror connection/quic_connection.go) ──

pub const HTTP_METHOD_KEY: &str = "HttpMethod";
pub const HTTP_HOST_KEY: &str = "HttpHost";
pub const HTTP_HEADER_KEY: &str = "HttpHeader";
pub const HTTP_STATUS_KEY: &str = "HttpStatus";

// ── Read side ────────────────────────────────────────────────────────────────

/// Read the preamble (signature + version), assert it's the data
/// stream, and decode the capnp `ConnectRequest` that follows.
pub async fn read_connect_request<R>(reader: &mut R) -> Result<ConnectRequest, TunnelError>
where
    R: futures::io::AsyncRead + Unpin,
{
    let mut sig = [0u8; 6];
    reader
        .read_exact(&mut sig)
        .await
        .map_err(|e| TunnelError::Internal(format!("read signature: {e}")))?;
    if sig != DATA_STREAM_SIGNATURE {
        return Err(TunnelError::Internal(format!(
            "unexpected stream signature: {sig:02x?}"
        )));
    }
    let mut ver = [0u8; 2];
    reader
        .read_exact(&mut ver)
        .await
        .map_err(|e| TunnelError::Internal(format!("read version: {e}")))?;
    debug!(version = %String::from_utf8_lossy(&ver), "stream preamble");

    let msg = serialize::read_message(reader, ReaderOptions::new())
        .await
        .map_err(|e| TunnelError::Internal(format!("read capnp message: {e}")))?;
    let root: quic_metadata_protocol_capnp::connect_request::Reader = msg
        .get_root()
        .map_err(|e| TunnelError::Internal(format!("capnp root: {e}")))?;

    let dest = root
        .get_dest()
        .map_err(|e| TunnelError::Internal(format!("dest: {e}")))?
        .to_string()
        .map_err(|e| TunnelError::Internal(format!("dest utf-8: {e}")))?;
    let conn_type = match root
        .get_type()
        .map_err(|e| TunnelError::Internal(format!("type: {e}")))?
    {
        quic_metadata_protocol_capnp::ConnectionType::Http => ConnectionType::Http,
        quic_metadata_protocol_capnp::ConnectionType::Websocket => ConnectionType::Websocket,
        quic_metadata_protocol_capnp::ConnectionType::Tcp => ConnectionType::Tcp,
    };
    let mut metadata = Vec::new();
    if let Ok(list) = root.get_metadata() {
        for i in 0..list.len() {
            let m = list.get(i);
            let k = m
                .get_key()
                .ok()
                .and_then(|t| t.to_string().ok())
                .unwrap_or_default();
            let v = m
                .get_val()
                .ok()
                .and_then(|t| t.to_string().ok())
                .unwrap_or_default();
            metadata.push((k, v));
        }
    }
    Ok(ConnectRequest {
        dest,
        conn_type,
        metadata,
    })
}

// ── Write side ───────────────────────────────────────────────────────────────

/// Borrowed key/value pair for write_connect_response.
pub type MetaPair<'a> = (&'a str, &'a str);

/// Write a `ConnectResponse` preceded by the data-stream preamble.
///
/// `error` is set when the local upstream refused / errored before
/// any body bytes flow. On the happy path it's empty and `metadata`
/// carries `HttpStatus` + each `HttpHeader:<Name>` the local
/// origin emitted.
pub async fn write_connect_response<W>(
    writer: &mut W,
    error: &str,
    metadata: &[MetaPair<'_>],
) -> Result<(), TunnelError>
where
    W: futures::io::AsyncWrite + Unpin,
{
    writer
        .write_all(&DATA_STREAM_SIGNATURE)
        .await
        .map_err(|e| TunnelError::Internal(format!("write signature: {e}")))?;
    writer
        .write_all(&PROTOCOL_V1)
        .await
        .map_err(|e| TunnelError::Internal(format!("write version: {e}")))?;

    let mut message = ::capnp::message::Builder::new_default();
    {
        let mut root: quic_metadata_protocol_capnp::connect_response::Builder = message.init_root();
        root.set_error(error);
        let mut meta = root.init_metadata(metadata.len() as u32);
        for (i, (k, v)) in metadata.iter().enumerate() {
            let mut entry = meta.reborrow().get(i as u32);
            entry.set_key(*k);
            entry.set_val(*v);
        }
    }
    serialize::write_message(&mut *writer, &message)
        .await
        .map_err(|e| TunnelError::Internal(format!("write capnp: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| TunnelError::Internal(format!("flush: {e}")))?;
    Ok(())
}

// ── Quinn stream → futures-io bridge ─────────────────────────────────────────

/// Wrap a quinn pair into the futures-io halves capnp + our framing
/// code expects.
pub fn split(
    send: quinn::SendStream,
    recv: quinn::RecvStream,
) -> (Compat<quinn::RecvStream>, Compat<quinn::SendStream>) {
    (recv.compat(), send.compat_write())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;

    #[tokio::test]
    async fn roundtrip_response_through_buffer() {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut cursor = Cursor::new(&mut buf);
            write_connect_response(
                &mut cursor,
                "",
                &[
                    ("HttpStatus", "200"),
                    ("HttpHeader:Content-Type", "text/plain"),
                ],
            )
            .await
            .unwrap();
        }
        // Sanity: starts with signature + version + at least 8 capnp bytes.
        assert_eq!(&buf[0..6], &DATA_STREAM_SIGNATURE);
        assert_eq!(&buf[6..8], &PROTOCOL_V1);
        assert!(buf.len() > 8 + 8, "capnp body present");
    }

    #[tokio::test]
    async fn rejects_wrong_signature() {
        let mut buf = vec![0u8; 16];
        // intentional garbage signature
        let mut r = Cursor::new(buf.as_mut_slice());
        let err = read_connect_request(&mut r).await.unwrap_err();
        assert!(matches!(err, TunnelError::Internal(s) if s.contains("signature")));
    }
}
