//! Per-request HTTP/1.1 proxy: bridge an inbound capnp-framed
//! stream from the edge to the local TCP listener the caller
//! wants to expose at `https://<sub>.trycloudflare.com`.
//!
//! Wire flow on the edge → tunnel side:
//!
//! 1. Edge writes the data-stream preamble + `ConnectRequest`
//!    (parsed by `stream::read_connect_request`).
//! 2. We connect a TCP socket to `127.0.0.1:<local_port>` and
//!    write a re-synthesised HTTP/1.1 request line + `Host` +
//!    every `HttpHeader:<Name>` the edge forwarded + an empty
//!    line. Body bytes (if any) follow from the QUIC stream.
//! 3. We parse the HTTP/1.1 response status + headers off the
//!    TCP socket, write them back as a `ConnectResponse` (status
//!    in `HttpStatus`, each header in `HttpHeader:<Name>`), and
//!    finally byte-pump the response body from TCP to QUIC.
//!
//! Mirrors `cloudflared/connection/quic_connection.go`
//! (`buildHTTPRequest` + `httpResponseAdapter`).

use std::time::Duration;

use futures::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tracing::{debug, warn};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::TunnelError;
use crate::stream::{
    self, ConnectRequest, ConnectionType, HTTP_HEADER_KEY, HTTP_HOST_KEY, HTTP_METHOD_KEY,
    HTTP_STATUS_KEY,
};

/// Byte counters the supervisor accumulates across all streams.
/// Cheap atomics so the proxy task can update without coordination.
#[derive(Debug, Default, Clone)]
pub struct StreamCounters {
    /// Bytes received from the edge (request bodies + ws frames).
    pub bytes_in: Arc<AtomicU64>,
    /// Bytes sent to the edge (response bodies + ws frames).
    pub bytes_out: Arc<AtomicU64>,
}

/// How long we wait for the local TCP listener to accept the first
/// byte. Quick tunnels are usually pointed at a process that just
/// finished booting; 5s is generous without making real failures
/// drag out.
pub const LOCAL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Hard cap on the response header section. Anything more is
/// almost certainly a misconfigured origin and we want to fail
/// before pumping its body.
const MAX_HEADER_BYTES: usize = 32 * 1024;

/// Drive one inbound request stream to completion. Reads the
/// `ConnectRequest` off `recv`, runs the matching proxy strategy,
/// writes the `ConnectResponse` back, and pumps the body bytes.
pub async fn handle_inbound_stream(
    local_port: u16,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    counters: StreamCounters,
) -> Result<(), TunnelError> {
    let (mut reader, mut writer) = stream::split(send, recv);
    let req = stream::read_connect_request(&mut reader).await?;
    debug!(dest = %req.dest, ty = ?req.conn_type, "inbound stream");

    match req.conn_type {
        ConnectionType::Http | ConnectionType::Websocket => {
            proxy_http(local_port, req, reader, writer, counters).await
        }
        ConnectionType::Tcp => {
            proxy_tcp(local_port, &req, &mut reader, &mut writer, &counters).await
        }
    }
}

// ── HTTP / Websocket ─────────────────────────────────────────────────────────

async fn proxy_http<R, W>(
    local_port: u16,
    request: ConnectRequest,
    mut from_edge: R,
    mut to_edge: W,
    counters: StreamCounters,
) -> Result<(), TunnelError>
where
    R: futures::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    // 1. Connect to local.
    let tcp = match tokio::time::timeout(
        LOCAL_CONNECT_TIMEOUT,
        TcpStream::connect(("127.0.0.1", local_port)),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            warn!(error = %e, local_port, "TCP connect refused");
            return write_error_response(&mut to_edge, 502, &format!("local connect: {e}")).await;
        }
        Err(_) => {
            warn!(local_port, "TCP connect timed out");
            return write_error_response(&mut to_edge, 504, "local connect timed out").await;
        }
    };

    let (mut tcp_read, mut tcp_write) = tcp.into_split();

    // 2. Synthesise + send HTTP/1.1 request head.
    let head = build_request_head(&request)?;
    tcp_write
        .write_all(head.as_bytes())
        .await
        .map_err(|e| TunnelError::Internal(format!("tcp write head: {e}")))?;

    // 3. Pump request body (QUIC → TCP) concurrently with response
    //    head read (TCP → us). We can't sequence them because
    //    HTTP/1.1 in practice often interleaves — and even a tiny
    //    HEAD/GET wants the request shut to flush. `join!` drives
    //    both halves; the body pump terminates first on EOF.
    let in_counter = counters.bytes_in.clone();
    let body_pump = async {
        let _ = pump_futures_to_tokio_counted(&mut from_edge, &mut tcp_write, &in_counter).await;
        let _ = tcp_write.shutdown().await;
    };
    let head_read = read_http_response_head(&mut tcp_read);
    let (_, head) = tokio::join!(body_pump, head_read);
    let (status, headers, leftover) = head?;
    debug!(status, header_count = headers.len(), "origin response");

    // 5. Write the ConnectResponse back to the edge with status +
    //    headers as metadata, then pump leftover + remaining body.
    let mut meta: Vec<(String, String)> = Vec::with_capacity(headers.len() + 1);
    meta.push((HTTP_STATUS_KEY.into(), status.to_string()));
    for (name, value) in &headers {
        meta.push((format!("{HTTP_HEADER_KEY}:{name}"), value.clone()));
    }
    let meta_refs: Vec<(&str, &str)> = meta.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    stream::write_connect_response(&mut to_edge, "", &meta_refs).await?;

    // Leftover bytes from header parse (anything captured past the
    // \r\n\r\n boundary) must be flushed first so chunked bodies
    // don't lose their initial frames.
    if !leftover.is_empty() {
        to_edge
            .write_all(&leftover)
            .await
            .map_err(|e| TunnelError::Internal(format!("write leftover body: {e}")))?;
        counters
            .bytes_out
            .fetch_add(leftover.len() as u64, Ordering::Relaxed);
    }

    // 6. Pump response body TCP → QUIC. Continues until the origin
    //    closes its half — typical HTTP/1.1 with Content-Length or
    //    chunked encoding will trigger that.
    pump_tokio_to_futures_counted(&mut tcp_read, &mut to_edge, &counters.bytes_out)
        .await
        .ok();

    to_edge
        .close()
        .await
        .map_err(|e| TunnelError::Internal(format!("close to_edge: {e}")))?;
    Ok(())
}

fn build_request_head(req: &ConnectRequest) -> Result<String, TunnelError> {
    let method = req.meta(HTTP_METHOD_KEY).unwrap_or("GET");
    let host = req.meta(HTTP_HOST_KEY).unwrap_or("");
    let path = extract_path(&req.dest);

    let mut head = String::with_capacity(256);
    head.push_str(method);
    head.push(' ');
    head.push_str(&path);
    head.push_str(" HTTP/1.1\r\n");
    if !host.is_empty() {
        head.push_str("Host: ");
        head.push_str(host);
        head.push_str("\r\n");
    }

    let mut saw_connection = false;
    let mut saw_content_length = false;
    let mut saw_transfer_encoding = false;
    for (k, v) in &req.metadata {
        if let Some(name) = k.strip_prefix(&format!("{HTTP_HEADER_KEY}:")) {
            // Skip Host — already emitted above.
            if name.eq_ignore_ascii_case("host") {
                continue;
            }
            if name.eq_ignore_ascii_case("connection") {
                saw_connection = true;
            }
            if name.eq_ignore_ascii_case("content-length") {
                saw_content_length = true;
            }
            if name.eq_ignore_ascii_case("transfer-encoding") {
                saw_transfer_encoding = true;
            }
            head.push_str(name);
            head.push_str(": ");
            head.push_str(v);
            head.push_str("\r\n");
        }
    }
    // If the edge didn't tell us about a body, advertise close-on-EOF
    // so a hung-open keep-alive doesn't stall the response.
    if !saw_connection {
        head.push_str("Connection: close\r\n");
    }
    let _ = (saw_content_length, saw_transfer_encoding);

    head.push_str("\r\n");
    Ok(head)
}

/// `dest` looks like `https://abc-123.trycloudflare.com/path?q=1`.
/// We want the `/path?q=1` portion. Pure-byte scan, no `url`
/// crate dep.
fn extract_path(dest: &str) -> String {
    if let Some(after_scheme) = dest.find("://") {
        let rest = &dest[after_scheme + 3..];
        if let Some(slash) = rest.find('/') {
            return rest[slash..].to_string();
        }
        return "/".into();
    }
    if dest.starts_with('/') {
        return dest.to_string();
    }
    "/".into()
}

async fn write_error_response<W>(writer: &mut W, status: u16, msg: &str) -> Result<(), TunnelError>
where
    W: futures::io::AsyncWrite + Unpin,
{
    let meta = [(HTTP_STATUS_KEY, status.to_string())];
    let refs: Vec<(&str, &str)> = meta.iter().map(|(k, v)| (*k, v.as_str())).collect();
    stream::write_connect_response(writer, msg, &refs).await?;
    Ok(())
}

async fn read_http_response_head(
    tcp: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), TunnelError> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 2048];
    loop {
        let n = tcp
            .read(&mut tmp)
            .await
            .map_err(|e| TunnelError::Internal(format!("tcp read head: {e}")))?;
        if n == 0 {
            return Err(TunnelError::Internal(
                "local origin closed before sending response head".into(),
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HEADER_BYTES {
            return Err(TunnelError::Internal(format!(
                "response header exceeds {MAX_HEADER_BYTES} bytes"
            )));
        }

        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut resp = httparse::Response::new(&mut headers);
        match resp
            .parse(&buf)
            .map_err(|e| TunnelError::Internal(format!("httparse: {e}")))?
        {
            httparse::Status::Complete(consumed) => {
                let status = resp
                    .code
                    .ok_or_else(|| TunnelError::Internal("response had no status code".into()))?;
                let pairs = resp
                    .headers
                    .iter()
                    .map(|h| {
                        let v = String::from_utf8_lossy(h.value).into_owned();
                        (h.name.to_string(), v)
                    })
                    .collect::<Vec<_>>();
                let leftover = buf.split_off(consumed);
                return Ok((status, pairs, leftover));
            }
            httparse::Status::Partial => {
                // need more bytes
            }
        }
    }
}

// ── Plain TCP ────────────────────────────────────────────────────────────────

async fn proxy_tcp<R, W>(
    local_port: u16,
    _request: &ConnectRequest,
    from_edge: &mut R,
    to_edge: &mut W,
    counters: &StreamCounters,
) -> Result<(), TunnelError>
where
    R: futures::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    let tcp = TcpStream::connect(("127.0.0.1", local_port))
        .await
        .map_err(|e| TunnelError::Internal(format!("tcp connect: {e}")))?;
    let (mut r, mut w) = tcp.into_split();

    // ACK: send empty ConnectResponse before bytes flow.
    stream::write_connect_response(to_edge, "", &[]).await?;

    let edge_to_local = pump_futures_to_tokio_counted(from_edge, &mut w, &counters.bytes_in);
    let local_to_edge = pump_tokio_to_futures_counted(&mut r, to_edge, &counters.bytes_out);
    let _ = futures::future::join(edge_to_local, local_to_edge).await;
    Ok(())
}

// ── Cross-IO byte pumps ──────────────────────────────────────────────────────

async fn pump_futures_to_tokio_counted<R, W>(
    mut src: R,
    dst: &mut W,
    counter: &AtomicU64,
) -> Result<(), TunnelError>
where
    R: futures::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = src
            .read(&mut buf)
            .await
            .map_err(|e| TunnelError::Internal(format!("read: {e}")))?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])
            .await
            .map_err(|e| TunnelError::Internal(format!("write: {e}")))?;
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
    Ok(())
}

async fn pump_tokio_to_futures_counted<R, W>(
    src: &mut R,
    dst: &mut W,
    counter: &AtomicU64,
) -> Result<(), TunnelError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = src
            .read(&mut buf)
            .await
            .map_err(|e| TunnelError::Internal(format!("read: {e}")))?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])
            .await
            .map_err(|e| TunnelError::Internal(format!("write: {e}")))?;
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_path_strips_scheme() {
        assert_eq!(
            extract_path("https://abc.trycloudflare.com/path?q=1"),
            "/path?q=1"
        );
        assert_eq!(extract_path("https://abc.trycloudflare.com"), "/");
        assert_eq!(extract_path("/relative/x"), "/relative/x");
    }

    #[test]
    fn build_head_includes_method_host_path() {
        let req = ConnectRequest {
            dest: "https://abc.trycloudflare.com/foo".into(),
            conn_type: ConnectionType::Http,
            metadata: vec![
                (HTTP_METHOD_KEY.into(), "POST".into()),
                (HTTP_HOST_KEY.into(), "abc.trycloudflare.com".into()),
                (format!("{HTTP_HEADER_KEY}:User-Agent"), "x/1".into()),
                (format!("{HTTP_HEADER_KEY}:X-Stuff"), "yo".into()),
            ],
        };
        let head = build_request_head(&req).unwrap();
        assert!(head.starts_with("POST /foo HTTP/1.1\r\n"));
        assert!(head.contains("Host: abc.trycloudflare.com\r\n"));
        assert!(head.contains("User-Agent: x/1\r\n"));
        assert!(head.contains("X-Stuff: yo\r\n"));
        assert!(head.ends_with("\r\n\r\n"));
    }
}
