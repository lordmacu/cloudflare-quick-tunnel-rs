//! POST `/tunnel` client for `api.trycloudflare.com`.
//!
//! Returns the credentials the edge expects on the subsequent
//! `RegisterConnection` RPC: a UUID-shaped `id`, the public
//! `hostname` (`<sub>.trycloudflare.com`), the `account_tag` to
//! quote on RPC, and 32 random bytes of `secret` that double as the
//! `TunnelSecret` in the auth blob.
//!
//! Mirrors `cmd/cloudflared/tunnel/quick_tunnel.go` upstream.

use std::time::Duration;

use serde::Deserialize;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::error::{QuickTunnelApiError, TunnelError};

/// Public-facing JSON envelope returned by `POST /tunnel`.
#[derive(Debug, Deserialize)]
pub struct QuickTunnelResponse {
    pub success: bool,
    #[serde(default)]
    pub result: Option<QuickTunnel>,
    #[serde(default)]
    pub errors: Vec<QuickTunnelApiError>,
}

/// The bits the QUIC + capnp-RPC dance needs.
///
/// `secret` is delivered as a base64 string in the JSON body; the
/// `serde_bytes_b64` helper decodes it back to raw bytes so callers
/// can stuff them straight into the capnp `TunnelAuth.tunnelSecret`
/// field. Mirror of cloudflared's `QuickTunnel` Go struct.
#[derive(Debug, Deserialize)]
pub struct QuickTunnel {
    pub id: String,
    pub name: String,
    pub hostname: String,
    pub account_tag: String,
    #[serde(with = "serde_bytes_b64")]
    pub secret: Vec<u8>,
}

/// Default endpoint (the public trycloudflare API).
pub const DEFAULT_SERVICE_URL: &str = "https://api.trycloudflare.com";

/// User-Agent we send. Mimic a recent `cloudflared` so the edge
/// doesn't trip a novelty filter. Bump in lockstep with the
/// schema commit pinned in `THIRD_PARTY_NOTICES.md`.
pub const DEFAULT_USER_AGENT: &str = "cloudflared/2024.12.0";

/// HTTP-level deadline for the POST.
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// How many times to retry on transient 5xx / network errors.
pub const MAX_RETRIES: u32 = 3;

/// Fetch a fresh quick-tunnel handshake. Retries 5xx + network
/// errors with exponential backoff (1s → 2s → 4s); never retries
/// 4xx. Business errors inside a 200 response surface as
/// [`TunnelError::ApiBusiness`].
pub async fn request_tunnel(
    service_url: &str,
    user_agent: &str,
) -> Result<QuickTunnel, TunnelError> {
    let url = format!("{}/tunnel", service_url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .user_agent(user_agent)
        .timeout(DEFAULT_HTTP_TIMEOUT)
        .build()
        .map_err(TunnelError::Api)?;

    let mut backoff = Duration::from_secs(1);
    let mut last_err: Option<TunnelError> = None;

    for attempt in 0..=MAX_RETRIES {
        debug!(attempt, %url, "POST /tunnel");
        match try_once(&client, &url).await {
            Ok(tunnel) => return Ok(tunnel),
            Err(err) => {
                if !err.is_transient() || attempt == MAX_RETRIES {
                    return Err(err);
                }
                warn!(
                    attempt,
                    error = %err,
                    backoff_ms = backoff.as_millis() as u64,
                    "POST /tunnel transient failure; retrying"
                );
                last_err = Some(err);
                sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        TunnelError::Internal("request_tunnel: retry loop fell through without an error".into())
    }))
}

async fn try_once(client: &reqwest::Client, url: &str) -> Result<QuickTunnel, TunnelError> {
    let resp = client
        .post(url)
        .header("Content-Type", "application/json")
        .send()
        .await?;

    let status = resp.status();
    let body = resp.bytes().await?;

    // 5xx with non-JSON body must surface as a transient error so
    // the retry loop kicks in. JSON 5xx envelopes are rare but
    // possible — they go through the parse path below.
    if status.is_server_error() && !looks_like_json(&body) {
        let snippet_len = 200usize.min(body.len());
        let body_snippet = String::from_utf8_lossy(&body[..snippet_len]).into_owned();
        return Err(TunnelError::ApiNonJson {
            status: status.as_u16(),
            body_snippet,
        });
    }

    // The edge sometimes hands back HTML when rate-limiting; surface
    // a snippet so the operator can read the actual reason instead
    // of staring at a bare "expected value at line 1 column 1".
    if !looks_like_json(&body) {
        let snippet_len = 200usize.min(body.len());
        let body_snippet = String::from_utf8_lossy(&body[..snippet_len]).into_owned();
        return Err(TunnelError::ApiNonJson {
            status: status.as_u16(),
            body_snippet,
        });
    }

    let envelope: QuickTunnelResponse = serde_json::from_slice(&body).map_err(|e| {
        TunnelError::Internal(format!("malformed JSON from /tunnel: {e}"))
    })?;

    if !envelope.success {
        return Err(TunnelError::ApiBusiness(envelope.errors));
    }

    envelope.result.ok_or_else(|| {
        TunnelError::Internal("POST /tunnel returned success=true but no `result` body".into())
    })
}

fn looks_like_json(body: &[u8]) -> bool {
    body.iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| *b == b'{' || *b == b'[')
}

mod serde_bytes_b64 {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s: String = Deserialize::deserialize(d)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

impl TunnelError {
    /// Errors a retry could plausibly recover (network / 5xx).
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            TunnelError::Api(e) => {
                e.is_timeout()
                    || e.is_connect()
                    || e.is_request()
                    || e.status().is_some_and(|s| s.is_server_error())
            }
            TunnelError::ApiNonJson { status, .. } => (500..600).contains(status),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_ok_body() -> serde_json::Value {
        serde_json::json!({
            "success": true,
            "result": {
                "id": "8f6d3c2a-1111-4d2e-9b9b-aaaaaaaaaaaa",
                "name": "quick-tunnel-abc",
                "hostname": "abc-123.trycloudflare.com",
                "account_tag": "deadbeefcafef00d",
                "secret": "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="
            },
            "errors": []
        })
    }

    #[tokio::test]
    async fn happy_path_parses_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tunnel"))
            .and(header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_ok_body()))
            .expect(1)
            .mount(&server)
            .await;

        let t = request_tunnel(&server.uri(), DEFAULT_USER_AGENT)
            .await
            .expect("happy path");
        assert_eq!(t.hostname, "abc-123.trycloudflare.com");
        assert_eq!(t.account_tag, "deadbeefcafef00d");
        assert_eq!(t.secret.len(), 32);
        assert_eq!(t.secret[0..4], [1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn business_error_does_not_retry() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "success": false,
            "errors": [{ "code": 1003, "message": "tunnel quota exceeded" }]
        });
        Mock::given(method("POST"))
            .and(path("/tunnel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;

        let err = request_tunnel(&server.uri(), DEFAULT_USER_AGENT)
            .await
            .expect_err("should fail");
        match err {
            TunnelError::ApiBusiness(errs) => {
                assert_eq!(errs.len(), 1);
                assert_eq!(errs[0].code, 1003);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn html_body_surfaces_snippet() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tunnel"))
            .respond_with(
                ResponseTemplate::new(429).set_body_string("<html><body>rate limited</body></html>"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let err = request_tunnel(&server.uri(), DEFAULT_USER_AGENT)
            .await
            .expect_err("should fail");
        match err {
            TunnelError::ApiNonJson {
                status,
                body_snippet,
            } => {
                assert_eq!(status, 429);
                assert!(body_snippet.contains("rate limited"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn five_xx_retries_then_succeeds() {
        let server = MockServer::start().await;

        // First call → 503 (non-JSON, transient), second → 200 OK.
        Mock::given(method("POST"))
            .and(path("/tunnel"))
            .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/tunnel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_ok_body()))
            .expect(1)
            .mount(&server)
            .await;

        // We can't override the 1s initial backoff without exposing
        // it, so the test does sit for ~1s. That's acceptable for
        // wiremock-class tests and keeps the public API surface
        // minimal.
        let t = request_tunnel(&server.uri(), DEFAULT_USER_AGENT)
            .await
            .expect("retry should succeed");
        assert_eq!(t.hostname, "abc-123.trycloudflare.com");
    }

    #[tokio::test]
    async fn four_xx_does_not_retry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tunnel"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "success": false,
                "errors": [{ "code": 400, "message": "bad request" }]
            })))
            .expect(1) // critical: only 1 hit, no retry
            .mount(&server)
            .await;

        let err = request_tunnel(&server.uri(), DEFAULT_USER_AGENT)
            .await
            .expect_err("should fail");
        // It's a business error (success=false), not a transport one.
        assert!(matches!(err, TunnelError::ApiBusiness(_)));
    }

    #[test]
    fn looks_like_json_handles_leading_whitespace() {
        assert!(looks_like_json(b"  \n  {"));
        assert!(looks_like_json(b"["));
        assert!(!looks_like_json(b"<html>"));
        assert!(!looks_like_json(b""));
    }
}
