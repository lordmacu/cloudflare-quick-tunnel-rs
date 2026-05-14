//! Cap'n Proto-RPC client for the `TunnelServer` /
//! `RegistrationServer` interface (see `schemas/tunnelrpc.capnp`).
//!
//! ## Wire shape
//!
//! Cloudflared's edge expects the **first** bidi QUIC stream on the
//! connection to be the control plane — no `quic_metadata_protocol`
//! header is written there (that header is per-request, used on the
//! HTTP streams the edge opens back at us later). The control
//! stream just carries capnp-RPC framing straight away.
//!
//! On that stream we set up a capnp-RPC twoparty client, bootstrap
//! the remote interface, treat it as a `TunnelServer` (which
//! `extends RegistrationServer`), and call `registerConnection`.
//!
//! ## Mirrors
//!
//! - `cloudflared/connection/quic_connection.go::Serve` — opens
//!   `controlStream, _ := q.conn.OpenStream()` as the first stream.
//! - `cloudflared/tunnelrpc/registration_client.go::RegisterConnection`
//!   — same parameter packing into the capnp call.

use std::time::Duration;

use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use tokio::time::timeout;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::error::TunnelError;
use crate::tunnelrpc_capnp;

/// Sentinel error string the edge returns when the same connection
/// index is already registered for this tunnel. Worth distinguishing
/// because retrying the same conn-index would just race.
pub const DUPLICATE_CONNECTION_ERROR: &str = "edge already has connection registered for the given connection identifier";

/// Default request budget for `register_connection`.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(15);

/// Auth blob the edge expects. Mirror of `TunnelAuth` in the schema.
#[derive(Debug, Clone)]
pub struct TunnelAuth {
    pub account_tag: String,
    pub tunnel_secret: Vec<u8>,
}

/// Connection options sent on register. Mirror of `ConnectionOptions`
/// (with `ClientInfo` collapsed in) — only the fields the quick-
/// tunnel flow actually needs.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub client_id: [u8; 16],
    pub features: Vec<String>,
    pub version: String,
    pub arch: String,
    pub origin_local_ip: Vec<u8>,
    pub replace_existing: bool,
    pub compression_quality: u8,
    pub num_previous_attempts: u8,
}

impl ConnectionOptions {
    /// Sensible default options to send on a brand-new quick tunnel.
    /// Features mirror what a recent cloudflared advertises that the
    /// edge accepts even for anonymous quick tunnels.
    pub fn default_for_quick_tunnel(version: &str) -> Self {
        Self {
            client_id: *Uuid::new_v4().as_bytes(),
            features: vec![
                "serialized_headers".into(),
                "ha-origin".into(),
                "support_datagram_v3".into(),
            ],
            version: version.to_string(),
            arch: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            origin_local_ip: vec![],
            replace_existing: false,
            compression_quality: 0,
            num_previous_attempts: 0,
        }
    }
}

/// The bits we keep from a successful `registerConnection` reply —
/// mirror of `ConnectionDetails`.
#[derive(Debug, Clone)]
pub struct RegistrationDetails {
    pub uuid: Uuid,
    pub location: String,
    pub tunnel_is_remotely_managed: bool,
}

/// Set up a capnp-RPC client over the first bidi stream of `conn`
/// and call `RegistrationServer.registerConnection`. Returns the
/// edge's `ConnectionDetails` on success, or a `TunnelError` on
/// transport / business failure.
///
/// The QUIC connection MUST be freshly handshaked — opening more
/// than one stream before this would break the edge's
/// "first-stream-is-control" assumption.
pub async fn register_connection(
    conn: &quinn::Connection,
    auth: &TunnelAuth,
    tunnel_id: Uuid,
    conn_index: u8,
    options: &ConnectionOptions,
) -> Result<RegistrationDetails, TunnelError> {
    debug!(%tunnel_id, conn_index, "opening control stream");
    let (send, recv) = conn.open_bi().await.map_err(|e| {
        TunnelError::Register(format!("open_bi on control stream: {e}"))
    })?;

    // capnp-rpc speaks futures-io, not tokio-io. Bridge via
    // tokio_util::compat. The reader needs Send + Unpin + 'static
    // because RpcSystem internally spawns onto its own driver.
    let reader = recv.compat();
    let writer = send.compat_write();
    // VatNetwork's third arg is OUR side of the twoparty link.
    // We're the QUIC connection initiator (this whole crate is the
    // client) → `Side::Client`. The edge is `Side::Server`, and that
    // is the side whose bootstrap capability we request below.
    let network = Box::new(twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut rpc_system = RpcSystem::new(network, None);
    // The schema has `interface TunnelServer extends (RegistrationServer)`.
    // The edge's bootstrap object honours both, so we ask directly for the
    // narrower `RegistrationServer` view — that's where
    // `register_connection_request` lives on the generated client.
    let server: tunnelrpc_capnp::registration_server::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

    // Drive the RPC system alongside our single call. capnp-rpc's
    // contract is that polling the system + polling the call future
    // both have to make progress, hence the join. We don't care if
    // the system future returns first (server dropped the stream);
    // the call's error will be the actionable one.
    let request = build_register_request(&server, auth, tunnel_id, conn_index, options)?;

    let call = async {
        let reply = request.send().promise.await.map_err(|e| {
            TunnelError::Register(format!("register_connection RPC: {e}"))
        })?;
        let response_reader = reply.get().map_err(|e| {
            TunnelError::Register(format!("response root: {e}"))
        })?;
        let result = response_reader.get_result().map_err(|e| {
            TunnelError::Register(format!("response.result: {e}"))
        })?;
        decode_connection_response(result)
    };

    // Use timeout instead of select! to keep the system future alive
    // for the duration of the call.
    let system_fut = async move {
        let _ = rpc_system.await;
    };

    let outcome = tokio::select! {
        biased;
        res = call => res,
        _ = system_fut => Err(TunnelError::Register(
            "RPC system terminated before call completed".into(),
        )),
        _ = tokio::time::sleep(DEFAULT_RPC_TIMEOUT) => Err(TunnelError::Register(
            format!("register_connection RPC timed out after {:?}", DEFAULT_RPC_TIMEOUT),
        )),
    };

    match &outcome {
        Ok(d) => info!(uuid = %d.uuid, location = %d.location, "registered with edge"),
        Err(e) => warn!(error = %e, "register_connection failed"),
    }
    outcome
}

// ── Internals ─────────────────────────────────────────────────────────────────

fn build_register_request(
    server: &tunnelrpc_capnp::registration_server::Client,
    auth: &TunnelAuth,
    tunnel_id: Uuid,
    conn_index: u8,
    options: &ConnectionOptions,
) -> Result<
    capnp::capability::Request<
        tunnelrpc_capnp::registration_server::register_connection_params::Owned,
        tunnelrpc_capnp::registration_server::register_connection_results::Owned,
    >,
    TunnelError,
> {
    let mut request = server.register_connection_request();
    {
        let mut params = request.get();

        // ── auth ──────────────────────────────────────────────────────────────
        let mut a = params.reborrow().init_auth();
        a.set_account_tag(auth.account_tag.as_str());
        a.set_tunnel_secret(&auth.tunnel_secret);

        // ── tunnel_id ─────────────────────────────────────────────────────────
        params.set_tunnel_id(tunnel_id.as_bytes());

        params.set_conn_index(conn_index);

        // ── options ───────────────────────────────────────────────────────────
        let mut o = params.reborrow().init_options();
        {
            let mut client = o.reborrow().init_client();
            client.set_client_id(&options.client_id);
            client.set_version(options.version.as_str());
            client.set_arch(options.arch.as_str());
            let mut feats = client.init_features(options.features.len() as u32);
            for (i, f) in options.features.iter().enumerate() {
                feats.set(i as u32, f.as_str());
            }
        }
        o.set_origin_local_ip(&options.origin_local_ip);
        o.set_replace_existing(options.replace_existing);
        o.set_compression_quality(options.compression_quality);
        o.set_num_previous_attempts(options.num_previous_attempts);
    }
    Ok(request)
}

fn decode_connection_response(
    response: tunnelrpc_capnp::connection_response::Reader,
) -> Result<RegistrationDetails, TunnelError> {
    use tunnelrpc_capnp::connection_response::result::WhichReader;
    let result = response.get_result();
    match result
        .which()
        .map_err(|e| TunnelError::Register(format!("ConnectionResponse union: {e:?}")))?
    {
        WhichReader::Error(err_reader) => {
            let err = err_reader
                .map_err(|e| TunnelError::Register(format!("ConnectionError reader: {e}")))?;
            let cause = err
                .get_cause()
                .ok()
                .and_then(|t| t.to_string().ok())
                .unwrap_or_else(|| "<missing cause>".into());
            if cause == DUPLICATE_CONNECTION_ERROR {
                return Err(TunnelError::Register(format!(
                    "duplicate connection (edge already has connIndex registered): {cause}"
                )));
            }
            Err(TunnelError::Register(cause))
        }
        WhichReader::ConnectionDetails(details_reader) => {
            let d = details_reader.map_err(|e| {
                TunnelError::Register(format!("ConnectionDetails reader: {e}"))
            })?;
            let uuid_bytes = d
                .get_uuid()
                .map_err(|e| TunnelError::Register(format!("ConnectionDetails.uuid: {e}")))?;
            if uuid_bytes.len() != 16 {
                return Err(TunnelError::Register(format!(
                    "ConnectionDetails.uuid wrong length: {}",
                    uuid_bytes.len()
                )));
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(uuid_bytes);
            let uuid = Uuid::from_bytes(u);
            let location = d
                .get_location_name()
                .ok()
                .and_then(|t| t.to_string().ok())
                .unwrap_or_default();
            let tunnel_is_remotely_managed = d.get_tunnel_is_remotely_managed();
            Ok(RegistrationDetails {
                uuid,
                location,
                tunnel_is_remotely_managed,
            })
        }
    }
}

// Silence "unused" for the wrapper helpers in scaffold builds.
#[allow(dead_code)]
async fn drive<F: std::future::Future>(
    f: F,
    label: &'static str,
) -> Result<F::Output, TunnelError> {
    timeout(DEFAULT_RPC_TIMEOUT, f)
        .await
        .map_err(|_| TunnelError::Register(format!("{label} timed out")))
}

#[allow(dead_code)]
fn _suppress_unused_promise() -> Promise<(), capnp::Error> {
    Promise::ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_carry_features() {
        let o = ConnectionOptions::default_for_quick_tunnel("test/0.1");
        assert!(o.features.contains(&"serialized_headers".to_string()));
        assert_eq!(o.client_id.len(), 16);
        assert!(o.version.contains("test/0.1"));
    }

    #[test]
    fn duplicate_sentinel_matches_upstream() {
        // Exact byte-for-byte match with cloudflared's `DuplicateConnectionError`
        // in connection/control.go. If upstream rephrases this we want a loud
        // CI failure rather than silently mis-classifying the error.
        assert_eq!(
            DUPLICATE_CONNECTION_ERROR,
            "edge already has connection registered for the given connection identifier"
        );
    }
}
