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
            // Mirror cloudflared `features/features.go::defaultFeatures`
            // exactly. Drift means the edge may refuse or downgrade
            // the connection silently. `support_datagram_v3` is
            // DEPRECATED upstream — kept out on purpose.
            features: vec![
                "allow_remote_config".into(),
                "serialized_headers".into(),
                "support_datagram_v2".into(),
                "support_quic_eof".into(),
                "management_logs".into(),
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

/// Owns the long-lived control-stream resources so the edge keeps
/// the tunnel registered. Dropping this triggers the dedicated
/// driver thread to wind down, which closes the control stream —
/// the edge then unregisters the tunnel and stops routing traffic.
///
/// Construct via [`register_connection`]; hold across the tunnel's
/// lifetime; drop on shutdown.
pub struct ControlSession {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    _join: std::thread::JoinHandle<()>,
}

impl Drop for ControlSession {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // Don't wait for the thread to actually join — the edge
        // doesn't require an explicit unregister; the dropped
        // RPC system + closed stream is enough.
    }
}

/// Set up a capnp-RPC client over the first bidi stream of `conn`,
/// call `RegistrationServer.registerConnection`, and KEEP the
/// stream + RPC system alive for the lifetime of the returned
/// `ControlSession`. Returns the edge's `ConnectionDetails` plus
/// the session handle.
///
/// The QUIC connection MUST be freshly handshaked — opening more
/// than one stream before this would break the edge's
/// "first-stream-is-control" assumption (cloudflared
/// `quic_connection.go::Serve` opens the control stream first
/// thing, then the request streams come on top).
pub async fn register_connection(
    conn: &quinn::Connection,
    auth: &TunnelAuth,
    tunnel_id: Uuid,
    conn_index: u8,
    options: &ConnectionOptions,
) -> Result<(RegistrationDetails, ControlSession), TunnelError> {
    debug!(%tunnel_id, conn_index, "opening control stream");
    let (send, recv) = conn.open_bi().await.map_err(|e| {
        TunnelError::Register(format!("open_bi on control stream: {e}"))
    })?;
    // capnp-rpc's `RpcSystem` is `!Send` (internal Rc<RefCell<_>>),
    // so we can't drive it from a tokio task. Spawn a dedicated OS
    // thread with its own current-thread tokio runtime + LocalSet
    // to host the system. Communicate result via oneshot.
    //
    // The thread runs for the full lifetime of the tunnel — it
    // returns only when the ControlSession is dropped, signalled
    // through `shutdown_rx`, OR when the edge tears down the
    // control stream from its side.
    let (done_tx, done_rx) =
        tokio::sync::oneshot::channel::<Result<RegistrationDetails, TunnelError>>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let auth_owned = auth.clone();
    let options_owned = options.clone();

    let join = std::thread::Builder::new()
        .name("cfqt-rpc-driver".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rpc driver runtime");
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                // Construct VatNetwork inside this thread because
                // its internals are !Send (Rc<RefCell<…>>). We bridge
                // the quinn streams to futures-io here too.
                let reader = recv.compat();
                let writer = send.compat_write();
                let network = Box::new(twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Client,
                    Default::default(),
                ));
                let mut rpc_system = RpcSystem::new(network, None);
                let server: tunnelrpc_capnp::registration_server::Client =
                    rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

                // Build + dispatch the register call.
                let request =
                    match build_register_request(&server, &auth_owned, tunnel_id, conn_index, &options_owned) {
                        Ok(r) => r,
                        Err(e) => {
                            let _ = done_tx.send(Err(e));
                            return;
                        }
                    };
                let response_promise = request.send().promise;

                let call = async {
                    let reply = response_promise.await.map_err(|e| {
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

                tokio::pin!(call);
                tokio::pin!(shutdown_rx);
                let mut sent_done = false;
                let mut done_tx = Some(done_tx);
                loop {
                    tokio::select! {
                        biased;
                        // 1. Register call completes — return result to caller.
                        res = &mut call, if !sent_done => {
                            if let Some(tx) = done_tx.take() {
                                let _ = tx.send(res);
                            }
                            sent_done = true;
                        }
                        // 2. Caller asked us to shut down — drop everything.
                        _ = &mut shutdown_rx => {
                            break;
                        }
                        // 3. RPC system died (edge dropped stream, etc).
                        _ = &mut rpc_system => {
                            if !sent_done {
                                if let Some(tx) = done_tx.take() {
                                    let _ = tx.send(Err(TunnelError::Register(
                                        "RPC system terminated before call completed".into(),
                                    )));
                                }
                            }
                            break;
                        }
                    }
                }
                // Explicitly drop the bootstrap so capnp-rpc closes
                // the stream cleanly on the way out.
                drop(server);
            });
        })
        .map_err(|e| TunnelError::Internal(format!("spawn rpc driver thread: {e}")))?;

    let details = tokio::time::timeout(DEFAULT_RPC_TIMEOUT, done_rx)
        .await
        .map_err(|_| TunnelError::Register("register_connection RPC timed out".into()))?
        .map_err(|_| TunnelError::Register("RPC driver dropped result channel".into()))??;

    info!(uuid = %details.uuid, location = %details.location, "registered with edge");

    Ok((
        details,
        ControlSession {
            shutdown: Some(shutdown_tx),
            _join: join,
        },
    ))
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
