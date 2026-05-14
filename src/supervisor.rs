//! Long-running task that owns the QUIC connection after register.
//!
//! Two concurrent loops:
//!
//!   1. Inbound-stream acceptor — `conn.accept_bi()` in a loop;
//!      every new stream is the edge wanting us to serve a single
//!      HTTP request, so we hand it to `proxy::handle_inbound_stream`
//!      on a spawned task.
//!   2. Shutdown watcher — selects on the shutdown channel; on
//!      signal, closes the connection with an application code so
//!      the edge sees a graceful close instead of an idle-timeout.
//!
//! QUIC-level keepalive (`keep_alive_interval = 1s` set in
//! `quic_dial::build_client_config`) handles the connection-liveness
//! side — without it the edge's `MaxIdleTimeout = 5s` would terminate
//! us once the control-stream RPC drains. We currently do NOT keep
//! the control stream's capnp-RPC system alive past register; if
//! the edge ever starts requiring `UnregisterConnection` to be sent
//! over the same stream, that's the place to bolt it on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::error::TunnelError;

#[derive(Debug, Default, Clone)]
pub struct SupervisorMetrics {
    pub streams_total: Arc<AtomicU64>,
    pub bytes_in: Arc<AtomicU64>,
    pub bytes_out: Arc<AtomicU64>,
}

impl SupervisorMetrics {
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.streams_total.load(Ordering::Relaxed),
            self.bytes_in.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
        )
    }
}

/// What `start_supervisor` hands back to the manager so it can
/// later trigger a graceful close.
pub struct SupervisorHandle {
    pub join: tokio::task::JoinHandle<()>,
    pub shutdown: oneshot::Sender<()>,
    pub metrics: SupervisorMetrics,
}

pub fn start_supervisor(conn: quinn::Connection, local_port: u16) -> SupervisorHandle {
    let metrics = SupervisorMetrics::default();
    let metrics_owned = metrics.clone();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let join = tokio::spawn(async move {
        info!(local_port, "tunnel supervisor running");
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    debug!("supervisor: shutdown signal");
                    conn.close(0u32.into(), b"client shutdown");
                    break;
                }
                accepted = conn.accept_bi() => {
                    match accepted {
                        Ok((send, recv)) => {
                            metrics_owned.streams_total.fetch_add(1, Ordering::Relaxed);
                            let local_port = local_port;
                            tokio::spawn(async move {
                                if let Err(e) =
                                    crate::proxy::handle_inbound_stream(local_port, send, recv).await
                                {
                                    warn!(error = %e, "stream proxy failed");
                                }
                            });
                        }
                        Err(quinn::ConnectionError::ApplicationClosed(_))
                        | Err(quinn::ConnectionError::LocallyClosed) => {
                            debug!("connection closed cleanly");
                            break;
                        }
                        Err(e) => {
                            warn!(error = %e, "accept_bi failed; supervisor exiting");
                            break;
                        }
                    }
                }
            }
        }
        info!("tunnel supervisor exited");
    });

    SupervisorHandle {
        join,
        shutdown: shutdown_tx,
        metrics,
    }
}

#[allow(dead_code)]
fn _signal_used(_: TunnelError) {}
