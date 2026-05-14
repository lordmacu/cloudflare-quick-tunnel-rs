//! Long-running task that owns one QUIC connection after register.
//!
//! Two concurrent arms in a biased select!:
//!
//!   1. Inbound-stream acceptor — `conn.accept_bi()` in a loop;
//!      every new stream is the edge wanting us to serve a single
//!      HTTP request, so we hand it to `proxy::handle_inbound_stream`
//!      on a spawned task.
//!   2. Shutdown watcher — receives on the shutdown channel; on
//!      signal, closes the connection with an application code so
//!      the edge sees a graceful close instead of an idle-timeout.
//!
//! Returns a [`SupervisorExit`] tag so the reactor in `manager.rs`
//! can tell apart "the operator asked to stop" from "the edge dropped
//! us and we should reconnect".
//!
//! QUIC-level keepalive (`keep_alive_interval = 1s` set in
//! `quic_dial::build_client_config`) handles the connection-liveness
//! side — without it the edge's `MaxIdleTimeout = 5s` would terminate
//! us once the control-stream RPC drains.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::proxy::StreamCounters;

#[derive(Debug, Default, Clone)]
pub struct SupervisorMetrics {
    pub streams_total: Arc<AtomicU64>,
    pub bytes_in: Arc<AtomicU64>,
    pub bytes_out: Arc<AtomicU64>,
}

impl SupervisorMetrics {
    fn stream_counters(&self) -> StreamCounters {
        StreamCounters {
            bytes_in: self.bytes_in.clone(),
            bytes_out: self.bytes_out.clone(),
        }
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.streams_total.load(Ordering::Relaxed),
            self.bytes_in.load(Ordering::Relaxed),
            self.bytes_out.load(Ordering::Relaxed),
        )
    }
}

/// Why the supervisor's accept loop returned. The reactor uses this
/// to decide between "exit cleanly" and "reconnect with backoff".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorExit {
    /// The operator signalled shutdown via the channel.
    Shutdown,
    /// The edge or local stack closed the QUIC connection out
    /// from under us. Reconnect candidate.
    ConnectionLost,
}

/// Run one accept-loop cycle on `conn`. Returns when the connection
/// closes (for any reason) or when `shutdown_rx` fires.
///
/// Callers own the shutdown channel so the reactor can stitch
/// multiple supervisor cycles together with the same QuickTunnelHandle.
pub async fn run(
    conn: quinn::Connection,
    local_port: u16,
    metrics: SupervisorMetrics,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> SupervisorExit {
    info!(local_port, "tunnel supervisor running");
    let exit = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                debug!("supervisor: shutdown signal");
                conn.close(0u32.into(), b"client shutdown");
                break SupervisorExit::Shutdown;
            }
            accepted = conn.accept_bi() => {
                match accepted {
                    Ok((send, recv)) => {
                        metrics.streams_total.fetch_add(1, Ordering::Relaxed);
                        let counters = metrics.stream_counters();
                        tokio::spawn(async move {
                            if let Err(e) =
                                crate::proxy::handle_inbound_stream(local_port, send, recv, counters).await
                            {
                                warn!(error = %e, "stream proxy failed");
                            }
                        });
                    }
                    Err(quinn::ConnectionError::ApplicationClosed(_))
                    | Err(quinn::ConnectionError::LocallyClosed) => {
                        debug!("connection closed cleanly");
                        // Locally closed is almost always us, but
                        // accept loops can race with the close; if
                        // we got here without seeing the shutdown
                        // signal, treat as a lost connection.
                        break SupervisorExit::ConnectionLost;
                    }
                    Err(e) => {
                        warn!(error = %e, "accept_bi failed; supervisor cycling");
                        break SupervisorExit::ConnectionLost;
                    }
                }
            }
        }
    };
    info!(?exit, "tunnel supervisor exited");
    exit
}
