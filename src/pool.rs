//! Bounded idle-TCP-connection pool against `127.0.0.1:<port>`.
//!
//! Reduces socket() + connect() overhead per inbound stream by
//! reusing keep-alive connections to the local origin. Each pooled
//! entry carries a release timestamp; entries older than `idle_ttl`
//! are reaped on acquire so we never hand back a connection the
//! origin has already half-closed by idle timeout.
//!
//! The pool only stores connections that are known to be in a
//! healthy "ready for next request" state — the proxy must
//! call [`Pool::release`] **only** after fully reading a response
//! whose framing it understood (Content-Length-bounded).

use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::trace;

/// Idle socket TTL — most servers (e.g. axum, hyper, nginx) idle
/// out keep-alive connections at 60-75s; 30s gives us a safe
/// buffer and matches the typical client-side default.
pub const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(30);

/// Soft cap on idle sockets per pool. Beyond this we drop the
/// freshly-released socket on the floor; the next acquire will
/// open a new one if needed.
pub const DEFAULT_MAX_IDLE: usize = 16;

struct Idle {
    stream: TcpStream,
    released_at: Instant,
}

pub struct Pool {
    port: u16,
    idle_ttl: Duration,
    max_idle: usize,
    idle: Mutex<Vec<Idle>>,
}

impl Pool {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            idle_ttl: DEFAULT_IDLE_TTL,
            max_idle: DEFAULT_MAX_IDLE,
            idle: Mutex::new(Vec::new()),
        }
    }

    /// Acquire a socket: pop a fresh idle entry if available,
    /// otherwise open a new TCP connection.
    pub async fn acquire(&self) -> std::io::Result<TcpStream> {
        // Drain stale entries up-front so callers never see a
        // stream older than `idle_ttl`. Locking inside the loop
        // gives the reaper write-access without blocking other
        // acquires for the connect path.
        {
            let mut g = self.idle.lock().await;
            while let Some(entry) = g.last() {
                if entry.released_at.elapsed() <= self.idle_ttl {
                    let entry = g.pop().expect("checked not-empty");
                    trace!(port = self.port, pool_size = g.len(), "pool hit");
                    return Ok(entry.stream);
                }
                g.pop();
            }
        }
        trace!(port = self.port, "pool miss; opening fresh TCP");
        TcpStream::connect(("127.0.0.1", self.port)).await
    }

    /// Return a socket to the pool. Drop on overflow.
    pub async fn release(&self, stream: TcpStream) {
        let mut g = self.idle.lock().await;
        if g.len() >= self.max_idle {
            trace!(port = self.port, "pool full; dropping released stream");
            return;
        }
        g.push(Idle {
            stream,
            released_at: Instant::now(),
        });
        trace!(port = self.port, pool_size = g.len(), "pool released");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Pool hits should reuse the same socket.
    #[tokio::test]
    async fn acquire_after_release_returns_same_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let _ = s.write_all(b"ping").await;
                });
            }
        });

        let pool = Pool::new(port);
        let s1 = pool.acquire().await.unwrap();
        let s1_local = s1.local_addr().unwrap();
        pool.release(s1).await;

        let s2 = pool.acquire().await.unwrap();
        assert_eq!(s2.local_addr().unwrap(), s1_local, "should reuse socket");
    }

    #[tokio::test]
    async fn pool_evicts_stale_entries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });
        let mut pool = Pool::new(port);
        pool.idle_ttl = Duration::from_millis(50);

        let s1 = pool.acquire().await.unwrap();
        let s1_local = s1.local_addr().unwrap();
        pool.release(s1).await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        let s2 = pool.acquire().await.unwrap();
        assert_ne!(
            s2.local_addr().unwrap(),
            s1_local,
            "stale entry should have been evicted"
        );
    }
}
