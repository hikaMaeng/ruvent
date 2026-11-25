//! TCP keepalive on every socket (2.2, B16, S13).
//!
//! Silence is how an idle connection looks, and NATs, load balancers and
//! firewalls drop silent connections without a FIN. The kernel finds out:
//! keepalive probes, answered by the peer's kernel (so a slow application
//! is never mistaken for a dead one), and on Linux `TCP_USER_TIMEOUT`, which
//! also ends a connection whose sent data stays unacknowledged. No
//! application timer, no heartbeat frame.

use std::io;
use std::time::Duration;
use tokio::net::TcpStream;

/// S13.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keepalive {
    /// Idle time before the first probe.
    pub idle: Duration,
    /// Time between probes.
    pub interval: Duration,
    /// Unanswered probes before the connection is dropped (where the
    /// platform lets it be set).
    pub retries: u32,
    /// Linux `TCP_USER_TIMEOUT`: how long sent data may stay unacknowledged.
    pub user_timeout: Duration,
}

impl Default for Keepalive {
    fn default() -> Self {
        Keepalive { idle: Duration::from_secs(15), interval: Duration::from_secs(5), retries: 3, user_timeout: Duration::from_secs(30) }
    }
}

/// `TCP_NODELAY`, keepalive, and on Linux `TCP_USER_TIMEOUT`.
pub fn tune(stream: &TcpStream, keepalive: &Keepalive) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let socket = socket2::SockRef::from(stream);
    let params = socket2::TcpKeepalive::new().with_time(keepalive.idle).with_interval(keepalive.interval);
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", target_os = "windows"))]
    let params = params.with_retries(keepalive.retries);
    socket.set_tcp_keepalive(&params)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket.set_tcp_user_timeout(Some(keepalive.user_timeout))?;
    Ok(())
}
