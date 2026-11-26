//! Server selection (2.8, B19): the place a router plugs in.
//!
//! The SDK hands a [`Selector`] only the servers it may use — connected and
//! not draining — and keeps the other constraint itself: a registration once sent is sent
//! again only to the same server. A selector never sees a retransmission.

use crate::line::backoff::Rng;
use crate::servers::Server;
use parking_lot::Mutex;
use ruvent::Channel;

/// One server a registration may go to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub server: Server,
    /// Bytes sent to it and not yet acknowledged: grows when it slows down.
    pub unacked_bytes: u64,
}

/// What is being placed.
#[derive(Debug)]
pub struct Choice<'a> {
    /// The registration's channel and type; `None` for a deletion.
    pub channel: Option<Channel>,
    pub event_type: Option<u16>,
    /// Sorted by server; never empty.
    pub candidates: &'a [Candidate],
}

/// Picks the server for one registration. `None` makes the send wait until
/// something changes (a commit, a connection). A router that keeps a channel
/// on one server picks by `channel`.
pub trait Selector: Send + Sync + 'static {
    fn select(&self, choice: &Choice<'_>) -> Option<usize>;
}

/// The default (B19): fewer unacknowledged bytes wins; with three or more
/// candidates, the better of two picked at random.
#[derive(Default)]
pub struct LeastUnacked {
    rng: Mutex<Rng>,
}

impl Selector for LeastUnacked {
    fn select(&self, choice: &Choice<'_>) -> Option<usize> {
        let candidates = choice.candidates;
        if candidates.is_empty() {
            return None;
        }
        let pool: Vec<usize> = (0..candidates.len()).collect();
        let lighter = |a: usize, b: usize| if candidates[b].unacked_bytes < candidates[a].unacked_bytes { b } else { a };
        match pool.len() {
            1 => Some(pool[0]),
            2 => Some(lighter(pool[0], pool[1])),
            len => {
                let mut rng = self.rng.lock();
                let first = rng.below(len);
                let second = (first + 1 + rng.below(len - 1)) % len;
                Some(lighter(pool[first], pool[second]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn candidates(bytes: &[u64]) -> Vec<Candidate> {
        bytes.iter().enumerate().map(|(index, unacked_bytes)| Candidate { server: Arc::from(format!("s{index}")), unacked_bytes: *unacked_bytes }).collect()
    }

    #[test]
    fn the_default_selector_prefers_fewer_unacknowledged_bytes() {
        let selector = LeastUnacked::default();
        let channel = Some(Channel::new());
        let two = candidates(&[500, 100]);
        assert_eq!(selector.select(&Choice { channel, event_type: Some(1), candidates: &two }), Some(1));
        let many = candidates(&[0, 900, 900, 900]);
        let picks: Vec<usize> = (0..200).filter_map(|_| selector.select(&Choice { channel, event_type: Some(1), candidates: &many })).collect();
        assert!(picks.iter().filter(|pick| **pick == 0).count() > 50, "the light server wins whenever it is one of the two");
        assert_eq!(selector.select(&Choice { channel, event_type: None, candidates: &[] }), None);
    }
}
