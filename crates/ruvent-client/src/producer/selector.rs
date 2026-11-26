//! Server selection (2.8, B19): the place a router plugs in.
//!
//! The SDK hands a [`Selector`] only the servers it may use — connected and
//! not draining — and keeps the other constraint itself: a registration once sent is sent
//! again only to the same server. A selector never sees a retransmission.

use crate::line::backoff::Rng;
use crate::servers::Server;
use parking_lot::Mutex;
use ruvent::{Channel, GroupId};
use std::hash::{DefaultHasher, Hash, Hasher};

/// One server a registration may go to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub server: Server,
    /// Bytes sent to it and not yet acknowledged: grows when it slows down.
    pub unacked_bytes: u64,
    /// The registration's type is paused there (2.6).
    pub paused: bool,
}

/// What is being placed.
#[derive(Debug)]
pub struct Choice<'a> {
    /// The registration's channel and type; `None` for a deletion.
    pub channel: Option<Channel>,
    pub event_type: Option<u16>,
    /// B26: set for events that must stay on one server.
    pub group: Option<GroupId>,
    /// Sorted by server; never empty.
    pub candidates: &'a [Candidate],
}

/// Picks the server for one registration. `None`, or a paused candidate,
/// makes the send wait until something changes (a commit, a resume, a connection).
pub trait Selector: Send + Sync + 'static {
    fn select(&self, choice: &Choice<'_>) -> Option<usize>;
}

/// The default (B19): fewer unacknowledged bytes wins; with three or more
/// candidates, the better of two picked at random. Paused servers come last.
/// A `group` is pinned by its hash over the candidates (B26; it moves when
/// the list changes).
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
        if let Some(group) = choice.group {
            let mut hasher = DefaultHasher::new();
            group.hash(&mut hasher);
            return Some((hasher.finish() % candidates.len() as u64) as usize);
        }
        let open: Vec<usize> = (0..candidates.len()).filter(|index| !candidates[*index].paused).collect();
        let pool = if open.is_empty() { (0..candidates.len()).collect() } else { open };
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

    fn candidates(bytes: &[(u64, bool)]) -> Vec<Candidate> {
        bytes.iter().enumerate().map(|(index, (unacked_bytes, paused))| Candidate { server: Arc::from(format!("s{index}")), unacked_bytes: *unacked_bytes, paused: *paused }).collect()
    }

    #[test]
    fn the_default_selector_prefers_fewer_unacknowledged_bytes_and_avoids_paused_servers() {
        let selector = LeastUnacked::default();
        let channel = Some(Channel::new());
        let two = candidates(&[(500, false), (100, false)]);
        assert_eq!(selector.select(&Choice { channel, event_type: Some(1), group: None, candidates: &two }), Some(1));
        let paused = candidates(&[(500, false), (100, true)]);
        assert_eq!(selector.select(&Choice { channel, event_type: Some(1), group: None, candidates: &paused }), Some(0));
        let many = candidates(&[(0, false), (900, false), (900, false), (900, false)]);
        let picks: Vec<usize> = (0..200).filter_map(|_| selector.select(&Choice { channel, event_type: Some(1), group: None, candidates: &many })).collect();
        assert!(picks.iter().filter(|pick| **pick == 0).count() > 50, "the light server wins whenever it is one of the two");
        let group = Some([7; 16]);
        let first = selector.select(&Choice { channel, event_type: Some(1), group, candidates: &many });
        assert!((0..20).all(|_| selector.select(&Choice { channel, event_type: Some(1), group, candidates: &many }) == first), "a group stays on one server");
    }
}
