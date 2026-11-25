//! The windows of one service over many servers (2.4 "여러 서버에서의 창"),
//! as pure state: no task, no clock, no I/O. The worker feeds it what
//! happened and sends the `Window` values it returns.
//!
//! * **Invariant.** The windows of all servers add up to at most the slots P.
//!   A server's idle window is window − in flight.
//! * **Starving.** A server is starving from its `Pending` (or an `Assign`
//!   that filled the window with `more`) until an idle window appears on it:
//!   a run there ends, or its filling `Assign` said nothing more waits. A slot
//!   granted to a starving server is not an idle window appearing: the server
//!   fills it at once with the work it said was waiting.
//! * **The one rule.** At every change (a signal, a run's end, a
//!   connection) if a server starves, one slot moves to the starving server
//!   with the smallest window. It comes from the largest idle window of a
//!   server that does not starve; failing that, from the server whose run
//!   just ended, if its window is at least 2 above the receiver's.
//!   "The server whose run just ended" is read as a server that was starving
//!   when its run ended: its new idle slot is about to be refilled, so it
//!   gives only under the 2-above rule (once all servers starve the windows
//!   settle within one and no frame follows). A server that was not starving
//!   when its run ended has a slot that is really idle, and gives it like any
//!   other non-starving server (P < S: a zero window is reached at that end).
//! * **Slots with no place** (the start, a server that left, a disconnected
//!   server's idle window) go to the starving server with the smallest
//!   window, else to the connected server with the smallest window; ties are
//!   broken at random, so each worker starts on a random server.
//! * A disconnected server keeps the share its runs hold (window = in
//!   flight) and gets its current value again when it is back.
//! * A `Pending` from a server known to have an idle window is a count gone
//!   wrong (a lease ran out and the server closed the window): its current
//!   value is sent again.
//! * An `Assign` beyond the window (a lowered window crossing an assignment)
//!   is an [`Arrival::Overflow`]: the worker gives it back at once.
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | per-server window book | (server, channel) → window, in flight, starving | worker service | `Join` ~ the server leaving the list | servers × channels | the worker ([`Book::remove`]) |
//! | execution slots | slot | worker service | the service's | P | none (fixed) |

use crate::line::backoff::Rng;
use std::collections::BTreeMap;

/// One server as the book sees it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Seat {
    /// Joined on a live connection.
    pub connected: bool,
    pub window: u32,
    pub in_flight: u32,
    pub starving: bool,
}

impl Seat {
    fn idle(&self) -> u32 {
        self.window.saturating_sub(self.in_flight)
    }
}

/// What to do with an arriving assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    Run,
    /// Past the window: give it back (`Release{failed: false}`).
    Overflow,
}

/// A `Window` value to send.
pub type Change<S> = (S, u32);

pub struct Book<S: Ord + Clone> {
    slots: u32,
    seats: BTreeMap<S, Seat>,
    closed: bool,
    rng: Rng,
}

impl<S: Ord + Clone> Book<S> {
    pub fn new(slots: u32) -> Self {
        Book { slots, seats: BTreeMap::new(), closed: false, rng: Rng::default() }
    }

    pub fn seats(&self) -> &BTreeMap<S, Seat> {
        &self.seats
    }

    pub fn seat(&self, server: &S) -> Option<Seat> {
        self.seats.get(server).copied()
    }

    /// Slots no window holds.
    pub fn unplaced(&self) -> u32 {
        self.slots.saturating_sub(self.seats.values().map(|seat| seat.window).sum())
    }

    /// The server is in the list (not connected yet).
    pub fn add(&mut self, server: S) {
        self.seats.entry(server).or_default();
    }

    /// The server left the list: its window has no place now.
    pub fn remove(&mut self, server: &S) -> Vec<Change<S>> {
        self.seats.remove(server);
        self.rebalance(None, Vec::new())
    }

    /// Joined on a new connection. The server's own value is always in the
    /// answer: it must be told its window again.
    pub fn connect(&mut self, server: &S) -> Vec<Change<S>> {
        let closed = self.closed;
        let Some(seat) = self.seats.get_mut(server) else { return Vec::new() };
        seat.connected = true;
        seat.starving = false;
        if closed {
            seat.window = 0;
        }
        let window = seat.window;
        self.rebalance(None, vec![(server.clone(), window)])
    }

    /// The connection went: keep what its runs hold, free the idle rest.
    pub fn disconnect(&mut self, server: &S) -> Vec<Change<S>> {
        let Some(seat) = self.seats.get_mut(server) else { return Vec::new() };
        seat.connected = false;
        seat.starving = false;
        seat.window = seat.in_flight;
        self.rebalance(None, Vec::new())
    }

    /// An assignment arrived; `more`: it filled the window and work is waiting.
    pub fn assigned(&mut self, server: &S, more: bool) -> (Arrival, Vec<Change<S>>) {
        let closed = self.closed;
        let Some(seat) = self.seats.get_mut(server) else { return (Arrival::Overflow, Vec::new()) };
        if closed || seat.in_flight >= seat.window {
            return (Arrival::Overflow, Vec::new());
        }
        seat.in_flight += 1;
        if seat.idle() == 0 {
            seat.starving = more;
            if more {
                return (Arrival::Run, self.rebalance(None, Vec::new()));
            }
        }
        (Arrival::Run, Vec::new())
    }

    /// `Pending`: a full window would take work that waits.
    pub fn pending(&mut self, server: &S) -> Vec<Change<S>> {
        let closed = self.closed;
        let Some(seat) = self.seats.get_mut(server) else { return Vec::new() };
        if !seat.connected || closed {
            return Vec::new();
        }
        if seat.idle() > 0 {
            // The count went wrong: say the value again.
            return vec![(server.clone(), seat.window)];
        }
        seat.starving = true;
        self.rebalance(None, Vec::new())
    }

    /// A run assigned by `server` ended (completed, released or given up).
    pub fn ended(&mut self, server: &S) -> Vec<Change<S>> {
        let Some(seat) = self.seats.get_mut(server) else { return Vec::new() };
        let was_starving = seat.starving;
        seat.in_flight = seat.in_flight.saturating_sub(1);
        if !seat.connected {
            seat.window = seat.in_flight;
        }
        if seat.idle() > 0 {
            seat.starving = false;
        }
        self.rebalance(was_starving.then(|| server.clone()), Vec::new())
    }

    /// Shutting down (2.9 "종료" 1): every window to 0, and none opens again.
    pub fn close(&mut self) -> Vec<Change<S>> {
        self.closed = true;
        let mut changes = Vec::new();
        for (server, seat) in self.seats.iter_mut() {
            if seat.window > 0 && seat.connected {
                changes.push((server.clone(), 0));
            }
            seat.window = 0;
            seat.starving = false;
        }
        changes
    }

    /// `ended`: the server whose run just ended, when it was starving then.
    fn rebalance(&mut self, ended: Option<S>, mut changes: Vec<Change<S>>) -> Vec<Change<S>> {
        if self.closed {
            return changes;
        }
        while self.unplaced() > 0 {
            let Some(target) = self.home() else { break };
            if let Some(seat) = self.seats.get_mut(&target) {
                seat.window += 1;
                changes.push((target.clone(), seat.window));
            }
        }
        if let Some(receiver) = self.smallest(|seat| seat.connected && seat.starving) {
            let receiving = self.seats[&receiver].window;
            let donor = self
                .seats
                .iter()
                .filter(|(server, seat)| seat.connected && !seat.starving && seat.idle() > 0 && **server != receiver && Some(*server) != ended.as_ref())
                .max_by_key(|(_, seat)| (seat.idle(), seat.window))
                .map(|(server, _)| server.clone())
                .or_else(|| ended.filter(|server| *server != receiver && self.seats.get(server).is_some_and(|seat| seat.connected && seat.window >= receiving + 2)));
            if let Some(donor) = donor {
                if let Some(seat) = self.seats.get_mut(&donor) {
                    seat.window -= 1;
                    changes.push((donor.clone(), seat.window));
                }
                if let Some(seat) = self.seats.get_mut(&receiver) {
                    seat.window += 1;
                    changes.push((receiver, seat.window));
                }
            }
        }
        // A later value of one server replaces an earlier one.
        let mut last: BTreeMap<S, u32> = BTreeMap::new();
        let mut order = Vec::new();
        for (server, window) in changes {
            if last.insert(server.clone(), window).is_none() {
                order.push(server);
            }
        }
        order
            .into_iter()
            .map(|server| {
                let window = last[&server];
                (server, window)
            })
            .collect()
    }

    /// Where a slot with no place goes.
    fn home(&mut self) -> Option<S> {
        self.smallest(|seat| seat.connected && seat.starving).or_else(|| self.smallest(|seat| seat.connected))
    }

    /// The server with the smallest window among those `wanted` picks, a tie broken at random.
    fn smallest(&mut self, wanted: impl Fn(&Seat) -> bool) -> Option<S> {
        let least = self.seats.values().filter(|seat| wanted(seat)).map(|seat| seat.window).min()?;
        let ties: Vec<&S> = self.seats.iter().filter(|(_, seat)| wanted(seat) && seat.window == least).map(|(server, _)| server).collect();
        let pick = self.rng.below(ties.len());
        ties.get(pick).map(|server| (*server).clone())
    }
}

#[cfg(test)]
mod tests;
