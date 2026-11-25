//! Reconnection delays: exponential back-off with full jitter (S20).
//!
//! The n-th failed attempt in a row waits a uniformly random time in
//! `[0, min(cap, initial × 2^n)]`. `Bye{draining}` changes nothing here: a
//! server that is being taken down for a restart is back as soon as it
//! listens again, and a single-server deployment must not wait longer than
//! the ordinary ceiling to find it. A draining server refuses a production
//! connection with one `Bye`, which costs it no more than the attempt.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// S20.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reconnect {
    /// The first attempt's ceiling.
    pub initial: Duration,
    /// The ceiling never rises past this. Shorter than the server's reconnect
    /// grace (3 s), so a live worker is back within it.
    pub cap: Duration,
}

impl Default for Reconnect {
    fn default() -> Self {
        Reconnect { initial: Duration::from_millis(50), cap: Duration::from_secs(1) }
    }
}

/// A small pseudo-random source (xorshift64*), seeded from the process's
/// hash keys. Not for anything secret: jitter and tie-breaking only.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Default for Rng {
    fn default() -> Self {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(0x9e37_79b9_7f4a_7c15);
        Rng(hasher.finish() | 1)
    }
}

impl Rng {
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `0..=bound`.
    pub fn up_to(&mut self, bound: u64) -> u64 {
        if bound == u64::MAX {
            return self.next_u64();
        }
        self.next_u64() % (bound + 1)
    }

    /// Uniform in `0..len` (`len` > 0).
    pub fn below(&mut self, len: usize) -> usize {
        (self.next_u64() % len.max(1) as u64) as usize
    }
}

/// The back-off state of one line.
///
/// | key | (server, role) |
/// | --- | --- |
/// | scope | one line |
/// | lifetime | a disconnection ~ the next welcome |
/// | bound | one counter |
/// | cleared by | [`Backoff::reset`] at a welcome |
#[derive(Clone, Debug)]
pub struct Backoff {
    config: Reconnect,
    failures: u32,
    rng: Rng,
}

impl Backoff {
    pub fn new(config: Reconnect) -> Self {
        Backoff { config, failures: 0, rng: Rng::default() }
    }

    /// A welcome: the next disconnection starts over.
    pub fn reset(&mut self) {
        self.failures = 0;
    }

    /// The wait before the next attempt.
    pub fn wait(&mut self) -> Duration {
        let grown = self.config.initial.saturating_mul(1u32.checked_shl(self.failures.min(31)).unwrap_or(u32::MAX));
        let ceiling = grown.min(self.config.cap);
        self.failures = self.failures.saturating_add(1);
        let ceiling = u64::try_from(ceiling.as_micros()).unwrap_or(u64::MAX);
        Duration::from_micros(self.rng.up_to(ceiling))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn back_off_grows_to_its_cap_with_full_jitter() {
        let config = Reconnect { initial: Duration::from_millis(50), cap: Duration::from_secs(1) };
        let mut backoff = Backoff::new(config);
        let mut ceilings = Vec::new();
        for attempt in 0..12u32 {
            let wait = backoff.wait();
            let ceiling = config.initial.saturating_mul(1 << attempt).min(config.cap);
            assert!(wait <= ceiling, "attempt {attempt}: {wait:?} > {ceiling:?}");
            ceilings.push(ceiling);
        }
        assert_eq!(ceilings.last(), Some(&config.cap));
        let waits: Vec<Duration> = (0..64).map(|_| backoff.wait()).collect();
        assert!(waits.iter().any(|wait| *wait < config.cap / 2), "full jitter: some waits are short");
        backoff.reset();
        assert!(backoff.wait() <= config.initial);
    }
}
