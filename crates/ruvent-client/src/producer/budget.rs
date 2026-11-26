//! The retry budget (2.9, S9): resending the same work must not multiply the
//! load. Every ordinary send earns `ratio` of a retry; a retry spends one.
//! The balance starts at `floor` retries and never rises past `ceiling`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Parts of one retry the balance is kept in.
const UNIT: u64 = 1_000_000;

/// | key | the producer handle |
/// | --- | --- |
/// | scope | one process |
/// | lifetime | the handle's |
/// | bound | one counter, at most `ceiling` retries |
/// | cleared by | spending |
#[derive(Debug)]
pub struct RetryBudget {
    balance: AtomicU64,
    earn: u64,
    ceiling: u64,
}

impl RetryBudget {
    /// S9: `ratio` 0.1 is one retry per ten ordinary sends.
    pub fn new(ratio: f64, floor: u64, ceiling: u64) -> Self {
        let earn = (ratio.clamp(0.0, 1.0) * UNIT as f64) as u64;
        RetryBudget { balance: AtomicU64::new(floor.saturating_mul(UNIT)), earn, ceiling: ceiling.max(floor).saturating_mul(UNIT) }
    }

    /// An ordinary send.
    pub fn earn(&self) {
        let _ = self.balance.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |balance| Some(balance.saturating_add(self.earn).min(self.ceiling)));
    }

    /// A retry: whether the budget allows it (and if so, it is spent).
    pub fn spend(&self) -> bool {
        self.balance.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |balance| balance.checked_sub(UNIT)).is_ok()
    }

    /// Whole retries available now.
    pub fn available(&self) -> u64 {
        self.balance.load(Ordering::Relaxed) / UNIT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_are_bounded_by_a_fraction_of_ordinary_sends() {
        let budget = RetryBudget::new(0.1, 1, 5);
        assert!(budget.spend());
        assert!(!budget.spend(), "the floor is spent");
        for _ in 0..10 {
            budget.earn();
        }
        assert!(budget.spend(), "ten sends earn one retry");
        assert!(!budget.spend());
        for _ in 0..1000 {
            budget.earn();
        }
        assert_eq!(budget.available(), 5, "the balance stops at the ceiling");
    }
}
