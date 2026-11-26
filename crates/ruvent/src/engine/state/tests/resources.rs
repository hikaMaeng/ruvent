//! Managed memory and its watermark (2.5, 2.6): past it the server releases
//! cached payloads and nothing else — no producer is paused, nothing is
//! refused, every lane is still assigned from (plan 9.15).

use super::*;

/// A volatile ledger whose budget is `budget` bytes, with metadata counted as 100 bytes per event.
fn tight(budget: u64) -> H {
    let memory = MemoryConfig { limit: Some(budget * 10 / 9 + 1), slot_bytes: 100, ..MemoryConfig::default() };
    H::with(EngineConfig { memory, producers: crate::engine::config::ProducerConfig { entry_bytes: 0, ..Default::default() }, ..config() }, false).cast()
}

/// Far past the high watermark and the limit, registrations are applied and
/// committed, follow-ups are registered, workers are assigned, and nobody is
/// told to wait, faulted or disconnected.
#[test]
fn memory_past_the_watermark_refuses_and_pauses_nothing() {
    let mut h = tight(1_000);
    let mut told = Vec::new();
    for _ in 0..100 {
        let effects = h.produce(P, work("x"));
        told.extend(sent(&effects, P));
        assert!(!closed(&effects, P));
    }
    assert!(h.s.memory_used() > h.s.memory_budget(), "100 events of 100 bytes are past a budget of 1000");
    assert_eq!(h.s.live(), 100, "every registration was applied");
    assert!(told.iter().all(|message| matches!(message, Outbound::Committed { .. })), "only commits were told to the producer: {told:?}");
    let joined = h.join(W1, "x", 1);
    let [assignment] = assigned(&joined, W1).try_into().unwrap_or_else(|_| panic!("the join was given work"));
    let done = h.complete(W1, &assignment, vec![work("y")]);
    assert!(faults(&done, W1).is_empty(), "a completion with a follow-up is applied over the limit");
    assert_eq!(h.waiting("y"), 1, "its follow-up waits like any other");
    let [next] = assigned(&done, W1).try_into().unwrap_or_else(|_| panic!("the window got the next event"));
    assert_eq!(next.event.channel, ch("x"));
}
