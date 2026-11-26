//! The subscription book, driven input by input with no task and no clock.
//! Answers are labelled with `&str` so each test reads as a trace.

use super::*;
use std::sync::Arc;

type Book = Channels<&'static str>;

/// A test channel: a UUIDv7-shaped id derived from a label, the same every time.
pub fn ch(label: &str) -> Channel {
    let fnv = |seed: u64| label.bytes().fold(seed, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3));
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&fnv(0xcbf2_9ce4_8422_2325).to_le_bytes());
    bytes[8..].copy_from_slice(&fnv(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Channel(bytes)
}

fn channel(label: &str) -> Channel {
    ch(label)
}

fn server(name: &str) -> Server {
    Arc::from(name)
}

fn sub(channel: Channel, ticket: u64) -> Call {
    Call::Subscribe { channel, ticket }
}

fn oks(step: &Step<&'static str>) -> Vec<&'static str> {
    step.answers.iter().filter(|(_, result)| result.is_ok()).map(|(label, _)| *label).collect()
}

fn errors(step: &Step<&'static str>) -> Vec<(&'static str, SubscribeError)> {
    step.answers.iter().filter_map(|(label, result)| (*result).err().map(|rejection| (*label, rejection))).collect()
}

fn audience(book: &Book, name: &str, on: &str) -> Vec<MemberId> {
    book.audience(&channel(name), &server(on))
}

/// Joins `user` to `channel` and runs it to "subscribed" on `on`: confirmation, then the call's answer.
fn joined(book: &mut Book, user: MemberId, channel: Channel, on: &str, label: &'static str) -> u64 {
    let step = book.join(user, channel, label);
    let Some(Call::Subscribe { ticket, .. }) = step.calls.first().cloned() else { panic!("no subscribe issued") };
    book.confirmed(&server(on), &channel, ticket);
    assert_eq!(oks(&book.answered(&channel, ticket, Ok(()))), vec![label]);
    ticket
}

// ── joining ──────────────────────────────────────────────────────────────

/// A first join issues one subscribe with the first ticket; the user is in
/// the audience from its server's confirmation, and is answered once the call comes back too.
#[test]
fn a_join_waits_for_its_own_confirmation_and_its_answer() {
    let mut book = Book::new();
    let step = book.join(1, channel("room"), "u1");
    assert_eq!(step.calls, vec![sub(channel("room"), 1)]);
    assert!(step.answers.is_empty(), "not in effect before the confirmation");
    assert_eq!(audience(&book, "room", "s0"), Vec::<MemberId>::new());
    let step = book.confirmed(&server("s0"), &channel("room"), 1);
    assert!(step.answers.is_empty(), "the call has not come back yet");
    assert_eq!(audience(&book, "room", "s0"), vec![1], "in effect on s0 from its confirmation");
    assert_eq!(oks(&book.answered(&channel("room"), 1, Ok(()))), vec!["u1"]);
}

/// The call's answer and the confirmation may be read in either order; the join is answered after both.
#[test]
fn the_answer_and_the_confirmation_settle_in_either_order() {
    let mut book = Book::new();
    book.join(1, channel("room"), "u1");
    assert!(book.answered(&channel("room"), 1, Ok(())).answers.is_empty(), "not in effect anywhere yet");
    assert_eq!(oks(&book.confirmed(&server("s0"), &channel("room"), 1)), vec!["u1"]);
}

/// Every join of a new user issues its own subscribe, even on a channel already subscribed.
#[test]
fn every_join_issues_its_own_subscribe() {
    let mut book = Book::new();
    joined(&mut book, 1, channel("room"), "s0", "u1");
    let step = book.join(2, channel("room"), "u2");
    assert_eq!(step.calls, vec![sub(channel("room"), 2)]);
    assert!(step.answers.is_empty());
}

/// A confirmation of an earlier subscribe does not activate a later joiner.
#[test]
fn an_earlier_confirmation_does_not_activate_a_later_joiner() {
    let mut book = Book::new();
    book.join(1, channel("room"), "u1");
    book.join(2, channel("room"), "u2");
    book.confirmed(&server("s0"), &channel("room"), 1);
    assert_eq!(audience(&book, "room", "s0"), vec![1], "u2's subscribe is not confirmed yet");
    book.confirmed(&server("s0"), &channel("room"), 2);
    assert_eq!(audience(&book, "room", "s0"), vec![1, 2]);
}

/// Joining a channel one's join was already answered on is answered at once and issues nothing.
#[test]
fn joining_twice_is_answered_at_once_without_a_call() {
    let mut book = Book::new();
    joined(&mut book, 1, channel("room"), "s0", "u1");
    let step = book.join(1, channel("room"), "again");
    assert!(step.calls.is_empty());
    assert_eq!(oks(&step), vec!["again"]);
}

/// A second join while the first still waits issues nothing and is answered with it.
#[test]
fn a_second_join_while_waiting_waits_with_the_first() {
    let mut book = Book::new();
    book.join(1, channel("room"), "first");
    assert!(book.join(1, channel("room"), "second").calls.is_empty());
    book.confirmed(&server("s0"), &channel("room"), 1);
    assert_eq!(oks(&book.answered(&channel("room"), 1, Ok(()))), vec!["first", "second"]);
}

/// A subscription is in effect per server: a confirmation from one server says nothing about another.
#[test]
fn a_user_is_in_effect_per_server() {
    let mut book = Book::new();
    book.join(1, channel("room"), "u1");
    book.confirmed(&server("s0"), &channel("room"), 1);
    assert_eq!((audience(&book, "room", "s0"), audience(&book, "room", "s1")), (vec![1], vec![]));
    book.confirmed(&server("s1"), &channel("room"), 1);
    assert_eq!(audience(&book, "room", "s1"), vec![1]);
}

/// Confirmations and answers for channels or tickets nobody waits for change nothing.
#[test]
fn confirmations_and_answers_nobody_asked_for_are_ignored() {
    let mut book = Book::new();
    assert!(book.confirmed(&server("s0"), &channel("ghost"), 9).answers.is_empty());
    assert!(!book.contains(&channel("ghost")), "a confirmation creates no entry");
    book.join(1, channel("room"), "u1");
    assert!(book.answered(&channel("room"), 7, Ok(())).answers.is_empty());
    assert!(book.answered(&channel("room"), 7, Err(Refusal::Forbidden)).answers.is_empty());
    assert!(book.contains(&channel("room")));
}

// ── leaving and `0 → 1 → 0 → 1` ─────────────────────────────────────────

/// Only the last user leaving unsubscribes, once, and drops the entry.
#[test]
fn only_the_last_user_leaving_unsubscribes() {
    let mut book = Book::new();
    joined(&mut book, 1, channel("room"), "s0", "u1");
    joined(&mut book, 2, channel("room"), "s0", "u2");
    assert!(book.leave(1, &channel("room")).calls.is_empty());
    assert_eq!(book.leave(2, &channel("room")).calls, vec![Call::Unsubscribe(channel("room"))]);
    assert!(!book.contains(&channel("room")));
    assert!(book.leave(2, &channel("room")).calls.is_empty(), "leaving again does nothing");
}

/// Leaving before the subscription took effect answers the waiting join as cancelled.
#[test]
fn leaving_before_the_confirmation_answers_the_waiting_join() {
    let mut book = Book::new();
    book.join(1, channel("room"), "u1");
    let step = book.leave(1, &channel("room"));
    assert_eq!(step.calls, vec![Call::Unsubscribe(channel("room"))]);
    let failed = errors(&step);
    assert_eq!(failed.len(), 1);
    assert_eq!((failed[0].0, failed[0].1), ("u1", SubscribeError::Left));
}

/// After `0 → 1 → 0 → 1` the user waits for the second subscribe's confirmation; the first's activates nothing.
#[test]
fn resubscribing_waits_for_the_second_subscribes_confirmation() {
    let mut book = Book::new();
    let first = joined(&mut book, 1, channel("room"), "s0", "u1");
    book.leave(1, &channel("room"));
    let step = book.join(1, channel("room"), "again");
    assert_eq!(step.calls, vec![sub(channel("room"), first + 1)]);
    book.confirmed(&server("s0"), &channel("room"), first);
    assert_eq!(audience(&book, "room", "s0"), Vec::<MemberId>::new(), "a late confirmation of the first subscribe leaks nothing");
    book.confirmed(&server("s0"), &channel("room"), first + 1);
    assert_eq!(audience(&book, "room", "s0"), vec![1]);
}

/// Leaving everything leaves every channel; leaving what one never joined does nothing.
#[test]
fn leaving_all_leaves_every_channel_and_unknown_ones_do_nothing() {
    let mut book = Book::new();
    joined(&mut book, 1, channel("a"), "s0", "a");
    joined(&mut book, 1, channel("b"), "s0", "b");
    let step = book.leave_all(1, [channel("a"), channel("b")]);
    assert_eq!(step.calls, vec![Call::Unsubscribe(channel("a")), Call::Unsubscribe(channel("b"))]);
    assert!(book.leave(1, &channel("never")).calls.is_empty());
}

// ── refusals ─────────────────────────────────────────────────────────────

/// A refused call fails the join it answers, keeps its cause, and the last refused member unsubscribes.
#[test]
fn a_refusal_fails_the_join_it_answers() {
    let mut book = Book::new();
    book.join(1, channel("secret"), "u1");
    let step = book.answered(&channel("secret"), 1, Err(Refusal::Forbidden));
    let failed = errors(&step);
    assert_eq!((failed[0].0, failed[0].1), ("u1", SubscribeError::Refused(Refusal::Forbidden)));
    assert_eq!(step.calls, vec![Call::Unsubscribe(channel("secret"))]);
    assert!(!book.contains(&channel("secret")));
}

// ── connections ──────────────────────────────────────────────────────────

/// A server's connection ending (or starting again) takes it out of every audience until it confirms again.
#[test]
fn a_reconnection_resets_a_server_until_it_confirms_again() {
    let mut book = Book::new();
    let ticket = joined(&mut book, 1, channel("room"), "s0", "u1");
    book.confirmed(&server("s1"), &channel("room"), ticket);
    book.reset(&server("s0"));
    assert_eq!((audience(&book, "room", "s0"), audience(&book, "room", "s1")), (vec![], vec![1]), "only s0 is reset");
    assert!(book.confirmed(&server("s0"), &channel("room"), ticket + 5).answers.is_empty(), "already answered: nothing more to say");
    assert_eq!(audience(&book, "room", "s0"), vec![1], "the subscription said again at the new connection");
}

/// With no server connected the call comes back at once; the join waits for the first server to confirm.
#[test]
fn a_join_with_no_server_connected_waits_for_the_first_confirmation() {
    let mut book = Book::new();
    book.join(1, channel("room"), "early");
    assert!(book.answered(&channel("room"), 1, Ok(())).answers.is_empty());
    assert_eq!(oks(&book.confirmed(&server("s0"), &channel("room"), 1)), vec!["early"], "said at the connection with the calls handled so far");
}

// ── one channel, no hierarchy ────────────────────────────────────────────

/// A channel is an id: a member of one channel hears nothing of any other,
/// however alike their labels were (there is no prefix any more).
#[test]
fn a_member_hears_only_its_own_channel() {
    let mut book = Book::new();
    joined(&mut book, 1, channel("room"), "s0", "u1");
    joined(&mut book, 2, channel("room.1"), "s0", "u2");
    assert_eq!(audience(&book, "room", "s0"), vec![1]);
    assert_eq!(audience(&book, "room.1", "s0"), vec![2]);
    assert!(audience(&book, "room.2", "s0").is_empty());
}
