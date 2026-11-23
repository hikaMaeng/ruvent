//! The subscription book, driven input by input with no task and no clock.
//! Answers are labelled with `&str` so each test reads as a trace.

use super::*;
use crate::message::Cause;

type Book = Channels<&'static str>;

fn name(text: &str) -> Name {
    Name::new(text)
}

fn connected() -> Book {
    let mut book = Book::new(None);
    assert!(book.connected().calls.is_empty(), "nothing is wanted yet");
    book
}

fn sub(channel: &str) -> Call {
    Call::Subscribe(name(channel))
}

fn unsub(channel: &str) -> Call {
    Call::Unsubscribe(name(channel))
}

fn oks(step: &Step<&'static str>) -> Vec<&'static str> {
    step.answers.iter().filter(|(_, result)| result.is_ok()).map(|(label, _)| *label).collect()
}

fn errors(step: &Step<&'static str>) -> Vec<(&'static str, Rejection)> {
    step.answers.iter().filter_map(|(label, result)| result.clone().err().map(|rejection| (*label, rejection))).collect()
}

fn audience(book: &Book, channel: &str) -> Vec<UserId> {
    let mut users: Vec<UserId> = book.audience(&name(channel)).collect();
    users.sort_unstable();
    users
}

// ── joining ──────────────────────────────────────────────────────────────

/// A first join sends one subscribe and the user waits; its marker makes it active.
#[test]
fn a_join_waits_for_the_marker_of_its_own_subscribe() {
    let mut book = connected();
    let step = book.join(1, name("room"), "u1");
    assert_eq!(step.calls, vec![sub("room")]);
    assert!(step.answers.is_empty(), "not active before the marker");
    assert_eq!(audience(&book, "room"), Vec::<UserId>::new());
    let step = book.marker(&name("room"));
    assert_eq!(oks(&step), vec!["u1"]);
    assert_eq!(audience(&book, "room"), vec![1]);
}

/// Every join issues its own subscribe, even on a channel already subscribed.
#[test]
fn every_join_issues_its_own_subscribe() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.marker(&name("room"));
    let step = book.join(2, name("room"), "u2");
    assert_eq!(step.calls, vec![sub("room")]);
    assert!(step.answers.is_empty());
}

/// A marker that answers an earlier subscribe does not activate a later joiner.
#[test]
fn an_earlier_marker_does_not_activate_a_later_joiner() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.join(2, name("room"), "u2");
    assert_eq!(oks(&book.marker(&name("room"))), vec!["u1"]);
    assert_eq!(audience(&book, "room"), vec![1], "u2's subscribe is still unanswered");
    assert_eq!(oks(&book.marker(&name("room"))), vec!["u2"]);
    assert_eq!(audience(&book, "room"), vec![1, 2]);
}

/// Joining a channel one is already active on is answered at once and sends nothing.
#[test]
fn joining_twice_is_answered_at_once_without_a_call() {
    let mut book = connected();
    book.join(1, name("room"), "first");
    book.marker(&name("room"));
    let step = book.join(1, name("room"), "again");
    assert!(step.calls.is_empty());
    assert_eq!(oks(&step), vec!["again"]);
}

/// Channels are independent: a marker on one never activates a waiter on another.
#[test]
fn a_marker_on_one_channel_leaves_other_channels_alone() {
    let mut book = connected();
    book.join(1, name("a"), "on a");
    book.join(1, name("b"), "on b");
    assert_eq!(oks(&book.marker(&name("a"))), vec!["on a"]);
    assert_eq!(audience(&book, "b"), Vec::<UserId>::new());
}

// ── markers nobody asked for ─────────────────────────────────────────────

/// A marker when every issued subscribe was already answered changes nothing.
#[test]
fn a_marker_nobody_asked_for_activates_nobody() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.marker(&name("room"));
    book.join(2, name("room"), "u2");
    book.marker(&name("room"));
    let step = book.marker(&name("room"));
    assert!(step.answers.is_empty() && step.calls.is_empty());
    let step = book.join(3, name("room"), "u3");
    assert!(step.answers.is_empty(), "the stray marker was not banked for the next joiner");
}

/// A marker for a channel the book does not hold is ignored.
#[test]
fn a_marker_for_an_unknown_channel_is_ignored() {
    let mut book = connected();
    let step = book.marker(&name("nowhere"));
    assert!(step.answers.is_empty() && step.calls.is_empty());
    assert!(!book.contains(&name("nowhere")));
}

// ── leaving ──────────────────────────────────────────────────────────────

/// The last user leaving sends one unsubscribe; a user who is not the last sends nothing.
#[test]
fn only_the_last_user_leaving_unsubscribes() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.join(2, name("room"), "u2");
    book.marker(&name("room"));
    book.marker(&name("room"));
    assert!(book.leave(1, &name("room")).calls.is_empty());
    assert_eq!(book.leave(2, &name("room")).calls, vec![unsub("room")]);
    assert!(!book.contains(&name("room")), "an idle channel is dropped from the book");
}

/// Leaving while the join still waits answers that join as cancelled, not silently.
#[test]
fn leaving_before_the_marker_answers_the_waiting_join() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    let step = book.leave(1, &name("room"));
    assert_eq!(step.calls, vec![unsub("room")]);
    let failed = errors(&step);
    assert_eq!(failed.len(), 1);
    assert_eq!((failed[0].0, failed[0].1.cause), ("u1", Cause::Unavailable));
}

/// The entry stays while a subscribe is unanswered, so its late marker is counted, then goes.
#[test]
fn an_entry_outlives_its_users_until_its_last_marker() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.leave(1, &name("room"));
    assert!(book.contains(&name("room")), "one subscribe is still unanswered");
    assert!(book.marker(&name("room")).answers.is_empty(), "nobody is waiting any more");
    assert!(!book.contains(&name("room")));
}

/// `0 → 1 → 0 → 1`: the second subscription's user waits for the second marker.
#[test]
fn resubscribing_waits_for_the_second_subscribes_marker() {
    let mut book = connected();
    book.join(1, name("room"), "first");
    book.marker(&name("room"));
    assert_eq!(book.leave(1, &name("room")).calls, vec![unsub("room")]);
    assert_eq!(book.join(1, name("room"), "second").calls, vec![sub("room")]);
    assert_eq!(oks(&book.marker(&name("room"))), vec!["second"]);
}

/// Leave and rejoin before the first marker: sub, unsub, sub; only the second marker activates.
#[test]
fn rejoining_before_the_first_marker_waits_for_the_second() {
    let mut book = connected();
    book.join(1, name("room"), "first");
    let left = book.leave(1, &name("room"));
    assert_eq!(left.calls, vec![unsub("room")]);
    book.join(1, name("room"), "second");
    assert!(oks(&book.marker(&name("room"))).is_empty(), "the first marker answers the cancelled subscribe");
    assert_eq!(oks(&book.marker(&name("room"))), vec!["second"]);
}

/// Detaching leaves every channel the user was on.
#[test]
fn leaving_all_leaves_every_channel() {
    let mut book = connected();
    for channel in ["a", "b"] {
        book.join(7, name(channel), "j");
        book.marker(&name(channel));
    }
    let step = book.leave_all(7, [name("a"), name("b")]);
    let mut calls = step.calls;
    calls.sort_by_key(|call| format!("{call:?}"));
    assert_eq!(calls, vec![unsub("a"), unsub("b")]);
}

/// Leaving a channel one never joined is a no-op.
#[test]
fn leaving_an_unknown_channel_does_nothing() {
    let mut book = connected();
    let step = book.leave(1, &name("nowhere"));
    assert!(step.calls.is_empty() && step.answers.is_empty());
}

// ── refusals ─────────────────────────────────────────────────────────────

/// A refused subscribe fails the waiters it answers, with the upstream's cause.
#[test]
fn a_refusal_fails_the_join_it_answers() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    let step = book.refused(&name("room"), &Rejection::forbidden("forbidden"));
    let failed = errors(&step);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].1, Rejection::forbidden("upstream refused: forbidden"));
    assert!(!book.contains(&name("room")), "nothing is left to keep");
}

/// A refusal answers only the subscribe it is for; a later joiner keeps waiting.
#[test]
fn a_refusal_counts_like_a_marker_for_ordering() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.join(2, name("room"), "u2");
    let step = book.refused(&name("room"), &Rejection::forbidden("no"));
    assert_eq!(errors(&step).iter().map(|(label, _)| *label).collect::<Vec<_>>(), vec!["u1"]);
    assert_eq!(oks(&book.marker(&name("room"))), vec!["u2"]);
}

/// A refusal nobody asked for is ignored like a stray marker.
#[test]
fn a_refusal_nobody_asked_for_is_ignored() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.marker(&name("room"));
    let step = book.refused(&name("room"), &Rejection::forbidden("no"));
    assert!(step.answers.is_empty());
    assert_eq!(audience(&book, "room"), vec![1]);
}

// ── the upstream connection ──────────────────────────────────────────────

/// While disconnected nothing is sent; a join made then is subscribed on connect and activated by that marker.
#[test]
fn a_join_before_the_upstream_connects_takes_effect_after_it_connects() {
    let mut book = Book::new(None);
    let step = book.join(1, name("room"), "early");
    assert!(step.calls.is_empty(), "no connection, no call");
    assert_eq!(book.connected().calls, vec![sub("room")]);
    assert_eq!(oks(&book.marker(&name("room"))), vec!["early"], "one subscribe sent, one marker awaited");
}

/// After the connection drops, every wanted channel is subscribed once again and active users stay active.
#[test]
fn a_new_connection_subscribes_every_wanted_channel_once() {
    let mut book = connected();
    book.join(1, name("a"), "a1");
    book.marker(&name("a"));
    book.join(2, name("b"), "b2");
    book.lost();
    assert!(book.leave(9, &name("a")).calls.is_empty(), "nothing is sent while disconnected");
    assert_eq!(book.connected().calls, vec![sub("a"), sub("b")]);
    assert_eq!(audience(&book, "a"), vec![1], "an active user stays active across a reconnect");
    assert_eq!(oks(&book.marker(&name("b"))), vec!["b2"], "the waiter now waits for the new marker");
}

/// Leaving while disconnected sends no unsubscribe, and the channel is not resubscribed.
#[test]
fn a_channel_left_while_disconnected_is_not_resubscribed() {
    let mut book = connected();
    book.join(1, name("room"), "u1");
    book.marker(&name("room"));
    book.lost();
    assert!(book.leave(1, &name("room")).calls.is_empty());
    assert!(book.connected().calls.is_empty());
    assert!(!book.contains(&name("room")));
}

// ── the internal channel ─────────────────────────────────────────────────

/// The internal channel is subscribed on every connection with no users, is never dropped, and settles on its marker.
#[test]
fn the_internal_channel_is_kept_and_settles_on_its_marker() {
    let mut book = Book::new(Some(name("proxy.auth")));
    assert!(!book.settled(&name("proxy.auth")), "not before a connection");
    assert_eq!(book.connected().calls, vec![sub("proxy.auth")]);
    assert!(!book.settled(&name("proxy.auth")), "not before its marker");
    book.marker(&name("proxy.auth"));
    assert!(book.settled(&name("proxy.auth")));
    book.lost();
    assert!(!book.settled(&name("proxy.auth")));
    assert!(book.contains(&name("proxy.auth")));
    assert_eq!(book.connected().calls, vec![sub("proxy.auth")]);
}

/// A user may join the internal channel; leaving it never unsubscribes.
#[test]
fn leaving_the_internal_channel_never_unsubscribes() {
    let mut book = Book::new(Some(name("proxy.auth")));
    book.connected();
    book.marker(&name("proxy.auth"));
    book.join(1, name("proxy.auth"), "u1");
    book.marker(&name("proxy.auth"));
    assert!(book.leave(1, &name("proxy.auth")).calls.is_empty());
    assert!(book.settled(&name("proxy.auth")));
}
