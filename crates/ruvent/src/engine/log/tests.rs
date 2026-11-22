//! What the readable log keeps when retention empties a channel.

use super::*;
use crate::envelope::Kind;

fn event(channel: &str, position: u64) -> Arc<Envelope> {
    let mut event = Envelope::draft(Kind::Fact, "a", channel, "{}");
    event.position = position;
    Arc::new(event)
}

/// An emptied channel that nobody waits on is removed, so the index does not
/// grow with every channel name ever used. One somebody waits on is kept, or
/// its reader would never be woken again.
#[test]
fn an_emptied_channel_is_kept_only_while_someone_waits_on_it() {
    let mut index = LogIndex::default();
    index.publish(event("waited", 1));
    index.publish(event("abandoned", 2));
    // Readers arrive after the events (publishing would already prune a
    // dead one); one of them has gone again by the time retention runs.
    let alive = Arc::new(Notify::new());
    let gone = Arc::new(Notify::new());
    index.channels.get_mut("waited").unwrap().wakers.push(Arc::downgrade(&alive));
    index.channels.get_mut("abandoned").unwrap().wakers.push(Arc::downgrade(&gone));
    drop(gone);

    index.drop_through(2);
    assert!(index.channels.contains_key("waited"), "a channel with a live reader is kept");
    assert_eq!(index.channels["waited"].wakers.len(), 1);
    assert!(!index.channels.contains_key("abandoned"), "an empty channel nobody waits on is removed");
    assert!(index.streams.is_empty(), "streams are emptied with it");
}

/// A transport without connections opens a subscription per request and
/// drops it. On a quiet channel that must not pile up dead wakers.
#[test]
fn reopening_subscriptions_on_a_quiet_channel_does_not_pile_up() {
    let shared = Arc::new(Shared::new());
    for _ in 0..100 {
        drop(Subscription::open(shared.clone(), vec!["quiet".to_owned()], From::Now));
    }
    let kept = Subscription::open(shared.clone(), vec!["quiet".to_owned()], From::Now);
    assert_eq!(shared.log.read().channels["quiet"].wakers.len(), 1, "only the live subscription's waker remains");
    drop(kept);
}
