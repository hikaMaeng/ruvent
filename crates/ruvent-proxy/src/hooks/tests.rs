//! The default rules, as functions an overriding hook calls.

use super::*;
use crate::message::Cause;

fn publish(channel: &str, reply: Option<&str>) -> Publish {
    let reply = reply.map(|reply| format!(r#","reply":"{reply}""#)).unwrap_or_default();
    serde_json::from_str(&format!(r#"{{"kind":"dispatch","channel":"{channel}","type":"t","payload":1{reply}}}"#)).unwrap()
}

/// Subscribing inside the scope is allowed; outside, forbidden with a fixed message.
#[test]
fn the_subscribe_rule_is_the_users_scope() {
    let user = User::new("u", ["room*"]);
    assert_eq!(within_scope(&user, "room.1"), Ok(()));
    let refused = within_scope(&user, "admin").unwrap_err();
    assert_eq!((refused.cause, refused.message.as_str()), (Cause::Forbidden, "forbidden channel"));
}

/// Publishing checks the channel first, then the reply channel.
#[test]
fn the_publish_rule_checks_the_channel_then_the_reply() {
    let user = User::new("u", ["room*"]);
    assert_eq!(publish_within_scope(&user, &publish("room", None)), Ok(()));
    assert_eq!(publish_within_scope(&user, &publish("room", Some("room.back"))), Ok(()));
    assert_eq!(publish_within_scope(&user, &publish("room", Some("admin"))).unwrap_err().message, "forbidden reply channel");
    assert_eq!(publish_within_scope(&user, &publish("admin", Some("admin"))).unwrap_err().message, "forbidden channel", "the channel is reported first");
}

/// The default deliver lets every event through.
#[test]
fn the_default_deliver_lets_everything_through() {
    let event = ruvent::Envelope::dispatch("c", "t", ruvent::Bytes::from_static(b"1"));
    assert!(Defaults.deliver(&User::new("u", Vec::<String>::new()), &event));
}
