//! Type-level promises, checked by the compiler.
//!
//! These tests do almost nothing at run time. They fail to *compile* when a
//! promise is broken: a handle that stops being shareable across threads, a
//! future that stops being `Send` because something holds a lock or an `Rc`
//! across an await, a type that stops being serializable.

use ruvent::{
    App, Audience, Bytes, Closed, Command, Context, Emit, Engine, EngineConfig, Envelope, Failure, From, Gateway, Identity, Journal, Kind, Lost, Outcome, Proposal, Record, Service, Started, Stats,
    Subscription,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

fn shared<T: Send + Sync + 'static>() {}
fn cloned<T: Clone>() {}
fn sendable<T: Send + 'static>() {}
fn send<F: Future + Send>(future: F) -> F {
    future
}

#[test]
fn handles_are_shareable_across_threads() {
    shared::<Engine>();
    cloned::<Engine>();
    shared::<Gateway>();
    cloned::<Gateway>();
    shared::<Context>();
    cloned::<Context>();
    sendable::<Subscription>();
    sendable::<App>();
    sendable::<Started>();
    sendable::<Service>();
    shared::<Envelope>();
    shared::<Command>();
    shared::<Record>();
    shared::<Failure>();
    shared::<Closed>();
}

#[test]
fn values_have_the_traits_callers_rely_on() {
    fn copy<T: Copy>() {}
    fn error<T: std::error::Error + Send + Sync + 'static>() {}
    fn durable<T: serde::Serialize + serde::de::DeserializeOwned>() {}
    fn from_lost<T: std::convert::From<Lost>>() {}
    copy::<Stats>();
    copy::<From>();
    copy::<ruvent::EventId>();
    error::<Closed>();
    error::<Lost>();
    durable::<Record>();
    durable::<Envelope>();
    durable::<Command>();
    from_lost::<Failure>();
    fn journal<J: Journal>() {}
    journal::<ruvent::Volatile>();
    journal::<ruvent::FileJournal>();
}

/// Every async method can be spawned. If one of these stops being `Send`,
/// an application can no longer call it from `tokio::spawn`.
#[tokio::test]
async fn every_async_call_can_be_spawned() {
    let engine = Engine::volatile(EngineConfig::default());
    let command = Arc::new(Envelope::draft(Kind::Command, "a", "c", "{}"));
    let emit = Emit { action: "a".into(), payload: Bytes::new(), kind: Kind::Fact, key: None, audience: Audience::Client };
    drop((
        send(engine.submit("q", Command::new("a", "c", "{}"))),
        send(engine.append(vec![])),
        send(engine.dequeue("q", 1, None, None)),
        send(engine.begin("q", 1)),
        send(engine.owns("k", 1)),
        send(engine.finish(command.clone(), 1, Outcome::Ok(Bytes::new()), "q", 1)),
        send(engine.release("k", 1, "q", 1, Duration::ZERO)),
        send(engine.emit(command, 1, emit)),
    ));
    drop((send(engine.ack("q", 1)), send(engine.nack("q", 1, Duration::ZERO)), send(engine.flush()), send(engine.compact()), send(engine.dead_letters("q")), send(engine.shutdown())));
    let mut subscription = engine.subscribe(vec!["c".to_owned()], From::Now);
    drop(send(subscription.next(1)));

    let app = App::volatile().start().await.unwrap();
    let identity = Identity { user: "u".to_owned() };
    drop(send(app.gateway().submit(&identity, Proposal::default())));
    drop(send(app.gateway().poll(&identity, vec!["c".to_owned()], From::Now, 1, Duration::ZERO)));
    drop(send(App::volatile().start()));
    drop(send(app.shutdown()));
}

/// A handler written as a plain async closure is accepted, and its future
/// must be `Send` — the compiler refuses one that is not (see the
/// `compile_fail` pairs in `src/compile_fail.rs`).
#[test]
fn a_closure_is_a_handler() {
    async fn handler(cx: Context, _command: Arc<Envelope>) -> Result<Bytes, Failure> {
        cx.fence().await;
        cx.fact("k", "a", "{}", Audience::Worker).await?;
        Ok(Bytes::new())
    }
    let _ = Service::new("s").accept("a", handler).on("b", |_cx: Context, _event: Arc<Envelope>| async { Ok(Bytes::new()) });
}
