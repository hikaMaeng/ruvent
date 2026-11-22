//! The event server over real TCP, with the socket client.

use ruvent::{Bytes, Client, Engine, EngineConfig, Envelope, EventId, FileJournal, Grant, Grants, Inbox, Incoming, Kind, Method, Methods, Name, Pattern, Refusal, WorkKey};
use ruvent_client::remote::{connect, Options};
use ruvent_server::{serve, Credentials, ServerOptions};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const TOKENS: &[(&str, &str)] = &[("workers", "worker-token"), ("clients", "client-token"), ("reader", "reader-token")];

async fn server(engine: &Engine) -> (String, JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let credentials = Arc::new(Credentials::new(TOKENS.iter().map(|(name, token)| (name.to_string(), token.to_string()))));
    let task = tokio::spawn(serve(listener, engine.clone(), credentials, ServerOptions::default()));
    (address, task)
}

async fn dial(address: &str, token: &str) -> (Client, Inbox) {
    connect(address, token, Options::default()).await.unwrap()
}

/// The next thing in an inbox. Waiting has a generous bound so a broken test fails instead of hanging.
async fn next(inbox: &mut Inbox) -> Incoming {
    tokio::time::timeout(Duration::from_secs(10), inbox.recv()).await.expect("nothing arrived").expect("inbox closed")
}

async fn subscribed(client: &Client, inbox: &mut Inbox, channel: &str) {
    client.subscribe(channel.into(), None).await.unwrap();
    assert_eq!(next(inbox).await, Incoming::Subscribed(channel.into()));
}

fn notice(incoming: Incoming) -> Envelope {
    match incoming {
        Incoming::Notice(event) => event,
        other => panic!("expected a notice, got {other:?}"),
    }
}

fn delivery(incoming: Incoming) -> Envelope {
    match incoming {
        Incoming::Delivery(event) => event,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

const LEASE: Duration = Duration::from_secs(60);

/// All seven methods work over TCP and their effects reach the right connections.
#[tokio::test]
async fn all_seven_methods_work_over_tcp() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (address, _server) = server(&engine).await;
    let (worker, mut work) = dial(&address, "worker-token").await;
    let (client, mut seen) = dial(&address, "client-token").await;
    subscribed(&worker, &mut work, "orders").await;
    subscribed(&client, &mut seen, "feed").await;
    subscribed(&client, &mut seen, "results").await;

    // 1 → notice; 3 → whole envelope; 4 with a follow-up → delivery.
    let (id, key) = client.register_claimable(Envelope::claimable("orders", "create", Bytes::from_static(b"p"))).await.unwrap();
    let announced = notice(next(&mut work).await);
    assert_eq!((announced.id, announced.key, announced.payload.len()), (id, Some(key), 0));
    let claimed = worker.claim(id, 7, LEASE).await.unwrap();
    assert_eq!((claimed.key, claimed.payload.as_ref(), claimed.principal.as_deref()), (Some(key), &b"p"[..], Some("clients")));
    let token = claimed.claim.unwrap().token;
    worker.complete(id, token, vec![Envelope::dispatch("feed", "created", Bytes::from_static(b"x"))]).await.unwrap();
    let created = delivery(next(&mut seen).await);
    assert_eq!((created.event_type.as_str(), created.payload.as_ref(), created.principal.as_deref()), ("created", &b"x"[..], Some("workers")));

    // 2 → delivery.
    let posted = client.register_dispatch(Envelope::dispatch("feed", "post", Bytes::new())).await.unwrap();
    assert_eq!(delivery(next(&mut seen).await).id, posted);

    // 5 → removed, and the outcome on the result channel.
    let (waiting, _) = client.register_claimable(Envelope::claimable("orders", "create", Bytes::new())).await.unwrap();
    assert_eq!(notice(next(&mut work).await).id, waiting);
    let tag = EventId::new_v7();
    assert!(client.delete(waiting, "results".into(), tag, None).await.unwrap());
    let outcome = delivery(next(&mut seen).await);
    assert_eq!((outcome.event_type.as_str(), outcome.correlation, &outcome.payload[..16]), (ruvent::DELETED, Some(tag), &waiting.0[..]));

    // 7 → nothing more from that channel; the marker on another channel arrives first.
    client.unsubscribe("feed".into()).await.unwrap();
    client.register_dispatch(Envelope::dispatch("feed", "post", Bytes::new())).await.unwrap();
    let marker = client.register_dispatch(Envelope::dispatch("results", "marker", Bytes::new())).await.unwrap();
    assert_eq!(delivery(next(&mut seen).await).id, marker);
}

/// A token no credential has is refused with the server's reason.
#[tokio::test]
async fn a_wrong_token_is_refused_with_its_reason() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (address, _server) = server(&engine).await;
    let error = connect(&address, "nope", Options::default()).await.err().unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("unknown token"), "{error}");
}

/// A credential may use only its granted methods, on its granted channels.
#[tokio::test]
async fn a_credential_may_use_only_its_granted_methods_and_channels() {
    let mut grants = Grants::new();
    grants.insert("reader".into(), Grant { methods: vec![Method::Subscribe, Method::Unsubscribe], channels: vec![Pattern("pub.*".into())], delegate: false });
    grants.insert("clients".into(), Grant::all());
    let engine = Engine::volatile(EngineConfig { grants: Some(grants), ..EngineConfig::default() }).await;
    let (address, _server) = server(&engine).await;
    let (reader, _inbox) = dial(&address, "reader-token").await;
    assert_eq!(reader.register_dispatch(Envelope::dispatch("pub.news", "post", Bytes::new())).await, Err(Refusal::Forbidden));
    assert_eq!(reader.subscribe("secret".into(), None).await, Err(Refusal::Forbidden));
    assert!(reader.subscribe("pub.news".into(), None).await.is_ok());
    let (client, _inbox) = dial(&address, "client-token").await;
    assert_eq!(client.register_dispatch(Envelope::dispatch("x", "y", Bytes::new()).with_delegate("alice")).await.map(|_| ()), Ok(()));
    // A credential with no grant at all may do nothing.
    let (workers, _inbox) = dial(&address, "worker-token").await;
    assert_eq!(workers.subscribe("pub.news".into(), None).await, Err(Refusal::Forbidden));
}

/// A backlog larger than one answer is read to its end by subscribing again after the last key.
#[tokio::test]
async fn a_backlog_larger_than_one_page_is_read_to_the_end_by_subscribing_again() {
    let engine = Engine::volatile(EngineConfig { backlog_page: 10, ..EngineConfig::default() }).await;
    let (address, _server) = server(&engine).await;
    let (client, _inbox) = dial(&address, "client-token").await;
    let mut registered = Vec::new();
    for index in 0..35u32 {
        registered.push(client.register_claimable(Envelope::claimable("orders", "create", Bytes::from(index.to_le_bytes().to_vec()))).await.unwrap());
    }
    let (worker, _work) = dial(&address, "worker-token").await;
    let mut read: Vec<(EventId, WorkKey)> = Vec::new();
    let mut after = None;
    let mut pages = 0;
    loop {
        let page = worker.subscribe("orders".into(), after).await.unwrap();
        pages += 1;
        assert!(page.notices.len() <= 10);
        read.extend(page.notices.iter().map(|notice| (notice.id, notice.key.unwrap())));
        after = page.last();
        if !page.more {
            break;
        }
    }
    assert_eq!(read, registered);
    assert_eq!(pages, 4);
}

/// One script's observable results, with the claim deadline (a clock reading) left out.
async fn script(worker: &Client, work: &mut Inbox, client: &Client, seen: &mut Inbox, (a, b, c): (EventId, EventId, EventId)) -> Vec<String> {
    let mut log = Vec::new();
    // Ids the server assigns differ from run to run; ours do not.
    let ours = [a, b, c];
    let show = |incoming: Incoming| match incoming {
        Incoming::Delivery(mut event) | Incoming::Notice(mut event) if !ours.contains(&event.id) => {
            event.id = EventId::NIL;
            format!("{event:?}")
        }
        other => format!("{other:?}"),
    };
    worker.subscribe("orders".into(), None).await.unwrap();
    log.push(show(next(work).await));
    client.subscribe("feed".into(), None).await.unwrap();
    log.push(show(next(seen).await));
    log.push(format!("{:?}", client.register_claimable(Envelope::claimable("orders", "create", Bytes::from_static(b"1")).with_id(a)).await));
    log.push(format!("{:?}", client.register_claimable(Envelope::claimable("orders", "create", Bytes::from_static(b"2")).with_id(a)).await));
    log.push(format!("{:?}", client.register_claimable(Envelope::claimable("orders", "create", Bytes::from_static(b"3")).with_id(b)).await));
    for _ in 0..2 {
        log.push(show(next(work).await));
    }
    let claimed = worker.claim(a, 1, LEASE).await.unwrap();
    let claim = claimed.claim.unwrap();
    log.push(format!("claimed {:?} {:?} {:?} {} {}", claimed.id, claimed.key, claim.token, claim.claimer, claim.worker));
    log.push(format!("{:?}", worker.claim(a, 1, LEASE).await.map(|_| ())));
    log.push(format!("{:?}", worker.complete(a, claim.token, vec![Envelope::dispatch("feed", "done", Bytes::new()).with_id(c), Envelope::claimable("orders", "next", Bytes::new()).with_id(b)]).await));
    log.push(format!("{:?}", worker.complete(a, claim.token, vec![Envelope::dispatch("feed", "done", Bytes::new()).with_id(c)]).await));
    log.push(format!("{:?}", worker.complete(a, claim.token, vec![]).await));
    log.push(show(next(seen).await));
    log.push(format!("{:?}", client.delete(b, "feed".into(), c, None).await));
    log.push(show(next(seen).await));
    log.push(format!("{:?}", worker.subscribe("orders".into(), None).await));
    log
}

/// The same calls give the same answers and the same deliveries in the same order, in-process and over TCP.
#[tokio::test]
async fn local_and_remote_connections_see_the_same_results_in_the_same_order() {
    let local = Engine::volatile(EngineConfig::default()).await;
    let (worker, mut work) = ruvent::connect(&local, "workers", 1024).await.unwrap();
    let (client, mut seen) = ruvent::connect(&local, "clients", 1024).await.unwrap();
    let id = |byte: u8| {
        let mut id = EventId::new_v7();
        id.0[15] = byte;
        id
    };
    let ids = (id(1), id(2), id(3));
    let in_process = script(&worker, &mut work, &client, &mut seen, ids).await;

    let remote = Engine::volatile(EngineConfig::default()).await;
    let (address, _server) = server(&remote).await;
    let (worker, mut work) = dial(&address, "worker-token").await;
    let (client, mut seen) = dial(&address, "client-token").await;
    let over_tcp = script(&worker, &mut work, &client, &mut seen, ids).await;

    assert_eq!(in_process, over_tcp);
    assert!(in_process.iter().any(|line| line.contains("Duplicate")), "{in_process:#?}");
    assert!(in_process.iter().any(|line| line.contains("Missing")), "{in_process:#?}");
}

/// A claim held by a remote worker that went away is reclaimed by the next registration and done by another worker under a new id.
#[tokio::test]
async fn a_claim_held_by_a_disconnected_remote_worker_is_reclaimed_by_the_next_registration() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (address, _server) = server(&engine).await;
    let (client, _seen) = dial(&address, "client-token").await;
    let (first, key) = client.register_claimable(Envelope::claimable("orders", "create", Bytes::new())).await.unwrap();

    let (gone, gone_inbox) = dial(&address, "worker-token").await;
    let token = gone.claim(first, 1, Duration::from_millis(200)).await.unwrap().claim.unwrap().token;
    drop((gone, gone_inbox));

    let (worker, mut work) = dial(&address, "worker-token").await;
    subscribed(&worker, &mut work, "orders").await;
    // Real time: the deadline has to pass on the server's clock.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Nothing reclaims until a registration arrives.
    assert_eq!(worker.claim(first, 2, LEASE).await.map(|_| ()), Err(Refusal::AlreadyClaimed));
    let (second, _) = client.register_claimable(Envelope::claimable("orders", "create", Bytes::new())).await.unwrap();

    let reclaimed = notice(next(&mut work).await);
    assert_eq!((reclaimed.key, reclaimed.retry), (Some(key), 1));
    assert_ne!(reclaimed.id, first, "a reclaim issues a new id");
    assert_eq!(notice(next(&mut work).await).id, second);
    assert_eq!(worker.claim(first, 2, LEASE).await.map(|_| ()), Err(Refusal::Missing));
    let again = worker.claim(reclaimed.id, 2, LEASE).await.unwrap();
    assert_ne!(again.claim.unwrap().token, token);
    worker.complete(reclaimed.id, again.claim.unwrap().token, vec![]).await.unwrap();
}

/// Resending a completion whose reply was lost has no effect: refused, and its follow-ups are not registered again.
#[tokio::test]
async fn a_resent_completion_after_a_lost_reply_has_no_effect_and_no_follow_ups() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (address, _server) = server(&engine).await;
    let (client, mut seen) = dial(&address, "client-token").await;
    let (worker, _work) = dial(&address, "worker-token").await;
    subscribed(&client, &mut seen, "feed").await;
    let (id, _) = client.register_claimable(Envelope::claimable("orders", "create", Bytes::new())).await.unwrap();
    let token = worker.claim(id, 1, LEASE).await.unwrap().claim.unwrap().token;
    let follow_up = || vec![Envelope::dispatch("feed", "done", Bytes::new())];
    worker.complete(id, token, follow_up()).await.unwrap();
    // The worker never heard back and sends it again.
    assert_eq!(worker.complete(id, token, follow_up()).await, Err(Refusal::Missing));
    assert_eq!(delivery(next(&mut seen).await).event_type.as_str(), "done");
    let marker = client.register_dispatch(Envelope::dispatch("feed", "marker", Bytes::new())).await.unwrap();
    assert_eq!(delivery(next(&mut seen).await).id, marker, "exactly one follow-up was delivered");
}

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("ruvent-server-{label}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// After the server restarts, a worker that survived it reports with its old id and token and is refused.
#[tokio::test]
async fn after_a_server_restart_a_surviving_workers_old_completion_is_void() {
    let directory = TempDir::new("restart");
    let path = directory.0.join("ruvent.journal");
    let config = EngineConfig { durability: ruvent::Durability::Strict, ..EngineConfig::default() };

    // `shutdown` returns after the journal (and its lock) is released, so it reopens at once.
    let (journal, recovered) = FileJournal::open(&path).unwrap();
    let engine = Engine::start(config.clone(), journal, recovered).await.unwrap();
    let (address, server_task) = server(&engine).await;
    let (worker, _work) = dial(&address, "worker-token").await;
    let (id, key) = worker.register_claimable(Envelope::claimable("orders", "create", Bytes::from_static(b"owed"))).await.unwrap();
    let token = worker.claim(id, 1, LEASE).await.unwrap().claim.unwrap().token;
    engine.shutdown().await.unwrap();
    server_task.abort();

    let (journal, recovered) = FileJournal::open(&path).unwrap();
    let engine = Engine::start(config, journal, recovered).await.unwrap();
    let (address, _server) = server(&engine).await;
    let (worker, _work) = dial(&address, "worker-token").await;
    assert_eq!(worker.complete(id, token, vec![Envelope::dispatch("feed", "done", Bytes::new())]).await, Err(Refusal::Missing));
    let backlog = worker.subscribe(Name::from("orders"), None).await.unwrap();
    assert_eq!(backlog.notices.len(), 1);
    let survivor = &backlog.notices[0];
    assert_eq!((survivor.key, survivor.retry, survivor.kind), (Some(key), 1, Kind::Claimable));
    assert_ne!(survivor.id, id);
    assert_eq!(worker.complete(survivor.id, token, vec![]).await, Err(Refusal::NotClaimed));
    let fresh = worker.claim(survivor.id, 1, LEASE).await.unwrap();
    assert_eq!(fresh.payload.as_ref(), b"owed");
    assert_eq!(worker.complete(survivor.id, token, vec![]).await, Err(Refusal::StaleToken));
    engine.shutdown().await.unwrap();
}

/// When the server lets go of a connection, the inbox says so and every later call fails with `Closed` instead of hanging.
#[tokio::test]
async fn when_the_server_goes_away_the_inbox_closes_and_calls_fail_closed() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (address, server_task) = server(&engine).await;
    let (client, mut inbox) = dial(&address, "client-token").await;
    engine.shutdown().await.unwrap();
    server_task.abort();
    assert_eq!(next(&mut inbox).await, Incoming::Closed);
    let call = client.register_dispatch(Envelope::dispatch("feed", "post", Bytes::new()));
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), call).await.expect("the call hung"), Err(Refusal::Closed));
}
