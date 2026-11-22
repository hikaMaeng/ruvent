//! One test per stated requirement. The quote above each test is the
//! requirement as it was given; the test is how it is observed.

use crate::memory::memory;
use ruvent::{
    async_trait, App, Audience, Begin, Bytes, Command, Compaction, Context, Credentials, Durability, Engine, EngineConfig, Envelope, Events, Failure, FileJournal, From, Identity, Journal, Module,
    Proposal, Record, Recovered, Retention, Service,
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

const NOW: Option<Duration> = Some(Duration::ZERO);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("ruvent-{}-accept-{name}.journal", std::process::id()));
    for stale in [path.clone(), path.with_extension("journal.lock"), path.with_extension("journal.tmp")] {
        let _ = std::fs::remove_file(stale);
    }
    path
}

fn alice() -> Identity {
    Identity { user: "alice".to_owned() }
}

fn propose(action: &str, key: &str, reply: &str, payload: Value) -> Proposal {
    Proposal { action: action.into(), payload: Bytes::from(payload.to_string()), transaction_key: Some(key.into()), reply_channel: Some(reply.into()), ..Proposal::default() }
}

async fn open_turn(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("turn.started:{}", command.transaction_key), "turn.started", "{}", Audience::Client).await?;
    Ok(Bytes::from_static(b"\"opened\""))
}

async fn call_model(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    for token in ["\"a\"", "\"b\"", "\"c\""] {
        cx.progress("model.delta", token);
    }
    cx.fact(format!("model.replied:{}", command.correlation_id), "model.replied", "{}", Audience::Worker).await?;
    Ok(Bytes::new())
}

async fn decide(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("turn.final:{}", command.correlation_id), "turn.final", "\"answer\"", Audience::Client).await?;
    Ok(Bytes::new())
}

fn turn_services(app: App) -> App {
    app.service(Service::new("turn").accept("turn.run", open_turn).on("model.replied", decide)).service(Service::new("inference").on("turn.started", call_model))
}

/// > 양방향 프로토콜은 플러그인 계층이 되길 원해. http롱폴링, 웹소켓, webRTC 등을 기본
/// > 지원하고 구글의 grpc등도 지원할 수 있는 식으로 다양한 프로토콜을 선택할 수 있게 한다.
///
/// What this observes: a protocol is a plugin. One written outside the crate,
/// against the public traits only, authenticates, subscribes, submits, streams
/// a whole reaction chain and resumes from a cursor with nothing lost or
/// repeated. It does **not** observe the bundled HTTP long-polling, WebSocket
/// or WebRTC transports: those do not exist yet.
#[tokio::test]
async fn a_protocol_is_a_plugin_written_against_public_traits() {
    let (transport, switchboard) = memory();
    let authenticate = |credentials: &Credentials| credentials.token.as_deref().filter(|token| *token == "good").map(|_| alice());
    let app = App::volatile().mount(turn_services).authenticator(authenticate).transport(transport).start().await.unwrap();

    let mut refused = switchboard.connect("bad").await;
    assert!(refused.next().await.is_none(), "an unauthenticated peer is disconnected without a word");

    let mut client = switchboard.connect("good").await;
    client.expect("ready").await;
    client.send(json!({"type": "subscribe", "channels": ["room"]})).await;
    client.expect("subscribed").await;
    client.send(json!({"type": "event", "action": "turn.cancel", "payload": {}, "replyChannel": "room"})).await;
    assert_eq!(client.expect("rejected").await["reason"], "not_admitted", "an action nobody registered is refused");
    client.send(json!({"type": "event", "action": "turn.run", "payload": {"text": "hi"}, "transactionKey": "t1", "replyChannel": "room"})).await;
    assert_eq!(client.expect("accepted").await["transactionKey"], "t1");

    let mut seen = Vec::new();
    while !(seen.iter().any(|frame: &Value| frame["event"]["action"] == "turn.final") && seen.iter().any(|frame: &Value| frame["event"]["action"] == "turn.run.result")) {
        seen.push(client.expect("event").await);
    }
    let positions: Vec<u64> = seen.iter().map(|frame| frame["cursor"].as_str().unwrap().parse().unwrap()).collect();
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "events arrive in commit order: {positions:?}");
    let actions: Vec<&str> = seen.iter().map(|frame| frame["event"]["action"].as_str().unwrap()).collect();
    assert!(!actions.contains(&"model.replied"), "worker-to-worker facts never reach a client: {actions:?}");
    assert_eq!(actions.iter().filter(|action| **action == "model.delta").count(), 3);
    assert_eq!(seen[0]["event"]["payload"], json!({"text": "hi"}), "a JSON payload arrives as the JSON that was sent");
    drop(client);

    // Reconnect from the second event's cursor: exactly the rest, in order.
    let mut resumed = switchboard.connect("good").await;
    resumed.expect("ready").await;
    resumed.send(json!({"type": "subscribe", "channels": ["room"], "cursor": seen[1]["cursor"]})).await;
    resumed.expect("subscribed").await;
    let mut rest = Vec::new();
    while rest.len() < seen.len() - 2 {
        rest.push(resumed.expect("event").await["event"]["eventId"].clone());
    }
    let expected: Vec<Value> = seen[2..].iter().map(|frame| frame["event"]["eventId"].clone()).collect();
    assert_eq!(rest, expected, "a resumed reader gets every later event once");
    app.shutdown().await.unwrap();
}

/// > 변하지 않는 원칙중에 하나는 이벤트는 반드시 하나의 워커가 가져간다는것이다. 혹은
/// > 발송되거나. 만약 둘이상의 워커나 프로세스에 할당되어야하면 이벤트 자체를 여러개
/// > 발급해야해. 우리는 구조적으로 하나의 이벤트가 라이프사이클을 별도로 유지하면서
/// > 여러 워커에게 사용되는 식의 복잡한 구조는 배제한다
///
/// What this observes: a fact two services react to is issued to them as two
/// events with different ids, each handled exactly once; and a command sent
/// three times at once under one transaction key runs once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_event_is_taken_by_one_worker_and_fan_out_issues_separate_events() {
    const ORDERS: usize = 100;
    let placed = Arc::new(AtomicUsize::new(0));
    let (seen, mut handled) = mpsc::unbounded_channel::<(&'static str, String, String)>();
    let place = {
        let placed = placed.clone();
        move |cx: Context, command: Arc<Envelope>| {
            let placed = placed.clone();
            async move {
                placed.fetch_add(1, Ordering::SeqCst);
                let payload = json!({"order": &*command.transaction_key}).to_string();
                cx.fact(format!("order.placed:{}", command.transaction_key), "order.placed", payload, Audience::Worker).await?;
                Ok(Bytes::new())
            }
        }
    };
    let reactor = |name: &'static str| {
        let seen = seen.clone();
        move |_cx: Context, event: Arc<Envelope>| {
            let seen = seen.clone();
            async move {
                let order: Value = event.json().unwrap();
                seen.send((name, order["order"].as_str().unwrap().to_owned(), event.event_id.to_string())).unwrap();
                Ok(Bytes::new())
            }
        }
    };
    let app = App::volatile()
        .service(Service::new("orders").accept("order.place", place))
        .service(Service::new("billing").on("order.placed", reactor("billing")))
        .service(Service::new("shipping").on("order.placed", reactor("shipping")))
        .start()
        .await
        .unwrap();

    let mut sends = Vec::new();
    for order in 0..ORDERS {
        for _copy in 0..3 {
            let gateway = app.gateway().clone();
            sends.push(tokio::spawn(async move { gateway.submit(&alice(), propose("order.place", &format!("o{order}"), "room", json!({}))).await.unwrap() }));
        }
    }
    for send in sends {
        send.await.unwrap();
    }

    let mut by_service: HashMap<&str, HashMap<String, Vec<String>>> = HashMap::new();
    for _ in 0..2 * ORDERS {
        let (service, order, event) = tokio::time::timeout(Duration::from_secs(30), handled.recv()).await.unwrap().unwrap();
        by_service.entry(service).or_default().entry(order).or_default().push(event);
    }
    app.shutdown().await.unwrap();
    assert!(handled.try_recv().is_err(), "no reactor ran more than once");

    assert_eq!(placed.load(Ordering::SeqCst), ORDERS, "three copies of one transaction ran once");
    let mut ids = HashSet::new();
    for service in ["billing", "shipping"] {
        let orders = &by_service[service];
        assert_eq!(orders.len(), ORDERS, "{service} saw every order");
        for events in orders.values() {
            assert_eq!(events.len(), 1, "{service} handled an order once");
            assert!(ids.insert(events[0].clone()), "an event id was given to two workers");
        }
    }
}

/// A journal whose writes wait for a permit, and that records every write it
/// was ever asked for.
struct Gate {
    permits: Arc<Semaphore>,
    started: Arc<Mutex<Vec<(u64, u64)>>>,
}

#[async_trait]
impl Journal for Gate {
    async fn append(&mut self, prev: u64, through: u64, _records: &[Record]) -> io::Result<()> {
        self.started.lock().unwrap().push((prev, through));
        self.permits.acquire().await.unwrap().forget();
        Ok(())
    }

    async fn rewrite(&mut self, at: u64, _records: &[Record]) -> io::Result<()> {
        self.started.lock().unwrap().push((at, at));
        self.permits.acquire().await.unwrap().forget();
        Ok(())
    }
}

/// > 단지 이벤트 큐는 입출력이 잦다. 따라서 100% 장애대응을 대비하지 않는다. 최대한
/// > 성능이 유지되는 상태에서 스케줄러가 디스크 저널링을 하는 구조를 염두하라 특히 이전
/// > 디스크 저널링이 진행중이면 덮어쓰지 않게 단지 시간만을 안전장치로 두면 안된다
///
/// What this observes: with a write stuck on the disk, (1) callers keep being
/// answered, (2) no second write starts however much time passes — an hour,
/// against a 10 ms window — and (3) the next write starts the moment the
/// first one lands, following it in the record chain.
#[tokio::test(start_paused = true)]
async fn a_write_in_progress_is_never_overtaken_whatever_the_clock_says() {
    let (permits, started) = (Arc::new(Semaphore::new(1)), Arc::new(Mutex::new(Vec::new())));
    let gate = Gate { permits: permits.clone(), started: started.clone() };
    let config = EngineConfig { durability: Durability::Scheduled { window: Duration::from_millis(10) }, ..EngineConfig::default() };
    let engine = Engine::start(config, gate, Recovered::default());

    engine.submit("q", Command::new("job", "c", "{}")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(started.lock().unwrap().len(), 2, "the epoch, then the first batch, which is now stuck");

    for index in 0..1000 {
        engine.submit("q", Command::new("job", "c", "{}").transaction_key(format!("k{index}"))).await.unwrap();
    }
    assert!(engine.stats().journal_lag >= 1000, "answered while the disk is stuck");
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(started.lock().unwrap().len(), 2, "an hour of 10 ms windows started no second write on top of the first");

    permits.add_permits(1);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(started.lock().unwrap().len(), 3, "the next write starts when the first lands");
    permits.add_permits(1000);
    engine.flush().await.unwrap();
    let chain = started.lock().unwrap().clone();
    assert!(chain.windows(2).all(|pair| pair[1].0 == pair[0].1), "each write follows the one before it: {chain:?}");
    assert_eq!(engine.stats().journal_lag, 0);
}

/// > 구현과 최적 구현은 다르다. 성능 최적 구현은 쓰레드를 깨우는 것조차 쓰레드 루프를
/// > 도는게 아니라 OS활성신호로 깨울거잖아. 스케줄러 역시 그래. 생각없이 스케줄러를
/// > 돌리지 않는다면 이벤트 트리거를 연동하여 체크할 때와 아닐때를 구분하겠지.
///
/// What this observes: a whole application — services, a transport and a
/// connected, subscribed client — costs the engine zero wakeups across a
/// day of idleness, and is woken by the next event, not by a later check.
#[tokio::test(start_paused = true)]
async fn an_idle_application_with_a_connected_client_does_not_wake() {
    let (transport, switchboard) = memory();
    let app = App::volatile().mount(turn_services).transport(transport).start().await.unwrap();
    let mut client = switchboard.connect("anyone").await;
    client.expect("ready").await;
    client.send(json!({"type": "subscribe", "channels": ["room"]})).await;
    client.expect("subscribed").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    app.engine().flush().await.unwrap();

    let before = app.engine().stats().wakeups;
    tokio::time::sleep(Duration::from_secs(24 * 3600)).await;
    assert_eq!(app.engine().stats().wakeups, before, "a day with nothing to do woke the engine");

    client.send(json!({"type": "event", "action": "turn.run", "payload": {}, "replyChannel": "room"})).await;
    client.expect("accepted").await;
    client.events_until("turn.final").await;
    app.shutdown().await.unwrap();
}

/// > 저널링도 마찬가지 무조건 저장할 이유는 없다. 이상적으로는 완료된 이벤트의 로그는 이
/// > 서버 입장에서 그다지 중요하지 않고 너무 많이 쌓이는 것도 곤란해 그러면 어떤걸
/// > 어디까지 어떤 기준으로 영속화시키는지도 신중히 고민할 필요가 있다.
///
/// What this observes: 20,000 commands run to completion through a service
/// on a real file journal. Memory holds no more events than the retention
/// bound; after a clean stop the journal holds no more than its compaction
/// threshold, however much work went through it; nothing is owed after a
/// restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn completed_work_does_not_pile_up_in_memory_or_on_disk() {
    const COMMANDS: usize = 20_000;
    const MAX_EVENTS: usize = 2_000;
    const MIN_RECORDS: u64 = 2_000;
    let path = scratch("pile");
    let done = Arc::new((AtomicUsize::new(0), tokio::sync::Notify::new()));
    let job = {
        let done = done.clone();
        move |_cx: Context, _command: Arc<Envelope>| {
            let done = done.clone();
            async move {
                if done.0.fetch_add(1, Ordering::SeqCst) + 1 == COMMANDS {
                    done.1.notify_one();
                }
                Ok(Bytes::from_static(b"\"ok\""))
            }
        }
    };
    let config = EngineConfig { retention: Retention { max_events: MAX_EVENTS, ..Retention::default() }, compaction: Compaction { min_records: MIN_RECORDS, ratio: 4 }, ..EngineConfig::default() };
    let app = App::open(&path).engine(config).service(Service::new("jobs").accept("job.run", job)).start().await.unwrap();

    let mut senders = Vec::new();
    for sender in 0..64 {
        let gateway = app.gateway().clone();
        senders.push(tokio::spawn(async move {
            for index in (sender..COMMANDS).step_by(64) {
                gateway.submit(&alice(), propose("job.run", &format!("j{index}"), "room", json!({}))).await.unwrap();
            }
        }));
    }
    for sender in senders {
        sender.await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(120), done.1.notified()).await.expect("every command ran");
    let engine = app.engine().clone();
    assert!(engine.stats().retained_events <= MAX_EVENTS as u64, "memory holds {} events, bound {MAX_EVENTS}", engine.stats().retained_events);
    app.shutdown().await.unwrap();
    assert_eq!(engine.stats().events, 2 * COMMANDS as u64, "each command logged once as a command and once as a result");

    let (journal, recovered) = FileJournal::open(&path).unwrap();
    drop(journal);
    assert!(
        (recovered.records.len() as u64) <= MIN_RECORDS,
        "{} records on disk after a clean stop following {COMMANDS} completed commands; the compaction threshold is {MIN_RECORDS}",
        recovered.records.len()
    );
    let engine = Engine::open(&path, EngineConfig::default()).await.unwrap();
    assert!(engine.dequeue("jobs", 10, None, NOW).await.unwrap().is_empty(), "nothing is owed");
    engine.shutdown().await.unwrap();
}

/// > 오히려 저널링은 개별 레코드보다 스냅샷으로 취합하여 남기는게 더 나은거 아닌가.
/// > 복원하는데도 문제는 없잖아.
///
/// What this observes: a journal that churns is folded into snapshots by
/// itself, and the file as a crash would leave it at that moment restores
/// exactly the work still owed, in order.
#[tokio::test]
async fn a_folded_journal_restores_exactly_what_is_owed() {
    let path = scratch("fold");
    let config = || EngineConfig { durability: Durability::Scheduled { window: Duration::from_secs(86_400) }, compaction: Compaction { min_records: 200, ratio: 2 }, ..EngineConfig::default() };
    let engine = Engine::open(&path, config()).await.unwrap();
    let backlog: Vec<String> = (0..300).map(|index| format!("b{index}")).collect();
    for key in &backlog {
        engine.submit("backlog", Command::new("job", "c", "{}").transaction_key(key.as_str())).await.unwrap();
    }
    for index in 0..2_000 {
        engine.submit("churn", Command::new("job", "c", "{}").transaction_key(format!("c{index}"))).await.unwrap();
        engine.flush().await.unwrap();
        let delivery = engine.dequeue("churn", 1, None, NOW).await.unwrap().remove(0);
        engine.ack("churn", delivery.id).await.unwrap();
        engine.flush().await.unwrap();
    }
    assert!(engine.stats().compactions >= 1, "the journal folded itself");

    let image = scratch("fold-image");
    std::fs::copy(&path, &image).unwrap();
    let restored = Engine::open(&image, config()).await.unwrap();
    let owed: Vec<String> = restored.dequeue("backlog", 1_000, None, NOW).await.unwrap().iter().map(|delivery| delivery.command.transaction_key.to_string()).collect();
    assert_eq!(owed, backlog, "the backlog, all of it, in order");
    assert!(restored.dequeue("churn", 10, None, NOW).await.unwrap().is_empty(), "and nothing that was finished");
    restored.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}

/// > 또한 프레임웍 설정수준에서 저널링을 않는 채널을 설정할 수도 있잖아
///
/// What this observes: set on the application, not in handlers — events on
/// an excluded channel prefix and a service marked transient leave nothing
/// on disk, while everything else survives a restart.
#[tokio::test]
async fn channels_and_services_can_be_kept_off_the_journal() {
    let path = scratch("off");
    let note = |cx: Context, command: Arc<Envelope>| async move {
        cx.fact(format!("noted:{}", command.transaction_key), "noted", "{}", Audience::Client).await?;
        Ok(Bytes::new())
    };
    let later = |_cx: Context, _command: Arc<Envelope>| async { Err::<Bytes, _>(Failure::retry(Duration::from_secs(3600))) };
    let app = App::open(&path)
        .persist_events(Events::Except(vec!["live.".to_owned()]))
        .service(Service::new("notes").accept("note", note))
        .service(Service::new("kept").accept("kept.job", later))
        .service(Service::new("scratch").accept("scratch.job", later).transient())
        .start()
        .await
        .unwrap();
    let mut audit = app.engine().subscribe(vec!["audit.room".to_owned(), "live.room".to_owned()], From::Now);
    for (action, key, reply) in [("note", "n1", "audit.room"), ("note", "n2", "live.room"), ("kept.job", "k1", "x"), ("scratch.job", "s1", "x")] {
        app.gateway().submit(&alice(), propose(action, key, reply, json!({}))).await.unwrap();
    }
    let mut results = 0;
    while results < 2 {
        results += audit.next(64).await.iter().filter(|event| &*event.action == "note.result").count();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    app.shutdown().await.unwrap();

    let engine = Engine::open(&path, EngineConfig { persist: ruvent::Persist { events: Events::Except(vec!["live.".to_owned()]), ..Default::default() }, ..EngineConfig::default() }).await.unwrap();
    let kept: Vec<(String, String)> =
        engine.subscribe(vec!["audit.room".to_owned(), "live.room".to_owned()], From::Start).poll(100).iter().map(|event| (event.channel.to_string(), event.action.to_string())).collect();
    assert!(kept.contains(&("audit.room".to_owned(), "noted".to_owned())), "events on a kept channel survive: {kept:?}");
    assert!(kept.iter().all(|(channel, _)| channel == "audit.room"), "nothing on the excluded channel survives: {kept:?}");
    assert_eq!(engine.dequeue("kept", 10, None, Some(Duration::from_secs(7200))).await.unwrap().len(), 1, "an ordinary service's owed work survives");
    assert!(engine.dequeue("scratch", 10, None, NOW).await.unwrap().is_empty(), "a transient service's work does not");
    engine.shutdown().await.unwrap();
}

struct Mailer;

impl Module for Mailer {
    fn install(self, app: App) -> App {
        let mail = |cx: Context, event: Arc<Envelope>| async move {
            cx.fact(format!("mail.sent:{}", event.correlation_id), "mail.sent", "{}", Audience::Client).await?;
            Ok(Bytes::new())
        };
        app.service(Service::new("mailer").on("order.placed", mail))
    }
}

fn orders(app: App) -> App {
    let place = |cx: Context, command: Arc<Envelope>| async move {
        cx.fact(format!("order.placed:{}", command.transaction_key), "order.placed", "{}", Audience::Worker).await?;
        Ok(Bytes::new())
    };
    app.service(Service::new("orders").accept("order.place", place))
}

/// > 스프링 프레임웍이 손쉽게 flux구조의 컨테이너나 함수를 추가하여 비동기 서버를 운영할
/// > 수 있는 프레임웍이었던 것처럼 우리 라이브러리도 러스트 프레임웍으로서 손쉽게 원하는
/// > 서버로 확장가능해야한다. … 요청 응답 구조가 아니라 이벤트 발행 및 수신 구조로
/// > 서비스를 작성하는 형태의 프레임웍이다.
///
/// What this observes: a feature added as a module reacts to another
/// module's fact without either naming the other, and a contradiction
/// between modules (one command, two owners) is refused at start.
#[tokio::test(start_paused = true)]
async fn a_feature_installs_itself_as_a_module() {
    let app = App::volatile().mount(orders).mount(Mailer).start().await.unwrap();
    let mut room = app.engine().subscribe(vec!["room".to_owned()], From::Now);
    app.gateway().submit(&alice(), propose("order.place", "o1", "room", json!({}))).await.unwrap();
    loop {
        if room.next(64).await.iter().any(|event| &*event.action == "mail.sent") {
            break;
        }
    }
    app.shutdown().await.unwrap();

    let refused = App::volatile().mount(orders).mount(orders).start().await.err().expect("one command, two owners");
    assert_eq!(refused.kind(), io::ErrorKind::InvalidInput);
}

/// > 이 러스트 라이브러리는 독립된 서버로 동작하는게 아니라 러스트 라이브러리로 활용되길 원한다.
///
/// What this observes: the engine is usable on its own, inside any program,
/// with no framework, no transport and no port — a command submitted, run
/// and answered through the engine API alone.
#[tokio::test]
async fn the_engine_alone_is_usable_without_the_framework() {
    let engine = Engine::volatile(EngineConfig::default());
    engine.submit("q", Command::new("job", "c", "{}").transaction_key("k").reply_to("r")).await.unwrap();
    let delivery = engine.dequeue("q", 1, None, None).await.unwrap().remove(0);
    let Begin::Claimed { command, attempt, .. } = engine.begin("q", delivery.id).await.unwrap() else { panic!("claimed") };
    assert!(engine.finish(command, attempt, ruvent::Outcome::Ok(Bytes::from_static(b"1")), "q", delivery.id).await.unwrap());
    assert_eq!(&*engine.subscribe(vec!["r".to_owned()], From::Start).poll(10)[0].action, "job.result");
    engine.shutdown().await.unwrap();
}
