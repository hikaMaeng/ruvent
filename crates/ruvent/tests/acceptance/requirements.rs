//! One test per stated requirement. The quote above each test is the
//! requirement as it was given; the test is how it is observed.
//!
//! Where the approved overhaul plan (`docs/proposals/library-overhaul.md`)
//! replaced an earlier requirement — reclaim no longer issues a new id (C3),
//! a connection now has a role (2.2) — the test quotes the statement the new
//! behaviour serves, not the replaced one.

use ruvent::envelope::codec::{self, Layout};
use ruvent::{connect, Assignment, Bytes, Client, Durability, Engine, EngineConfig, Envelope, EventId, Hello, Inbox, Incoming, Methods, Recovered, SegmentLog, Target, DELETED, DELETE_FAILED};
use std::path::PathBuf;
use std::time::Duration;

fn scratch(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("ruvent-accept-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory.join("journal")
}

fn work() -> Envelope {
    Envelope::claimable("work", "do", Bytes::from_static(b"{}"))
}

async fn next(inbox: &mut Inbox) -> Incoming {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), inbox.recv()).await.expect("something arrives").expect("open") {
            Incoming::Subscribed(_) => continue,
            other => return other,
        }
    }
}

async fn assignment(inbox: &mut Inbox) -> Assignment {
    loop {
        if let Incoming::Assign(assignment) = next(inbox).await {
            return assignment;
        }
    }
}

async fn quiet(inbox: &mut Inbox) -> bool {
    loop {
        match tokio::time::timeout(Duration::from_millis(80), inbox.recv()).await {
            Err(_) => return true,
            Ok(Some(Incoming::Subscribed(_))) => continue,
            Ok(_) => return false,
        }
    }
}

async fn roles(engine: &Engine, credential: &str) -> ((Client, Inbox), (Client, Inbox), (Client, Inbox)) {
    (connect(engine, Hello::producer(credential), 64).await.unwrap(), connect(engine, Hello::worker(credential), 64).await.unwrap(), connect(engine, Hello::observer(credential), 64).await.unwrap())
}

/// > 5. 이벤트 삭제(아직 대기열에 있어야함) 이미 발송되거나 착수되면 발송용 삭제 실패 이벤트 등록. 성공시에도 발송용 성공이벤트 등록
#[tokio::test]
async fn deletion_removes_only_a_waiting_event_and_always_announces_its_outcome() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, mut assignments), (observer, mut inbox)) = roles(&engine, "svc").await;
    observer.subscribe(Target::Channel("results".into()), None).await.unwrap();
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    let (_, waiting) = producer.register(work()).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let claimed = assignment(&mut assignments).await.event.id;
    let (_, sent) = producer.register(Envelope::dispatch("feed", "x", Bytes::new()).with_id(EventId::new_v7())).await.unwrap();
    let (_, second) = producer.register(work()).await.unwrap();
    assert_ne!(second, claimed);
    // The outcome names the deleted id in its payload: the envelope carries no flow id.
    for (target, announced) in [(claimed, DELETE_FAILED), (sent, DELETE_FAILED)] {
        producer.delete(target, "results".into(), None).await.unwrap();
        let Incoming::Delivery(outcome) = next(&mut inbox).await else { panic!("no outcome event") };
        assert_eq!((&*outcome.event_type, &outcome.payload[..16], outcome.kind), (announced, &target.0[..], ruvent::Kind::Dispatch));
    }
    let _ = waiting;
    producer.delete(second, "results".into(), None).await.unwrap();
    let Incoming::Delivery(outcome) = next(&mut inbox).await else { panic!("no outcome event") };
    assert_eq!((&*outcome.event_type, &outcome.payload[..]), (DELETED, &second.0[..]));
}

/// > 2. 착수마킹(이벤트 자체에 상태를 직접 변경):가져간 워커정보를 비롯한 착수정보 기록. 착수시에 타임아웃이 일어날 시간을 워커가 마킹함
/// > 3. 완료보고(큐에서 지움)
#[tokio::test]
async fn a_claim_carries_the_attempt_and_the_time_out_the_worker_chose_and_completion_removes_it() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, _), (observer, _)) = roles(&engine, "svc").await;
    let (_, id) = producer.register(work()).await.unwrap();
    let before = engine.uptime();
    let claimed = worker.claim(id, Duration::from_secs(90)).await.unwrap();
    assert_eq!(claimed.lease_ms, 90_000, "the worker chose the time-out");
    assert!(claimed.until >= before + 90_000, "the time it runs out at: {}", claimed.until);
    assert_eq!(claimed.event.id, id);
    let seq = worker.complete(id, claimed.token, Vec::new()).await.unwrap();
    worker.committed(seq).await.unwrap();
    assert!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.is_empty(), "completion removed it from the queue");
}

/// > 6. 하지만 등록, 착수, 완료는 멱등을 보장해야한다.
#[tokio::test(start_paused = true)]
async fn registration_claim_and_completion_are_idempotent_and_a_reclaimed_event_keeps_its_id() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, mut assignments), (observer, _)) = roles(&engine, "svc").await;
    // Registration: the same id and content again is not a second event.
    let event = work().with_id(EventId::new_v7());
    producer.register(event.clone()).await.unwrap();
    producer.register_committed(event.clone()).await.unwrap();
    assert_eq!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.len(), 1);
    // A lease that runs out gives the event back under the same id; the late report has no effect.
    worker.join("work".into(), Vec::new(), Duration::from_secs(1), false).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let first = assignment(&mut assignments).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let (other, mut others) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
    other.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    other.window("work".into(), 1, 0).await.unwrap();
    let second = assignment(&mut others).await;
    assert_eq!(second.event.id, event.id, "the logical id does not change");
    let late = worker.complete(event.id, first.token, Vec::new()).await.unwrap();
    worker.committed(late).await.unwrap();
    assert_eq!(observer.stats().await.unwrap().claimed, 1, "the stale completion changed nothing");
    // Completion: the right attempt removes it once.
    let done = other.complete(event.id, second.token, Vec::new()).await.unwrap();
    other.committed(done).await.unwrap();
    assert_eq!(observer.stats().await.unwrap().claimed + observer.stats().await.unwrap().waiting, 0);
}

/// > 5. 워커의 실패시 이벤트는 완료처리됨. 이후 처리를 위한 것은 새로운 이벤트로 등록됨
/// > 6. 재시도 카운트가 늘어난 대기 이벤트의 처리 역시 워커가 착수하고 처리함. 일정 재시도 횟수가 넘어가면 그냥 완료처리하고 새로운 이벤트를 등록할지 실패용 이벤트보고를 할지 말지도 전부 해당 이벤트를 처리할 워커 내부로직으로 해결
#[tokio::test]
async fn a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, mut assignments), (observer, mut inbox)) = roles(&engine, "svc").await;
    observer.subscribe(Target::Channel("failures".into()), None).await.unwrap();
    observer.subscribe(Target::Channel("retry".into()), None).await.unwrap();
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let (_, id) = producer.register(work()).await.unwrap();
    let claimed = assignment(&mut assignments).await;
    // The worker failed, and decided: report it, and try again as a new event.
    let follow_ups = vec![Envelope::dispatch("failures", "work.failed", Bytes::from_static(b"boom")), Envelope::claimable("retry", "do", claimed.event.payload.clone())];
    worker.complete(id, claimed.token, follow_ups).await.unwrap();
    let mut seen = [next(&mut inbox).await, next(&mut inbox).await];
    seen.sort_by_key(|incoming| matches!(incoming, Incoming::Notice(_)));
    assert!(matches!(&seen[0], Incoming::Delivery(event) if &*event.event_type == "work.failed"));
    assert!(matches!(&seen[1], Incoming::Notice(event) if event.failures == 0 && event.id != id));
    assert!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.is_empty(), "the failed event itself is completed");
}

/// > ID가 같은 이벤트는 등록될 수 없고 ID는 영원히 고유하지 않아도 충분하지만 비용이 저렴한 UUID7 등을 쓰는 것이 성능에 문제가 없다면 사용한다.
#[tokio::test]
async fn an_active_id_is_not_registered_twice_ids_are_uuidv7_and_a_departed_id_may_return() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, mut producer_inbox), (worker, _), (observer, _)) = roles(&engine, "svc").await;
    let (_, id) = producer.register_committed(work()).await.unwrap();
    assert_eq!(id.0[6] >> 4, 7, "a UUIDv7");
    producer.register_committed(Envelope::claimable("work", "do", Bytes::from_static(b"other")).with_id(id)).await.unwrap();
    assert!(matches!(next(&mut producer_inbox).await, Incoming::Fault { kind: ruvent::FaultKind::IdConflict, .. }), "other content under an active id is a conflict, told");
    assert_eq!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.len(), 1);
    let claimed = worker.claim(id, Duration::from_secs(5)).await.unwrap();
    let seq = worker.complete(id, claimed.token, Vec::new()).await.unwrap();
    worker.committed(seq).await.unwrap();
    producer.register_committed(work().with_id(id)).await.unwrap();
    assert_eq!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices[0].id, id, "not unique forever: once gone it may be used again");
}

/// > 이벤트에는 발송 전용 이벤트와 워커가 접수할 이벤트가 다르다. 발송전용 이벤트는 발송을 통해 큐에서 제거되어야하기 때문이다. 아니면 언제 큐에서 제거할지 판단하기 어렵다.
/// > 발송 전용큐를 운영하는것도 방법이다. 접수해서 처리할 이벤트큐와 발송전용큐를 따로 운영한다면 발송큐에 있는건 그 시점에 발송해줘야할 연결된 클라이언트(직접 혹은 프록시) 등에게 발송하는 것으로 큐에서 지워질것이다.
#[tokio::test]
async fn a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (sender, _) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (early, mut early_inbox) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    let (late, mut late_inbox) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    early.subscribe(Target::Channel("feed".into()), None).await.unwrap();
    sender.register(Envelope::dispatch("feed", "news", Bytes::new())).await.unwrap();
    assert!(matches!(next(&mut early_inbox).await, Incoming::Delivery(event) if &*event.event_type == "news"));
    let backlog = late.subscribe(Target::Channel("feed".into()), None).await.unwrap();
    assert!(backlog.notices.is_empty() && quiet(&mut late_inbox).await, "sent once to who was there, kept nowhere");
}

/// > 저널링은 메소드가 아니다. 서버는 저널링을 유지하고 비정상적, 정상적 종료 후 다시 재기동할 때 미처리 이벤트를 메모리에 로드하기 위한 목적을 최우선으로 저널링을 운영한다. 그외에는 서버 설정에 따라 특정 채널이나 이벤트를 일정 단위로 모아 스냅샷화하여 저장한다. 이런 기능은 메소드가 아니다.
#[test]
fn unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash() {
    for crash in [false, true] {
        let path = scratch(if crash { "crash" } else { "stop" });
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (journal, recovered) = SegmentLog::open(&path).unwrap();
            let engine = Engine::start(EngineConfig { durability: Durability::Strict, ..EngineConfig::default() }, journal, recovered).await.unwrap();
            let ((producer, _), (worker, _), _) = roles(&engine, "svc").await;
            for _ in 0..3 {
                producer.register_committed(work()).await.unwrap();
            }
            let (_, done) = producer.register_committed(work()).await.unwrap();
            let claimed = worker.claim(done, Duration::from_secs(5)).await.unwrap();
            let seq = worker.complete(done, claimed.token, Vec::new()).await.unwrap();
            worker.committed(seq).await.unwrap();
            if !crash {
                drop((producer, worker));
                engine.shutdown().await.unwrap();
            }
        });
        drop(runtime);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (journal, recovered) = SegmentLog::open(&path).unwrap();
            let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
            let (observer, _) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
            assert_eq!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.len(), 3, "crash={crash}: the three unprocessed events, not the completed one");
        });
    }
}

/// > 엔벨롭은 적어도 ID, 채널명, 재시도카운트, 착수여부, 착수관련 정보 를 내포할 수 있어야한다.
/// > 발송전용 이벤트도 이벤트 자체는 한가지 타입의 엔벨롭만 사용한다.
/// > 특히 서버나 클라이언트를 특정하지 않기 때문에 큰 내용은 필요없다
#[tokio::test]
async fn one_small_envelope_carries_id_channel_failures_and_the_attempt_for_both_kinds() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, _), (observer, mut inbox)) = roles(&engine, "svc").await;
    let (_, id) = producer.register(Envelope::claimable("work", "do", Bytes::new())).await.unwrap();
    let claimed = worker.claim(id, Duration::from_secs(30)).await.unwrap();
    assert_eq!((claimed.event.id, &*claimed.event.channel, claimed.event.failures), (id, "work", 0));
    assert!(claimed.token.seq < u64::MAX && claimed.until > 0, "the attempt: a token and the time it runs out at");
    let bare = Envelope { principal: None, ..claimed.event.clone() };
    assert_eq!(codec::size(&bare, Layout::Assigned), 37, "28-byte head, a 5-byte work key, a 4-byte payload length: no names or addresses");
    observer.subscribe(Target::Channel("feed".into()), None).await.unwrap();
    producer.register(Envelope::dispatch("feed", "news", Bytes::new())).await.unwrap();
    let Incoming::Delivery(sent) = next(&mut inbox).await else { panic!() };
    // The same type, `Envelope`, for a dispatch event; 32 bytes without the server's principal.
    let sent: Envelope = sent;
    assert_eq!(codec::size(&Envelope { principal: None, ..sent }, Layout::Delivery), 32);
}

/// > 변하지 않는 원칙중에 하나는 이벤트는 반드시 하나의 워커가 가져간다는것이다. 혹은 발송되거나. 만약 둘이상의 워커나 프로세스에 할당되어야하면 이벤트 자체를 여러개 발급해야해.
#[tokio::test]
async fn of_many_workers_racing_for_one_event_exactly_one_takes_it() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (_, id) = producer.register_committed(work()).await.unwrap();
    let mut racers = Vec::new();
    for _ in 0..16 {
        let (client, _) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
        racers.push(tokio::spawn(async move { client.claim(id, Duration::from_secs(30)).await.is_ok() }));
    }
    let mut winners = 0;
    for racer in racers {
        winners += usize::from(racer.await.unwrap());
    }
    assert_eq!(winners, 1);
}

/// > 근데 말야..아주 엄밀하게 워커서버란 존재하지 않아. 왜냐면 이벤트 서버입장에서는 딱히 클라이언트와 워커서버를 구분할 이유가 없단말이지.
/// > 그저 메소드로 이벤트 등록, 채널 수신, 이벤트착수, 이벤트 완료 라는 기능을 제공해줄 뿐이고 다 자기에게 달라붙은 클라이언트일뿐이야. 인프로세스 워커조차 메소드로 호출하고 통신층을 안거칠 뿐인거지
#[tokio::test]
async fn any_program_may_open_any_role_and_the_server_never_asks_what_it_is() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    // A "client", a "worker" and a "proxy" credential each open every role the same way.
    let ((client, _), (_, _), (_, _)) = roles(&engine, "client").await;
    let ((_, _), (worker, mut assigned), (_, _)) = roles(&engine, "worker").await;
    let ((proxy_producer, _), (proxy_worker, _), (proxy, mut proxied)) = roles(&engine, "proxy").await;
    proxy.subscribe(Target::Channel("done".into()), None).await.unwrap();
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let (_, id) = client.register(work()).await.unwrap();
    let claimed = assignment(&mut assigned).await;
    worker.complete(id, claimed.token, vec![Envelope::dispatch("done", "ok", Bytes::new())]).await.unwrap();
    assert!(matches!(next(&mut proxied).await, Incoming::Delivery(event) if &*event.channel == "done"));
    // And any of them may do what another did.
    let (_, again) = proxy_producer.register_committed(work()).await.unwrap();
    assert!(proxy_worker.claim(again, Duration::from_secs(30)).await.is_err(), "the worker's open window took it first");
    assert_eq!(assignment(&mut assigned).await.event.id, again);
}

/// > 매우 섬세하게 사이사이 사소한 로직도 귀찮다고 가장 안전하고 무거운 구현을 하면 안된다.
/// > 내가 바라는 이벤트 서버는 엄청 고가용성을 원하고 있어
#[tokio::test]
async fn a_claim_does_not_wait_for_the_disk_even_under_strict() {
    /// Lets the first `good` writes through; every later write never finishes.
    struct Stuck {
        good: u32,
    }
    #[ruvent::async_trait]
    impl ruvent::Journal for Stuck {
        async fn append(&mut self, _: ruvent::Batch) -> std::io::Result<ruvent::Written> {
            if self.good > 0 {
                self.good -= 1;
                return Ok(ruvent::Written::default());
            }
            std::future::pending().await
        }
    }
    // The startup write and the registration's write pass; then the disk is stuck.
    let engine = Engine::start(EngineConfig { durability: Durability::Strict, ..EngineConfig::default() }, Stuck { good: 2 }, Recovered::default()).await.unwrap();
    let ((producer, _), (worker, _), _) = roles(&engine, "svc").await;
    let (_, id) = producer.register_committed(work()).await.unwrap();
    let claimed = tokio::time::timeout(Duration::from_secs(2), worker.claim(id, Duration::from_secs(30))).await;
    assert!(matches!(claimed, Ok(Ok(_))), "the disk is stuck and the claim is answered anyway");
    // A registration's acknowledgement now waits for the stuck disk, as it must.
    let (seq, _) = producer.register(work()).await.unwrap();
    let acknowledged = tokio::time::timeout(Duration::from_millis(80), producer.committed(seq)).await;
    assert!(acknowledged.is_err());
}

/// > 나는 초 고성능 이벤트 서버를 통해 이벤트가 들어차고 신속하게 비워지는걸 원해.
#[tokio::test]
async fn an_open_window_takes_work_as_it_arrives_without_a_request_per_event() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, mut assignments), (observer, _)) = roles(&engine, "svc").await;
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window("work".into(), 64, 0).await.unwrap();
    for _ in 0..200 {
        producer.register(work()).await.unwrap();
    }
    let mut done = 0;
    while done < 200 {
        let claimed = assignment(&mut assignments).await;
        worker.complete(claimed.event.id, claimed.token, Vec::new()).await.unwrap();
        done += 1;
    }
    let stats = observer.stats().await.unwrap();
    assert_eq!((stats.waiting, stats.claimed), (0, 0), "filled and emptied: assignment, claim and delivery are one step, nothing is asked per event");
}

/// > 헌데 고성능 이벤트서버는 모든 이벤트 수신에 대해 수신완료 반송을 하지 않는다.
#[tokio::test]
async fn registrations_are_not_answered_one_by_one_but_acknowledged_cumulatively() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, mut inbox) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let mut last = 0;
    for _ in 0..100 {
        last = producer.register(work()).await.unwrap().0;
    }
    producer.committed(last).await.unwrap();
    assert!(quiet(&mut inbox).await, "no answer per registration; the acknowledgement is one number");
}

/// > 거절은 안보내. 그런것들은 전부 부하를 일으켜 … 모든 부하를 클라이언트로 옮겨야 서버가 고가용상태가 된다.
#[tokio::test]
async fn a_full_server_refuses_nothing_and_tells_the_producer_of_the_full_channel_to_hold() {
    let memory = ruvent::MemoryConfig { limit: Some(40_000), slot_bytes: 1_000, ..ruvent::MemoryConfig::default() };
    let engine = Engine::volatile(EngineConfig { memory, ..EngineConfig::default() }).await;
    let (producer, mut inbox) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (observer, _) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    let mut last = 0;
    for _ in 0..40 {
        last = producer.register(work()).await.unwrap().0;
    }
    producer.committed(last).await.unwrap();
    // The root paused at the end of a batch; the next registration to it is told.
    let (more, _) = producer.register(work()).await.unwrap();
    producer.committed(more).await.unwrap();
    assert_eq!(next(&mut inbox).await, Incoming::Paused("work".into()), "flow control, not a refusal");
    let stats = observer.stats().await.unwrap();
    assert_eq!(stats.waiting, 41, "every registration was applied");
    assert!(stats.roots.iter().any(|root| root.root.as_str() == "work" && root.paused));
}
