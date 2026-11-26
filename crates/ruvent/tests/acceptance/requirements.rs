//! One test per stated requirement. The quote above each test is the
//! requirement as it was given; the test is how it is observed.
//!
//! Where the approved overhaul plan (`docs/proposals/library-overhaul.md`)
//! replaced an earlier requirement — reclaim no longer issues a new id (C3),
//! a connection now has a role (2.2) — the test quotes the statement the new
//! behaviour serves, not the replaced one.

use ruvent::envelope::codec::{self, Layout};
use ruvent::WorkerId;
use ruvent::{
    connect, delete_outcome, Assignment, Bytes, Channel, Client, DeleteFailure, Durability, Engine, EngineConfig, Envelope, EventId, Hello, Inbox, Incoming, Methods, Recovered, SegmentLog,
    SERVER_TYPE,
};
use std::path::PathBuf;
use std::time::Duration;

/// Each test worker's uuid: what its claims name (never nil).
pub const WORKER: WorkerId = [0x5A; 16];
pub const OTHER: WorkerId = [0x5B; 16];
pub const PROXY_WORKER: WorkerId = [0x5E; 16];

fn scratch(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("ruvent-accept-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory.join("journal")
}

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

fn work() -> Envelope {
    Envelope::claimable(ch("work"), 1, Bytes::from_static(b"{}"))
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
    observer.subscribe(ch("results"), None).await.unwrap();
    let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    let (_, waiting) = producer.register(work()).await.unwrap();
    worker.window(worker_join, 1, 0).await.unwrap();
    let claimed = assignment(&mut assignments).await.event.id;
    let (_, sent) = producer.register(Envelope::dispatch(ch("feed"), 6, Bytes::new()).with_id(EventId::new_v7())).await.unwrap();
    let (_, second) = producer.register(work()).await.unwrap();
    assert_ne!(second, claimed);
    // The outcome names the deleted id in its payload: the envelope carries no flow id.
    for (target, failure) in [(claimed, DeleteFailure::Claimed), (sent, DeleteFailure::Missing)] {
        producer.delete(target, ch("results"), None).await.unwrap();
        let Incoming::Delivery(outcome) = next(&mut inbox).await else { panic!("no outcome event") };
        assert_eq!((outcome.event_type, outcome.kind), (SERVER_TYPE, ruvent::Kind::Dispatch));
        assert_eq!(delete_outcome(&outcome.payload), Some((target, Err(failure))));
    }
    let _ = waiting;
    producer.delete(second, ch("results"), None).await.unwrap();
    let Incoming::Delivery(outcome) = next(&mut inbox).await else { panic!("no outcome event") };
    assert_eq!((outcome.event_type, delete_outcome(&outcome.payload)), (SERVER_TYPE, Some((second, Ok(())))));
}

/// > 2. 착수마킹(이벤트 자체에 상태를 직접 변경):가져간 워커정보를 비롯한 착수정보 기록. 착수시에 타임아웃이 일어날 시간을 워커가 마킹함
/// > 3. 완료보고(큐에서 지움)
#[tokio::test]
async fn a_claim_carries_the_attempt_and_the_time_out_the_worker_chose_and_completion_removes_it() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, _), (observer, _)) = roles(&engine, "svc").await;
    let (_, id) = producer.register(work()).await.unwrap();
    let before = engine.uptime();
    let claimed = worker.claim(id, WORKER, Duration::from_secs(90)).await.unwrap();
    assert_eq!(claimed.lease_ms, 90_000, "the worker chose the time-out");
    assert!(claimed.until >= before + 90_000, "the time it runs out at: {}", claimed.until);
    assert_eq!(claimed.event.id, id);
    let seq = worker.complete(id, WORKER, Vec::new()).await.unwrap();
    worker.committed(seq).await.unwrap();
    assert!(observer.subscribe(ch("work"), None).await.unwrap().notices.is_empty(), "completion removed it from the queue");
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
    assert_eq!(observer.subscribe(ch("work"), None).await.unwrap().notices.len(), 1);
    // A lease that runs out gives the event back under the same id; the late report has no effect.
    let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(1), false).await.unwrap();
    worker.window(worker_join, 1, 0).await.unwrap();
    let first = assignment(&mut assignments).await;
    assert_eq!(first.event.worker, Some(WORKER), "claimed under the uuid its join named");
    tokio::time::advance(Duration::from_secs(2)).await;
    let (other, mut others) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
    let other_join = other.join(OTHER, Some(ch("work")), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    other.window(other_join, 1, 0).await.unwrap();
    let second = assignment(&mut others).await;
    assert_eq!(second.event.id, event.id, "the logical id does not change");
    let late = worker.complete(event.id, WORKER, Vec::new()).await.unwrap();
    worker.committed(late).await.unwrap();
    assert_eq!(observer.stats().await.unwrap().claimed, 1, "the stale completion changed nothing");
    // Completion: the right attempt removes it once.
    let done = other.complete(event.id, OTHER, Vec::new()).await.unwrap();
    other.committed(done).await.unwrap();
    assert_eq!(observer.stats().await.unwrap().claimed + observer.stats().await.unwrap().waiting, 0);
}

/// > 5. 워커의 실패시 이벤트는 완료처리됨. 이후 처리를 위한 것은 새로운 이벤트로 등록됨
/// > 6. 재시도 카운트가 늘어난 대기 이벤트의 처리 역시 워커가 착수하고 처리함. 일정 재시도 횟수가 넘어가면 그냥 완료처리하고 새로운 이벤트를 등록할지 실패용 이벤트보고를 할지 말지도 전부 해당 이벤트를 처리할 워커 내부로직으로 해결
#[tokio::test]
async fn a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, mut assignments), (observer, mut inbox)) = roles(&engine, "svc").await;
    observer.subscribe(ch("failures"), None).await.unwrap();
    observer.subscribe(ch("retry"), None).await.unwrap();
    let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window(worker_join, 1, 0).await.unwrap();
    let (_, id) = producer.register(work()).await.unwrap();
    let claimed = assignment(&mut assignments).await;
    // The worker failed, and decided: report it, and try again as a new event.
    let follow_ups = vec![Envelope::dispatch(ch("failures"), 31, Bytes::from_static(b"boom")), Envelope::claimable(ch("retry"), 1, claimed.event.payload.clone())];
    worker.complete(id, WORKER, follow_ups).await.unwrap();
    let mut seen = [next(&mut inbox).await, next(&mut inbox).await];
    seen.sort_by_key(|incoming| matches!(incoming, Incoming::Notice(_)));
    assert!(matches!(&seen[0], Incoming::Delivery(event) if event.event_type == 31));
    assert!(matches!(&seen[1], Incoming::Notice(event) if event.failures == 0 && event.id != id));
    assert!(observer.subscribe(ch("work"), None).await.unwrap().notices.is_empty(), "the failed event itself is completed");
}

/// > 사실 착수마크도 필요없지 착수 워커의 uuid가 있냐 없냐로 식별되니까
/// > 그리고 착수 요청 자체가 uuid제출 없이는 불가능해야하고
/// > 실패보고 역시 착수 uuid와 이벤트 uuid를 보내지
/// > 두개가 맞아야 실패처리를 해주면서 이벤트에서 착수 uuid를 제거하고 실패카운트를 올리는것으로 다른 워커가 착수할 수 있게 해주는거지
#[tokio::test]
async fn a_claim_is_the_event_and_the_worker_uuid_and_a_matching_failure_report_frees_it_for_another_worker() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, _), (worker, _), (observer, _)) = roles(&engine, "svc").await;
    let (other, _) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
    let (_, id) = producer.register_committed(work()).await.unwrap();
    // No claim request without a worker uuid.
    assert_eq!(worker.claim(id, [0; 16], Duration::from_secs(30)).await.err(), Some(ruvent::Refusal::Malformed), "an explicit claim without a uuid");
    assert_eq!(worker.join([0; 16], Some(ch("work")), Vec::new(), Duration::from_secs(30), false).await.err(), Some(ruvent::Refusal::Malformed), "a join without a uuid");
    // The worker uuid's presence is the claim.
    let claimed = worker.claim(id, WORKER, Duration::from_secs(30)).await.unwrap();
    assert_eq!((claimed.event.worker, claimed.event.failures), (Some(WORKER), 0));
    assert_eq!(other.claim(id, OTHER, Duration::from_secs(30)).await.err(), Some(ruvent::Refusal::AlreadyClaimed), "claimed: another worker cannot take it");
    // A failure report must name the event and the claiming worker: another uuid changes nothing.
    other.release(id, OTHER, Duration::ZERO, true).await.unwrap();
    let stats = observer.stats().await.unwrap();
    assert_eq!((stats.claimed, stats.waiting, stats.counters.stale), (1, 0, 1), "a report whose worker does not match is stale");
    // Both match: the uuid leaves the event, its failure count rises, and another worker may claim it.
    worker.release(id, WORKER, Duration::ZERO, true).await.unwrap();
    let stats = observer.stats().await.unwrap();
    assert_eq!((stats.claimed, stats.waiting), (0, 1), "waiting again, claimed by nobody");
    let again = other.claim(id, OTHER, Duration::from_secs(30)).await.unwrap();
    assert_eq!((again.event.id, again.event.worker, again.event.failures), (id, Some(OTHER), 1), "the same event, now the other worker's, one failure counted");
    // The first worker's late completion names a claim that is no longer there.
    let late = worker.complete(id, WORKER, Vec::new()).await.unwrap();
    worker.committed(late).await.unwrap();
    assert_eq!(observer.stats().await.unwrap().claimed, 1, "the other worker still holds it");
    let done = other.complete(id, OTHER, Vec::new()).await.unwrap();
    other.committed(done).await.unwrap();
    let stats = observer.stats().await.unwrap();
    assert_eq!((stats.claimed, stats.waiting), (0, 0));
}

/// > ID가 같은 이벤트는 등록될 수 없고 ID는 영원히 고유하지 않아도 충분하지만 비용이 저렴한 UUID7 등을 쓰는 것이 성능에 문제가 없다면 사용한다.
#[tokio::test]
async fn an_active_id_is_not_registered_twice_ids_are_uuidv7_and_a_departed_id_may_return() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let ((producer, mut producer_inbox), (worker, _), (observer, _)) = roles(&engine, "svc").await;
    let (_, id) = producer.register_committed(work()).await.unwrap();
    assert_eq!(id.0[6] >> 4, 7, "a UUIDv7");
    producer.register_committed(Envelope::claimable(ch("work"), 1, Bytes::from_static(b"other")).with_id(id)).await.unwrap();
    assert!(matches!(next(&mut producer_inbox).await, Incoming::Fault { kind: ruvent::FaultKind::IdConflict, .. }), "other content under an active id is a conflict, told");
    assert_eq!(observer.subscribe(ch("work"), None).await.unwrap().notices.len(), 1);
    worker.claim(id, WORKER, Duration::from_secs(5)).await.unwrap();
    let seq = worker.complete(id, WORKER, Vec::new()).await.unwrap();
    worker.committed(seq).await.unwrap();
    producer.register_committed(work().with_id(id)).await.unwrap();
    assert_eq!(observer.subscribe(ch("work"), None).await.unwrap().notices[0].id, id, "not unique forever: once gone it may be used again");
}

/// > 이벤트에는 발송 전용 이벤트와 워커가 접수할 이벤트가 다르다. 발송전용 이벤트는 발송을 통해 큐에서 제거되어야하기 때문이다. 아니면 언제 큐에서 제거할지 판단하기 어렵다.
/// > 발송 전용큐를 운영하는것도 방법이다. 접수해서 처리할 이벤트큐와 발송전용큐를 따로 운영한다면 발송큐에 있는건 그 시점에 발송해줘야할 연결된 클라이언트(직접 혹은 프록시) 등에게 발송하는 것으로 큐에서 지워질것이다.
#[tokio::test]
async fn a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (sender, _) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (early, mut early_inbox) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    let (late, mut late_inbox) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    early.subscribe(ch("feed"), None).await.unwrap();
    sender.register(Envelope::dispatch(ch("feed"), 30, Bytes::new())).await.unwrap();
    assert!(matches!(next(&mut early_inbox).await, Incoming::Delivery(event) if event.event_type == 30));
    let backlog = late.subscribe(ch("feed"), None).await.unwrap();
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
            worker.claim(done, WORKER, Duration::from_secs(5)).await.unwrap();
            let seq = worker.complete(done, WORKER, Vec::new()).await.unwrap();
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
            assert_eq!(observer.subscribe(ch("work"), None).await.unwrap().notices.len(), 3, "crash={crash}: the three unprocessed events, not the completed one");
        });
    }
}

/// > 엔벨롭은 적어도 ID, 채널명, 재시도카운트, 착수여부, 착수관련 정보 를 내포할 수 있어야한다.
/// > 발송전용 이벤트도 이벤트 자체는 한가지 타입의 엔벨롭만 사용한다.
/// > 특히 서버나 클라이언트를 특정하지 않기 때문에 큰 내용은 필요없다
#[tokio::test]
async fn one_small_envelope_carries_id_channel_failures_and_the_attempt_for_both_kinds() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (worker, _) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
    let (observer, mut inbox) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    let (_, id) = producer.register(Envelope::claimable(ch("work"), 1, Bytes::new())).await.unwrap();
    let claimed = worker.claim(id, WORKER, Duration::from_secs(30)).await.unwrap();
    assert_eq!((claimed.event.id, claimed.event.channel, claimed.event.failures), (id, ch("work"), 0));
    // The claim is in the envelope as the uuid of the worker that took it, nothing else.
    assert_eq!(claimed.event.worker, Some(WORKER), "an assigned event carries the claiming worker's uuid");
    assert_eq!(claimed.lease_ms, 30_000, "the lease it runs under comes beside it");
    assert!(claimed.until > 0, "and the time it runs out at");
    assert_eq!(
        codec::size(&claimed.event, Layout::Assigned),
        38 + 5 + 16 + 4,
        "38-byte head (id, channel id, type, flags, failures), a 5-byte work key, the worker uuid (16), a 4-byte payload length: no names, addresses or tokens"
    );
    observer.subscribe(ch("feed"), None).await.unwrap();
    producer.register(Envelope::dispatch(ch("feed"), 30, Bytes::new())).await.unwrap();
    let Incoming::Delivery(sent) = next(&mut inbox).await else { panic!() };
    // The same type, `Envelope`, for a dispatch event: the head and the payload length, 42 bytes.
    let sent: Envelope = sent;
    assert_eq!(codec::size(&sent, Layout::Delivery), 42);
}

/// > 변하지 않는 원칙중에 하나는 이벤트는 반드시 하나의 워커가 가져간다는것이다. 혹은 발송되거나. 만약 둘이상의 워커나 프로세스에 할당되어야하면 이벤트 자체를 여러개 발급해야해.
#[tokio::test]
async fn of_many_workers_racing_for_one_event_exactly_one_takes_it() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (_, id) = producer.register_committed(work()).await.unwrap();
    let mut racers = Vec::new();
    for racer in 0..16u8 {
        let (client, _) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
        racers.push(tokio::spawn(async move { client.claim(id, [0x60 + racer; 16], Duration::from_secs(30)).await.is_ok() }));
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
    proxy.subscribe(ch("done"), None).await.unwrap();
    let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window(worker_join, 1, 0).await.unwrap();
    let (_, id) = client.register(work()).await.unwrap();
    assert_eq!(assignment(&mut assigned).await.event.worker, Some(WORKER), "claimed under the worker's uuid");
    worker.complete(id, WORKER, vec![Envelope::dispatch(ch("done"), 32, Bytes::new())]).await.unwrap();
    assert!(matches!(next(&mut proxied).await, Incoming::Delivery(event) if event.channel == ch("done")));
    // And any of them may do what another did.
    let (_, again) = proxy_producer.register_committed(work()).await.unwrap();
    assert!(proxy_worker.claim(again, PROXY_WORKER, Duration::from_secs(30)).await.is_err(), "the worker's open window took it first");
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
    let claimed = tokio::time::timeout(Duration::from_secs(2), worker.claim(id, WORKER, Duration::from_secs(30))).await;
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
    let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window(worker_join, 64, 0).await.unwrap();
    for _ in 0..200 {
        producer.register(work()).await.unwrap();
    }
    let mut done = 0;
    while done < 200 {
        let claimed = assignment(&mut assignments).await;
        worker.complete(claimed.event.id, WORKER, Vec::new()).await.unwrap();
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
async fn a_full_server_refuses_nothing_and_holds_up_no_producer() {
    let memory = ruvent::MemoryConfig { limit: Some(40_000), slot_bytes: 1_000, ..ruvent::MemoryConfig::default() };
    let engine = Engine::volatile(EngineConfig { memory, ..EngineConfig::default() }).await;
    let (producer, mut inbox) = connect(&engine, Hello::producer("svc"), 64).await.unwrap();
    let (observer, _) = connect(&engine, Hello::observer("svc"), 64).await.unwrap();
    let mut last = 0;
    for _ in 0..40 {
        last = producer.register(work()).await.unwrap().0;
    }
    producer.committed(last).await.unwrap();
    // Past the memory limit: still applied and acknowledged, and nobody is told to hold (plan 9.15).
    let (more, _) = producer.register(work()).await.unwrap();
    producer.committed(more).await.unwrap();
    assert!(quiet(&mut inbox).await, "no refusal, no pause: a producer is never held up by the server");
    let stats = observer.stats().await.unwrap();
    assert_eq!(stats.waiting, 41, "every registration was applied");
    assert!(stats.memory_used >= stats.memory_limit, "the limit was passed: {} of {}", stats.memory_used, stats.memory_limit);
}
