//! One test per stated requirement. The quote above each test is the
//! requirement as it was given; the test is how it is observed.

use ruvent::envelope::codec::{self, Layout};
use ruvent::{connect, Bytes, Durability, Engine, EngineConfig, Envelope, EventId, FileJournal, Inbox, Incoming, Methods, Recovered, Refusal, DELETED, DELETE_FAILED};
use std::path::PathBuf;
use std::time::Duration;

fn scratch(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("ruvent-accept-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory.join("events.journal")
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

async fn quiet(inbox: &mut Inbox) -> bool {
    loop {
        match tokio::time::timeout(Duration::from_millis(80), inbox.recv()).await {
            Err(_) => return true,
            Ok(Some(Incoming::Subscribed(_))) => continue,
            Ok(_) => return false,
        }
    }
}

/// > 1. 착수가능한 이벤트 등록
/// > 2. 발송전용 이벤트 등록
/// > 3. 특정 이벤트 착수 마킹
/// > 4. 특정 이벤트 완료보고
/// > 5. 이벤트 삭제(아직 대기열에 있어야함) 이미 발송되거나 착수되면 발송용 삭제 실패 이벤트 등록. 성공시에도 발송용 성공이벤트 등록
/// > 6. 특정 채널 구독 등록
/// > 7. 특정 채널 구독 취소
#[tokio::test]
async fn the_server_offers_exactly_seven_methods_and_deletion_always_announces_its_outcome() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = connect(&engine, "svc", 64).await.unwrap();
    client.subscribe("results".into(), None).await.unwrap();
    let (waiting, _) = client.register_claimable(work()).await.unwrap();
    let (claimed, _) = client.register_claimable(work()).await.unwrap();
    client.claim(claimed, 1, Duration::from_secs(30)).await.unwrap();
    let sent = client.register_dispatch(Envelope::dispatch("feed", "x", Bytes::new())).await.unwrap();
    for (target, removed, announced) in [(waiting, true, DELETED), (claimed, false, DELETE_FAILED), (sent, false, DELETE_FAILED)] {
        let tag = EventId::new_v7();
        assert_eq!(client.delete(target, "results".into(), tag, None).await.unwrap(), removed);
        let Incoming::Delivery(outcome) = next(&mut inbox).await else { panic!("no outcome event") };
        assert_eq!((&*outcome.event_type, outcome.correlation, outcome.kind), (announced, Some(tag), ruvent::Kind::Dispatch));
    }
    client.unsubscribe("results".into()).await.unwrap();
    assert!(quiet(&mut inbox).await);
    // The contract has these seven methods and no others: `Methods` is the whole API a connection gets.
    fn _seven<M: Methods>(_: &M) {}
    _seven(&client);
}

/// > 2. 착수마킹(이벤트 자체에 상태를 직접 변경):가져간 워커정보를 비롯한 착수정보 기록. 착수시에 타임아웃이 일어날 시간을 워커가 마킹함
/// > 3. 완료보고(큐에서 지움)
#[tokio::test]
async fn a_claim_records_the_worker_and_its_deadline_in_the_event_and_completion_removes_it() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    let (id, _) = client.register_claimable(work()).await.unwrap();
    let claimed = client.claim(id, 42, Duration::from_secs(90)).await.unwrap();
    let claim = claimed.claim.expect("the claim is in the event itself");
    assert!(claimed.is_claimed());
    assert_eq!(claim.worker, 42);
    assert!(claim.deadline >= 90_000, "the worker chose the timeout: {}", claim.deadline);
    client.complete(id, claim.token, Vec::new()).await.unwrap();
    assert!(client.subscribe("work".into(), None).await.unwrap().notices.is_empty(), "completion removed it from the queue");
}

/// > 4. 지연확인 -> 재시도 카운트 증가 후 대기 상태로 전환. 이때 이벤트 ID는 재발급된다.
/// > 착수이벤트의 타임아웃은 일반적으로 스케줄러가 필요없어.
/// > 활성화된 이벤트서버는 지속적으로 이벤트가 수신되기 때문에 이벤트 등록시점에 착수이벤트 중 시간이 초과된것을 일괄 미착수 상태로 돌리기만 하면 되기 때문이야. 단 이 때 이벤트 고유번호를 업데이트한다.
/// > 그러면 타임아웃 이후에 워커가 완료보고해도 그 이벤트 번호에 해당되는 이벤트는 존재하지 않기 때문에 무시하면 된다.
#[tokio::test(start_paused = true)]
async fn a_timed_out_claim_is_returned_to_waiting_at_the_next_registration_with_a_new_id_and_the_late_report_is_ignored() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = connect(&engine, "svc", 64).await.unwrap();
    client.subscribe("work".into(), None).await.unwrap();
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(client.register_claimable(work()).await.unwrap().0);
        let _ = next(&mut inbox).await;
    }
    let mut tokens = Vec::new();
    for id in &ids {
        tokens.push(client.claim(*id, 1, Duration::from_secs(1)).await.unwrap().claim.unwrap().token);
    }
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(quiet(&mut inbox).await, "without a registration nothing happens: there is no scheduler");
    client.register_dispatch(Envelope::dispatch("elsewhere", "x", Bytes::new())).await.unwrap();
    let mut renewed = Vec::new();
    for _ in 0..3 {
        let Incoming::Notice(notice) = next(&mut inbox).await else { panic!("expected a renewed notice") };
        assert_eq!(notice.retry, 1);
        assert!(!ids.contains(&notice.id), "a new id");
        renewed.push(notice.id);
    }
    for (id, token) in ids.iter().zip(tokens) {
        assert_eq!(client.complete(*id, token, Vec::new()).await, Err(Refusal::Missing), "the old number no longer exists");
    }
    assert_eq!(client.subscribe("work".into(), None).await.unwrap().notices.len(), 3, "all three are waiting again");
}

/// > 5. 워커의 실패시 이벤트는 완료처리됨. 이후 처리를 위한 것은 새로운 이벤트로 등록됨
/// > 6. 재시도 카운트가 늘어난 대기 이벤트의 처리 역시 워커가 착수하고 처리함. 일정 재시도 횟수가 넘어가면 그냥 완료처리하고 새로운 이벤트를 등록할지 실패용 이벤트보고를 할지 말지도 전부 해당 이벤트를 처리할 워커 내부로직으로 해결
#[tokio::test]
async fn a_failure_is_a_completion_and_what_follows_is_new_events_the_worker_chose() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = connect(&engine, "svc", 64).await.unwrap();
    client.subscribe("failures".into(), None).await.unwrap();
    client.subscribe("retry".into(), None).await.unwrap();
    let (id, _) = client.register_claimable(work()).await.unwrap();
    let claimed = client.claim(id, 1, Duration::from_secs(30)).await.unwrap();
    // The worker failed, and decided: report it, and try again as a new event.
    let follow_ups = vec![Envelope::dispatch("failures", "work.failed", Bytes::from_static(b"boom")), Envelope::claimable("retry", "do", claimed.payload.clone())];
    client.complete(id, claimed.claim.unwrap().token, follow_ups).await.unwrap();
    let mut seen = [next(&mut inbox).await, next(&mut inbox).await];
    seen.sort_by_key(|incoming| matches!(incoming, Incoming::Notice(_)));
    assert!(matches!(&seen[0], Incoming::Delivery(event) if &*event.event_type == "work.failed"));
    assert!(matches!(&seen[1], Incoming::Notice(event) if event.retry == 0 && event.id != id));
    assert!(client.subscribe("work".into(), None).await.unwrap().notices.is_empty(), "the failed event itself is completed");
}

/// > ID가 같은 이벤트는 등록될 수 없고 ID는 영원히 고유하지 않아도 충분하지만 비용이 저렴한 UUID7 등을 쓰는 것이 성능에 문제가 없다면 사용한다.
#[tokio::test]
async fn an_id_in_the_queue_cannot_be_registered_again_ids_are_uuidv7_and_a_departed_id_may_return() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    let (id, _) = client.register_claimable(work()).await.unwrap();
    assert_eq!(id.0[6] >> 4, 7, "a UUIDv7");
    assert_eq!(client.register_claimable(work().with_id(id)).await, Err(Refusal::Duplicate));
    assert_eq!(client.register_dispatch(Envelope::dispatch("x", "t", Bytes::new()).with_id(id)).await, Err(Refusal::Duplicate));
    let token = client.claim(id, 1, Duration::from_secs(5)).await.unwrap().claim.unwrap().token;
    client.complete(id, token, Vec::new()).await.unwrap();
    assert_eq!(client.register_claimable(work().with_id(id)).await.unwrap().0, id, "not unique forever: once gone it may be used again");
}

/// > 이벤트에는 발송 전용 이벤트와 워커가 접수할 이벤트가 다르다. 발송전용 이벤트는 발송을 통해 큐에서 제거되어야하기 때문이다. 아니면 언제 큐에서 제거할지 판단하기 어렵다.
/// > 발송 전용큐를 운영하는것도 방법이다. 접수해서 처리할 이벤트큐와 발송전용큐를 따로 운영한다면 발송큐에 있는건 그 시점에 발송해줘야할 연결된 클라이언트(직접 혹은 프록시) 등에게 발송하는 것으로 큐에서 지워질것이다.
#[tokio::test]
async fn a_dispatch_event_goes_to_whoever_is_connected_at_that_moment_and_is_gone() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (sender, _) = connect(&engine, "svc", 64).await.unwrap();
    let (early, mut early_inbox) = connect(&engine, "svc", 64).await.unwrap();
    let (late, mut late_inbox) = connect(&engine, "svc", 64).await.unwrap();
    early.subscribe("feed".into(), None).await.unwrap();
    let id = sender.register_dispatch(Envelope::dispatch("feed", "news", Bytes::new())).await.unwrap();
    assert!(matches!(next(&mut early_inbox).await, Incoming::Delivery(event) if event.id == id));
    let backlog = late.subscribe("feed".into(), None).await.unwrap();
    assert!(backlog.notices.is_empty() && quiet(&mut late_inbox).await, "sent once to who was there, kept nowhere");
    assert_eq!(sender.delete(id, "feed".into(), EventId::new_v7(), None).await, Ok(false), "already sent: nothing to delete");
}

/// > 저널링은 메소드가 아니다. 서버는 저널링을 유지하고 비정상적, 정상적 종료 후 다시 재기동할 때 미처리 이벤트를 메모리에 로드하기 위한 목적을 최우선으로 저널링을 운영한다. 그외에는 서버 설정에 따라 특정 채널이나 이벤트를 일정 단위로 모아 스냅샷화하여 저장한다. 이런 기능은 메소드가 아니다.
#[test]
fn unprocessed_events_come_back_after_a_normal_stop_and_after_a_crash() {
    for crash in [false, true] {
        let path = scratch(if crash { "crash" } else { "stop" });
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (journal, recovered) = FileJournal::open(&path).unwrap();
            let engine = Engine::start(EngineConfig { durability: Durability::Strict, ..EngineConfig::default() }, journal, recovered).await.unwrap();
            let (client, _) = connect(&engine, "svc", 64).await.unwrap();
            for _ in 0..3 {
                client.register_claimable(work()).await.unwrap();
            }
            let (done, _) = client.register_claimable(work()).await.unwrap();
            let token = client.claim(done, 1, Duration::from_secs(5)).await.unwrap().claim.unwrap().token;
            client.complete(done, token, Vec::new()).await.unwrap();
            if !crash {
                drop(client);
                engine.shutdown().await.unwrap();
            }
        });
        drop(runtime);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (journal, recovered) = FileJournal::open(&path).unwrap();
            let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
            let (client, _) = connect(&engine, "svc", 64).await.unwrap();
            assert_eq!(client.subscribe("work".into(), None).await.unwrap().notices.len(), 3, "crash={crash}: the three unprocessed events, not the completed one");
        });
    }
}

/// > 엔벨롭은 적어도 ID, 채널명, 재시도카운트, 착수여부, 착수관련 정보 를 내포할 수 있어야한다.
/// > 발송전용 이벤트도 이벤트 자체는 한가지 타입의 엔벨롭만 사용한다.
/// > 특히 서버나 클라이언트를 특정하지 않기 때문에 큰 내용은 필요없다
#[tokio::test]
async fn one_small_envelope_carries_id_channel_retry_and_the_claim_for_both_kinds() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = connect(&engine, "svc", 64).await.unwrap();
    let (id, _) = client.register_claimable(Envelope::claimable("work", "do", Bytes::new())).await.unwrap();
    let claimed: Envelope = client.claim(id, 7, Duration::from_secs(30)).await.unwrap();
    assert_eq!((claimed.id, &*claimed.channel, claimed.retry, claimed.is_claimed()), (id, "work", 0, true));
    assert_eq!(codec::size(&claimed, Layout::Claimed) - codec::size(&Envelope { principal: None, ..claimed.clone() }, Layout::Claimed), 4, "the principal is the only optional field the server adds");
    assert_eq!(codec::size(&Envelope { principal: None, ..claimed.clone() }, Layout::Claimed), 63, "the claim information is 24 bytes of small numbers, no names or addresses");
    client.subscribe("feed".into(), None).await.unwrap();
    client.register_dispatch(Envelope::dispatch("feed", "news", Bytes::new())).await.unwrap();
    let Incoming::Delivery(sent) = next(&mut inbox).await else { panic!() };
    // The same type, `Envelope`, for a dispatch event; 31 bytes without the server's principal.
    let sent: Envelope = sent;
    assert_eq!(codec::size(&Envelope { principal: None, ..sent }, Layout::Delivery), 31);
}

/// > 변하지 않는 원칙중에 하나는 이벤트는 반드시 하나의 워커가 가져간다는것이다. 혹은 발송되거나. 만약 둘이상의 워커나 프로세스에 할당되어야하면 이벤트 자체를 여러개 발급해야해.
#[tokio::test]
async fn of_many_workers_racing_for_one_event_exactly_one_takes_it() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = connect(&engine, "svc", 64).await.unwrap();
    let (id, _) = producer.register_claimable(work()).await.unwrap();
    let mut racers = Vec::new();
    for worker in 0..16u32 {
        let (client, _) = connect(&engine, "svc", 64).await.unwrap();
        racers.push(tokio::spawn(async move { client.claim(id, worker, Duration::from_secs(30)).await.is_ok() }));
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
async fn every_connection_gets_the_same_methods_and_the_server_never_asks_what_it_is() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    // A "client", a "worker" and a "proxy" are three connections opened the same way.
    let (client, _) = connect(&engine, "client", 64).await.unwrap();
    let (worker, _) = connect(&engine, "worker", 64).await.unwrap();
    let (proxy, mut proxied) = connect(&engine, "proxy", 64).await.unwrap();
    proxy.subscribe("done".into(), None).await.unwrap();
    let (id, _) = client.register_claimable(work()).await.unwrap();
    let token = worker.claim(id, 1, Duration::from_secs(30)).await.unwrap().claim.unwrap().token;
    worker.complete(id, token, vec![Envelope::dispatch("done", "ok", Bytes::new())]).await.unwrap();
    assert!(matches!(next(&mut proxied).await, Incoming::Delivery(event) if &*event.channel == "done"));
    // And any of them may do what another did.
    let (again, _) = proxy.register_claimable(work()).await.unwrap();
    assert!(client.claim(again, 2, Duration::from_secs(30)).await.is_ok());
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
        async fn append(&mut self, _: Vec<ruvent::Record>) -> std::io::Result<()> {
            if self.good > 0 {
                self.good -= 1;
                return Ok(());
            }
            std::future::pending().await
        }
        async fn rewrite(&mut self, _: Vec<ruvent::Record>) -> std::io::Result<()> {
            std::future::pending().await
        }
    }
    // The startup write and the registration's write pass; then the disk is stuck.
    let engine = Engine::start(EngineConfig { durability: Durability::Strict, ..EngineConfig::default() }, Stuck { good: 2 }, Recovered::default()).await.unwrap();
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    let (id, _) = client.register_claimable(work()).await.unwrap();
    let claimed = tokio::time::timeout(Duration::from_secs(2), client.claim(id, 1, Duration::from_secs(30))).await;
    assert!(matches!(claimed, Ok(Ok(_))), "the disk is stuck and the claim is answered anyway");
    // A registration now waits for the stuck disk, as Strict says it must.
    let registering = tokio::spawn({
        let client = client.clone();
        async move { client.register_claimable(work()).await }
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!registering.is_finished());
}
