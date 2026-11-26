//! The producer (2.3, 2.9): acknowledgements, resumption, restarts, pauses,
//! one id in two processes.

mod support;

use ruvent::{ByeReason, Engine, EngineConfig, Envelope, EventId, Hello, MaxBacklog, Methods, Role, TypeRule, TypeSet, TypeSettings};
use ruvent_client::link::{Connector, Tcp};
use ruvent_client::producer::{Producer, ProducerConfig, ProducerError};
use ruvent_client::servers::Servers;
use std::sync::Arc;
use std::time::Duration;
use support::*;

fn job(channel: &str, index: usize) -> Envelope {
    job_of(channel, 1, index)
}

fn job_of(channel: &str, event_type: u16, index: usize) -> Envelope {
    Envelope::claimable(ch(channel), event_type, format!("{index}").into_bytes()).with_id(EventId::new_v7())
}

/// S8, B2: the buffer holds only what the cumulative `Committed` has not
/// reached; once everything is acknowledged it is empty.
#[tokio::test]
async fn cumulative_acknowledgements_empty_the_unacknowledged_buffer_over_tcp() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let producer = Producer::start(tcp(), &Servers::new([&server.address]), ProducerConfig::default());
    for index in 0..500 {
        within("send", producer.send(job("work", index))).await.unwrap();
    }
    within("flush", producer.flush()).await.unwrap();
    assert!(producer.unacked().iter().all(|(_, bytes)| *bytes == 0), "{:?}", producer.unacked());
    let stats = stats(&engine).await;
    assert_eq!((stats.waiting, stats.counters.retransmits), (500, 0));
}

/// 2.3: after a reconnection the producer drops what `Welcome.resume` covers
/// and sends the rest under the same numbers: every event once.
#[tokio::test]
async fn a_reconnection_resumes_after_welcome_resume_with_no_duplicates() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let relay = Relay::start(&server.address).await;
    let producer = Producer::start(tcp(), &Servers::new([&relay.address]), ProducerConfig::default());
    let total = 600;
    for index in 0..total {
        if index % 150 == 75 {
            relay.cut(Cut::Both);
        }
        within("send", producer.send(job("work", index))).await.unwrap();
    }
    within("flush", producer.flush()).await.unwrap();
    let stats = stats(&engine).await;
    assert_eq!(stats.waiting, total as u64, "every event once: none lost, none twice");
    assert_eq!(stats.counters.duplicates, 0);
}

/// 2.3, 2.9: a restart with the journal keeps the producer's record (resume
/// goes on); one without it lost the record and the numbers start over from
/// `resume + 1` — no gap, no refusal.
#[tokio::test]
async fn a_restart_with_the_journal_resumes_and_one_without_it_renumbers() {
    let path = scratch("producer-restart");
    let local = Arc::new(ruvent_client::link::Local::new());
    let engine = durable(&path, EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    let producer = Producer::start(local.clone(), &Servers::new(["s0"]), ProducerConfig::default());
    for index in 0..50 {
        within("send", producer.send(job("work", index))).await.unwrap();
    }
    within("flush", producer.flush()).await.unwrap();
    engine.shutdown().await.unwrap();
    let engine = durable(&path, EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    for index in 50..100 {
        within("send after the restart", producer.send(job("work", index))).await.unwrap();
    }
    within("flush", producer.flush()).await.unwrap();
    let after = stats(&engine).await;
    assert_eq!((after.waiting, after.counters.retransmits), (100, 0));
    assert!(producer.restarts() >= 1, "the new generation was noticed");

    // No journal: the record is gone with the process.
    engine.shutdown().await.unwrap();
    let volatile = Engine::volatile(EngineConfig::default()).await;
    local.insert("s0", volatile.clone());
    for index in 0..30 {
        within("send to a server that lost the record", producer.send(job("work", index))).await.unwrap();
    }
    within("flush", producer.flush()).await.unwrap();
    assert_eq!(stats(&volatile).await.waiting, 30);
    assert_eq!(producer.refused(), None);
}

/// 2.6, 2.9: `Paused{type}` holds that root type's sends to that server;
/// another type goes on, on the same channel too (plan 9.15: the channel
/// says nothing about who floods); `Resumed` lets the held send go.
#[tokio::test]
async fn a_paused_type_holds_only_its_own_sends() {
    const SLOW: u16 = 7;
    const FAST: u16 = 8;
    let mut config = EngineConfig::default();
    config.types.rules.insert(0, TypeRule { types: TypeSet::of([SLOW]), settings: TypeSettings { max_backlog: Some(MaxBacklog { count: 3, bytes: u64::MAX }), ..TypeSettings::default() } });
    let (local, servers, engines) = locals(1, config).await;
    let producer = Producer::start(local, &servers, ProducerConfig::default());
    // Fill `SLOW` until a send stays waiting.
    let mut held = None;
    for index in 0..64 {
        let sending = {
            let producer = producer.clone();
            tokio::spawn(async move { producer.send(job_of("work", SLOW, index)).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        if !sending.is_finished() {
            held = Some(sending);
            break;
        }
        sending.await.unwrap().unwrap();
    }
    let held = held.expect("a send of the paused type waits");
    for index in 0..20 {
        tokio::time::timeout(Duration::from_secs(2), producer.send(job_of("work", FAST, index))).await.expect("another type goes on, on the same channel").unwrap();
    }
    assert!(!held.is_finished(), "still held");
    // Drain `SLOW`: below 7/8 of its share at the pause it resumes.
    let (worker, mut inbox) = ruvent::connect(&engines[0], Hello { role: Role::Work, ..Hello::worker("w") }, 1024).await.unwrap();
    let worker_join = worker.join(None, vec![SLOW], false, Duration::from_secs(30), false).await.unwrap();
    worker.window(worker_join, 100, 0).await.unwrap();
    let drained = tokio::spawn(async move {
        while let Some(incoming) = inbox.recv().await {
            if let ruvent::Incoming::Assign(assignment) = incoming {
                worker.complete(assignment.event.id, assignment.token, Vec::new()).await.unwrap();
            }
        }
    });
    within("the held send", held).await.unwrap().unwrap();
    drained.abort();
}

/// M8-7: two processes with one producer id. The older one is told
/// `superseded`, fails its sends and does not come back; the newer one goes
/// on and is not pushed out again.
#[tokio::test]
async fn a_superseded_producer_stops_and_the_other_goes_on_without_flapping() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let servers = Servers::new([&server.address]);
    let id = EventId::new_v7().0;
    let first = Producer::start(tcp(), &servers, ProducerConfig { id: Some(id), nonce: Some([1; 16]), ..ProducerConfig::default() });
    within("first send", first.send_committed(job("work", 0))).await.unwrap();
    let second = Producer::start(tcp(), &servers, ProducerConfig { id: Some(id), nonce: Some([2; 16]), ..ProducerConfig::default() });
    within("second send", second.send_committed(job("work", 1))).await.unwrap();
    eventually("the first producer is told", || first.refused() == Some(ByeReason::Superseded)).await;
    assert_eq!(within("a send after", first.send(job("work", 2))).await, Err(ProducerError::Refused(ByeReason::Superseded)));
    tokio::time::sleep(Duration::from_millis(500)).await;
    for index in 3..10 {
        within("the second goes on", second.send_committed(job("work", index))).await.unwrap();
    }
    assert_eq!(second.refused(), None, "nobody pushed the second out");
    assert_eq!(stats(&engine).await.waiting, 9);
}

/// M8-7: the same process reconnects three times while the server still
/// holds its old connection (only the client's side was cut): the same nonce
/// replaces it quietly, nothing stalls, nothing is lost or doubled.
#[tokio::test]
async fn the_same_nonce_reconnects_three_times_through_a_one_sided_cut_without_stalling() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let relay = Relay::start(&server.address).await;
    let producer = Producer::start(tcp(), &Servers::new([&relay.address]), ProducerConfig::default());
    let mut sent = 0;
    for _ in 0..3 {
        for _ in 0..50 {
            within("send", producer.send(job("work", sent))).await.unwrap();
            sent += 1;
        }
        relay.cut(Cut::ClientOnly);
    }
    for _ in 0..50 {
        within("send after the third cut", producer.send(job("work", sent))).await.unwrap();
        sent += 1;
    }
    within("flush", producer.flush()).await.unwrap();
    assert_eq!(producer.refused(), None);
    let stats = stats(&engine).await;
    assert_eq!(stats.waiting, sent as u64);
}

/// 2.9's `Bye` table: a token the server does not know ends the producer
/// with the reason; it does not knock again.
#[tokio::test]
async fn a_refused_token_ends_the_producer_with_its_reason() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let connector: Arc<dyn Connector> = Arc::new(Tcp::new("not-a-token"));
    let producer = Producer::start(connector, &Servers::new([&server.address]), ProducerConfig::default());
    assert_eq!(within("send", producer.send(job("work", 0))).await, Err(ProducerError::Refused(ByeReason::Token)));
}

/// 2.9 `draining`: the server's final `Committed` settles what it applied;
/// the rest goes to another server; the draining one is not a candidate.
#[tokio::test]
async fn a_draining_server_hands_its_unapplied_rest_to_another_server_once() {
    let (local, servers, engines) = locals(2, EngineConfig::default()).await;
    let producer = Producer::start(local, &servers, ProducerConfig::default());
    for index in 0..100 {
        within("send", producer.send(job("work", index))).await.unwrap();
    }
    engines[0].drain().await.unwrap();
    for index in 100..200 {
        within("send while draining", producer.send(job("work", index))).await.unwrap();
    }
    within("flush", producer.flush()).await.unwrap();
    let (first, second) = (stats(&engines[0]).await.waiting, stats(&engines[1]).await.waiting);
    assert_eq!(first + second, 200, "every event exactly once over both servers ({first} + {second})");
}
