//! The requester (2.8, 2.9): results as claimable events on a reply channel,
//! fragments ahead of results, nothing lost or run twice across restarts
//! and reconnections.

mod support;

use ruvent::{EngineConfig, Envelope};
use ruvent_client::requester::{Part, RequestError, RequestOptions, Requester, RequesterConfig};
use ruvent_client::servers::Servers;
use ruvent_client::worker::{Service, Worker};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use support::*;
use tokio::sync::Notify;

fn ask(payload: &str) -> Envelope {
    Envelope::claimable("orders", "price", payload.as_bytes().to_vec())
}

/// 2.8: fragments come on the same work connection as the result, so they
/// arrive first and in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_gets_its_fragments_then_its_result_over_tcp() {
    let engine = ruvent::Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let list = Servers::new([&server.address]);
    let worker = Worker::builder(tcp(), &list)
        .service(Service::new("orders").on("price", |cx| async move {
            for piece in ["a", "b", "c"] {
                cx.fragment(piece.as_bytes().to_vec()).await.unwrap();
            }
            let answer = format!("{}!", String::from_utf8_lossy(cx.payload()));
            cx.reply(answer.into_bytes());
            Ok(())
        }))
        .start();
    let requester = Requester::start(tcp(), &list, RequesterConfig::default());
    let response = within("the response", requester.request(ask("x"), RequestOptions::default())).await.unwrap();
    assert_eq!(&response.result.payload[..], b"x!");
    let pieces: Vec<&[u8]> = response.fragments.iter().map(|fragment| &fragment.payload[..]).collect();
    assert_eq!(pieces, [b"a", b"b", b"c"]);
    assert!(!response.fragments_lost);
    worker.shutdown().await;
    server.stop().await;
}

/// M8-9: the server restarts (no drain) while a request runs. The run goes
/// on, its result lands after the restart and reaches the requester once
/// both are back: no result lost, nothing run twice, no fragment after the
/// result; the fragments sent while the connections changed are marked lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_during_a_request_loses_no_result_and_runs_nothing_twice() {
    let path = scratch("requester-restart");
    let local = Arc::new(ruvent_client::link::Local::new());
    let engine = durable(&path, EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    let list = Servers::new(["s0"]);
    let runs = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(Notify::new());
    let (handler_runs, handler_gate) = (runs.clone(), gate.clone());
    let worker = Worker::builder(local.clone(), &list)
        .service(Service::new("orders").slots(1).on("price", move |cx| {
            let (runs, gate) = (handler_runs.clone(), handler_gate.clone());
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                cx.fragment(&b"before"[..]).await.unwrap();
                gate.notified().await;
                let _ = cx.fragment(&b"after"[..]).await;
                cx.reply(&b"done"[..]);
                Ok(())
            }
        }))
        .start();
    let requester = Requester::start(local.clone(), &list, RequesterConfig::default());
    let mut stream = within("the request", requester.stream(ask("x"), RequestOptions::default())).await.unwrap();
    match within("the first fragment", stream.next()).await.unwrap() {
        Some(Part::Fragment(fragment)) => assert_eq!((&fragment.payload[..], fragment.correlation), (&b"before"[..], Some(stream.correlation()))),
        other => panic!("expected the first fragment, got {other:?}"),
    }
    engine.shutdown().await.unwrap();
    let engine = durable(&path, EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    eventually("the requester joined again", || !requester.joined().is_empty()).await;
    gate.notify_one();
    let mut parts = Vec::new();
    while let Some(part) = within("the rest", stream.next()).await.unwrap() {
        parts.push(part);
    }
    let result = parts.iter().position(|part| matches!(part, Part::Result(_))).expect("the result arrived");
    assert_eq!(result, parts.len() - 1, "nothing after the result: {parts:?}");
    let Part::Result(answer) = &parts[result] else { unreachable!() };
    assert_eq!(&answer.payload[..], b"done");
    assert!(stream.fragments_lost(), "the connection changed while the request ran");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "run once");
    worker.shutdown().await;
}

/// 2.9 "요청자": losing the connection does not end a waiter. The requester
/// reconnects and the result, kept as a claimable event, is assigned to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_requester_that_reconnects_still_gets_its_result_once() {
    let engine = ruvent::Engine::volatile(EngineConfig::default()).await;
    let server = TcpServer::start(&engine).await;
    let relay = Relay::start(&server.address).await;
    let runs = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(Notify::new());
    let (handler_runs, handler_gate) = (runs.clone(), gate.clone());
    let worker = Worker::builder(tcp(), &Servers::new([&server.address]))
        .service(Service::new("orders").on("price", move |cx| {
            let (runs, gate) = (handler_runs.clone(), handler_gate.clone());
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                gate.notified().await;
                cx.reply(&b"kept"[..]);
                Ok(())
            }
        }))
        .start();
    let requester = Requester::start(tcp(), &Servers::new([&relay.address]), RequesterConfig::default());
    let waiting = {
        let requester = requester.clone();
        tokio::spawn(async move { requester.request(ask("x"), RequestOptions::default()).await })
    };
    eventually("the run started", || runs.load(Ordering::SeqCst) == 1).await;
    relay.cut(Cut::Both);
    eventually("the requester saw the cut", || requester.joined().is_empty()).await;
    gate.notify_one();
    let response = within("the response", waiting).await.unwrap().unwrap();
    assert_eq!(&response.result.payload[..], b"kept");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    worker.shutdown().await;
}

/// 2.9: the call deadline. With no worker the request times out; a retry
/// at the deadline resends the same id (the original swallows it), and a
/// late worker answers the one waiter once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_past_its_deadline_times_out_and_a_retry_keeps_one_waiter() {
    let (local, list, engines) = locals(1, EngineConfig::default()).await;
    let requester = Requester::start(local.clone(), &list, RequesterConfig::default());
    let quick = RequestOptions { deadline: Duration::from_millis(150), retries: 0 };
    assert_eq!(within("timeout", requester.request(ask("lost"), quick)).await, Err(RequestError::Timeout));
    let patient = RequestOptions { deadline: Duration::from_millis(300), retries: 3 };
    let waiting = {
        let requester = requester.clone();
        tokio::spawn(async move { requester.request(ask("late"), patient).await })
    };
    tokio::time::sleep(Duration::from_millis(400)).await;
    let runs = Arc::new(AtomicU64::new(0));
    let handler_runs = runs.clone();
    let worker = Worker::builder(local, &list)
        .service(Service::new("orders").on("price", move |cx| {
            let runs = handler_runs.clone();
            async move {
                if cx.payload()[..] == b"late"[..] {
                    runs.fetch_add(1, Ordering::SeqCst);
                }
                cx.reply(cx.payload().clone());
                Ok(())
            }
        }))
        .start();
    let response = within("the late answer", waiting).await.unwrap().unwrap();
    assert_eq!(&response.result.payload[..], b"late");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the resend under the same id ran nothing again");
    let _ = &engines;
    worker.shutdown().await;
}
