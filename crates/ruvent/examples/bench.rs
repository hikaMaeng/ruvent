//! Throughput of the lifecycle through the in-process client, and the cost of
//! one envelope.
//!
//! ```text
//! cargo run --release -p ruvent --example bench
//! ```
//!
//! What is measured, exactly:
//!
//! * `cycles/s` — completed lifecycles per second: register a claimable event,
//!   claim it, complete it, each call waiting for its answer, from `workers`
//!   concurrent tasks. The numerator counts completions that returned `Ok`.
//! * `encode ns` — nanoseconds to encode one envelope body in the given
//!   layout (names already numbered), averaged over a loop. No framing.
//! * `bytes` — the encoded size of that body with a 16-byte payload.

#![allow(clippy::print_stdout)]

use ruvent::envelope::codec::{self, Layout};
use ruvent::{connect, Bytes, Claim, ClaimToken, Durability, Engine, EngineConfig, Envelope, EventId, FileJournal, Methods, WorkKey};
use std::time::{Duration, Instant};

const WORKERS: usize = 64;

async fn lifecycles(engine: &Engine, total: usize) -> (usize, Duration) {
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..WORKERS {
        let (client, _) = connect(engine, "bench", 1024).await.expect("connect");
        let share = total / WORKERS;
        tasks.push(tokio::spawn(async move {
            let mut done = 0;
            for _ in 0..share {
                let (id, _) = client.register_claimable(Envelope::claimable("work", "do", Bytes::from_static(&[0; 16]))).await.expect("register");
                let claimed = client.claim(id, 1, Duration::from_secs(60)).await.expect("claim");
                if client.complete(id, claimed.claim.expect("claimed").token, Vec::new()).await.is_ok() {
                    done += 1;
                }
            }
            done
        }));
    }
    let mut done = 0;
    for task in tasks {
        done += task.await.expect("task");
    }
    (done, started.elapsed())
}

fn encode_cost(event: &Envelope, layout: Layout) -> (usize, f64) {
    let rounds = 200_000;
    let mut out = Vec::with_capacity(256);
    let started = Instant::now();
    for _ in 0..rounds {
        out.clear();
        codec::encode(event, layout, &mut |_| 1, &mut out).expect("encodes");
    }
    (out.len(), started.elapsed().as_nanos() as f64 / f64::from(rounds))
}

#[tokio::main]
async fn main() {
    let directory = std::env::temp_dir().join(format!("ruvent-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("scratch");

    println!("workers={WORKERS} payload=16B");
    let volatile = Engine::volatile(EngineConfig::default()).await;
    let (done, took) = lifecycles(&volatile, 200_000).await;
    println!("mode=volatile cycles={done} seconds={:.3} cycles/s={:.0}", took.as_secs_f64(), done as f64 / took.as_secs_f64());
    volatile.shutdown().await.expect("shutdown");

    for (name, durability, total) in [("file-scheduled", Durability::Scheduled, 200_000), ("file-strict", Durability::Strict, 20_000)] {
        let (journal, recovered) = FileJournal::open(directory.join(format!("{name}.journal"))).expect("journal");
        let engine = Engine::start(EngineConfig { durability, ..EngineConfig::default() }, journal, recovered).await.expect("start");
        let (done, took) = lifecycles(&engine, total).await;
        println!("mode={name} cycles={done} seconds={:.3} cycles/s={:.0}", took.as_secs_f64(), done as f64 / took.as_secs_f64());
        engine.shutdown().await.expect("shutdown");
    }

    let key = WorkKey { generation: 1, arrival: 1 };
    let claim = Claim { token: ClaimToken { generation: 1, seq: 1 }, claimer: 1, worker: 1, deadline: 30_000 };
    let payload = Bytes::from_static(&[0; 16]);
    let dispatch = Envelope::dispatch("feed", "news", payload.clone()).with_id(EventId::new_v7());
    let waiting = Envelope { key: Some(key), ..Envelope::claimable("work", "do", payload.clone()).with_id(EventId::new_v7()) };
    let claimed = Envelope { claim: Some(claim), ..waiting.clone() };
    for (label, event, layout) in
        [("dispatch/delivery", &dispatch, Layout::Delivery), ("waiting/journal", &waiting, Layout::Journal), ("claimed/claim-answer", &claimed, Layout::Claimed), ("notice", &waiting, Layout::Notice)]
    {
        let (bytes, nanos) = encode_cost(event, layout);
        println!("envelope={label} bytes={bytes} encode_ns={nanos:.1}");
    }
    let started = Instant::now();
    for _ in 0..1_000_000 {
        std::hint::black_box(EventId::new_v7());
    }
    println!("uuidv7 ns={:.1}", started.elapsed().as_nanos() as f64 / 1_000_000.0);
    let _ = std::fs::remove_dir_all(&directory);
}
