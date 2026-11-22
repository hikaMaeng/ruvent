//! Two measurements.
//!
//! **round trip** — one command through the engine by hand:
//!
//!   submit -> dequeue -> begin (record command + take ownership) -> finish (result event + delete)
//!
//! Four engine operations; what the TypeScript original spends about nineteen
//! PostgreSQL round trips on.
//!
//! **chain** — the framework doing it: a client command handled by one
//! service, whose fact is reacted to by a second, whose fact is reacted to by
//! a third. Three executions, three facts, three results and one command
//! event per submission, through the real worker runtime.
//!
//!   cargo run --release --example bench -- [commands] [concurrency]

// A measurement program reports on the terminal; the library never does.
#![allow(clippy::print_stdout)]

use ruvent::{App, Audience, Begin, Bytes, Command, Context, Durability, Engine, EngineConfig, Envelope, Failure, From, Identity, Outcome, Proposal, Service};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn scratch(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("ruvent-bench-{}-{name}.journal", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn report(label: &str, what: &str, done: usize, concurrency: usize, applied: Duration, durable: Duration, engine: &Engine) {
    let stats = engine.stats();
    println!(
        "{label:<34} {what}={done:>8} concurrency={concurrency:>4} done_in={:>8.1}ms per_sec={:>9.0} on_disk_in={:>8.1}ms events={} journal_records={} journal_writes={} compactions={} engine_wakeups={}",
        applied.as_secs_f64() * 1e3,
        done as f64 / applied.as_secs_f64(),
        durable.as_secs_f64() * 1e3,
        stats.events,
        stats.journal_records,
        stats.journal_writes,
        stats.compactions,
        stats.wakeups,
    );
}

async fn round_trip(label: &str, engine: Engine, commands: usize, concurrency: usize) {
    let payload = Bytes::from(vec![b'x'; 256]);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for task in 0..concurrency {
        let (engine, payload) = (engine.clone(), payload.clone());
        tasks.push(tokio::spawn(async move {
            let (queue, reply): (ruvent::Text, ruvent::Text) = ("q".into(), format!("reply-{task}").into());
            for index in 0..commands / concurrency {
                let command = Command::new("job.run", "requests", payload.clone()).transaction_key(format!("t{task}-{index}")).reply_to(reply.clone());
                engine.submit(queue.clone(), command).await.unwrap();
                let delivery = engine.dequeue(queue.clone(), 1, None, None).await.unwrap().remove(0);
                let Begin::Claimed { command, attempt, .. } = engine.begin(queue.clone(), delivery.id).await.unwrap() else { panic!("unclaimed") };
                engine.finish(command, attempt, Outcome::Ok(payload.clone()), queue.clone(), delivery.id).await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let applied = started.elapsed();
    engine.flush().await.unwrap();
    report(label, "round_trips", commands / concurrency * concurrency, concurrency, applied, started.elapsed(), &engine);
    engine.shutdown().await.unwrap();
}

async fn first(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("first:{}", command.transaction_key), "first.done", command.payload.clone(), Audience::Worker).await?;
    Ok(Bytes::new())
}

async fn second(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("second:{}", command.correlation_id), "second.done", command.payload.clone(), Audience::Worker).await?;
    Ok(Bytes::new())
}

async fn third(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("third:{}", command.correlation_id), "chain.done", command.payload.clone(), Audience::Client).await?;
    Ok(Bytes::new())
}

async fn chain(label: &str, app: App, commands: usize, concurrency: usize) {
    let app = app
        .service(Service::new("first").accept("chain.run", first).max_in_flight(concurrency))
        .service(Service::new("second").on("first.done", second).max_in_flight(concurrency))
        .service(Service::new("third").on("second.done", third).max_in_flight(concurrency))
        .start()
        .await
        .unwrap();
    let mut done = app.engine().subscribe(vec!["done".to_owned()], From::Now);
    let payload = Bytes::from(vec![b'x'; 256]);
    let identity = Identity { user: "bench".to_owned() };

    let started = Instant::now();
    let mut tasks = Vec::new();
    for task in 0..concurrency {
        let (gateway, payload, identity) = (app.gateway().clone(), payload.clone(), identity.clone());
        tasks.push(tokio::spawn(async move {
            for index in 0..commands / concurrency {
                let proposal = Proposal {
                    action: "chain.run".into(),
                    payload: payload.clone(),
                    transaction_key: Some(format!("c{task}-{index}").into()),
                    reply_channel: Some("done".into()),
                    ..Proposal::default()
                };
                gateway.submit(&identity, proposal).await.unwrap();
            }
        }));
    }
    let total = commands / concurrency * concurrency;
    let mut finished = 0;
    while finished < total {
        finished += done.next(4096).await.iter().filter(|event| &*event.action == "chain.done").count();
    }
    for task in tasks {
        task.await.unwrap();
    }
    let applied = started.elapsed();
    app.engine().flush().await.unwrap();
    report(label, "chains", total, concurrency, applied, started.elapsed(), app.engine());
    app.shutdown().await.unwrap();
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let commands: usize = args.next().and_then(|value| value.parse().ok()).unwrap_or(200_000);
    let concurrency: usize = args.next().and_then(|value| value.parse().ok()).unwrap_or(256);
    let strict = || EngineConfig { durability: Durability::Strict, ..EngineConfig::default() };

    round_trip("round trip, no journal", Engine::volatile(EngineConfig::default()), commands, concurrency).await;
    round_trip("round trip, file, window 100ms", Engine::open(scratch("rt-scheduled"), EngineConfig::default()).await.unwrap(), commands, concurrency).await;
    round_trip("round trip, file, strict", Engine::open(scratch("rt-strict"), strict()).await.unwrap(), commands / 10, concurrency).await;

    chain("chain of 3, no journal", App::volatile(), commands, concurrency).await;
    chain("chain of 3, file, window 100ms", App::open(scratch("chain-scheduled")), commands, concurrency).await;
    chain("chain of 3, file, strict", App::open(scratch("chain-strict")).durability(Durability::Strict), commands / 10, concurrency).await;
}
