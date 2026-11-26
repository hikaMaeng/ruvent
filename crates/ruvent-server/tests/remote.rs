//! The event server over real TCP: the socket client for whole flows, raw
//! wire v2 connections where a test must send what a client never would
//! (retransmissions, gaps, unread sockets, version 1).

use ruvent::{
    Assignment, ByeReason, Bytes, Call, Channel, Client, Engine, EngineConfig, Envelope, EventId, FaultKind, Grant, Grants, Hello, Inbox, Incoming, MemoryConfig, Method, Methods, Name, Outbound,
    Refusal, Role, SegmentLog, Welcome,
};
use ruvent_client::remote::{connect, Options, Refused};
use ruvent_protocol::{features, Body, Decoder, Encoder, Message, RecordReader, Side, VERSION};
use ruvent_server::{serve, Credentials, ServerOptions};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOKEN: &str = "svc-token";
const CREDENTIALS: &[(&str, &str)] = &[("svc", TOKEN), ("reader", "reader-token")];
/// Generous: a broken test fails instead of hanging.
const WAIT: Duration = Duration::from_secs(10);
/// How long "nothing arrives" is watched for.
const QUIET: Duration = Duration::from_millis(150);
const LEASE: Duration = Duration::from_secs(60);

struct Server {
    address: String,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}

impl Server {
    async fn start(engine: &Engine, options: ServerOptions) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let credentials = Arc::new(Credentials::new(CREDENTIALS.iter().map(|(name, token)| (name.to_string(), token.to_string()))));
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(serve(listener, engine.clone(), credentials, options, async move {
            let _ = stopped.await;
        }));
        Server { address, stop: Some(stop), task }
    }

    /// Takes the server down in the order of 2.8 (the engine too) and waits
    /// until every connection closed.
    async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        tokio::time::timeout(WAIT, &mut self.task).await.expect("the server stops").unwrap().unwrap();
    }
}

async fn server(engine: &Engine) -> Server {
    Server::start(engine, ServerOptions::default()).await
}

async fn dial(server: &Server, hello: Hello) -> (Client, Inbox) {
    connect(&server.address, TOKEN, hello, Options::default()).await.unwrap()
}

async fn next(inbox: &mut Inbox) -> Incoming {
    tokio::time::timeout(WAIT, inbox.recv()).await.expect("nothing arrived").expect("inbox closed")
}

async fn assignment(inbox: &mut Inbox) -> Assignment {
    loop {
        match next(inbox).await {
            Incoming::Assign(assignment) => return assignment,
            Incoming::Pending(_) => continue,
            other => panic!("expected an assignment, got {other:?}"),
        }
    }
}

async fn delivery(inbox: &mut Inbox) -> Envelope {
    loop {
        match next(inbox).await {
            Incoming::Delivery(event) => return event,
            Incoming::Subscribed(_) => continue,
            other => panic!("expected a delivery, got {other:?}"),
        }
    }
}

fn scratch(name: &str) -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let directory = std::env::temp_dir().join(format!("ruvent-server-{name}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&directory);
    directory
}

async fn durable(path: &PathBuf, config: EngineConfig) -> Engine {
    let (journal, recovered) = SegmentLog::open(path).unwrap();
    Engine::start(config, journal, recovered).await.unwrap()
}

fn job(channel: &str, payload: &'static [u8]) -> Envelope {
    Envelope::claimable(ch(channel), 1, Bytes::from_static(payload)).with_id(EventId::new_v7())
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

fn raw_hello(role: Role, producer: [u8; 16], nonce: [u8; 16]) -> Hello {
    Hello { role, credential: Name::new(""), producer: (role != Role::Observe).then_some(producer), nonce, linger_ms: None, acks: true }
}

/// One wire v2 connection driven by hand.
struct Raw {
    reader: RawReader,
    writer: RawWriter,
}

struct RawReader {
    reader: RecordReader<OwnedReadHalf>,
    decoder: Decoder,
}

struct RawWriter {
    write: OwnedWriteHalf,
    encoder: Encoder,
}

impl RawReader {
    /// The next message; `None` once the server closed cleanly. A reset is an error.
    async fn recv(&mut self) -> io::Result<Option<Message>> {
        tokio::time::timeout(WAIT, async {
            loop {
                let Some(raw) = self.reader.head().await? else { return Ok(None) };
                let body = if self.decoder.admit(&raw)? && raw.body_len > 0 { Body::Bytes(self.reader.body(raw.body_len).await?) } else { Body::None };
                if let Some(message) = self.decoder.decode(&raw, body)? {
                    return Ok(Some(message));
                }
            }
        })
        .await
        .expect("the server said nothing")
    }

    async fn expect(&mut self) -> Message {
        self.recv().await.unwrap().expect("the server closed")
    }
}

impl RawWriter {
    async fn send(&mut self, messages: impl IntoIterator<Item = Message>) -> io::Result<()> {
        for message in messages {
            self.encoder.push(&message).unwrap();
        }
        self.encoder.take().write_to(&mut self.write).await
    }
}

impl Raw {
    async fn open(address: &str, token: &str, hello: Hello) -> (Raw, Message) {
        let stream = TcpStream::connect(address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (read, write) = stream.into_split();
        let mut raw = Raw { reader: RawReader { reader: RecordReader::new(read), decoder: Decoder::new(Side::Client) }, writer: RawWriter { write, encoder: Encoder::new() } };
        raw.writer.send([Message::Hello { version: VERSION, features: features::ALL, token: token.into(), hello }]).await.unwrap();
        let first = raw.reader.expect().await;
        (raw, first)
    }

    async fn welcomed(address: &str, hello: Hello) -> (Raw, Welcome) {
        match Raw::open(address, TOKEN, hello).await {
            (raw, Message::Welcome { welcome, .. }) => (raw, welcome),
            (_, other) => panic!("expected a welcome, got {other:?}"),
        }
    }

    async fn send(&mut self, messages: impl IntoIterator<Item = Message>) {
        self.writer.send(messages).await.unwrap();
    }

    async fn expect(&mut self) -> Message {
        self.reader.expect().await
    }
}

fn call(call: Call) -> Message {
    Message::Call(call)
}

fn register(seq: u64, event: Envelope) -> Message {
    call(Call::Register { seq: Some(seq), event })
}

async fn stats(server: &Server) -> ruvent::Stats {
    let (observer, _) = dial(server, Hello::observer("x")).await;
    observer.stats().await.unwrap()
}

/// Each role sends its own calls and gets its own traffic (2.2): a producer
/// registers and is acknowledged, a worker's window takes the work and its
/// completion's follow-up reaches an observer, and a call the role does not
/// send is refused.
#[tokio::test]
async fn each_role_does_its_own_work_over_tcp() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (producer, _produced) = dial(&server, Hello::producer("x")).await;
    let worker_hello = Hello::worker("x");
    let worker_uuid = worker_hello.producer.unwrap();
    let (worker, mut work) = dial(&server, worker_hello).await;
    let (observer, mut seen) = dial(&server, Hello::observer("x")).await;
    observer.subscribe(ch("feed"), None).await.unwrap();
    let worker_join = worker.join(Some(ch("orders")), Vec::new(), LEASE, false).await.unwrap();
    worker.window(worker_join, 4, 0).await.unwrap();

    let (seq, id) = producer.register(job("orders", b"p")).await.unwrap();
    producer.committed(seq).await.unwrap();
    let assigned = assignment(&mut work).await;
    assert_eq!((assigned.event.id, assigned.event.payload.as_ref()), (id, &b"p"[..]));
    let claim = assigned.event.claim.expect("an assignment carries its claim mark");
    assert_eq!((claim.worker, claim.token, claim.lease_ms), (worker_uuid, assigned.token(), LEASE.as_millis() as u64), "the claim mark names the worker's uuid");
    assert!(assigned.until > 0 && assigned.until <= LEASE.as_millis() as u64, "the lease left when it was written: {}", assigned.until);
    let done = worker.complete(id, assigned.token(), vec![Envelope::dispatch(ch("feed"), 2, Bytes::from_static(b"x"))]).await.unwrap();
    worker.committed(done).await.unwrap();
    let created = delivery(&mut seen).await;
    assert_eq!((created.event_type, created.payload.as_ref(), created.claim), (2, &b"x"[..], None));

    let (_, posted) = producer.register(Envelope::dispatch(ch("feed"), 3, Bytes::new()).with_id(EventId::new_v7())).await.unwrap();
    assert_eq!(delivery(&mut seen).await.id, posted);
    assert_eq!(producer.join(Some(ch("orders")), Vec::new(), LEASE, false).await, Err(Refusal::Malformed));
    let stats = observer.stats().await.unwrap();
    assert_eq!((stats.waiting, stats.claimed), (0, 0));
}

/// A wrong token, a production connection whose credential may not
/// register, and an observer asking for what its grant lacks are refused
/// with their reason (2.7). A grant is methods alone (plan 9.15): a
/// subscription hands the reader every type on the channel; a channel that is
/// not a UUIDv7 is refused as malformed.
#[tokio::test]
async fn a_wrong_token_and_a_forbidden_method_are_refused_with_their_reason() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant::all());
    grants.insert("reader".into(), Grant { methods: vec![Method::Subscribe, Method::Unsubscribe] });
    let engine = Engine::volatile(EngineConfig { grants: Some(grants), ..EngineConfig::default() }).await;
    let server = server(&engine).await;
    let error = connect(&server.address, "nope", Hello::producer("x"), Options::default()).await.err().unwrap();
    assert_eq!((error.kind(), Refused::of(&error)), (io::ErrorKind::PermissionDenied, Some(ByeReason::Token)));
    let error = connect(&server.address, "reader-token", Hello::producer("x"), Options::default()).await.err().unwrap();
    assert_eq!(Refused::of(&error), Some(ByeReason::Forbidden), "it may read, not register");
    let (reader, mut inbox) = connect(&server.address, "reader-token", Hello::observer("x"), Options::default()).await.unwrap();
    assert_eq!(reader.subscribe(Channel([0; 16]), None).await, Err(Refusal::Malformed));
    assert!(reader.subscribe(ch("pub.news"), None).await.is_ok());
    let (producer, _) = dial(&server, Hello::producer("x")).await;
    for event_type in [5, 150] {
        producer.register_committed(Envelope::dispatch(ch("pub.news"), event_type, Bytes::new()).with_id(EventId::new_v7())).await.unwrap();
    }
    assert_eq!((delivery(&mut inbox).await.event_type, delivery(&mut inbox).await.event_type), (5, 150), "every type on the channel");
    assert_eq!(reader.stats().await, Err(Refusal::Forbidden));
    assert_eq!(reader.drain().await, Err(Refusal::Forbidden));
}

/// A sequence number at or below the last applied one is a retransmission:
/// dropped without a word (2.3).
#[tokio::test]
async fn a_retransmission_is_dropped_quietly() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (mut raw, welcome) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [1; 16], [2; 16])).await;
    assert_eq!(welcome.resume, 0);
    raw.send([register(1, job("work", b"a")), register(2, job("work", b"b")), register(1, job("work", b"a")), register(2, job("work", b"b")), register(3, job("work", b"c"))]).await;
    loop {
        match raw.expect().await {
            Message::Outbound(Outbound::Committed { seq: 3 }) => break,
            Message::Outbound(Outbound::Committed { .. }) => {}
            other => panic!("nothing but acknowledgements, got {other:?}"),
        }
    }
    let stats = stats(&server).await;
    assert_eq!((stats.waiting, stats.counters.retransmits, stats.counters.faults), (3, 2, 0));
}

/// Registrations are not answered one by one: `Committed` is cumulative (2.3).
#[tokio::test]
async fn acknowledgements_are_cumulative_not_one_per_registration() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (mut raw, _) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [3; 16], [4; 16])).await;
    raw.send((1..=2000).map(|seq| register(seq, Envelope::dispatch(ch("feed"), 4, Bytes::new())))).await;
    let mut acknowledgements = 0;
    loop {
        if let Message::Outbound(Outbound::Committed { seq }) = raw.expect().await {
            acknowledgements += 1;
            if seq == 2000 {
                break;
            }
        }
    }
    assert!(acknowledgements < 2000 / 4, "{acknowledgements} acknowledgements for 2000 registrations");
}

/// A gap in the sequence is a protocol error: told once, then `Bye{protocol}`
/// and a close (2.3, 2.7).
#[tokio::test]
async fn a_sequence_gap_is_told_once_and_closes_the_connection() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (mut raw, _) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [5; 16], [6; 16])).await;
    raw.send([register(1, job("work", b"a")), register(3, job("work", b"c")), register(4, job("work", b"d"))]).await;
    let mut seen = Vec::new();
    while let Some(message) = raw.reader.recv().await.unwrap() {
        seen.push(message);
    }
    let faults: Vec<&Message> = seen.iter().filter(|message| matches!(message, Message::Outbound(Outbound::Fault { .. }))).collect();
    assert_eq!(faults, [&Message::Outbound(Outbound::Fault { kind: FaultKind::SequenceGap, detail: "sequence number skipped ahead" })]);
    assert_eq!(seen.last(), Some(&Message::Outbound(Outbound::Bye(ByeReason::Protocol))));
    assert_eq!(stats(&server).await.waiting, 1);
}

/// The same producer id with the same nonce is the same process
/// reconnecting: the old connection is replaced quietly and the new one
/// resumes. Another nonce pushes the old one out with `Bye{superseded}` and
/// says so in `Welcome` (2.3, M8-7).
#[tokio::test]
async fn the_same_nonce_reconnects_quietly_and_another_nonce_supersedes() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (mut first, _) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [7; 16], [1; 16])).await;
    first.send([register(1, job("work", b"a"))]).await;
    assert_eq!(first.expect().await, Message::Outbound(Outbound::Committed { seq: 1 }));
    let (mut second, welcome) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [7; 16], [1; 16])).await;
    assert_eq!((welcome.resume, welcome.superseded), (1, false));
    assert_eq!(first.reader.recv().await.unwrap(), None, "replaced without a word");
    let (_third, welcome) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [7; 16], [2; 16])).await;
    assert_eq!((welcome.resume, welcome.superseded), (1, true));
    assert_eq!(second.expect().await, Message::Outbound(Outbound::Bye(ByeReason::Superseded)));
    assert_eq!(second.reader.recv().await.unwrap(), None);
}

/// A window takes work up to its size; the assignment that fills it while
/// work waits says `more`; a completion opens the slot for the next one (2.4).
#[tokio::test]
async fn a_window_takes_work_up_to_its_size_and_a_completion_frees_a_slot() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (producer, _) = dial(&server, Hello::producer("x")).await;
    for payload in [&b"1"[..], b"2", b"3"] {
        producer.register_committed(Envelope::claimable(ch("orders"), 1, Bytes::from_static(payload))).await.unwrap();
    }
    let (worker, mut work) = dial(&server, Hello::worker("x")).await;
    let worker_join = worker.join(Some(ch("orders")), Vec::new(), LEASE, false).await.unwrap();
    worker.window(worker_join, 2, 0).await.unwrap();
    let first = assignment(&mut work).await;
    let second = assignment(&mut work).await;
    assert!(second.more, "the window is full and one more waits");
    assert!(tokio::time::timeout(QUIET, assignment(&mut work)).await.is_err(), "no more than the window");
    let seq = worker.complete(first.event.id, first.token(), Vec::new()).await.unwrap();
    let third = assignment(&mut work).await;
    worker.committed(seq).await.unwrap();
    let mut payloads = vec![first.event.payload, second.event.payload, third.event.payload];
    payloads.sort();
    assert_eq!(payloads, [Bytes::from_static(b"1"), Bytes::from_static(b"2"), Bytes::from_static(b"3")]);
}

/// A large body is read straight into its own buffer and arrives whole, both
/// ways (2.2).
#[tokio::test]
async fn a_large_body_arrives_whole() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let body = Bytes::from((0..8u32 << 20).map(|n| (n % 251) as u8).collect::<Vec<u8>>());
    let (producer, _) = dial(&server, Hello::producer("x")).await;
    let (worker, mut work) = dial(&server, Hello::worker("x")).await;
    let (observer, mut seen) = dial(&server, Hello::observer("x")).await;
    observer.subscribe(ch("feed"), None).await.unwrap();
    let worker_join = worker.join(Some(ch("big")), Vec::new(), LEASE, false).await.unwrap();
    worker.window(worker_join, 1, 0).await.unwrap();
    producer.register_committed(Envelope::claimable(ch("big"), 1, body.clone())).await.unwrap();
    let assigned = assignment(&mut work).await;
    assert_eq!(assigned.event.payload, body);
    worker.complete(assigned.event.id, assigned.token(), vec![Envelope::dispatch(ch("feed"), 5, body.clone())]).await.unwrap();
    assert_eq!(delivery(&mut seen).await.payload, body);
    // Over the bound, the client refuses it before it leaves (B13).
    let over = Bytes::from(vec![0u8; ruvent_protocol::MAX_BODY + 1]);
    assert_eq!(producer.register(Envelope::dispatch(ch("feed"), 6, over)).await.map(|_| ()), Err(Refusal::Closed));
    producer.register_committed(Envelope::dispatch(ch("feed"), 7, Bytes::new())).await.unwrap();
}

/// Under memory pressure a work connection does not stop reading: a
/// follow-up's body is streamed to disk, and the event survives a restart
/// pointing at its body (2.6 item 4). Nothing pauses the producer meanwhile.
#[tokio::test]
async fn a_work_connection_streams_follow_up_bodies_to_disk_under_pressure() {
    let path = scratch("spool");
    let durable_body = Bytes::from(vec![7u8; 20_000]);
    {
        let memory = MemoryConfig { limit: Some(10_000), slot_bytes: 100, ..MemoryConfig::default() };
        let engine = durable(&path, EngineConfig { memory, ..EngineConfig::default() }).await;
        let server = server(&engine).await;
        let (producer, _) = dial(&server, Hello::producer("x")).await;
        let (worker, mut work) = dial(&server, Hello::worker("x")).await;
        producer.register_committed(job("work", b"p")).await.unwrap();
        let worker_join = worker.join(Some(ch("work")), Vec::new(), LEASE, false).await.unwrap();
        worker.window(worker_join, 1, 0).await.unwrap();
        let assigned = assignment(&mut work).await;
        for _ in 0..11 {
            producer.register_committed(Envelope::claimable(ch("filler"), 2, Bytes::from(vec![0u8; 1000]))).await.unwrap();
        }
        assert!(engine.body_to_disk(durable_body.len() as u64), "{:?}", engine.pressure());
        let follow_ups = vec![Envelope::claimable(ch("next"), 1, durable_body.clone())];
        let seq = worker.complete(assigned.event.id, assigned.token(), follow_ups).await.unwrap();
        worker.committed(seq).await.unwrap();
        assert_eq!(stats(&server).await.waiting, 12, "every registration was taken: nothing paused the producer");
        drop((producer, worker));
        server.stop().await;
    }
    let engine = durable(&path, EngineConfig::default()).await;
    let server = server(&engine).await;
    let (worker, mut work) = dial(&server, Hello::worker("x")).await;
    let worker_join = worker.join(Some(ch("next")), Vec::new(), LEASE, false).await.unwrap();
    worker.window(worker_join, 1, 0).await.unwrap();
    assert_eq!(assignment(&mut work).await.event.payload, durable_body);
}

/// An observation connection that does not read is cut off at its byte
/// bound with `Bye{slow}`; what it already had still arrives before it (S7, 2.6).
#[tokio::test]
async fn an_observer_that_does_not_read_is_sent_bye_slow() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = Server::start(&engine, ServerOptions { unsent: 1 << 20, ..ServerOptions::default() }).await;
    let (mut observer, _) = Raw::welcomed(&server.address, raw_hello(Role::Observe, [0; 16], [8; 16])).await;
    observer.send([call(Call::Subscribe { req: 1, channel: ch("feed"), after: None })]).await;
    assert!(matches!(observer.expect().await, Message::Outbound(Outbound::Subscribed { req: 1, .. })));
    let (producer, _) = dial(&server, Hello::producer("x")).await;
    let payload = Bytes::from(vec![1u8; 1 << 20]);
    let sent = 64;
    let mut last = 0;
    for _ in 0..sent {
        last = producer.register(Envelope::dispatch(ch("feed"), 8, payload.clone())).await.unwrap().0;
    }
    producer.committed(last).await.unwrap();
    let mut deliveries = 0;
    let bye = loop {
        match observer.reader.recv().await.unwrap() {
            Some(Message::Outbound(Outbound::Delivery(_))) => deliveries += 1,
            Some(Message::Outbound(Outbound::Bye(reason))) => break reason,
            other => panic!("expected deliveries then bye, got {other:?}"),
        }
    };
    assert_eq!(bye, ByeReason::Slow);
    assert!(deliveries < sent, "{deliveries} of {sent} delivered");
    assert_eq!(observer.reader.recv().await.unwrap(), None, "closed after the bye");
}

/// Draining while producers send: every production connection gets its
/// final `Committed`, then `Bye{draining}`, and what they were told is
/// exactly what the server holds; a new production connection is sent away
/// (2.8, B22, M8-5).
#[tokio::test]
async fn draining_while_producers_send_misses_no_final_acknowledgement() {
    let path = scratch("drain");
    let engine = durable(&path, EngineConfig { durability: ruvent::Durability::Strict, ..EngineConfig::default() }).await;
    let server = server(&engine).await;
    let mut producers = Vec::new();
    for index in 0..4u8 {
        let (raw, _) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [index + 10; 16], [index; 16])).await;
        let Raw { mut reader, mut writer } = raw;
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let sending = tokio::spawn(async move {
            let mut seq = 0;
            while !stopped.load(Ordering::Relaxed) {
                let batch: Vec<Message> = (0..20)
                    .map(|_| {
                        seq += 1;
                        register(seq, Envelope::claimable(ch("work"), 1, Bytes::from_static(b"p")))
                    })
                    .collect();
                if writer.send(batch).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            writer
        });
        producers.push(tokio::spawn(async move {
            let mut acknowledged = 0;
            let bye = loop {
                match reader.recv().await.unwrap() {
                    Some(Message::Outbound(Outbound::Committed { seq })) => acknowledged = acknowledged.max(seq),
                    Some(Message::Outbound(Outbound::Bye(reason))) => break reason,
                    other => panic!("expected acknowledgements then bye, got {other:?}"),
                }
            };
            let end = reader.recv().await.unwrap();
            stop.store(true, Ordering::Relaxed);
            drop(sending.await.unwrap());
            (acknowledged, bye, end)
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (operator, _) = dial(&server, Hello::observer("x")).await;
    operator.drain().await.unwrap();
    let mut told = 0;
    for producer in producers {
        let (acknowledged, bye, end) = producer.await.unwrap();
        assert_eq!((bye, end), (ByeReason::Draining, None), "final acknowledgement, bye, then a clean close");
        assert!(acknowledged > 0);
        told += acknowledged;
    }
    let error = connect(&server.address, TOKEN, Hello::producer("x"), Options::default()).await.err().unwrap();
    assert_eq!(Refused::of(&error), Some(ByeReason::Draining));
    let stats = operator.stats().await.unwrap();
    assert!(stats.draining);
    assert_eq!(stats.waiting, told, "every applied registration was acknowledged, and nothing more");
}

/// A claim survives a graceful restart: the worker comes back with the same
/// token, extends and completes it, and nothing was reassigned or counted
/// as a failure (2.4, M8-6, raw connections). A peer that never closes after
/// its `Bye` holds the shutdown up to the close deadline, far past the
/// reconnect grace, and still cannot make the claim run out meanwhile.
#[tokio::test]
async fn a_claim_survives_a_graceful_restart_and_completes_under_its_token() {
    let path = scratch("restart");
    let (id, token, generation) = {
        let engine = durable(&path, EngineConfig { reconnect_grace: Duration::from_millis(100), ..EngineConfig::default() }).await;
        let server = Server::start(&engine, ServerOptions { close: Duration::from_secs(1), ..ServerOptions::default() }).await;
        let (_lingering, _) = Raw::welcomed(&server.address, raw_hello(Role::Observe, [0; 16], [23; 16])).await;
        let (producer, _) = dial(&server, Hello::producer("x")).await;
        let (_, id) = producer.register_committed(job("work", b"p")).await.unwrap();
        let (mut worker, welcome) = Raw::welcomed(&server.address, raw_hello(Role::Work, [21; 16], [22; 16])).await;
        worker.send([call(Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), lease_ms: 30_000, dispatch: false }), call(Call::Window { join: 1, count: 1, bytes: 0 })]).await;
        assert_eq!(worker.expect().await, Message::Outbound(Outbound::Joined { req: 1 }));
        let token = match worker.expect().await {
            Message::Outbound(Outbound::Assign(assignment)) => assignment.token(),
            other => panic!("expected an assignment, got {other:?}"),
        };
        drop(producer);
        let stopping = tokio::spawn(server.stop());
        assert_eq!(worker.expect().await, Message::Outbound(Outbound::Bye(ByeReason::Draining)));
        assert_eq!(worker.reader.recv().await.unwrap(), None);
        drop(worker);
        let started = Instant::now();
        stopping.await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(500), "the lingering peer held the shutdown to the deadline");
        assert!(engine.stopped().await.is_ok(), "the server shut the engine down");
        (id, token, welcome.generation)
    };
    let engine = durable(&path, EngineConfig::default()).await;
    let server = server(&engine).await;
    let (mut worker, welcome) = Raw::welcomed(&server.address, raw_hello(Role::Work, [21; 16], [22; 16])).await;
    assert!(welcome.generation > generation);
    worker
        .send([
            call(Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), lease_ms: 30_000, dispatch: false }),
            call(Call::Window { join: 1, count: 1, bytes: 0 }),
            call(Call::Extend { id, token }),
            call(Call::Complete { seq: 1, id, token, follow_ups: Vec::new(), spooled: Vec::new() }),
        ])
        .await;
    assert_eq!(worker.expect().await, Message::Outbound(Outbound::Joined { req: 1 }));
    assert_eq!(worker.expect().await, Message::Outbound(Outbound::Committed { seq: 1 }), "accepted: no assignment came first");
    let stats = stats(&server).await;
    assert_eq!((stats.waiting, stats.claimed, stats.counters.reclaimed, stats.counters.stale), (0, 0, 0, 0));
}

/// Every `Bye` is followed by a staged close: the peer reads the `Bye` and a
/// clean end even though it had sent far more than the server read, and the
/// server keeps reading (and dropping) until the peer closes (2.2).
#[tokio::test]
async fn a_bye_is_read_whole_even_when_the_peer_had_sent_more_than_the_server_read() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (raw, _) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [31; 16], [32; 16])).await;
    let Raw { mut reader, mut writer } = raw;
    let pumping = tokio::spawn(async move {
        let filler = Bytes::from(vec![0u8; 64 << 10]);
        let mut messages = vec![register(1, job("work", b"a")), register(3, job("work", b"gap"))];
        messages.extend((4..68).map(|seq| register(seq, Envelope::dispatch(ch("feed"), 9, filler.clone()))));
        writer.send(messages).await.unwrap();
        writer
    });
    let mut seen = Vec::new();
    while let Some(message) = reader.recv().await.expect("a clean close, never a reset") {
        seen.push(message);
    }
    assert_eq!(seen.last(), Some(&Message::Outbound(Outbound::Bye(ByeReason::Protocol))));
    let mut writer = pumping.await.unwrap();
    writer.write.write_all(&[0u8; 4096]).await.expect("the server is still reading after its bye");
}

/// A peer that never closes after `Bye` is closed at the deadline (S20).
#[tokio::test]
async fn a_peer_that_never_closes_after_bye_is_closed_at_the_deadline() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let deadline = Duration::from_millis(300);
    let server = Server::start(&engine, ServerOptions { close: deadline, ..ServerOptions::default() }).await;
    let (mut raw, _) = Raw::welcomed(&server.address, raw_hello(Role::Produce, [33; 16], [34; 16])).await;
    raw.send([register(2, job("work", b"gap"))]).await;
    while raw.reader.recv().await.unwrap().is_some() {}
    let ended = Instant::now();
    let closed = loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if raw.writer.write.write_all(&[0u8; 1024]).await.is_err() {
            break ended.elapsed();
        }
        assert!(ended.elapsed() < WAIT, "the server never closed");
    };
    assert!(closed >= deadline / 2, "closed after {closed:?}, the deadline is {deadline:?}");
}

/// Version 1 is refused (B12): a version-1 hello gets a version-1 `Bye` it
/// can read, a version-2 hello with another version gets `Bye{version}`.
#[tokio::test]
async fn version_1_and_other_versions_are_refused() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let mut old = TcpStream::connect(&server.address).await.unwrap();
    old.write_all(&[11, 0, 0, 0, 1, 0, 0, 0, 0x01, 1, 0, 2, 0, b'a', b'b']).await.unwrap();
    let mut answer = Vec::new();
    tokio::time::timeout(WAIT, old.read_to_end(&mut answer)).await.unwrap().unwrap();
    assert_eq!(&answer[4..9], &[1, 0, 0, 0, 0x03], "one record: bye");
    assert!(String::from_utf8_lossy(&answer).contains("unsupported protocol version 1"));

    let mut encoder = Encoder::new();
    encoder.push(&Message::Hello { version: VERSION, features: 0, token: TOKEN.into(), hello: Hello::producer("x") }).unwrap();
    let mut bytes = encoder.take().concat();
    // Version 3 (the type as a name number) is another version now.
    bytes[7] = 3;
    let stream = TcpStream::connect(&server.address).await.unwrap();
    let (read, mut write) = stream.into_split();
    write.write_all(&bytes).await.unwrap();
    let mut reader = RawReader { reader: RecordReader::new(read), decoder: Decoder::new(Side::Client) };
    assert_eq!(reader.expect().await, Message::Outbound(Outbound::Bye(ByeReason::Version)));
    assert_eq!(reader.recv().await.unwrap(), None);
}

/// Assignments the server never finished writing go back to waiting at once
/// when the worker goes, with their failure count unchanged; the written ones
/// wait for the reconnect grace (2.4, M8-2).
#[tokio::test]
async fn assignments_never_written_go_back_at_once_when_the_worker_goes() {
    let engine = Engine::volatile(EngineConfig { reconnect_grace: Duration::from_secs(60), ..EngineConfig::default() }).await;
    let server = server(&engine).await;
    let (producer, _) = dial(&server, Hello::producer("x")).await;
    let payload = Bytes::from(vec![3u8; 1 << 20]);
    let total = 48;
    for _ in 0..total {
        producer.register_committed(Envelope::claimable(ch("big"), 1, payload.clone())).await.unwrap();
    }
    let (mut stalled, _) = Raw::welcomed(&server.address, raw_hello(Role::Work, [41; 16], [42; 16])).await;
    stalled.send([call(Call::Join { req: 1, channel: Some(ch("big")), types: Vec::new(), lease_ms: 600_000, dispatch: false }), call(Call::Window { join: 1, count: total, bytes: 0 })]).await;
    loop {
        let stats = stats(&server).await;
        if stats.claimed == u64::from(total) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(stalled);
    let (worker, mut work) = dial(&server, Hello::worker("x")).await;
    let worker_join = worker.join(Some(ch("big")), Vec::new(), LEASE, false).await.unwrap();
    worker.window(worker_join, total, 0).await.unwrap();
    let mut returned = 0;
    while let Ok(incoming) = tokio::time::timeout(Duration::from_secs(1), work.recv()).await {
        if let Some(Incoming::Assign(assignment)) = incoming {
            assert_eq!(assignment.event.failures, 0);
            returned += 1;
        }
    }
    assert!(returned > 0, "the unwritten ones came back without waiting for the grace");
    assert!(returned < total, "the written ones wait for the grace: {returned} of {total}");
}

/// A backlog larger than one answer is read to the end by subscribing again after the last key.
#[tokio::test]
async fn a_backlog_larger_than_one_page_is_read_to_the_end_by_subscribing_again() {
    let engine = Engine::volatile(EngineConfig { backlog_page: 3, ..EngineConfig::default() }).await;
    let server = server(&engine).await;
    let (producer, _) = dial(&server, Hello::producer("x")).await;
    for _ in 0..7 {
        producer.register_committed(job("orders", b"")).await.unwrap();
    }
    let (observer, _) = dial(&server, Hello::observer("x")).await;
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = observer.subscribe(ch("orders"), after).await.unwrap();
        seen.extend(page.notices.iter().map(|notice| notice.key));
        after = page.last();
        if !page.more {
            break;
        }
    }
    assert_eq!(seen.len(), 7);
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "in work-key order");
}

/// When the server lets go of a connection the inbox closes and later calls
/// fail with `Closed` instead of hanging.
#[tokio::test]
async fn when_the_server_goes_away_the_inbox_closes_and_calls_fail_closed() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let server = server(&engine).await;
    let (observer, mut inbox) = dial(&server, Hello::observer("x")).await;
    engine.shutdown().await.unwrap();
    loop {
        match next(&mut inbox).await {
            Incoming::Closed => break,
            Incoming::Bye(_) => {}
            other => panic!("expected the end, got {other:?}"),
        }
    }
    assert_eq!(observer.stats().await, Err(Refusal::Closed));
    drop(server);
}

/// Every socket the server accepts gets TCP keepalive and no delay; on Linux
/// also `TCP_USER_TIMEOUT` (2.2, B16, S13).
#[tokio::test]
async fn a_tuned_socket_has_keepalive_and_no_delay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    ruvent_server::connection::socket::tune(&stream, &ruvent_server::Keepalive::default()).unwrap();
    let socket = socket2::SockRef::from(&stream);
    assert!(socket.keepalive().unwrap());
    assert!(stream.nodelay().unwrap());
    #[cfg(target_os = "linux")]
    assert_eq!(socket.tcp_user_timeout().unwrap(), Some(Duration::from_secs(30)));
}
