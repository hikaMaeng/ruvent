//! Journals that can be held, failed or watched, and small helpers.

use ruvent::{async_trait, connect, Batch, Bytes, Client, Engine, Envelope, Hello, Inbox, Incoming, Journal};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

/// Writes only when a permit is released, and remembers what it wrote.
#[derive(Clone)]
pub struct Gate {
    pub written: Arc<Mutex<Vec<Batch>>>,
    pub permits: Arc<Semaphore>,
}

impl Gate {
    /// One permit is there for the startup write.
    pub fn new() -> Self {
        Gate { written: Arc::default(), permits: Arc::new(Semaphore::new(1)) }
    }

    pub fn release(&self, writes: usize) {
        self.permits.add_permits(writes);
    }
}

#[async_trait]
impl Journal for Gate {
    async fn append(&mut self, batch: Batch) -> io::Result<()> {
        self.permits.acquire().await.map_err(io::Error::other)?.forget();
        self.written.lock().unwrap().push(batch);
        Ok(())
    }

    async fn rewrite(&mut self, batch: Batch) -> io::Result<()> {
        self.append(batch).await
    }
}

/// Fails every write after the first `good`.
pub struct Failing {
    pub good: u32,
}

#[async_trait]
impl Journal for Failing {
    async fn append(&mut self, _batch: Batch) -> io::Result<()> {
        if self.good == 0 {
            return Err(io::Error::other("disk on fire"));
        }
        self.good -= 1;
        Ok(())
    }

    async fn rewrite(&mut self, batch: Batch) -> io::Result<()> {
        self.append(batch).await
    }
}

pub fn scratch(name: &str) -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let directory = std::env::temp_dir().join(format!("ruvent-runtime-{name}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

pub fn work(channel: &str) -> Envelope {
    Envelope::claimable(channel, "do", Bytes::from_static(b"p"))
}

pub async fn producer(engine: &Engine) -> (Client, Inbox) {
    connect(engine, Hello::producer("svc"), 64).await.unwrap()
}

pub async fn worker(engine: &Engine) -> (Client, Inbox) {
    connect(engine, Hello::worker("svc"), 64).await.unwrap()
}

pub async fn observer(engine: &Engine) -> (Client, Inbox) {
    connect(engine, Hello::observer("svc"), 64).await.unwrap()
}

/// How long "nothing arrives" is watched for. Only for asserting absence.
pub const QUIET: Duration = Duration::from_millis(80);

/// The next message, skipping subscription markers.
pub async fn next_event(inbox: &mut Inbox) -> Option<Incoming> {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), inbox.recv()).await.ok()?? {
            Incoming::Subscribed(_) => continue,
            other => return Some(other),
        }
    }
}

/// The next assignment, skipping everything else.
pub async fn next_assignment(inbox: &mut Inbox) -> ruvent::Assignment {
    loop {
        match next_event(inbox).await {
            Some(Incoming::Assign(assignment)) => return assignment,
            Some(_) => continue,
            None => panic!("no assignment arrived"),
        }
    }
}

/// Whether anything other than a subscription marker arrives within [`QUIET`].
pub async fn stays_quiet(inbox: &mut Inbox) -> bool {
    loop {
        match tokio::time::timeout(QUIET, inbox.recv()).await {
            Err(_) => return true,
            Ok(Some(Incoming::Subscribed(_))) => continue,
            Ok(_) => return false,
        }
    }
}
