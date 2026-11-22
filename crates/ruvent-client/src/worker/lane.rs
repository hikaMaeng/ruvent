//! One service's runtime: its permits, the notices it keeps, its executions.
//!
//! A lane is one task that owns all of that state and reacts to messages;
//! executions run in their own tasks and report back. Nothing waits on a timer
//! except an execution's lease.

use super::handler::{CatchUnwind, HandlerFn};
use super::{Context, Failure};
use ruvent::{Envelope, EventId, Methods, Name, WorkKey};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// What a lane reacts to.
pub(crate) enum LaneMsg {
    Notice(Envelope),
    /// An execution's permit came back.
    Released,
    /// An execution, completion included, is over.
    Finished,
    /// A backlog read came back. `resume` is where to continue if it stopped early.
    Refilled {
        notices: Vec<Envelope>,
        resume: Option<WorkKey>,
    },
    Shutdown(oneshot::Sender<()>),
}

/// What every execution of one service shares.
pub(crate) struct Shared {
    pub methods: Arc<dyn Methods>,
    pub channel: Name,
    pub handlers: HashMap<Name, HandlerFn>,
    pub lease: Duration,
    pub worker: u32,
    pub max_retries: u16,
    pub failures: Option<Name>,
}

pub(crate) struct Lane {
    shared: Arc<Shared>,
    rx: mpsc::UnboundedReceiver<LaneMsg>,
    tx: mpsc::UnboundedSender<LaneMsg>,
    free: usize,
    inflight: usize,
    limit: usize,
    /// Notices kept while no permit was free.
    ///
    /// | scope | one service |
    /// | --- | --- |
    /// | bound | `pending_limit` |
    /// | cleared by | `pump` (started), shutdown |
    pending: VecDeque<Envelope>,
    /// Ids in `pending`, so a repeated notice is kept once. Same bound and lifetime.
    queued: HashSet<EventId>,
    /// A backlog read still owed: `Some(None)` from the start (a notice was
    /// dropped), `Some(Some(key))` after that key (a read stopped at the limit).
    resume: Option<Option<WorkKey>>,
    refilling: bool,
    stopping: Option<oneshot::Sender<()>>,
}

impl Lane {
    pub(crate) fn new(shared: Arc<Shared>, permits: usize, limit: usize) -> (Lane, mpsc::UnboundedSender<LaneMsg>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let lane = Lane { shared, rx, tx: tx.clone(), free: permits, inflight: 0, limit, pending: VecDeque::new(), queued: HashSet::new(), resume: None, refilling: false, stopping: None };
        (lane, tx)
    }

    pub(crate) async fn run(mut self) {
        while let Some(message) = self.rx.recv().await {
            match message {
                LaneMsg::Notice(notice) => self.offer(notice),
                LaneMsg::Released => self.free += 1,
                LaneMsg::Finished => self.inflight -= 1,
                LaneMsg::Refilled { notices, resume } => {
                    self.refilling = false;
                    for notice in notices {
                        self.offer(notice);
                    }
                    if let (Some(after), None) = (resume, self.resume) {
                        self.resume = Some(Some(after));
                    }
                }
                LaneMsg::Shutdown(done) => {
                    self.stopping = Some(done);
                    self.pending.clear();
                    self.queued.clear();
                }
            }
            if self.stopping.is_some() {
                if self.inflight == 0 {
                    if let Some(done) = self.stopping.take() {
                        let _ = done.send(());
                    }
                    return;
                }
                continue;
            }
            self.pump();
        }
    }

    /// Keeps a notice for later, or notes that the backlog must be re-read.
    fn offer(&mut self, notice: Envelope) {
        if !self.shared.handlers.contains_key(&notice.event_type) || self.queued.contains(&notice.id) {
            return;
        }
        if self.pending.len() < self.limit {
            self.queued.insert(notice.id);
            self.pending.push_back(notice);
        } else {
            self.resume = Some(None);
        }
    }

    fn pump(&mut self) {
        while self.free > 0 {
            let Some(notice) = self.pending.pop_front() else { break };
            self.queued.remove(&notice.id);
            self.free -= 1;
            self.inflight += 1;
            tokio::spawn(execute(self.shared.clone(), notice.id, self.tx.clone()));
        }
        if self.free > 0 && self.pending.is_empty() && !self.refilling {
            if let Some(after) = self.resume.take() {
                self.refilling = true;
                tokio::spawn(refill(self.shared.clone(), after, self.limit, self.tx.clone()));
            }
        }
    }
}

/// Reads the waiting backlog from `after`, up to `room` notices.
async fn refill(shared: Arc<Shared>, mut after: Option<WorkKey>, room: usize, tx: mpsc::UnboundedSender<LaneMsg>) {
    let mut notices = Vec::new();
    let resume = loop {
        match shared.methods.subscribe(shared.channel.clone(), after).await {
            Ok(page) => {
                let last = page.last();
                notices.extend(page.notices);
                if !page.more {
                    break None;
                }
                if notices.len() >= room {
                    break last;
                }
                after = last;
            }
            // The connection is gone; the app ends through its inbox.
            Err(_) => break None,
        }
    };
    let _ = tx.send(LaneMsg::Refilled { notices, resume });
}

/// The failure event for `event`, if the service reports failures.
fn failure_event(shared: &Shared, event: &Envelope, failure: &Failure) -> Vec<Envelope> {
    match &shared.failures {
        Some(channel) => vec![Envelope::dispatch(channel.clone(), event.event_type.clone(), failure.0.clone().into_bytes()).with_id(EventId::new_v7()).with_correlation(event.id)],
        None => Vec::new(),
    }
}

/// One execution: claim, run, complete. The permit goes back before the completion.
async fn execute(shared: Arc<Shared>, id: EventId, tx: mpsc::UnboundedSender<LaneMsg>) {
    let finish = |tx: &mpsc::UnboundedSender<LaneMsg>| {
        let _ = tx.send(LaneMsg::Released);
        let _ = tx.send(LaneMsg::Finished);
    };
    // The claim answers with whatever has this id now; if its work key differs
    // from the notice's, it is simply another piece of work, and run as such.
    let event = match shared.methods.claim(id, shared.worker, shared.lease).await {
        Ok(event) => event,
        Err(_) => return finish(&tx),
    };
    let Some(token) = event.claim.map(|claim| claim.token) else { return finish(&tx) };

    let follow_ups = if event.retry > shared.max_retries {
        failure_event(&shared, &event, &Failure::new("retries exceeded"))
    } else {
        let Some(handler) = shared.handlers.get(&event.event_type).cloned() else {
            // Another type now has this id: not ours to run, so leave it to the lease.
            return finish(&tx);
        };
        let context = Context::new(event.clone(), shared.methods.clone());
        let running = CatchUnwind::new(handler(context.clone()));
        // The lease: the one place an execution waits on the clock.
        match tokio::time::timeout(shared.lease, running).await {
            // Past the lease the claim is no longer ours to complete; the
            // server reclaims it at the next registration, with retry + 1.
            Err(_) => return finish(&tx),
            Ok(Ok(Ok(()))) => context.take_follow_ups(),
            Ok(Ok(Err(failure))) => failure_event(&shared, &event, &failure),
            Ok(Err(())) => failure_event(&shared, &event, &Failure::new("handler panicked")),
        }
    };
    let _ = tx.send(LaneMsg::Released);
    // A refusal here means the claim is gone already (reclaimed, or the server
    // restarted): the completion has no effect, by the server's rules.
    let _ = shared.methods.complete(event.id, token, follow_ups).await;
    let _ = tx.send(LaneMsg::Finished);
}
