//! The app: services and listeners registered up front; the framework drives them.

use super::handler::{listener, ListenerFn};
use super::lane::{Lane, LaneMsg, Shared};
use super::{Service, WorkerError};
use ruvent::{Envelope, Inbox, Incoming, Methods, Name};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Services and listeners on one connection.
pub struct App {
    methods: Arc<dyn Methods>,
    inbox: Inbox,
    services: Vec<Service>,
    listeners: HashMap<Name, ListenerFn>,
}

type Outcome = Option<Result<(), WorkerError>>;

/// A started app.
pub struct Running {
    lanes: Vec<(mpsc::UnboundedSender<LaneMsg>, JoinHandle<()>)>,
    stop: Option<oneshot::Sender<()>>,
    outcome: watch::Receiver<Outcome>,
}

impl App {
    /// An app on `methods`, reading `inbox` (both from one connection).
    pub fn new(methods: impl Methods, inbox: Inbox) -> Self {
        Self::shared(Arc::new(methods), inbox)
    }

    pub fn shared(methods: Arc<dyn Methods>, inbox: Inbox) -> Self {
        App { methods, inbox, services: Vec::new(), listeners: HashMap::new() }
    }

    /// Adds a service. One service per channel; a second one for the same channel replaces the first.
    pub fn service(mut self, service: Service) -> Self {
        self.services.retain(|existing| existing.channel != service.channel);
        self.services.push(service);
        self
    }

    /// Runs `function` for every dispatch event on `channel`, one at a time, in arrival order.
    pub fn listen<F, Fut>(mut self, channel: impl Into<Name>, function: F) -> Self
    where
        F: Fn(Envelope) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.listeners.insert(channel.into(), listener(function));
        self
    }

    /// Subscribes every channel and starts. Fails if a subscription is refused.
    pub async fn start(self) -> Result<Running, WorkerError> {
        let App { methods, inbox, services, listeners } = self;
        let mut listener_lanes = HashMap::new();
        for (channel, function) in listeners {
            methods.subscribe(channel.clone(), None).await.map_err(WorkerError::Refused)?;
            let (tx, mut rx) = mpsc::unbounded_channel::<Envelope>();
            tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    // A panicking listener loses that one event, not the listener.
                    let _ = tokio::spawn(function(event)).await;
                }
            });
            listener_lanes.insert(channel, tx);
        }

        let mut lanes = Vec::new();
        let mut routes = HashMap::new();
        for service in services {
            let shared = Arc::new(Shared {
                methods: methods.clone(),
                channel: service.channel.clone(),
                handlers: service.handlers,
                lease: service.lease,
                worker: service.worker,
                max_retries: service.max_retries,
                failures: service.failures,
            });
            let first = methods.subscribe(service.channel.clone(), None).await.map_err(WorkerError::Refused)?;
            let (lane, tx) = Lane::new(shared, service.permits, service.pending_limit);
            let resume = if first.more { first.last() } else { None };
            let _ = tx.send(LaneMsg::Refilled { notices: first.notices, resume });
            routes.insert(service.channel, tx.clone());
            lanes.push((tx, tokio::spawn(lane.run())));
        }

        let (outcome_tx, outcome) = watch::channel(None);
        let (stop, stop_rx) = oneshot::channel();
        tokio::spawn(dispatch(methods, inbox, routes, listener_lanes, stop_rx, outcome_tx));
        Ok(Running { lanes, stop: Some(stop), outcome })
    }
}

/// Routes the inbox: notices to their service, dispatch events to their listener.
///
/// It holds the methods too: dropping the last handle would close the
/// connection, and an app with only listeners has nothing else holding it.
async fn dispatch(
    methods: Arc<dyn Methods>,
    mut inbox: Inbox,
    routes: HashMap<Name, mpsc::UnboundedSender<LaneMsg>>,
    listeners: HashMap<Name, mpsc::UnboundedSender<Envelope>>,
    mut stop: oneshot::Receiver<()>,
    outcome: watch::Sender<Outcome>,
) {
    let result = loop {
        let incoming = tokio::select! {
            incoming = inbox.recv() => incoming,
            _ = &mut stop => break Ok(()),
        };
        match incoming {
            Some(Incoming::Notice(notice)) => {
                if let Some(lane) = routes.get(&notice.channel) {
                    let _ = lane.send(LaneMsg::Notice(notice));
                }
            }
            Some(Incoming::Delivery(event)) => {
                if let Some(listener) = listeners.get(&event.channel) {
                    let _ = listener.send(event);
                }
            }
            Some(Incoming::Subscribed(_)) => {}
            Some(Incoming::Closed) | None => break Err(WorkerError::Disconnected),
        }
    };
    drop(methods);
    let _ = outcome.send(Some(result));
}

impl Running {
    /// Resolves when the app stops: `Err(Disconnected)` when the connection
    /// ends (the engine stopped, the socket closed), `Ok` after [`Running::shutdown`].
    pub async fn wait(&self) -> Result<(), WorkerError> {
        let mut outcome = self.outcome.clone();
        let result = match outcome.wait_for(Option::is_some).await {
            Ok(state) => state.clone().unwrap_or(Ok(())),
            Err(_) => Err(WorkerError::Disconnected),
        };
        result
    }

    /// Stops claiming, waits for the executions in progress (completions
    /// included), and stops routing.
    pub async fn shutdown(mut self) {
        let mut waits = Vec::new();
        for (lane, _) in &self.lanes {
            let (done, wait) = oneshot::channel();
            if lane.send(LaneMsg::Shutdown(done)).is_ok() {
                waits.push(wait);
            }
        }
        for wait in waits {
            let _ = wait.await;
        }
        for (_, task) in self.lanes.drain(..) {
            let _ = task.await;
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = self.wait().await;
    }
}
