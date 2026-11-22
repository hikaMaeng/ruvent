//! The framework surface.
//!
//! A service here is not a set of endpoints that answer requests. It is a set
//! of functions that react to events and record what they did:
//!
//! ```ignore
//! App::open("data/app.journal")
//!     .service(Service::new("turn")
//!         .accept("turn.run", open_turn)        // a command a client may submit
//!         .on("model.replied", decide))         // a reaction to a recorded fact
//!     .service(Service::new("inference")
//!         .on("turn.started", call_model)
//!         .max_in_flight(64))
//!     .transport(ruvent_ws::WebSocket::bind("0.0.0.0:8080"))
//!     .run()
//!     .await
//! ```
//!
//! Nothing above names a queue or writes a reaction table. Both are derived:
//! each [`Service`] is one queue with its own concurrency budget, `accept`
//! puts an action on the client allow-list and routes it to that queue, and
//! `on` adds the service's queue to the reactions of that fact. No function
//! names another — a handler records a fact and stops, and whatever was
//! registered for that fact runs next. The program is the table.

mod service;

use crate::engine::{Durability, Engine, EngineConfig, Events};
use crate::gateway::{Anonymous, Authenticator, Gateway, Open, Policy, Transport};
use crate::worker::{Running, Worker};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use service::Service;

/// A bundle of services and transports that installs itself into an [`App`].
///
/// This is how a feature ships as a unit: its services, the transports it
/// needs and its configuration go in together, and an application is a list
/// of modules.
pub trait Module {
    fn install(self, app: App) -> App;
}

impl<F: FnOnce(App) -> App> Module for F {
    fn install(self, app: App) -> App {
        self(app)
    }
}

pub struct App {
    journal: Option<PathBuf>,
    config: EngineConfig,
    services: Vec<Service>,
    transports: Vec<Box<dyn Transport>>,
    authenticator: Arc<dyn Authenticator>,
    policy: Arc<dyn Policy>,
}

impl App {
    /// An application whose state lives in the journal at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self::with(Some(path.into()))
    }

    /// An application that forgets everything when it stops. For tests.
    pub fn volatile() -> Self {
        Self::with(None)
    }

    fn with(journal: Option<PathBuf>) -> Self {
        Self { journal, config: EngineConfig::default(), services: Vec::new(), transports: Vec::new(), authenticator: Arc::new(Anonymous), policy: Arc::new(Open) }
    }

    pub fn service(mut self, service: Service) -> Self {
        self.services.push(service);
        self
    }

    pub fn mount(self, module: impl Module) -> Self {
        module.install(self)
    }

    /// Adds a way for clients to reach this application. Any number may run
    /// side by side; they all drive the same gateway.
    pub fn transport(mut self, transport: impl Transport) -> Self {
        self.transports.push(Box::new(transport));
        self
    }

    pub fn authenticator(mut self, authenticator: impl Authenticator) -> Self {
        self.authenticator = Arc::new(authenticator);
        self
    }

    pub fn policy(mut self, policy: impl Policy) -> Self {
        self.policy = Arc::new(policy);
        self
    }

    /// Which channels' events are written to the journal. By default none
    /// are: events are for readers, and the work they cause is queued.
    pub fn persist_events(mut self, events: Events) -> Self {
        self.config.persist.events = events;
        self
    }

    pub fn durability(mut self, durability: Durability) -> Self {
        self.config.durability = durability;
        self
    }

    /// Replaces the engine configuration. Reactions registered through
    /// services are added to whatever table this carries.
    pub fn engine(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }

    /// Opens the engine, starts every service and every transport.
    pub async fn start(self) -> io::Result<Started> {
        let mut config = self.config;
        let mut routes = HashMap::new();
        for service in &self.services {
            for action in &service.accepts {
                if let Some(other) = routes.insert(action.clone(), service.name.clone()) {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("command `{action}` is accepted by both `{other}` and `{}`; a command has one owner", service.name)));
                }
            }
            if service.transient {
                config.persist.transient_queues.insert(service.name.as_str().into());
            }
            // A fact may have any number of reactors. Each is issued its own
            // event: one event is never shared between two workers.
            for action in &service.reacts {
                let queues = config.reactions.entry(action.clone()).or_default();
                if !queues.contains(&service.name) {
                    queues.push(service.name.clone());
                }
            }
        }

        let engine = match self.journal {
            Some(path) => Engine::open(path, config).await?,
            None => Engine::volatile(config),
        };
        let workers = self.services.into_iter().map(|service| Worker::new(engine.clone(), service.config.clone()).route(service.name.clone(), service.into_router()).start()).collect();

        let gateway = Gateway::new(engine.clone(), self.authenticator, self.policy, routes);
        let stop = CancellationToken::new();
        let transports = {
            let (gateway, stop, transports) = (gateway.clone(), stop.clone(), self.transports);
            tokio::spawn(async move { gateway.launch(transports, stop).await })
        };
        Ok(Started { engine, gateway, workers, transports, stop })
    }

    /// Starts, runs until Ctrl-C, then shuts down in order.
    pub async fn run(self) -> io::Result<()> {
        let started = self.start().await?;
        tokio::signal::ctrl_c().await?;
        started.shutdown().await
    }
}

/// A running application.
pub struct Started {
    engine: Engine,
    gateway: Gateway,
    workers: Vec<Running>,
    transports: JoinHandle<io::Result<()>>,
    stop: CancellationToken,
}

impl Started {
    /// The engine, for publishing and reading from inside the process.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The gateway, for driving it from a transport created after start or
    /// from in-process code that should be subject to the same policy.
    pub fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    /// Stops accepting, lets in-flight work finish, writes the journal out
    /// and releases it — in that order, so nothing is cut off mid-step.
    pub async fn shutdown(self) -> io::Result<()> {
        self.stop.cancel();
        let transports = self.transports.await.unwrap_or_else(|error| Err(io::Error::other(error)));
        for worker in self.workers {
            worker.shutdown().await;
        }
        let _ = self.engine.shutdown().await;
        transports
    }
}
