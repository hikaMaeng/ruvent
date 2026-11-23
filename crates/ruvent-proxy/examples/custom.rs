//! A proxy server with a domain's own rules: write the hooks, nothing else.
//!
//! ```text
//! cargo run -p ruvent-proxy --example custom -- crates/ruvent-proxy/proxy.example.json
//! ```
//!
//! * **authenticate** — a `session` cookie names a session of this program,
//!   looked up in its own table (an unknown session is turned away); without
//!   a cookie, the configured `auth.mode` decides (service tokens, say).
//! * **publish** — only an operator may register an `admin.*` type; then the
//!   default scope rule checks the channel and the reply channel.
//! * **deliver** — an event whose payload has `"to"` reaches only that user.
//!
//! `subscribe` is not written, so it keeps the default: the user's channel scope.

#[cfg(any(feature = "ws", feature = "http"))]
mod domain {
    use ruvent_proxy::hooks::publish_within_scope;
    use ruvent_proxy::{async_trait, Context, Denied, Envelope, Handshake, Hooks, Publish, Rejection, User};
    use std::collections::HashMap;

    /// What this domain knows about a user, carried on [`User`].
    struct Member {
        operator: bool,
    }

    /// One session of this program.
    pub struct Session {
        pub user: &'static str,
        pub tenant: &'static str,
        pub operator: bool,
    }

    pub struct Domain {
        /// session id → session. A real program asks its own store.
        pub sessions: HashMap<&'static str, Session>,
    }

    #[async_trait]
    impl Hooks for Domain {
        async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
            let Some(id) = handshake.cookie("session") else {
                return cx.authenticate_by_config(handshake.token.as_deref()).await;
            };
            let session = self.sessions.get(id).ok_or(Denied::Rejected)?;
            Ok(User::new(session.user, [format!("tenant.{}.*", session.tenant)]).with_data(Member { operator: session.operator }))
        }

        async fn publish(&self, _cx: Context<'_>, user: &User, publish: &mut Publish) -> Result<(), Rejection> {
            let operator = user.data::<Member>().is_some_and(|member| member.operator);
            if publish.event_type.starts_with("admin.") && !operator {
                return Err(Rejection::forbidden("operators only"));
            }
            publish_within_scope(user, publish)
        }

        fn deliver(&self, user: &User, event: &Envelope) -> bool {
            match serde_json::from_slice::<serde_json::Value>(&event.payload) {
                Ok(payload) => payload.get("to").and_then(|to| to.as_str()).is_none_or(|to| to == user.name),
                Err(_) => true,
            }
        }
    }
}

#[cfg(any(feature = "ws", feature = "http"))]
#[tokio::main]
async fn main() -> Result<(), ruvent_proxy::LaunchError> {
    use domain::{Domain, Session};
    let sessions = std::collections::HashMap::from([("s-alice", Session { user: "alice", tenant: "acme", operator: true }), ("s-bob", Session { user: "bob", tenant: "acme", operator: false })]);
    ruvent_proxy::Server::new().hooks(Domain { sessions }).run().await
}

/// Built with no protocol there is no server to run.
#[cfg(not(any(feature = "ws", feature = "http")))]
fn main() {}
