//! A proxy server with a domain's own rules: write the hooks, nothing else.
//!
//! ```text
//! cargo run -p ruvent-proxy --example custom -- crates/ruvent-proxy/proxy.example.json
//! ```
//!
//! * **authenticate** — a `session` cookie names a session of this program;
//!   without one, the configured `auth.mode` decides (service tokens, say).
//! * **publish** — only an operator may register an `admin.*` type; then the
//!   default scope rule applies.
//! * **deliver** — an event whose payload has `"to"` reaches only that user.
//!
//! `subscribe` is not written, so it keeps the default: the user's channel scope.

use ruvent_proxy::hooks::publish_within_scope;
use ruvent_proxy::{async_trait, Context, Denied, Envelope, Handshake, Hooks, Publish, Rejection, User};
use std::collections::HashMap;

/// What this domain knows about a user, carried on [`User`].
struct Member {
    operator: bool,
}

struct Domain {
    /// session id → (user, tenant, operator). A real program asks its own store.
    sessions: HashMap<&'static str, (&'static str, &'static str, bool)>,
}

#[async_trait]
impl Hooks for Domain {
    async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        let Some(session) = handshake.cookie("session") else {
            return cx.authenticate_by_config(handshake.token.as_deref()).await;
        };
        let (name, tenant, operator) = *self.sessions.get(session).ok_or(Denied::Rejected)?;
        Ok(User::new(name, [format!("tenant.{tenant}.*")]).with_data(Member { operator }))
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

#[tokio::main]
async fn main() -> Result<(), ruvent_proxy::LaunchError> {
    let sessions = HashMap::from([("s-alice", ("alice", "acme", true)), ("s-bob", ("bob", "acme", false))]);
    ruvent_proxy::Server::new().hooks(Domain { sessions }).run().await
}
