//! The first record of a connection: `Hello`.
//!
//! A version-2 `Hello` with another version is answered `Bye{version}`. A
//! version-1 client's hello is recognised by its first bytes and answered
//! with a version-1 `Bye` frame it can read (B12: refused, not translated).

use super::Credentials;
use ruvent::{ByeReason, Hello, Name};
use ruvent_protocol::records::HELLO;
use ruvent_protocol::{sniff_v1, Body, Decoder, Message, ProtocolError, RecordReader};
use std::time::Duration;
use tokio::net::tcp::OwnedReadHalf;

/// Why a connection did not get past its first record.
pub(super) enum Refusal {
    /// Answer with this `Bye`.
    Bye(ByeReason),
    /// A version-1 client: answer with a version-1 `Bye` frame.
    VersionOne,
    /// The peer closed or broke before saying anything; nobody to answer.
    Gone,
}

/// Reads and checks the `Hello`, within `deadline`, and resolves its token
/// to the credential the engine is told.
pub(super) async fn hello(reader: &mut RecordReader<OwnedReadHalf>, decoder: &mut Decoder, credentials: &Credentials, deadline: Duration) -> Result<Hello, Refusal> {
    // The hello's deadline: one of the two places a connection waits on the clock.
    let read = tokio::time::timeout(deadline, first(reader, decoder)).await.unwrap_or(Err(Refusal::Bye(ByeReason::Protocol)))?;
    let (token, mut hello) = read;
    let Some(credential) = credentials.credential(&token) else { return Err(Refusal::Bye(ByeReason::Token)) };
    hello.credential = Name::new(credential);
    Ok(hello)
}

async fn first(reader: &mut RecordReader<OwnedReadHalf>, decoder: &mut Decoder) -> Result<(String, Hello), Refusal> {
    match reader.peek(1).await {
        Ok([]) | Err(_) => return Err(Refusal::Gone),
        Ok([HELLO]) => {}
        Ok(_) => {
            let first = reader.peek(11).await.map_err(|_| Refusal::Gone)?;
            return Err(if sniff_v1(first) { Refusal::VersionOne } else { Refusal::Bye(ByeReason::Protocol) });
        }
    }
    let raw = match reader.head().await {
        Ok(Some(raw)) => raw,
        Ok(None) | Err(_) => return Err(Refusal::Gone),
    };
    match decoder.decode(&raw, Body::None) {
        Ok(Some(Message::Hello { token, hello, .. })) => Ok((token, hello)),
        Err(ProtocolError::Version(_)) => Err(Refusal::Bye(ByeReason::Version)),
        _ => Err(Refusal::Bye(ByeReason::Protocol)),
    }
}
