//! Per-connection, per-direction name dictionaries.
//!
//! The sender ([`Outgoing`]) assigns numbers in send order and writes a
//! definition record before a name's first use. When a message needs names
//! that would take the dictionary past its bound, the sender writes a reset
//! first and defines everything that message needs again. The receiver
//! ([`Incoming`]) holds the same bound and refuses a number defined twice
//! without a reset in between.

use crate::error::ProtocolError;
use crate::records::{put_text, Records, NAME, RESET};
use bytes::BufMut;
use ruvent::Name;
use std::collections::HashMap;

/// The sending side's dictionary.
///
/// | key | name text |
/// | --- | --- |
/// | scope | one direction of one connection |
/// | lifetime | until the next reset record, or the connection's end |
/// | bound | `cap` entries |
/// | cleared by | [`Outgoing::define`], by writing a reset when the bound would be passed |
pub struct Outgoing {
    numbers: HashMap<Name, u32>,
    cap: usize,
}

impl Outgoing {
    pub fn new(cap: usize) -> Self {
        Outgoing { numbers: HashMap::new(), cap: cap.max(1) }
    }

    /// Makes sure every name in `names` has a number the receiver knows,
    /// writing a reset and definitions into `records` as needed.
    pub fn define<'a>(&mut self, names: impl IntoIterator<Item = &'a Name>, records: &mut Records) -> Result<(), ProtocolError> {
        let mut needed: Vec<&Name> = Vec::new();
        for name in names {
            if !needed.contains(&name) {
                needed.push(name);
            }
        }
        if needed.len() > self.cap {
            return Err(ProtocolError::Dictionary("one message uses more distinct names than the dictionary holds"));
        }
        let mut missing: Vec<&Name> = needed.iter().copied().filter(|name| !self.numbers.contains_key(*name)).collect();
        if missing.is_empty() {
            return Ok(());
        }
        if self.numbers.len() + missing.len() > self.cap {
            self.numbers.clear();
            records.buffer.put_u8(RESET);
            records.count += 1;
            missing = needed;
        }
        for name in missing {
            let number = self.numbers.len() as u32;
            self.numbers.insert(name.clone(), number);
            records.buffer.put_u8(NAME);
            records.buffer.put_u32_le(number);
            put_text(&mut records.buffer, name.as_str());
            records.count += 1;
        }
        Ok(())
    }

    /// The number of a name [`Outgoing::define`] has defined.
    pub fn number(&self, name: &Name) -> u32 {
        self.numbers.get(name).copied().unwrap_or(u32::MAX)
    }

    pub fn len(&self) -> usize {
        self.numbers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.numbers.is_empty()
    }
}

/// The receiving side's dictionary.
///
/// | key | the number the sender assigned |
/// | --- | --- |
/// | scope | one direction of one connection |
/// | lifetime | until the next reset record, or the connection's end |
/// | bound | `cap` entries; a definition past it refuses the frame |
/// | cleared by | a reset record ([`Incoming::reset`]) |
pub struct Incoming {
    names: HashMap<u32, Name>,
    cap: usize,
}

impl Incoming {
    pub fn new(cap: usize) -> Self {
        Incoming { names: HashMap::new(), cap: cap.max(1) }
    }

    pub fn define(&mut self, number: u32, name: Name) -> Result<(), ProtocolError> {
        if self.names.contains_key(&number) {
            return Err(ProtocolError::Dictionary("a number was defined twice without a reset"));
        }
        if self.names.len() >= self.cap {
            return Err(ProtocolError::Dictionary("more definitions than the bound"));
        }
        self.names.insert(number, name);
        Ok(())
    }

    pub fn reset(&mut self) {
        self.names.clear();
    }

    pub fn get(&self, number: u32) -> Option<Name> {
        self.names.get(&number).cloned()
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}
