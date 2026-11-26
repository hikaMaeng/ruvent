//! Interned names: service credentials and the end users a proxy registers for.
//!
//! Channels are ids and types are numbers; the names left are the
//! credential and the delegate. Inside the server an event holds them as a
//! shared [`Name`] handle, not as a number. A handle keeps its text alive for as long
//! as anything holds it — a queue entry, a send buffer, the journal writer —
//! so a name can never change meaning under a reader that is still behind.
//!
//! [`Names`] is the server's dictionary. Only the engine task changes it.
//! Entries nobody else holds any more are dropped by a sweep that runs when
//! the dictionary has doubled since the last one, so its size stays within
//! twice the number of live names plus a constant, and no sweep runs on a timer.

use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

/// A shared, immutable name.
#[derive(Clone)]
pub struct Name(Arc<str>);

impl Name {
    pub fn new(text: &str) -> Self {
        Name(Arc::from(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// How many handles share this text, the dictionary's own included.
    pub fn holders(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
    }
}

impl Eq for Name {}

impl Hash for Name {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialOrd for Name {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Name {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl Borrow<str> for Name {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Deref for Name {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Name {
    fn from(text: &str) -> Self {
        Name::new(text)
    }
}

impl From<String> for Name {
    fn from(text: String) -> Self {
        Name(Arc::from(text))
    }
}

/// The smallest dictionary size at which a sweep runs.
const FLOOR: usize = 1024;

/// The server's dictionary: one handle per distinct text.
///
/// | key | the text |
/// | --- | --- |
/// | scope | one engine |
/// | lifetime | an entry lives while any handle other than the dictionary's holds it |
/// | bound | at most `2 × live + 1024` entries; reached only between sweeps |
/// | cleared by | [`Names::intern`], when the bound is reached |
pub struct Names {
    set: HashSet<Name>,
    sweep_at: usize,
}

impl Default for Names {
    fn default() -> Self {
        Self { set: HashSet::new(), sweep_at: FLOOR }
    }
}

impl Names {
    /// The one handle for `text`, created if this is its first use.
    pub fn intern(&mut self, text: &str) -> Name {
        if let Some(name) = self.set.get(text) {
            return name.clone();
        }
        self.insert(Name::new(text))
    }

    /// The dictionary's handle for the same text as `name`. A handle decoded
    /// from a socket or a file is replaced, so equal names share one allocation.
    pub fn adopt(&mut self, name: &Name) -> Name {
        if let Some(found) = self.set.get(name.as_str()) {
            return found.clone();
        }
        self.insert(name.clone())
    }

    fn insert(&mut self, name: Name) -> Name {
        if self.set.len() >= self.sweep_at {
            self.sweep();
        }
        self.set.insert(name.clone());
        name
    }

    /// Drops every entry only the dictionary still holds.
    fn sweep(&mut self) {
        self.set.retain(|name| name.holders() > 1);
        self.sweep_at = (self.set.len() * 2).max(FLOOR);
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

#[cfg(test)]
mod tests;
