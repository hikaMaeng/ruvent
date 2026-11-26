//! Sets of event types: what settings and grants are keyed by (plan 9.15).
//!
//! A channel is an id with no meaning the server reads, so the only category
//! an event has is its 16-bit type. A [`TypeSet`] is a sorted list of
//! disjoint inclusive ranges; membership is a binary search. Its text form,
//! for configuration files, is a list of items: `*` (every type), `N`, or
//! `A-B`.

use std::fmt;

/// A set of event types.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TypeSet {
    /// Sorted, disjoint and not adjacent: `[(1, 3), (7, 7)]`, never `[(1, 3), (4, 5)]`.
    ranges: Vec<(u16, u16)>,
}

impl TypeSet {
    /// Every type.
    pub fn all() -> Self {
        TypeSet { ranges: vec![(0, u16::MAX)] }
    }

    /// No type.
    pub fn none() -> Self {
        TypeSet::default()
    }

    /// Exactly these types.
    pub fn of(types: impl IntoIterator<Item = u16>) -> Self {
        Self::from_ranges(types.into_iter().map(|kind| (kind, kind)))
    }

    /// The union of these inclusive ranges. A range whose start is past its end is empty.
    pub fn from_ranges(ranges: impl IntoIterator<Item = (u16, u16)>) -> Self {
        let mut sorted: Vec<(u16, u16)> = ranges.into_iter().filter(|(low, high)| low <= high).collect();
        sorted.sort_unstable();
        let mut merged: Vec<(u16, u16)> = Vec::with_capacity(sorted.len());
        for (low, high) in sorted {
            match merged.last_mut() {
                Some(last) if u32::from(low) <= u32::from(last.1) + 1 => last.1 = last.1.max(high),
                _ => merged.push((low, high)),
            }
        }
        TypeSet { ranges: merged }
    }

    pub fn contains(&self, kind: u16) -> bool {
        let at = self.ranges.partition_point(|(_, high)| *high < kind);
        self.ranges.get(at).is_some_and(|(low, _)| *low <= kind)
    }

    /// Whether every type of `types` is in the set.
    pub fn contains_all(&self, types: &[u16]) -> bool {
        types.iter().all(|kind| self.contains(*kind))
    }

    pub fn is_all(&self) -> bool {
        self.ranges == [(0, u16::MAX)]
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn ranges(&self) -> &[(u16, u16)] {
        &self.ranges
    }

    /// Reads the text form: items `*`, `N` or `A-B`.
    pub fn parse<S: AsRef<str>>(items: &[S]) -> Result<Self, ParseError> {
        let mut ranges = Vec::with_capacity(items.len());
        for item in items {
            ranges.push(parse_item(item.as_ref())?);
        }
        Ok(Self::from_ranges(ranges))
    }
}

fn parse_item(item: &str) -> Result<(u16, u16), ParseError> {
    let item = item.trim();
    if item == "*" {
        return Ok((0, u16::MAX));
    }
    let number = |text: &str| text.trim().parse::<u16>().map_err(|_| ParseError(item.to_string()));
    match item.split_once('-') {
        Some((low, high)) => {
            let (low, high) = (number(low)?, number(high)?);
            if low > high {
                return Err(ParseError(item.to_string()));
            }
            Ok((low, high))
        }
        None => number(item).map(|kind| (kind, kind)),
    }
}

/// An item that is neither `*`, a type number, nor `A-B` with `A ≤ B`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError(pub String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} is not a type set item (`*`, a number 0-65535, or `A-B`)", self.0)
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests;
