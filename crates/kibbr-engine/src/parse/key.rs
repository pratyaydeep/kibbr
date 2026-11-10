// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The identity of a flattened key.

use std::fmt;

/// The member names walked to reach one leaf, kept as segments because joining is lossy:
/// `Display` escapes `.` and `\` so `{"a.b":1}` and `{"a":{"b":2}}` render apart.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct KeyPath(Vec<String>);

impl KeyPath {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn push(&mut self, segment: &str) {
        self.0.push(segment.to_string());
    }

    pub fn pop(&mut self) {
        self.0.pop();
    }

    /// The leaf member name, taken from the path because splitting a rendered key on `.`
    /// misreads `{"a.event":1}`.
    pub fn last(&self) -> Option<&str> {
        self.0.last().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for KeyPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            for ch in segment.chars() {
                if ch == '.' || ch == '\\' {
                    f.write_str("\\")?;
                }
                write!(f, "{ch}")?;
            }
        }
        Ok(())
    }
}

impl FromIterator<String> for KeyPath {
    fn from_iter<I: IntoIterator<Item = String>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// The inverse of `Display`, beside it so the two cannot drift; lenient on a backslash
/// the rendering never emits.
#[cfg(test)]
pub fn parse(text: &str) -> KeyPath {
    let mut segments = vec![String::new()];
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        let current = segments.last_mut().expect("never emptied");
        match ch {
            '.' => segments.push(String::new()),
            '\\' => match chars.next() {
                Some(next @ ('.' | '\\')) => current.push(next),
                // A backslash this rendering never emits, and one at the very end.
                Some(next) => {
                    current.push('\\');
                    current.push(next);
                }
                None => current.push('\\'),
            },
            other => current.push(other),
        }
    }
    KeyPath(segments)
}

#[cfg(test)]
mod tests;
