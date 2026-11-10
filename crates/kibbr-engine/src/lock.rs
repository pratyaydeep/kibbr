// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `LOCK`: one process owns a data directory, because the catalog and the manifest are
//! append-only logs each written by exactly one handle.
//!
//! An advisory `flock`, so the kernel releases it when the holder dies; what the file says
//! is a hint - who holds it, a server's address, and the token its `/load` asks for.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const LOCK_FILE: &str = "LOCK";

/// What `LOCK` says about the process holding it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holder {
    pub pid: u32,
    /// A server's listening address, once it is accepting requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// A bulk load running alone.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub load: bool,
    /// What a server takes a `/load` only with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// A compaction running alone, which says `load` as well so that an older build reads
    /// it as a load and gives up at once.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub compact: bool,
}

impl Holder {
    pub fn server(address: Option<String>, token: Option<String>) -> Self {
        Self {
            pid: std::process::id(),
            address,
            load: false,
            token,
            compact: false,
        }
    }

    pub fn load() -> Self {
        Self {
            pid: std::process::id(),
            address: None,
            load: true,
            token: None,
            compact: false,
        }
    }

    pub fn compact() -> Self {
        Self {
            compact: true,
            ..Self::load()
        }
    }

    fn describe(&self) -> String {
        match (&self.address, self.compact, self.load) {
            (Some(address), _, _) => format!("a server at {address} (pid {})", self.pid),
            (None, true, _) => format!("a compaction running alone (pid {})", self.pid),
            (None, false, true) => format!("a load running alone (pid {})", self.pid),
            (None, false, false) => format!("a server still starting (pid {})", self.pid),
        }
    }
}

#[derive(Debug, Error)]
pub enum LockError {
    #[error(
        "{path} is held by {holder}: one process owns a data directory. Stop it, or point \
         --data-dir somewhere else"
    )]
    Held { path: PathBuf, holder: String },
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// The lock on a data directory, held until this is dropped.
#[derive(Debug)]
pub struct DataLock {
    file: File,
    path: PathBuf,
}

impl DataLock {
    /// Take the directory's lock without waiting, or say who holds it.
    pub fn acquire(base: &Path) -> Result<Self, LockError> {
        let path = base.join(LOCK_FILE);
        let io = |source| LockError::Io {
            path: path.clone(),
            source,
        };
        std::fs::create_dir_all(base).map_err(io)?;
        let mut open = OpenOptions::new();
        open.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
        let file = open.open(&path).map_err(io)?;
        // A file from before the token, or made under another umask: the token must not be
        // readable by an account that could not read the directory's data.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
        match file.try_lock() {
            Ok(()) => Ok(Self { file, path }),
            Err(TryLockError::WouldBlock) => Err(LockError::Held {
                holder: holder(base)
                    .map_or_else(|| "another kibbr process".to_string(), |h| h.describe()),
                path,
            }),
            Err(TryLockError::Error(source)) => Err(io(source)),
        }
    }

    /// Say who holds the lock, unflushed: the lock is the kernel's, and this is a hint that
    /// only a running holder's readers need.
    pub fn announce(&mut self, holder: &Holder) -> Result<(), LockError> {
        let mut text = serde_json::to_vec(holder).unwrap_or_default();
        text.push(b'\n');
        self.file
            .set_len(0)
            .and_then(|()| self.file.seek(SeekFrom::Start(0)).map(|_| ()))
            .and_then(|()| self.file.write_all(&text))
            .map_err(|source| LockError::Io {
                path: self.path.clone(),
                source,
            })
    }
}

/// What `LOCK` says, if it says anything readable; the lock itself is not consulted, so a
/// holder that died left its last words here.
pub fn holder(base: &Path) -> Option<Holder> {
    let mut text = String::new();
    File::open(base.join(LOCK_FILE))
        .ok()?
        .take(4096)
        .read_to_string(&mut text)
        .ok()?;
    serde_json::from_str(text.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_holder_is_refused_and_told_who_holds_it() {
        let dir = std::env::temp_dir().join(format!("kibbr-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut first = DataLock::acquire(&dir).expect("first");
        first
            .announce(&Holder::server(Some("127.0.0.1:7700".into()), None))
            .expect("announce");
        match DataLock::acquire(&dir) {
            Err(LockError::Held { holder, .. }) => {
                assert!(holder.contains("127.0.0.1:7700"), "{holder}");
            }
            other => panic!("a second holder got {other:?}"),
        }
        assert_eq!(
            holder(&dir).and_then(|h| h.address).as_deref(),
            Some("127.0.0.1:7700")
        );
        drop(first);
        assert!(DataLock::acquire(&dir).is_ok(), "released on drop");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_compaction_alone_is_named_as_one_and_reads_as_a_load_to_an_older_build() {
        let words = serde_json::to_value(Holder::compact()).expect("serialize");
        assert_eq!(words["compact"], true, "{words}");
        assert_eq!(words["load"], true, "{words}");
        let said = Holder::compact().describe();
        assert!(said.contains("a compaction running alone"), "{said}");
        // What a build before the field wrote: no `compact`, and still a load.
        let older: Holder = serde_json::from_str(r#"{"pid":7,"load":true}"#).expect("parse");
        assert!(!older.compact);
        assert!(older.describe().contains("a load running alone"));
    }
}
