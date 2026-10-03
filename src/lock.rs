//! `LOCK`: one process owns a data directory.
//!
//! The catalog and the manifest are append-only logs, each written by exactly one handle
//! that keeps its own end offset. Two processes appending to either would write over each
//! other's records, and two catalogs handing out template ids from the same counter would
//! give one id two templates. So one process owns the directory - a server, or a bulk load
//! or a compaction running alone - and it holds an advisory lock on `<base>/LOCK` for its
//! whole life.
//!
//! **Why a lock and not the port.** A server used to claim its directory by binding its
//! port first: a second server on the same port failed before touching a file. Two servers
//! on two ports - or both on `--port 0`, as every script runs them - both got through, and a
//! load running alone binds nothing. The lock is the directory's own, whatever listens.
//!
//! **Held by the kernel, not by the file's contents.** `flock` is released when the process
//! dies, `kill -9` included, so a crash never leaves a lock someone must delete by hand; the
//! file itself is never deleted, because a process deleting it while another opened it
//! would let two think they hold it. What the file *says* is only a hint: who holds it, and
//! for a server, where to reach it - which is how `kibbr load` finds the server it must ask
//! to run the load instead of writing beside it.
//!
//! **And the key to its `/load`.** A server reads the files a load names, with its own
//! permissions, and serves their rows to anyone who can query it. So a load is taken only
//! from a client that shows a token the server wrote here - readable, the file being
//! `0600`, only by the account that owns the directory. `/ingest` is a door for lines a
//! client already has; without this, `/load` would be a door to every file the server can
//! read, open to whoever can reach its port.

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
    /// A compaction running alone. It says `load` as well: a build from before this field
    /// reads `load` and gives up at once, where a holder it could not place is taken for a
    /// server still starting and waited on for thirty seconds.
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
    /// Take the directory's lock, or say who holds it. Never waits.
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

    /// Say who holds the lock. Not flushed: the lock is the kernel's, and this is a hint
    /// that only a running holder's readers need.
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

/// What `LOCK` says, if it says anything readable. The lock itself is not consulted: a
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
