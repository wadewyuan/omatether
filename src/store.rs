//! Per-thread state that has to survive a restart.
//!
//! Only what cannot be recovered from anywhere else lives here: which agent
//! session a chat thread is bound to, and which directory it works in. The
//! conversation itself stays in the agent's own store — duplicating transcripts
//! would make this a second source of truth for something we do not own.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};

/// Everything switchboard remembers about one chat thread.
#[derive(Debug, Clone)]
pub struct ThreadState {
    pub key: String,
    /// The agent's handle for this conversation, so a restart resumes rather
    /// than starting cold.
    ///
    /// A string rather than a UUID because the two agents disagree about who
    /// owns it: Claude accepts one we choose, Codex assigns its own and reports
    /// it back. Whoever decides, it round-trips through here.
    pub session_id: Option<String>,
    pub cwd: String,
    /// Which agent this thread talks to.
    pub agent: String,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let conn = Connection::open(path)
            .with_context(|| format!("opening state database at {}", path.display()))?;

        // WAL so a reader (a future `switchboard status`) never blocks the
        // service mid-write.
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS threads (
                 key        TEXT PRIMARY KEY,
                 session_id TEXT,
                 cwd        TEXT NOT NULL,
                 updated_at INTEGER NOT NULL
             );",
        )?;

        // Added after the first release. Failing means the column is already
        // there, which is the common case.
        conn.execute(
            "ALTER TABLE threads ADD COLUMN agent TEXT NOT NULL DEFAULT 'claude'",
            [],
        )
        .ok();

        Ok(Self { conn })
    }

    /// An in-memory store, for tests.
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE threads (
                 key        TEXT PRIMARY KEY,
                 session_id TEXT,
                 cwd        TEXT NOT NULL,
                 updated_at INTEGER NOT NULL,
                 agent      TEXT NOT NULL DEFAULT 'claude'
             );",
        )?;
        Ok(Self { conn })
    }

    /// Fetch a thread's state, creating it against the defaults on first sight.
    pub fn get_or_create(
        &self,
        key: &str,
        default_cwd: &str,
        default_agent: &str,
    ) -> Result<ThreadState> {
        if let Some(state) = self.get(key)? {
            return Ok(state);
        }

        let state = ThreadState {
            key: key.to_string(),
            // Left for the agent to fill in on its first turn.
            session_id: None,
            cwd: default_cwd.to_string(),
            agent: default_agent.to_string(),
        };
        self.put(&state)?;
        Ok(state)
    }

    pub fn get(&self, key: &str) -> Result<Option<ThreadState>> {
        let row = self
            .conn
            .query_row(
                "SELECT session_id, cwd, agent FROM threads WHERE key = ?1",
                [key],
                |row| {
                    let session_id: Option<String> = row.get(0)?;
                    let cwd: String = row.get(1)?;
                    let agent: String = row.get(2)?;
                    Ok((session_id, cwd, agent))
                },
            )
            .optional()?;

        Ok(row.map(|(session_id, cwd, agent)| ThreadState {
            key: key.to_string(),
            session_id,
            cwd,
            agent,
        }))
    }

    pub fn put(&self, state: &ThreadState) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO threads (key, session_id, cwd, updated_at, agent)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET
                 session_id = excluded.session_id,
                 cwd        = excluded.cwd,
                 updated_at = excluded.updated_at,
                 agent      = excluded.agent",
            rusqlite::params![state.key, state.session_id, state.cwd, now, state.agent],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sight_creates_a_session_and_it_sticks() {
        let store = Store::in_memory().unwrap();

        let first = store
            .get_or_create("telegram:1", "/home/wy/Work", "claude")
            .unwrap();
        let again = store
            .get_or_create("telegram:1", "/somewhere/else", "codex")
            .unwrap();

        assert_eq!(first.session_id, again.session_id);
        assert_eq!(again.cwd, "/home/wy/Work", "default must not overwrite");
        assert_eq!(again.agent, "claude", "default must not overwrite");
    }

    #[test]
    fn cwd_and_session_survive_a_rewrite() {
        let store = Store::in_memory().unwrap();
        let mut state = store.get_or_create("telegram:2", "/a", "claude").unwrap();
        assert_eq!(state.session_id, None, "no id until the agent assigns one");

        state.cwd = "/b".into();
        state.session_id = Some("thread-from-codex".into());
        state.agent = "codex".into();
        store.put(&state).unwrap();

        let read = store.get("telegram:2").unwrap().unwrap();
        assert_eq!(read.cwd, "/b");
        assert_eq!(read.session_id.as_deref(), Some("thread-from-codex"));
        assert_eq!(read.agent, "codex");
    }

    #[test]
    fn unknown_thread_is_none() {
        let store = Store::in_memory().unwrap();
        assert!(store.get("telegram:missing").unwrap().is_none());
    }
}
