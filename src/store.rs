//! Per-thread state that has to survive a restart.
//!
//! Only what cannot be recovered from anywhere else lives here: which agent
//! session a chat thread is bound to, and which directory it works in. The
//! conversation itself stays in the agent's own store — duplicating transcripts
//! would make this a second source of truth for something we do not own.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};

/// Everything omatether remembers about one chat thread.
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
    /// The best answer to "what is running", for saying so.
    ///
    /// Remembered rather than asked for, because it is wanted at moments when
    /// no agent process exists — `/new` answers before anything has spawned.
    /// Written by whoever last knew: the agent's own report at session start,
    /// or a `/model` the agent confirmed. Cleared whenever the answer could
    /// have changed underneath it, so it is never stale — only absent, which
    /// prints as unknown.
    pub model: Option<String>,
    /// The model `/model` asked for, spelt as it was typed.
    ///
    /// A different fact from `model`, and the reason both exist: this is what
    /// gets passed to the next process, while `model` is what came back. An
    /// alias asked for (`opus`) is not what is reported (`claude-opus-5`), and
    /// a thread that asked for nothing must keep asking for nothing rather
    /// than pinning itself to whatever the agent happened to default to on the
    /// day it was first told.
    pub requested_model: Option<String>,
    /// Approve tool calls without asking.
    ///
    /// On by default, and that is a deliberate product decision rather than an
    /// oversight: a gate on every Bash call turns a phone into a tap-Allow
    /// machine, and a prompt nobody reads is worse than no prompt at all
    /// because it looks like review. `/auto off` puts the gate back, per
    /// thread, and `/status` always says which world this thread is in.
    pub auto: bool,
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

        // WAL so a reader (a future `omatether status`) never blocks the
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

        migrate(&conn)?;

        Ok(Self { conn })
    }

    /// Wrap an already-open connection. For tests that build an older schema.
    #[cfg(test)]
    fn from_conn(conn: Connection) -> Result<Self> {
        migrate(&conn)?;
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
                 agent      TEXT NOT NULL DEFAULT 'claude',
                 auto       INTEGER NOT NULL DEFAULT 1,
                 model      TEXT,
                 requested_model TEXT
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
            // Left for the agent to report on its first turn, like the session.
            model: None,
            // Nothing asked for, so the agent picks — which is what someone who
            // has never run /model means by not having run it.
            requested_model: None,
            auto: true,
        };
        self.put(&state)?;
        Ok(state)
    }

    pub fn get(&self, key: &str) -> Result<Option<ThreadState>> {
        let row = self
            .conn
            .query_row(
                "SELECT session_id, cwd, agent, auto, model, requested_model
                 FROM threads WHERE key = ?1",
                [key],
                |row| {
                    let auto: i64 = row.get(3)?;
                    Ok(ThreadState {
                        key: key.to_string(),
                        session_id: row.get(0)?,
                        cwd: row.get(1)?,
                        agent: row.get(2)?,
                        auto: auto != 0,
                        model: row.get(4)?,
                        requested_model: row.get(5)?,
                    })
                },
            )
            .optional()?;

        Ok(row)
    }

    pub fn put(&self, state: &ThreadState) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        self.conn.execute(
            "INSERT INTO threads
                 (key, session_id, cwd, updated_at, agent, auto, model, requested_model)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(key) DO UPDATE SET
                 session_id = excluded.session_id,
                 cwd        = excluded.cwd,
                 updated_at = excluded.updated_at,
                 agent      = excluded.agent,
                 auto       = excluded.auto,
                 model      = excluded.model,
                 requested_model = excluded.requested_model",
            rusqlite::params![
                state.key,
                state.session_id,
                state.cwd,
                now,
                state.agent,
                state.auto as i64,
                state.model,
                state.requested_model
            ],
        )?;
        Ok(())
    }
}

/// Bring an existing database up to the current schema.
///
/// Both steps are needed by databases created before the column they add, and
/// both are no-ops afterwards.
fn migrate(conn: &Connection) -> Result<()> {
    // Which agent a thread talks to. Added in the multi-agent release; the
    // error on an existing column is the common case and is not interesting.
    conn.execute(
        "ALTER TABLE threads ADD COLUMN agent TEXT NOT NULL DEFAULT 'claude'",
        [],
    )
    .ok();

    // Whether a thread approves tool calls itself. Defaulting to 1 back-fills
    // every existing thread into auto mode, which is the point: the prompts
    // were the complaint.
    conn.execute(
        "ALTER TABLE threads ADD COLUMN auto INTEGER NOT NULL DEFAULT 1",
        [],
    )
    .ok();

    // The model the agent last reported. Nullable with no default: a thread
    // that existed before this column has genuinely never been told, and
    // "unknown" is a different answer from a guess.
    conn.execute("ALTER TABLE threads ADD COLUMN model TEXT", [])
        .ok();

    // What `/model` asked for, which is not what the agent reported. Also
    // nullable with no default, and for the same reason: "asked for nothing"
    // is the answer for every thread that predates the command, and it is the
    // answer that keeps letting the agent choose.
    conn.execute("ALTER TABLE threads ADD COLUMN requested_model TEXT", [])
        .ok();

    // `session_id` began as NOT NULL, back when omatether chose the id
    // itself. Codex assigns its own on the first turn, so a thread now starts
    // without one — and SQLite cannot drop a NOT NULL in place, which means a
    // table rebuild rather than an ALTER. It runs last on purpose: it copies
    // every column by name, so each `ADD COLUMN` above has to have happened
    // before the SELECT can name it.
    if !session_id_is_nullable(conn)? {
        conn.execute_batch(
            "BEGIN;
             CREATE TABLE threads_migrated (
                 key        TEXT PRIMARY KEY,
                 session_id TEXT,
                 cwd        TEXT NOT NULL,
                 updated_at INTEGER NOT NULL,
                 agent      TEXT NOT NULL DEFAULT 'claude',
                 auto       INTEGER NOT NULL DEFAULT 1,
                 model      TEXT,
                 requested_model TEXT
             );
             INSERT INTO threads_migrated
                 (key, session_id, cwd, updated_at, agent, auto, model, requested_model)
                 SELECT key, NULLIF(session_id, ''), cwd, updated_at, agent, auto,
                        model, requested_model FROM threads;
             DROP TABLE threads;
             ALTER TABLE threads_migrated RENAME TO threads;
             COMMIT;",
        )
        .context("migrating threads.session_id to nullable")?;

        tracing::info!("migrated thread store: session_id is now optional");
    }

    Ok(())
}

fn session_id_is_nullable(conn: &Connection) -> Result<bool> {
    let mut statement = conn.prepare("PRAGMA table_info(threads)")?;
    let mut rows = statement.query([])?;

    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == "session_id" {
            let not_null: i64 = row.get(3)?;
            return Ok(not_null == 0);
        }
    }
    // No such column: a fresh database, created correctly.
    Ok(true)
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
        assert!(first.auto, "a new thread approves its own tools");
    }

    #[test]
    fn cwd_and_session_survive_a_rewrite() {
        let store = Store::in_memory().unwrap();
        let mut state = store.get_or_create("telegram:2", "/a", "claude").unwrap();
        assert_eq!(state.session_id, None, "no id until the agent assigns one");

        assert_eq!(state.model, None, "no model until the agent reports one");
        assert_eq!(state.requested_model, None, "and none asked for");

        state.cwd = "/b".into();
        state.session_id = Some("thread-from-codex".into());
        state.agent = "codex".into();
        state.model = Some("gpt-5-codex".into());
        state.requested_model = Some("gpt-5-codex-high".into());
        state.auto = false;
        store.put(&state).unwrap();

        let read = store.get("telegram:2").unwrap().unwrap();
        assert_eq!(read.cwd, "/b");
        assert_eq!(read.session_id.as_deref(), Some("thread-from-codex"));
        assert_eq!(read.agent, "codex");
        assert_eq!(read.model.as_deref(), Some("gpt-5-codex"));
        // Stored apart from what came back, because they are different facts:
        // one is passed to the next process, the other was reported by the last.
        assert_eq!(read.requested_model.as_deref(), Some("gpt-5-codex-high"));
        assert!(!read.auto, "a thread that asked for the gate keeps it");
    }

    /// The schema omatether shipped before agents chose their own session id.
    fn legacy_database() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE threads (
                 key        TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL,
                 cwd        TEXT NOT NULL,
                 updated_at INTEGER NOT NULL
             );
             INSERT INTO threads (key, session_id, cwd, updated_at)
                 VALUES ('telegram:5', 'old-uuid', '/home/wy/Work', 1);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn an_old_database_accepts_a_thread_with_no_session_id() {
        let store = Store::from_conn(legacy_database()).unwrap();

        // The row that was already there survives intact.
        let existing = store.get("telegram:5").unwrap().unwrap();
        assert_eq!(existing.session_id.as_deref(), Some("old-uuid"));
        assert_eq!(existing.cwd, "/home/wy/Work");
        assert_eq!(existing.agent, "claude", "back-filled by the migration");
        assert!(existing.auto, "back-filled by the migration");
        assert_eq!(
            existing.model, None,
            "never told, which is not the same as a default"
        );
        assert_eq!(
            existing.requested_model, None,
            "and never asked, which is what leaves the agent to choose"
        );

        // And a new thread, which has no id until its agent assigns one, no
        // longer trips a NOT NULL constraint.
        let fresh = store
            .get_or_create("photon:any;-;+15551234567", "/home/wy/src", "claude")
            .unwrap();
        assert_eq!(fresh.session_id, None);
    }

    #[test]
    fn a_database_from_the_last_release_gains_the_new_column() {
        // The upgrade path an installed omatether actually takes: session_id is
        // already nullable, so the rebuild is skipped and the bare ADD COLUMN
        // is the only thing standing between a live database and a query that
        // names `requested_model`.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE threads (
                 key        TEXT PRIMARY KEY,
                 session_id TEXT,
                 cwd        TEXT NOT NULL,
                 updated_at INTEGER NOT NULL,
                 agent      TEXT NOT NULL DEFAULT 'claude',
                 auto       INTEGER NOT NULL DEFAULT 1,
                 model      TEXT
             );
             INSERT INTO threads (key, session_id, cwd, updated_at, agent, auto, model)
                 VALUES ('telegram:7', NULL, '/home/wy/Work', 1, 'claude', 1, 'claude-opus-5');",
        )
        .unwrap();

        let store = Store::from_conn(conn).unwrap();
        let existing = store.get("telegram:7").unwrap().unwrap();
        assert_eq!(existing.model.as_deref(), Some("claude-opus-5"));
        assert_eq!(existing.requested_model, None);
    }

    #[test]
    fn migrating_twice_is_harmless() {
        let conn = legacy_database();
        migrate(&conn).unwrap();
        migrate(&conn).unwrap();
        let store = Store::from_conn(conn).unwrap();
        assert!(store.get("telegram:5").unwrap().is_some());
    }

    #[test]
    fn unknown_thread_is_none() {
        let store = Store::in_memory().unwrap();
        assert!(store.get("telegram:missing").unwrap().is_none());
    }
}
