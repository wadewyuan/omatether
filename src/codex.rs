//! Codex adapter.
//!
//! `codex exec --json` is a different animal from Claude's persistent session:
//! one process per turn, emitting JSONL and exiting. Continuity comes from
//! `codex exec resume <thread_id>`, and the thread id is assigned by Codex on
//! the first turn rather than chosen by us — which is why [`AgentEvent::Ready`]
//! carries a session id the core persists.
//!
//! It also cannot gate tools. `--approve-for-me` routes approvals through an
//! automatic review inside a workspace-write sandbox; there is no callback for
//! a human. [`Agent::gates_tools`] reports false so the core can say so rather
//! than leaving someone waiting for a prompt that will never come.
//!
//! Event vocabulary confirmed against codex-cli 0.152.1: `thread.started`,
//! `turn.started`, `turn.completed`, `turn.failed`, `item.started`,
//! `item.updated`, `item.completed`, with item types `agent_message`,
//! `reasoning`, `command_execution`, `file_change`, `mcp_tool_call`,
//! `web_search`, `todo_list` and `error`.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::agent::Agent;
use crate::event::AgentEvent;

const EVENT_BUFFER: usize = 256;

pub struct Config {
    pub cwd: PathBuf,
    /// Codex's own thread id, when resuming.
    pub thread_id: Option<String>,
}

pub struct CodexSession {
    cwd: PathBuf,
    /// Assigned by Codex on the first turn; used to resume every turn after.
    thread_id: Arc<Mutex<Option<String>>>,
    events: mpsc::Sender<AgentEvent>,
    busy: Arc<AtomicBool>,
    /// The running turn's process, so `cancel` has something to kill.
    child: Arc<Mutex<Option<tokio::process::Child>>>,
}

impl CodexSession {
    pub fn new(config: Config) -> (Self, mpsc::Receiver<AgentEvent>) {
        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        (
            Self {
                cwd: config.cwd,
                thread_id: Arc::new(Mutex::new(config.thread_id)),
                events: tx,
                busy: Arc::new(AtomicBool::new(false)),
                child: Arc::new(Mutex::new(None)),
            },
            rx,
        )
    }
}

#[async_trait]
impl Agent for CodexSession {
    fn name(&self) -> &'static str {
        "codex"
    }

    /// `codex exec` approves its own tools inside a sandbox; there is no
    /// callback to route to a human.
    fn gates_tools(&self) -> bool {
        false
    }

    fn streams(&self) -> bool {
        true
    }

    fn is_busy(&self) -> bool {
        self.busy.load(Ordering::SeqCst)
    }

    async fn prompt(&mut self, text: &str) -> Result<()> {
        if self.busy.swap(true, Ordering::SeqCst) {
            bail!("a turn is already running — cancel it first");
        }

        let resume = self.thread_id.lock().await.clone();

        let mut command = Command::new("codex");
        command.arg("exec");
        // `resume <id> <prompt>` continues the conversation; a bare `exec
        // <prompt>` starts one.
        if let Some(id) = &resume {
            command.arg("resume").arg(id);
        }
        command
            .arg("--json")
            // The only non-interactive approval mode there is.
            .arg("--approve-for-me")
            .arg("--")
            .arg(text)
            .current_dir(&self.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Leave the busy flag honest if the spawn fails. Without this one
        // missing `codex` binary wedges the thread on "a turn is already
        // running" until the service restarts, because nothing else ever
        // clears the flag — only the reader task does, and there is no reader
        // task when there is no process.
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                self.busy.store(false, Ordering::SeqCst);
                return Err(e).context("spawning `codex` — is it on PATH?");
            }
        };

        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take().expect("stderr was piped");
        *self.child.lock().await = Some(child);

        tokio::spawn(read_events(
            stdout,
            self.events.clone(),
            self.busy.clone(),
            self.thread_id.clone(),
            self.child.clone(),
            resume.is_none(),
        ));
        tokio::spawn(log_stderr(stderr));

        Ok(())
    }

    async fn cancel(&mut self) -> Result<()> {
        if let Some(child) = self.child.lock().await.as_mut() {
            child.kill().await.ok();
        }
        self.busy.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.cancel().await
    }
}

async fn read_events(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<AgentEvent>,
    busy: Arc<AtomicBool>,
    thread_id: Arc<Mutex<Option<String>>>,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    announce_ready: bool,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut ended = false;

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        let frame: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(e) => {
                let _ = tx
                    .send(AgentEvent::Error {
                        message: format!("unparseable codex frame: {e}: {line}"),
                    })
                    .await;
                continue;
            }
        };

        // Capture the thread id before normalizing, so the core can persist it
        // and resume this conversation after a restart.
        if frame.get("type").and_then(Value::as_str) == Some("thread.started") {
            if let Some(id) = frame.get("thread_id").and_then(Value::as_str) {
                *thread_id.lock().await = Some(id.to_string());
            }
        }

        for event in normalize(&frame, announce_ready) {
            if matches!(event, AgentEvent::TurnEnd { .. }) {
                ended = true;
            }
            if tx.send(event).await.is_err() {
                return;
            }
        }
    }

    // The process exits at the end of every turn, so a missing turn.completed
    // means it died rather than finished — say so instead of leaving the chat
    // waiting forever.
    if !ended {
        let _ = tx
            .send(AgentEvent::TurnEnd {
                ok: false,
                detail: Some("codex exited without completing the turn".into()),
            })
            .await;
    }

    *child.lock().await = None;
    busy.store(false, Ordering::SeqCst);
}

/// Turn one Codex frame into zero or more normalized events.
pub fn normalize(frame: &Value, announce_ready: bool) -> Vec<AgentEvent> {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or_default();

    match kind {
        "thread.started" => {
            if !announce_ready {
                return Vec::new();
            }
            vec![AgentEvent::Ready {
                session_id: frame
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                model: None,
                cwd: None,
                tools: Vec::new(),
            }]
        }

        // Turn framing carries nothing a chat channel renders.
        "turn.started" => Vec::new(),

        "turn.completed" => vec![AgentEvent::TurnEnd {
            ok: true,
            detail: None,
        }],

        "turn.failed" => vec![AgentEvent::TurnEnd {
            ok: false,
            detail: frame
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(String::from),
        }],

        // Codex reports an item once it is done rather than streaming its
        // innards, so only the completed form carries content.
        "item.completed" => match frame.get("item") {
            Some(item) => item_events(item),
            None => vec![AgentEvent::Unknown { raw: frame.clone() }],
        },

        // Transport-level retries and warnings. Real, but not a turn outcome.
        "error" => vec![AgentEvent::Error {
            message: frame
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("codex error")
                .to_string(),
        }],

        "item.started" | "item.updated" => Vec::new(),

        _ => vec![AgentEvent::Unknown { raw: frame.clone() }],
    }
}

fn item_events(item: &Value) -> Vec<AgentEvent> {
    let text_at = |key: &str| item.get(key).and_then(Value::as_str).map(String::from);
    let id = text_at("id").unwrap_or_default();

    match item.get("type").and_then(Value::as_str).unwrap_or_default() {
        "agent_message" => {
            // The field is `text`; fall back to `message` rather than dropping
            // the reply if that ever changes.
            let text = text_at("text").or_else(|| text_at("message"));
            match text {
                Some(text) => vec![AgentEvent::Text { text }],
                None => vec![AgentEvent::Unknown { raw: item.clone() }],
            }
        }

        "reasoning" => vec![AgentEvent::Thinking {
            text: text_at("text").unwrap_or_default(),
        }],

        "command_execution" => vec![AgentEvent::ToolCall {
            id,
            name: "Bash".to_string(),
            input: serde_json::json!({ "command": text_at("command").unwrap_or_default() }),
        }],

        "file_change" => vec![AgentEvent::ToolCall {
            id,
            name: "Edit".to_string(),
            input: item.get("changes").cloned().unwrap_or(Value::Null),
        }],

        "mcp_tool_call" => vec![AgentEvent::ToolCall {
            id,
            name: text_at("server").unwrap_or_else(|| "mcp".to_string()),
            input: item.get("arguments").cloned().unwrap_or(Value::Null),
        }],

        "web_search" => vec![AgentEvent::ToolCall {
            id,
            name: "WebSearch".to_string(),
            input: serde_json::json!({ "query": text_at("query").unwrap_or_default() }),
        }],

        "error" => vec![AgentEvent::Error {
            message: text_at("message").unwrap_or_else(|| "codex item error".into()),
        }],

        // todo_list and anything newer.
        _ => vec![AgentEvent::Unknown { raw: item.clone() }],
    }
}

async fn log_stderr(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "omatether::codex", "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Frames below are verbatim from a real `codex exec --json` run
    // (codex-cli 0.152.1).

    #[test]
    fn thread_started_carries_the_resumable_id() {
        let frame = json!({
            "type": "thread.started",
            "thread_id": "01a07410-3987-7630-9fee-53881cc51d72"
        });
        match normalize(&frame, true).as_slice() {
            [AgentEvent::Ready { session_id, .. }] => {
                assert_eq!(session_id, "01a07410-3987-7630-9fee-53881cc51d72");
            }
            other => panic!("expected Ready, got {other:?}"),
        }
        // On a resumed turn it is not news.
        assert!(normalize(&frame, false).is_empty());
    }

    #[test]
    fn turn_failed_reports_the_reason() {
        let frame = json!({
            "type": "turn.failed",
            "error": { "message": "unexpected status 401 Unauthorized" }
        });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::TurnEnd { ok, detail }] => {
                assert!(!ok);
                assert!(detail.as_deref().unwrap().contains("401"));
            }
            other => panic!("expected TurnEnd, got {other:?}"),
        }
    }

    #[test]
    fn error_items_surface_as_errors() {
        let frame = json!({
            "type": "item.completed",
            "item": {
                "id": "item_0",
                "type": "error",
                "message": "Falling back from WebSockets to HTTPS transport."
            }
        });
        assert!(matches!(
            normalize(&frame, false).as_slice(),
            [AgentEvent::Error { .. }]
        ));
    }

    #[test]
    fn agent_message_becomes_text() {
        let frame = json!({
            "type": "item.completed",
            "item": { "id": "item_1", "type": "agent_message", "text": "pong" }
        });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::Text { text }] => assert_eq!(text, "pong"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn command_execution_maps_onto_the_shared_tool_vocabulary() {
        let frame = json!({
            "type": "item.completed",
            "item": {
                "id": "item_2", "type": "command_execution",
                "command": "ls -la", "exit_code": 0
            }
        });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::ToolCall { name, input, .. }] => {
                // Normalized to the same name Claude uses, so the renderer and
                // the gate list do not need per-agent knowledge.
                assert_eq!(name, "Bash");
                assert_eq!(input["command"], "ls -la");
            }
            other => panic!("expected ToolCall, got {other:?}"),
        }
    }

    #[test]
    fn turn_framing_is_not_rendered() {
        assert!(normalize(&json!({ "type": "turn.started" }), false).is_empty());
        assert!(normalize(&json!({ "type": "item.started" }), false).is_empty());
    }

    #[test]
    fn unmodelled_frames_survive_as_unknown() {
        let frame = json!({ "type": "something.new", "payload": 1 });
        assert!(matches!(
            normalize(&frame, false).as_slice(),
            [AgentEvent::Unknown { .. }]
        ));
    }
}
