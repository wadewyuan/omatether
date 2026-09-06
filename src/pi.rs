//! Pi adapter.
//!
//! `pi -p --mode json` is the same shape as Codex: one process per turn,
//! emitting JSONL on stdout and exiting. Pi assigns its own session id, reported
//! in the first frame (`{"type":"session","id":…}`), and a later turn continues
//! the conversation with `--session <id>` — which is why [`AgentEvent::Ready`]
//! carries a session id the core persists.
//!
//! Pi cannot gate tools in this mode. `-p` runs its tools as it decides on
//! them; there is no approval callback to route to a human.
//! [`Agent::gates_tools`] reports false so the core says so up front, the same
//! deal as the Codex tier.
//!
//! Event vocabulary confirmed against a live `pi -p --mode json` run
//! (pi 0.84.4): `session`, `agent_start`/`agent_end`, `turn_start`/`turn_end`,
//! `message_start`/`message_update`/`message_end`, `agent_settled`, with
//! `message_update` carrying `assistantMessageEvent` deltas (`thinking_delta`,
//! `text_delta`, …). Completed tool calls appear in `message_end`'s content
//! array as `toolCall` entries.

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
    /// Pi's own session id, when resuming a conversation.
    pub session_id: Option<String>,
}

pub struct PiSession {
    cwd: PathBuf,
    /// Assigned by Pi in the first `session` frame; used to resume every turn
    /// after.
    session_id: Arc<Mutex<Option<String>>>,
    events: mpsc::Sender<AgentEvent>,
    busy: Arc<AtomicBool>,
    /// The running turn's process, so `cancel` has something to kill.
    child: Arc<Mutex<Option<tokio::process::Child>>>,
}

impl PiSession {
    pub fn new(config: Config) -> (Self, mpsc::Receiver<AgentEvent>) {
        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        (
            Self {
                cwd: config.cwd,
                session_id: Arc::new(Mutex::new(config.session_id)),
                events: tx,
                busy: Arc::new(AtomicBool::new(false)),
                child: Arc::new(Mutex::new(None)),
            },
            rx,
        )
    }
}

#[async_trait]
impl Agent for PiSession {
    fn name(&self) -> &'static str {
        "pi"
    }

    /// `-p` runs tools as Pi decides on them; there is no approval callback to
    /// route to a human.
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

        let resume = self.session_id.lock().await.clone();

        let mut command = Command::new("pi");
        command
            .arg("-p")
            .arg("--mode")
            .arg("json");
        // `--session <id>` continues the conversation; omitting it starts one.
        if let Some(id) = &resume {
            command.arg("--session").arg(id);
        }
        command
            // The prompt goes after `--` so a leading `-` in the text is not
            // read as a pi flag. One caveat, pi's own: `@file` tokens in a
            // message are expanded as file references, and `--` does not
            // disable that — a chat message that starts with `@` attaches the
            // file. Accepted for now; a phone prompt starting with `@` is rare
            // and the attachment is visible in the transcript.
            .arg("--")
            .arg(text)
            .current_dir(&self.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        // Leave the busy flag honest if the spawn fails — same lesson as the
        // Codex adapter: nothing else clears it when there is no reader task.
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                self.busy.store(false, Ordering::SeqCst);
                return Err(e).context("spawning `pi` — is it on PATH?");
            }
        };

        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take().expect("stderr was piped");
        *self.child.lock().await = Some(child);

        tokio::spawn(read_events(
            stdout,
            self.events.clone(),
            self.busy.clone(),
            self.session_id.clone(),
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
    session_id: Arc<Mutex<Option<String>>>,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    announce_ready: bool,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut ended = false;

    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        // The mise shim that resolves `pi` on this machine prints its
        // activation line ("mise … tools: pi@…") to *stdout* before exec, and
        // any wrapper noise would break serde the same way. Only braces can
        // open a JSON frame; everything else is logged, never surfaced.
        if !trimmed.starts_with('{') {
            tracing::info!(target: "switchboard::pi", "non-json line: {trimmed}");
            continue;
        }

        let frame: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(e) => {
                let _ = tx
                    .send(AgentEvent::Error {
                        message: format!("unparseable pi frame: {e}: {trimmed}"),
                    })
                    .await;
                continue;
            }
        };

        // Capture the session id before normalizing, so the core can persist it
        // and resume this conversation after a restart.
        if frame.get("type").and_then(Value::as_str) == Some("session") {
            if let Some(id) = frame.get("id").and_then(Value::as_str) {
                *session_id.lock().await = Some(id.to_string());
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

    // The process exits at the end of every turn, so a missing turn_end means
    // it died rather than finished — say so instead of leaving the chat
    // waiting forever.
    if !ended {
        let _ = tx
            .send(AgentEvent::TurnEnd {
                ok: false,
                detail: Some("pi exited without finishing the turn".into()),
            })
            .await;
    }

    *child.lock().await = None;
    busy.store(false, Ordering::SeqCst);
}

/// Turn one Pi frame into zero or more normalized events.
pub fn normalize(frame: &Value, announce_ready: bool) -> Vec<AgentEvent> {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or_default();

    match kind {
        "session" => {
            if !announce_ready {
                return Vec::new();
            }
            vec![AgentEvent::Ready {
                session_id: frame
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                model: None,
                cwd: frame.get("cwd").and_then(Value::as_str).map(String::from),
                tools: Vec::new(),
            }]
        }

        // Turn framing carries nothing a chat channel renders. The
        // `message_start` pairs echo what `message_update` streams and
        // `message_end` completes.
        "agent_start" | "turn_start" | "message_start" | "agent_end" | "agent_settled" => {
            Vec::new()
        }

        "turn_end" => vec![AgentEvent::TurnEnd {
            ok: true,
            detail: None,
        }],

        "turn_aborted" => vec![AgentEvent::TurnEnd {
            ok: false,
            detail: frame
                .get("reason")
                .and_then(Value::as_str)
                .map(String::from)
                .or_else(|| Some("turn aborted".into())),
        }],

        // Incremental message content. Text streams as deltas; thinking deltas
        // are dropped deliberately — the same call the Codex adapter makes,
        // where a chat channel wants to see the agent working, not its
        // reasoning verbatim.
        "message_update" => match frame
            .get("assistantMessageEvent")
            .and_then(|e| e.get("type"))
            .and_then(Value::as_str)
        {
            Some("text_delta") => vec![AgentEvent::TextDelta {
                text: frame
                    .get("assistantMessageEvent")
                    .and_then(|e| e.get("delta"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            }],
            _ => Vec::new(),
        },

        // The completed assistant message. Its text was already streamed as
        // deltas; what only exists here is the tool call list.
        "message_end" => frame
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
            .map(|content| {
                content
                    .iter()
                    .filter(|part| part.get("type").and_then(Value::as_str) == Some("toolCall"))
                    .map(|part| AgentEvent::ToolCall {
                        id: part
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        name: normalize_tool_name(
                            part.get("name").and_then(Value::as_str).unwrap_or_default(),
                        ),
                        input: tool_input(part),
                    })
                    .collect()
            })
            .unwrap_or_default(),

        _ => vec![AgentEvent::Unknown { raw: frame.clone() }],
    }
}

/// Map Pi's lowercase tool names onto the shared vocabulary Claude uses, so
/// the renderer and the gate list do not need per-agent knowledge. Unknown
/// names pass through as-is.
fn normalize_tool_name(name: &str) -> String {
    match name {
        "bash" => "Bash".to_string(),
        "read" => "Read".to_string(),
        "edit" => "Edit".to_string(),
        "write" => "Write".to_string(),
        other => other.to_string(),
    }
}

/// Pi's tool call arguments arrive as a flat object (`{"command": "ls"}` for
/// bash); pass them through unchanged.
fn tool_input(part: &Value) -> Value {
    part.get("arguments").cloned().unwrap_or(Value::Null)
}

async fn log_stderr(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "switchboard::pi", "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Frames below are verbatim from a real `pi -p --mode json` run
    // (pi 0.84.4, provider kimi-coding).

    #[test]
    fn session_frame_carries_the_resumable_id() {
        let frame = json!({
            "type": "session",
            "version": 3,
            "id": "01a074d0-4a3a-7dc5-958a-f6c240d26081",
            "timestamp": "2026-09-06T03:43:22.682Z",
            "cwd": "/tmp/pi-probe"
        });
        match normalize(&frame, true).as_slice() {
            [AgentEvent::Ready { session_id, cwd, .. }] => {
                assert_eq!(session_id, "01a074d0-4a3a-7dc5-958a-f6c240d26081");
                assert_eq!(cwd.as_deref(), Some("/tmp/pi-probe"));
            }
            other => panic!("expected Ready, got {other:?}"),
        }
        // On a resumed turn the id is not news.
        assert!(normalize(&frame, false).is_empty());
    }

    #[test]
    fn text_delta_streams() {
        let frame = json!({
            "type": "message_update",
            "assistantMessageEvent": { "type": "text_delta", "contentIndex": 1, "delta": "p" }
        });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::TextDelta { text }] => assert_eq!(text, "p"),
            other => panic!("expected TextDelta, got {other:?}"),
        }
    }

    #[test]
    fn thinking_deltas_are_dropped() {
        let frame = json!({
            "type": "message_update",
            "assistantMessageEvent": {
                "type": "thinking_delta",
                "contentIndex": 0,
                "delta": "The user wants"
            }
        });
        assert!(normalize(&frame, false).is_empty());
    }

    #[test]
    fn completed_tool_calls_map_onto_the_shared_vocabulary() {
        let frame = json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [
                    { "type": "toolCall",
                      "id": "tool_Vdbl8KqhUYdA0z4Sq2WXXD1t",
                      "name": "bash",
                      "arguments": { "command": "ls /tmp/pi-probe" } }
                ]
            }
        });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::ToolCall { name, input, .. }] => {
                assert_eq!(name, "Bash");
                assert_eq!(input["command"], "ls /tmp/pi-probe");
            }
            other => panic!("expected ToolCall, got {other:?}"),
        }
    }

    #[test]
    fn text_in_message_end_is_not_duplicated() {
        // The text was already streamed as deltas; only tool calls are news
        // in message_end.
        let frame = json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [
                    { "type": "text", "text": "Just one item." }
                ]
            }
        });
        assert!(normalize(&frame, false).is_empty());
    }

    #[test]
    fn turn_end_closes_the_turn() {
        let frame = json!({
            "type": "turn_end",
            "message": { "role": "assistant", "content": [] },
            "toolResults": []
        });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::TurnEnd { ok, detail }] => {
                assert!(ok);
                assert!(detail.is_none());
            }
            other => panic!("expected TurnEnd, got {other:?}"),
        }
    }

    #[test]
    fn turn_aborted_reports_failure() {
        let frame = json!({ "type": "turn_aborted", "reason": "interrupted" });
        match normalize(&frame, false).as_slice() {
            [AgentEvent::TurnEnd { ok, detail }] => {
                assert!(!ok);
                assert_eq!(detail.as_deref(), Some("interrupted"));
            }
            other => panic!("expected TurnEnd, got {other:?}"),
        }
    }

    #[test]
    fn turn_framing_is_not_rendered() {
        for frame in [
            json!({ "type": "agent_start" }),
            json!({ "type": "turn_start" }),
            json!({ "type": "agent_end" }),
            json!({ "type": "agent_settled" }),
        ] {
            assert!(normalize(&frame, false).is_empty(), "{frame}");
        }
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
