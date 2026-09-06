//! Claude Code adapter.
//!
//! Runs `claude` in bidirectional `stream-json` mode: one long-lived process
//! per session, prompts written to its stdin as they arrive, events read off
//! its stdout. Permission questions arrive on the same pipe as
//! `control_request` frames, which is why this needs no MCP server and no
//! `--permission-prompt-tool`.

pub mod wire;

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::agent::Agent;
use crate::claude::wire::PermissionKind;
use crate::event::{AgentEvent, Decision};

/// Permission questions awaiting an answer, and which dialect each was asked
/// in. Shared with the reader task, which is where the questions arrive.
type PendingMap = Arc<std::sync::Mutex<HashMap<String, PermissionKind>>>;

/// How many events may queue before the reader task applies backpressure to
/// the agent's stdout. Generous: a tool-heavy turn is bursty.
const EVENT_BUFFER: usize = 256;

/// Session-identity variables Claude Code exports to its own children.
///
/// If switchboard is itself launched from inside a Claude Code session, the
/// agent we spawn inherits these, decides it is a nested session, and resolves
/// its permission mode from the parent instead of from our `--permission-mode`
/// flag — silently, reporting `default` in the init frame. Scrubbing them also
/// makes a service's agent environment deterministic rather than a function of
/// whatever happened to start it.
///
/// Deliberately a fixed list, not a `CLAUDE*` prefix sweep: variables like
/// `CLAUDE_CODE_USE_BEDROCK` are legitimate operator configuration.
const INHERITED_SESSION_VARS: &[&str] = &[
    "CLAUDECODE",
    "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_EXECPATH",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_CHILD_SESSION",
    "CLAUDE_CODE_BRIDGE_SESSION_ID",
    "CLAUDE_CODE_MESSAGING_SOCKET",
    "CLAUDE_CODE_MESSAGING_TOKEN",
    "CLAUDE_PID",
    "CLAUDE_EFFORT",
];

/// Our handle for the PreToolUse hook registered during the handshake.
const PRE_TOOL_USE_CALLBACK: &str = "switchboard-pretooluse";

/// Request id for the handshake.
///
/// Fixed rather than taken from the counter because the reader task has to be
/// watching for the answer before the question is written.
const INIT_REQUEST_ID: &str = "switchboard-init";

/// How long to wait for the agent to confirm the gate is installed.
///
/// Measured against the installed CLI: the answer arrives in about 900ms,
/// almost all of it node starting up. This is fifteen times that, so a loaded
/// machine is not mistaken for a missing gate.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// The reader task's half of the handshake: it watches for the answer to
/// [`INIT_REQUEST_ID`] and reports it back to `spawn`.
struct Handshake {
    reply: tokio::sync::oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Working directory for the session. This is the "which repo" answer,
    /// and it is explicit state — never inferred.
    pub cwd: PathBuf,
    /// `Some` to pick up a conversation from an earlier turn or an earlier
    /// process (e.g. after a service restart); `None` to start a fresh one.
    ///
    /// These are not interchangeable flags on the CLI: `--session-id` names a
    /// *new* conversation and errors with "already in use" if that id already
    /// has a transcript, while resuming an existing one needs `--resume`.
    /// Conflating them is what broke every returning thread the first time
    /// this adapter tried to resume one.
    pub session_id: Option<Uuid>,
    /// `manual` makes the agent ask before every tool, which is what exercises
    /// the permission round-trip. `auto` approves most things itself.
    pub permission_mode: String,
    /// Print every raw frame to stderr as it arrives.
    pub raw: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cwd: PathBuf::from("."),
            session_id: None,
            permission_mode: "manual".to_string(),
            raw: false,
        }
    }
}

pub struct ClaudeSession {
    child: Child,
    stdin: ChildStdin,
    session_id: String,
    /// Shared with the reader task, which clears it when a turn ends.
    busy: Arc<AtomicBool>,
    pending: PendingMap,
    next_request: u64,
}

impl ClaudeSession {
    /// Spawn the agent and start pumping its output.
    ///
    /// The receiver is the only way events leave this session; dropping it
    /// stops the reader task at its next send.
    pub async fn spawn(config: Config) -> Result<(Self, mpsc::Receiver<AgentEvent>)> {
        // `--session-id` claims a new id; `--resume` picks up an existing one.
        // Passing a previously-used id to `--session-id` is rejected with
        // "already in use", so which flag to use depends on whether this is a
        // conversation's first turn or a later one.
        let session_id = config.session_id.unwrap_or_else(Uuid::new_v4);

        let mut command = Command::new("claude");
        command
            .arg("--print")
            .arg("--verbose")
            .args(["--output-format", "stream-json"])
            .args(["--input-format", "stream-json"])
            .arg("--include-partial-messages");
        if config.session_id.is_some() {
            command.args(["--resume", &session_id.to_string()]);
        } else {
            command.args(["--session-id", &session_id.to_string()]);
        }
        command
            .args(["--permission-mode", &config.permission_mode])
            .current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Answers "what happens to a running turn when the service
            // restarts": the child dies with us, and --session-id resumes the
            // conversation on the way back up.
            .kill_on_drop(true);

        for var in INHERITED_SESSION_VARS {
            command.env_remove(var);
        }

        let mut child = command
            .spawn()
            .context("failed to spawn `claude` — is it on PATH?")?;

        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take().expect("stderr was piped");

        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let busy = Arc::new(AtomicBool::new(false));
        let pending: PendingMap = Arc::new(std::sync::Mutex::new(HashMap::new()));

        // Started before the handshake is written, so the answer cannot arrive
        // before there is anything listening for it.
        let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(read_events(
            stdout,
            tx.clone(),
            busy.clone(),
            pending.clone(),
            config.raw,
            Some(Handshake { reply: handshake_tx }),
        ));
        tokio::spawn(log_stderr(stderr));

        let mut session = Self {
            child,
            stdin,
            session_id: session_id.to_string(),
            busy,
            pending,
            next_request: 0,
        };

        session.initialize().await?;

        // Block on the answer. A session whose gate failed to install is a
        // session that runs every tool without asking, and it looks exactly
        // like a well-behaved one until the moment it matters: no
        // PermissionRequest ever arrives, which reads as "the agent didn't need
        // permission for that" rather than "there is no gate". The whole pitch
        // is a shell you approve from your phone, so this is a startup
        // assertion, not a hope.
        //
        // Dropping `session` on the way out kills the child: it was spawned
        // with kill_on_drop.
        let confirmed = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake_rx)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "`claude` did not answer switchboard's handshake within {}s, so \
                     there is no confirmed tool gate — refusing to start a session \
                     that would run tools unasked",
                    HANDSHAKE_TIMEOUT.as_secs()
                )
            })?
            .map_err(|_| {
                anyhow::anyhow!(
                    "`claude` exited during switchboard's handshake — see the \
                     switchboard::claude log for what it said"
                )
            })?;

        if let Err(reason) = confirmed {
            bail!(
                "`claude` refused switchboard's PreToolUse hook, so no tool call \
                 would ever be routed here for a decision: {reason}"
            );
        }

        Ok((session, rx))
    }

    /// The handshake, which is also where the tool gate is installed.
    ///
    /// `--permission-mode manual` does not survive `--print`: the CLI reports
    /// `default` in its init frame and approves tools itself. Registering a
    /// `PreToolUse` hook here is the gate that does fire — the CLI says as
    /// much in its own diagnostics ("To gate every tool call, use a PreToolUse
    /// hook"). Each tool call then arrives as a `hook_callback` control
    /// request, which we answer as a permission decision.
    async fn initialize(&mut self) -> Result<()> {
        let frame = json!({
            "type": "control_request",
            "request_id": INIT_REQUEST_ID,
            "request": {
                "subtype": "initialize",
                // Shape per the CLI's own validation error: hook events map to
                // arrays of matchers, each a string matcher plus an array of
                // callback ids.
                "hooks": {
                    "PreToolUse": [{
                        "matcher": "*",
                        "hookCallbackIds": [PRE_TOOL_USE_CALLBACK],
                    }]
                },
            }
        });
        self.write_frame(&frame).await
    }

    async fn write_frame(&mut self, frame: &Value) -> Result<()> {
        let mut line = serde_json::to_vec(frame)?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .context("agent stdin closed")?;
        self.stdin.flush().await?;
        Ok(())
    }

    fn request_id(&mut self) -> String {
        self.next_request += 1;
        format!("switchboard-{}", self.next_request)
    }
}

#[async_trait]
impl Agent for ClaudeSession {
    fn name(&self) -> &'static str {
        "claude"
    }

    /// The PreToolUse hook installed during the handshake routes every tool
    /// call out for a decision.
    fn gates_tools(&self) -> bool {
        true
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

        let frame = json!({
            "type": "user",
            "message": { "role": "user", "content": [{ "type": "text", "text": text }] },
            "parent_tool_use_id": null,
            "session_id": self.session_id,
        });

        // Leave the busy flag honest if the write fails.
        if let Err(e) = self.write_frame(&frame).await {
            self.busy.store(false, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    async fn decide(&mut self, request_id: &str, decision: Decision) -> Result<()> {
        // Answer in the dialect the question was asked in.
        let kind = self
            .pending
            .lock()
            .expect("pending map poisoned")
            .remove(request_id)
            .unwrap_or(PermissionKind::PreToolUseHook);

        let payload = match kind {
            PermissionKind::CanUseTool => decision.to_can_use_tool_payload(),
            PermissionKind::PreToolUseHook => decision.to_hook_payload(),
        };

        let frame = json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": payload,
            }
        });
        self.write_frame(&frame).await
    }

    async fn cancel(&mut self) -> Result<()> {
        let id = self.request_id();
        let frame = json!({
            "type": "control_request",
            "request_id": id,
            "request": { "subtype": "interrupt" }
        });
        self.write_frame(&frame).await?;
        self.busy.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        // Closing stdin is the polite exit; kill_on_drop is the backstop.
        self.stdin.shutdown().await.ok();
        match tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait()).await {
            Ok(status) => {
                status?;
            }
            Err(_) => {
                tracing::warn!("agent did not exit in 5s, killing");
                self.child.kill().await.ok();
            }
        }
        Ok(())
    }
}

async fn read_events(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<AgentEvent>,
    busy: Arc<AtomicBool>,
    pending: PendingMap,
    raw: bool,
    mut handshake: Option<Handshake>,
) {
    let mut lines = BufReader::new(stdout).lines();
    let mut ended = false;
    let mut exit_reason = "agent exited".to_string();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(e) => {
                exit_reason = format!("reading agent stdout: {e}");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let frame: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let _ = tx
                    .send(AgentEvent::Error {
                        message: format!("unparseable frame: {e}: {line}"),
                    })
                    .await;
                continue;
            }
        };

        if raw {
            eprintln!("<<< {}", serde_json::to_string(&frame).unwrap_or(line));
        }

        // The handshake's answer is the first frame the CLI sends, before even
        // system/init. Hand it to whoever is waiting on it, once.
        if let Some(waiting) = handshake.take() {
            match handshake_outcome(&frame, INIT_REQUEST_ID) {
                Some(outcome) => {
                    let _ = waiting.reply.send(outcome);
                }
                None => handshake = Some(waiting),
            }
        }

        for event in wire::normalize(&frame) {
            if matches!(event, AgentEvent::TurnEnd { .. }) {
                busy.store(false, Ordering::SeqCst);
                ended = true;
            }
            if let AgentEvent::PermissionRequest { request_id, .. } = &event {
                if let Some(kind) = wire::permission_kind(&frame) {
                    pending
                        .lock()
                        .expect("pending map poisoned")
                        .insert(request_id.clone(), kind);
                }
            }
            if tx.send(event).await.is_err() {
                return;
            }
        }
    }

    busy.store(false, Ordering::SeqCst);

    // The process is long-lived across turns, so its stdout closing mid-turn
    // is a crash, not a normal end — say so as a failed turn rather than a
    // bare `Error`, which a non-editable channel (nothing to append text to
    // once the turn is already "finished") would otherwise never flush.
    if !ended {
        let _ = tx
            .send(AgentEvent::TurnEnd {
                ok: false,
                detail: Some(exit_reason),
            })
            .await;
    }
}

/// Whether this frame answers the handshake, and what it said.
///
/// `None` means "not the answer — keep waiting".
fn handshake_outcome(frame: &Value, request_id: &str) -> Option<Result<(), String>> {
    if frame.get("type").and_then(Value::as_str) != Some("control_response") {
        return None;
    }

    let response = frame.get("response")?;
    let answered = response.get("request_id").and_then(Value::as_str);

    // An error-shaped response that names no request is still ours: the
    // handshake is the only thing outstanding this early, and failing closed on
    // an unattributed error beats waiting out the timeout to reach the same
    // conclusion with a vaguer message.
    let is_error = response.get("subtype").and_then(Value::as_str) == Some("error");
    if answered != Some(request_id) && !(answered.is_none() && is_error) {
        return None;
    }

    match response.get("subtype").and_then(Value::as_str) {
        Some("success") => Some(Ok(())),
        Some("error") => Some(Err(response
            .get("error")
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unspecified".into()))),
        other => Some(Err(format!(
            "unexpected answer to the handshake: subtype {}",
            other.unwrap_or("(none)")
        ))),
    }
}

async fn log_stderr(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "switchboard::claude", "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim from the installed CLI, answering the handshake this adapter
    /// sends. It is the first frame out, ahead of system/init, and arrives in
    /// about 900ms. The real `response` object carries the full command and
    /// agent inventory; it is elided here because nothing reads it.
    fn success() -> Value {
        json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": "switchboard-init",
                "response": { "commands": [], "roots": [] }
            }
        })
    }

    #[test]
    fn a_successful_handshake_confirms_the_gate() {
        assert_eq!(handshake_outcome(&success(), INIT_REQUEST_ID), Some(Ok(())));
    }

    #[test]
    fn a_refused_hook_registration_is_reported_not_ignored() {
        // The shape the CLI returned when the hook schema was wrong — the error
        // that taught us the schema in the first place. Before this, it was
        // parsed into an event nothing acted on, and the session carried on
        // with no gate at all.
        let frame = json!({
            "type": "control_response",
            "response": {
                "subtype": "error",
                "request_id": "switchboard-init",
                "error": "hooks.PreToolUse must be an array of matchers"
            }
        });
        match handshake_outcome(&frame, INIT_REQUEST_ID) {
            Some(Err(reason)) => assert!(reason.contains("PreToolUse")),
            other => panic!("expected the reason to survive, got {other:?}"),
        }
    }

    #[test]
    fn an_unattributed_error_still_fails_closed() {
        // No request_id to match on. The handshake is the only thing
        // outstanding this early, so this is ours and the answer is "no gate".
        let frame = json!({
            "type": "control_response",
            "response": { "subtype": "error", "error": "unsupported" }
        });
        assert!(matches!(
            handshake_outcome(&frame, INIT_REQUEST_ID),
            Some(Err(_))
        ));
    }

    #[test]
    fn other_traffic_is_not_mistaken_for_the_answer() {
        // Frames that arrive around the handshake, none of which answer it.
        let init = json!({ "type": "system", "subtype": "init", "session_id": "abc" });
        assert_eq!(handshake_outcome(&init, INIT_REQUEST_ID), None);

        // The reply to a later interrupt, which must not be read as the
        // handshake's — the wait would otherwise end on the wrong frame.
        let interrupt = json!({
            "type": "control_response",
            "response": { "subtype": "success", "request_id": "switchboard-1" }
        });
        assert_eq!(handshake_outcome(&interrupt, INIT_REQUEST_ID), None);
    }
}
