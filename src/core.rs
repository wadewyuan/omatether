//! The router: chat threads on one side, agent sessions on the other.
//!
//! One thread maps to one agent session, and one turn runs at a time within it.
//! Everything vendor-specific stays behind the two seams — this module talks
//! only in [`Inbound`] and [`AgentEvent`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::agent::Agent;
use crate::channel::telegram::Telegram;
use crate::channel::{Channel, Inbound, InboundKind, ThreadKey};
use crate::claude::{ClaudeSession, Config};
use crate::command::{self, Command};
use crate::event::{AgentEvent, Decision};
use crate::render::TurnRenderer;
use crate::store::Store;

/// How often a streaming turn is pushed to the chat. Comfortably inside
/// Telegram's roughly-one-per-second-per-chat budget, and slow enough that a
/// reader is not watching text reflow constantly.
const FLUSH_INTERVAL: Duration = Duration::from_millis(1500);

/// Tools that require a human decision. Everything else is approved by
/// switchboard without bothering anyone.
///
/// A gate on every tool is unusable from a phone — the agent reads a dozen
/// files before it does anything consequential. Gating what writes or executes
/// keeps the prompts meaningful.
const GATED_TOOLS: &[&str] = &["Bash", "Write", "Edit", "NotebookEdit"];

struct Thread {
    session: ClaudeSession,
    renderer: TurnRenderer,
    /// The permission question waiting on a human, if any. At most one: the
    /// turn is blocked on it anyway.
    pending: Option<PendingPermission>,
}

struct PendingPermission {
    request_id: String,
    tool: String,
    message_id: String,
}

pub struct Core {
    store: Store,
    channel: Arc<Telegram>,
    default_cwd: PathBuf,
    threads: HashMap<ThreadKey, Thread>,
    agent_tx: mpsc::Sender<(ThreadKey, AgentEvent)>,
    agent_rx: mpsc::Receiver<(ThreadKey, AgentEvent)>,
}

impl Core {
    pub fn new(store: Store, channel: Arc<Telegram>, default_cwd: PathBuf) -> Self {
        let (agent_tx, agent_rx) = mpsc::channel(256);
        Self {
            store,
            channel,
            default_cwd,
            threads: HashMap::new(),
            agent_tx,
            agent_rx,
        }
    }

    pub async fn run(mut self, mut inbound: mpsc::Receiver<Inbound>) -> Result<()> {
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(message) = inbound.recv() => {
                    if let Err(e) = self.on_inbound(message).await {
                        tracing::error!("handling inbound: {e:#}");
                    }
                }

                Some((key, event)) = self.agent_rx.recv() => {
                    if let Err(e) = self.on_agent_event(&key, event).await {
                        tracing::error!("handling agent event: {e:#}");
                    }
                }

                _ = ticker.tick() => {
                    if let Err(e) = self.flush_all().await {
                        tracing::error!("flushing: {e:#}");
                    }
                }

                else => break,
            }
        }
        Ok(())
    }

    // ---- inbound -------------------------------------------------------

    async fn on_inbound(&mut self, message: Inbound) -> Result<()> {
        // The audit line for a service that can run shell commands on request.
        tracing::info!(
            thread = %message.thread,
            user = %message.user_id,
            "inbound"
        );

        match message.kind {
            InboundKind::Decision { allow, token } => {
                self.on_decision(&message.thread, allow, &token).await
            }
            InboundKind::Text(text) => match command::parse(&text) {
                Command::Help => self.say(&message.thread, command::HELP).await,
                Command::New => self.on_new(&message.thread).await,
                Command::Stop => self.on_stop(&message.thread).await,
                Command::Status => self.on_status(&message.thread).await,
                Command::Cd(path) => self.on_cd(&message.thread, &path).await,
                Command::Allow => self.decide(&message.thread, Decision::allow(), None).await,
                Command::Deny(why) => {
                    self.decide(&message.thread, Decision::deny(why), None).await
                }
                Command::Prompt(text) => self.on_prompt(&message.thread, &text).await,
            },
        }
    }

    async fn on_prompt(&mut self, key: &ThreadKey, text: &str) -> Result<()> {
        self.ensure_thread(key).await?;
        let thread = self.threads.get_mut(key).expect("just ensured");

        // One turn at a time. Rejecting is more predictable from a phone than
        // queueing, and far simpler than interleaving.
        if let Err(e) = thread.session.prompt(text).await {
            let note = format!("Busy — {e}. Send /stop to interrupt.");
            return self.say(key, &note).await;
        }

        thread.renderer.reset();
        Ok(())
    }

    async fn on_new(&mut self, key: &ThreadKey) -> Result<()> {
        if let Some(mut thread) = self.threads.remove(key) {
            thread.session.shutdown().await.ok();
        }

        let mut state = self
            .store
            .get_or_create(&key.to_string(), &self.default_cwd.to_string_lossy())?;
        state.session_id = uuid::Uuid::new_v4();
        self.store.put(&state)?;

        self.say(key, &format!("New session in {}.", state.cwd)).await
    }

    async fn on_stop(&mut self, key: &ThreadKey) -> Result<()> {
        match self.threads.get_mut(key) {
            Some(thread) => {
                thread.session.cancel().await?;
                thread.renderer.reset();
                thread.pending = None;
                self.say(key, "Stopped.").await
            }
            None => self.say(key, "Nothing running.").await,
        }
    }

    async fn on_status(&mut self, key: &ThreadKey) -> Result<()> {
        let state = self
            .store
            .get_or_create(&key.to_string(), &self.default_cwd.to_string_lossy())?;

        let (running, awaiting) = match self.threads.get(key) {
            Some(thread) => (
                thread.session.is_busy(),
                thread.pending.as_ref().map(|p| p.tool.clone()),
            ),
            None => (false, None),
        };

        let mut note = format!(
            "agent    claude\ndir      {}\nsession  {}\nstate    {}",
            state.cwd,
            state.session_id,
            if running { "turn running" } else { "idle" }
        );
        if let Some(tool) = awaiting {
            note.push_str(&format!("\nwaiting  decision on {tool}"));
        }
        self.say(key, &note).await
    }

    async fn on_cd(&mut self, key: &ThreadKey, path: &str) -> Result<()> {
        if path.is_empty() {
            return self.say(key, "Usage: /cd <path>").await;
        }

        let expanded = expand_home(path);
        if !expanded.is_dir() {
            let note = format!("Not a directory: {}", expanded.display());
            return self.say(key, &note).await;
        }

        // The working directory is fixed when the agent process starts, so
        // changing it means a new session. Simpler than trying to move a live
        // conversation, and the old one is still resumable by its id.
        if let Some(mut thread) = self.threads.remove(key) {
            thread.session.shutdown().await.ok();
        }

        let mut state = self
            .store
            .get_or_create(&key.to_string(), &self.default_cwd.to_string_lossy())?;
        state.cwd = expanded.to_string_lossy().to_string();
        state.session_id = uuid::Uuid::new_v4();
        self.store.put(&state)?;

        let note = format!("Working in {}. New session.", state.cwd);
        self.say(key, &note).await
    }

    async fn on_decision(&mut self, key: &ThreadKey, allow: bool, token: &str) -> Result<()> {
        let decision = if allow {
            Decision::allow()
        } else {
            Decision::deny("denied from chat")
        };
        self.decide(key, decision, Some(token)).await
    }

    /// Answer the thread's outstanding permission question.
    async fn decide(
        &mut self,
        key: &ThreadKey,
        decision: Decision,
        token: Option<&str>,
    ) -> Result<()> {
        let pending = match self.threads.get_mut(key).and_then(|t| t.pending.take()) {
            Some(pending) => pending,
            None => {
                if let Some(token) = token {
                    self.channel.ack_decision(token, "Nothing pending").await.ok();
                }
                return self.say(key, "Nothing waiting for a decision.").await;
            }
        };

        let allowed = matches!(decision, Decision::Allow { .. });

        let thread = self.threads.get_mut(key).expect("pending implies a thread");
        thread.session.decide(&pending.request_id, decision).await?;

        let verdict = if allowed { "Allowed" } else { "Denied" };
        if let Some(token) = token {
            self.channel.ack_decision(token, verdict).await.ok();
        }

        // Rewrite the question so the buttons are no longer live and the
        // transcript records what was decided.
        let settled = format!("{} {}", verdict, pending.tool);
        self.channel
            .edit(key, &pending.message_id, &settled)
            .await
            .ok();

        Ok(())
    }

    // ---- agent events --------------------------------------------------

    async fn on_agent_event(&mut self, key: &ThreadKey, event: AgentEvent) -> Result<()> {
        match &event {
            AgentEvent::PermissionRequest {
                request_id,
                tool,
                input,
                ..
            } => {
                return self
                    .on_permission(key, request_id.clone(), tool.clone(), input.clone())
                    .await;
            }

            AgentEvent::RateLimit {
                five_hour,
                seven_day,
            } => {
                // Only worth an interruption once it could plausibly cut a
                // reply short.
                let worst = five_hour.unwrap_or(0.0).max(seven_day.unwrap_or(0.0));
                if worst > 0.9 {
                    let note = format!("Heads up: {:.0}% of a usage window used.", worst * 100.0);
                    return self.say(key, &note).await;
                }
                return Ok(());
            }

            AgentEvent::Unknown { raw } => {
                tracing::debug!("unmodelled frame: {raw}");
                return Ok(());
            }

            _ => {}
        }

        let finished = match self.threads.get_mut(key) {
            Some(thread) => {
                thread.renderer.apply(&event);
                thread.renderer.is_finished()
            }
            None => return Ok(()),
        };

        // A finished turn is flushed immediately rather than waiting out the
        // debounce — the last word should not arrive a second and a half late.
        if finished {
            self.flush(key).await?;
            if let Some(thread) = self.threads.get_mut(key) {
                thread.renderer.reset();
            }
        }
        Ok(())
    }

    async fn on_permission(
        &mut self,
        key: &ThreadKey,
        request_id: String,
        tool: String,
        input: serde_json::Value,
    ) -> Result<()> {
        // Auto-approve anything that only reads. Asking about every file the
        // agent opens trains you to tap Allow without reading it.
        if !GATED_TOOLS.iter().any(|t| *t == tool) {
            if let Some(thread) = self.threads.get_mut(key) {
                thread.session.decide(&request_id, Decision::allow()).await?;
            }
            return Ok(());
        }

        // Show the pending work before asking, so the question has context.
        self.flush(key).await?;

        let detail = serde_json::to_string_pretty(&input).unwrap_or_default();
        let question = format!("Run {tool}?\n\n{detail}");
        let message_id = self.channel.ask_permission(key, &question).await?;

        if let Some(thread) = self.threads.get_mut(key) {
            thread.pending = Some(PendingPermission {
                request_id,
                tool,
                message_id,
            });
        }
        Ok(())
    }

    // ---- output --------------------------------------------------------

    async fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<ThreadKey> = self.threads.keys().cloned().collect();
        for key in keys {
            self.flush(&key).await?;
        }
        Ok(())
    }

    /// Push a thread's pending text, editing the turn's message in place.
    async fn flush(&mut self, key: &ThreadKey) -> Result<()> {
        let (text, message_id) = match self.threads.get_mut(key) {
            Some(thread) => match thread.renderer.take_pending() {
                Some(text) => (text, thread.renderer.message_id.clone()),
                None => return Ok(()),
            },
            None => return Ok(()),
        };

        match message_id {
            Some(id) => self.channel.edit(key, &id, &text).await?,
            None => {
                let id = self.channel.send(key, &text).await?;
                if let Some(thread) = self.threads.get_mut(key) {
                    thread.renderer.message_id = Some(id);
                }
            }
        }
        Ok(())
    }

    /// Post a standalone note, outside any turn's message.
    async fn say(&self, key: &ThreadKey, text: &str) -> Result<()> {
        self.channel.send(key, text).await?;
        Ok(())
    }

    // ---- sessions ------------------------------------------------------

    async fn ensure_thread(&mut self, key: &ThreadKey) -> Result<()> {
        if self.threads.contains_key(key) {
            return Ok(());
        }

        let state = self
            .store
            .get_or_create(&key.to_string(), &self.default_cwd.to_string_lossy())?;

        let (session, events) = ClaudeSession::spawn(Config {
            cwd: PathBuf::from(&state.cwd),
            session_id: state.session_id,
            // The PreToolUse hook installed during the handshake is what
            // actually gates tools; this flag is inert under --print.
            permission_mode: "default".to_string(),
            raw: false,
        })
        .await
        .with_context(|| format!("starting agent for {key}"))?;

        forward(key.clone(), events, self.agent_tx.clone());

        self.threads.insert(
            key.clone(),
            Thread {
                session,
                renderer: TurnRenderer::new(),
                pending: None,
            },
        );
        Ok(())
    }
}

/// Tag one session's events with its thread and merge them into the core's
/// single stream, so the run loop selects over one receiver rather than N.
fn forward(
    key: ThreadKey,
    mut events: mpsc::Receiver<AgentEvent>,
    tx: mpsc::Sender<(ThreadKey, AgentEvent)>,
) {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if tx.send((key.clone(), event)).await.is_err() {
                break;
            }
        }
    });
}

fn expand_home(path: &str) -> PathBuf {
    match path.strip_prefix("~/") {
        Some(rest) => match std::env::var_os("HOME") {
            Some(home) => PathBuf::from(home).join(rest),
            None => PathBuf::from(path),
        },
        None => PathBuf::from(path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_writing_and_executing_tools_are_gated() {
        assert!(GATED_TOOLS.contains(&"Bash"));
        assert!(GATED_TOOLS.contains(&"Edit"));
        assert!(!GATED_TOOLS.contains(&"Read"));
        assert!(!GATED_TOOLS.contains(&"Grep"));
    }

    #[test]
    fn home_is_expanded_only_at_the_start() {
        std::env::set_var("HOME", "/home/test");
        assert_eq!(expand_home("~/src/x"), PathBuf::from("/home/test/src/x"));
        assert_eq!(expand_home("/abs/path"), PathBuf::from("/abs/path"));
        assert_eq!(expand_home("a/~/b"), PathBuf::from("a/~/b"));
    }
}
