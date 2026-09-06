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

use crate::agent::{self, Agent};
use crate::channel::{Channel, Inbound, InboundKind, ThreadKey};
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

/// Above this many characters, a reply is spilled to a file and the chat gets a
/// pointer instead.
///
/// Chat is a bad place for a 500-line diff, and both channels clip long
/// messages anyway — which loses the tail silently. A file plus the command to
/// read it loses nothing, and the tailnet already makes it reachable.
const SPILL_THRESHOLD: usize = 2500;

struct Thread {
    session: Box<dyn Agent>,
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
    /// Where replies too long for chat are written.
    spill_dir: PathBuf,
    /// What a thread talks to before anyone says otherwise.
    default_agent: String,
    /// Every channel this instance serves, keyed by the name it puts in a
    /// [`ThreadKey`]. The core never names a concrete channel.
    channels: HashMap<&'static str, Arc<dyn Channel>>,
    default_cwd: PathBuf,
    threads: HashMap<ThreadKey, Thread>,
    agent_tx: mpsc::Sender<(ThreadKey, AgentEvent)>,
    agent_rx: mpsc::Receiver<(ThreadKey, AgentEvent)>,
}

impl Core {
    pub fn new(
        store: Store,
        channels: Vec<Arc<dyn Channel>>,
        default_cwd: PathBuf,
        default_agent: String,
        spill_dir: PathBuf,
    ) -> Self {
        let (agent_tx, agent_rx) = mpsc::channel(256);
        Self {
            store,
            spill_dir,
            default_agent,
            channels: channels.into_iter().map(|c| (c.name(), c)).collect(),
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

    /// This thread's stored state, created against the defaults on first sight.
    fn state(&self, key: &ThreadKey) -> Result<crate::store::ThreadState> {
        self.store.get_or_create(
            &key.to_string(),
            &self.default_cwd.to_string_lossy(),
            &self.default_agent,
        )
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
                Command::Agent(name) => self.on_agent(&message.thread, &name).await,
                Command::Attach => self.on_attach(&message.thread).await,
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

        // Where the reply cannot arrive progressively, this is the only sign
        // anything is happening.
        if let Some(channel) = self.channel(key) {
            if !channel.can_edit() {
                channel.typing(key).await.ok();
            }
        }
        Ok(())
    }

    async fn on_new(&mut self, key: &ThreadKey) -> Result<()> {
        if let Some(mut thread) = self.threads.remove(key) {
            thread.session.shutdown().await.ok();
        }

        let mut state = self.state(key)?;
        // Forget the agent's handle rather than inventing one: the next turn
        // starts a conversation and the agent tells us what to call it.
        state.session_id = None;
        self.store.put(&state)?;

        self.say(key, &format!("New {} session in {}.", state.agent, state.cwd))
            .await
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
        let state = self.state(key)?;

        let (running, awaiting, live_agent) = match self.threads.get(key) {
            Some(thread) => (
                thread.session.is_busy(),
                thread.pending.as_ref().map(|p| p.tool.clone()),
                Some(thread.session.name()),
            ),
            None => (false, None, None),
        };

        let mut note = format!(
            "agent    {}\ndir      {}\nsession  {}\nstate    {}",
            live_agent.unwrap_or(&state.agent),
            state.cwd,
            state.session_id.as_deref().unwrap_or("(new)"),
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

        let mut state = self.state(key)?;
        state.cwd = expanded.to_string_lossy().to_string();
        state.session_id = None;
        self.store.put(&state)?;

        let note = format!("Working in {}. New session.", state.cwd);
        self.say(key, &note).await
    }

    /// Switch agents. Each has its own conversation, so this starts a fresh
    /// one rather than pretending a transcript can move between them.
    async fn on_agent(&mut self, key: &ThreadKey, name: &str) -> Result<()> {
        if name.is_empty() {
            let state = self.state(key)?;
            let known: Vec<&str> = agent::AGENTS.iter().map(|(n, _)| *n).collect();
            let note = format!("Using {}. Available: {}", state.agent, known.join(", "));
            return self.say(key, &note).await;
        }

        if agent::backend_for(name).is_none() {
            let known: Vec<&str> = agent::AGENTS.iter().map(|(n, _)| *n).collect();
            let note = format!("Unknown agent '{name}'. Try: {}", known.join(", "));
            return self.say(key, &note).await;
        }

        if let Some(mut thread) = self.threads.remove(key) {
            thread.session.shutdown().await.ok();
        }

        let mut state = self.state(key)?;
        state.agent = name.to_string();
        state.session_id = None;
        self.store.put(&state)?;

        // Say what changes about the experience, not just the name. Waiting for
        // an approval prompt that will never arrive is a bad way to find out.
        let mut note = format!("Now using {name} in {}.", state.cwd);
        match agent::backend_for(name) {
            Some(agent::Backend::Claude) => {}
            Some(agent::Backend::Codex) => note
                .push_str("\n\nCodex approves its own tools inside a sandbox — no Allow/Deny here."),
            // The gate is this product's one safety feature, and this tier does
            // not have it. Someone who just approved a Bash call on claude is
            // one command away from an agent that approves its own — that
            // difference has to be visible at the moment it changes, not
            // discovered when a prompt never arrives.
            Some(agent::Backend::Tmux) => note.push_str(
                "\n\nNo Allow/Deny on this tier: it runs with its own auto-approve \
                 flags, unsandboxed, and nothing here can stop a tool call. It has \
                 no structured output either, so nothing streams back — /attach to \
                 take over at a terminal.",
            ),
            None => {}
        }
        self.say(key, &note).await
    }

    async fn on_attach(&mut self, key: &ThreadKey) -> Result<()> {
        let state = self.state(key)?;
        let session = crate::tmux::session_name(&key.to_string());
        let host = hostname();

        let note = format!(
            "Take over at a terminal:\n\n  ssh {host} -t tmux attach -t {session}\n\n\
             That session exists only for detached agents ({}). For {} the \
             conversation lives in the agent's own store — resume it with its session id: {}",
            "pi, omp, opencode, crush, grok, gemini, copilot",
            state.agent,
            state.session_id.as_deref().unwrap_or("(none yet)")
        );
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
                if let (Some(token), Some(channel)) = (token, self.channel(key)) {
                    channel.ack_decision(token, "Nothing pending").await.ok();
                }
                return self.say(key, "Nothing waiting for a decision.").await;
            }
        };

        let allowed = matches!(decision, Decision::Allow { .. });

        let thread = self.threads.get_mut(key).expect("pending implies a thread");
        thread.session.decide(&pending.request_id, decision).await?;

        let verdict = if allowed { "Allowed" } else { "Denied" };

        let channel = match self.channel(key) {
            Some(channel) => channel.clone(),
            None => return Ok(()),
        };

        if let Some(token) = token {
            channel.ack_decision(token, verdict).await.ok();
        }

        // Where messages can be rewritten, retire the buttons and record the
        // outcome in place. Where they cannot, say it in a new message —
        // otherwise a tap looks like it did nothing.
        let settled = format!("{} {}", verdict, pending.tool);
        if channel.can_edit() {
            channel.edit(key, &pending.message_id, &settled).await.ok();
        } else {
            channel.send(key, &settled).await.ok();
        }

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

            // The agent names its own conversation — Codex assigns a thread id
            // on the first turn — so record whatever it reports.
            AgentEvent::Ready { session_id, .. } if !session_id.is_empty() => {
                let mut state = self.state(key)?;
                if state.session_id.as_deref() != Some(session_id.as_str()) {
                    state.session_id = Some(session_id.clone());
                    self.store.put(&state)?;
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

        let channel = match self.channel(key) {
            Some(channel) => channel.clone(),
            None => return Ok(()),
        };

        let detail = serde_json::to_string_pretty(&input).unwrap_or_default();
        let question = format!("Run {tool}?\n\n{detail}");
        let message_id = channel.ask_permission(key, &question).await?;

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

    /// The channel a thread belongs to. Absent only if a thread outlives the
    /// channel that created it, which would be a configuration change.
    fn channel(&self, key: &ThreadKey) -> Option<&Arc<dyn Channel>> {
        self.channels.get(key.channel)
    }

    async fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<ThreadKey> = self.threads.keys().cloned().collect();
        for key in keys {
            self.flush(&key).await?;
        }
        Ok(())
    }

    /// Push a thread's pending text.
    ///
    /// On a channel that can edit, this grows one message as the turn runs. On
    /// one that cannot — iMessage — mid-turn flushes are skipped entirely and
    /// the turn arrives as a single finished message, because the alternative
    /// is a stream of fragments nobody wants to read on a phone.
    async fn flush(&mut self, key: &ThreadKey) -> Result<()> {
        let channel = match self.channel(key) {
            Some(channel) => channel.clone(),
            None => return Ok(()),
        };

        let (text, message_id) = match self.threads.get_mut(key) {
            Some(thread) => {
                // Two reasons to hold a turn back until it is done: a channel
                // that cannot rewrite a message, and an agent that produces
                // nothing worth showing until the end. Either makes a mid-turn
                // flush a wasted message.
                let deliver_whole = !channel.can_edit() || !thread.session.streams();
                if deliver_whole && !thread.renderer.is_finished() {
                    return Ok(());
                }
                match thread.renderer.take_pending() {
                    Some(text) => (text, thread.renderer.message_id.clone()),
                    None => return Ok(()),
                }
            }
            None => return Ok(()),
        };

        let text = self.spill_if_long(key, text);

        match message_id {
            Some(id) => channel.edit(key, &id, &text).await?,
            None => {
                let id = channel.send(key, &text).await?;
                if let Some(thread) = self.threads.get_mut(key) {
                    thread.renderer.message_id = Some(id);
                }
            }
        }
        Ok(())
    }

    /// Write an over-long reply to a file and hand back a pointer to it.
    ///
    /// Falls back to the untouched text if the file cannot be written — a
    /// clipped reply is worse than a whole one, but both beat no reply.
    fn spill_if_long(&self, key: &ThreadKey, text: String) -> String {
        if text.chars().count() <= SPILL_THRESHOLD {
            return text;
        }

        let name = format!(
            "{}-{}.txt",
            key.to_string().replace([':', '/'], "-"),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        );
        let path = self.spill_dir.join(name);

        if std::fs::create_dir_all(&self.spill_dir).is_err() || std::fs::write(&path, &text).is_err()
        {
            tracing::warn!("could not spill long output to {}", path.display());
            return text;
        }

        let head: String = text.chars().take(SPILL_THRESHOLD).collect();
        format!(
            "{head}\n\n… {} characters in all. Read the rest with:\n\n  ssh {} -t 'cat {}'",
            text.chars().count(),
            hostname(),
            path.display()
        )
    }

    /// Post a standalone note, outside any turn's message.
    async fn say(&self, key: &ThreadKey, text: &str) -> Result<()> {
        if let Some(channel) = self.channel(key) {
            channel.send(key, text).await?;
        }
        Ok(())
    }

    // ---- sessions ------------------------------------------------------

    async fn ensure_thread(&mut self, key: &ThreadKey) -> Result<()> {
        if self.threads.contains_key(key) {
            return Ok(());
        }

        let state = self.state(key)?;

        let (session, events) = agent::spawn(agent::SpawnConfig {
            agent: state.agent.clone(),
            cwd: PathBuf::from(&state.cwd),
            session_id: state.session_id.clone(),
            label: key.to_string(),
        })
        .await
        .with_context(|| format!("starting {} for {key}", state.agent))?;

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

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|name| name.trim().to_string())
        .unwrap_or_else(|_| "localhost".to_string())
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
    fn short_replies_are_left_alone_and_long_ones_are_spilled() {
        let dir = std::env::temp_dir().join(format!("sb-spill-{}", std::process::id()));
        let core = Core::new(
            Store::in_memory().unwrap(),
            Vec::new(),
            PathBuf::from("/tmp"),
            "claude".into(),
            dir.clone(),
        );
        let key = ThreadKey {
            channel: "telegram",
            chat_id: "5".into(),
            topic_id: None,
        };

        assert_eq!(core.spill_if_long(&key, "short".into()), "short");

        let long = "x".repeat(SPILL_THRESHOLD + 500);
        let pointed = core.spill_if_long(&key, long.clone());
        assert!(pointed.chars().count() < long.chars().count());
        assert!(pointed.contains("ssh "), "must say how to read the rest");
        assert!(pointed.contains(&format!("{}", SPILL_THRESHOLD + 500)));

        // The whole thing is on disk, not just the part that fit.
        let written: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().collect();
        assert_eq!(written.len(), 1);
        assert_eq!(
            std::fs::read_to_string(written[0].path()).unwrap().len(),
            long.len()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

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
