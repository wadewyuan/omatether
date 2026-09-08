//! The router: chat threads on one side, agent sessions on the other.
//!
//! One thread maps to one agent session, and one turn runs at a time within it.
//! Everything vendor-specific stays behind the two seams — this module talks
//! only in [`Inbound`] and [`AgentEvent`].

use std::collections::HashMap;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::agent::{self, Agent};
use crate::channel::{Channel, Inbound, InboundKind, ThreadKey};
use crate::command::{self, Command};
use crate::event::{AgentEvent, Decision};
use crate::outbox::{OutJob, Outbox};
use crate::render::TurnRenderer;
use crate::store::Store;

/// How often a streaming turn is pushed to the chat. Comfortably inside
/// Telegram's roughly-one-per-second-per-chat budget, and slow enough that a
/// reader is not watching text reflow constantly.
const FLUSH_INTERVAL: Duration = Duration::from_millis(1500);

/// Tools that require a human decision. Everything else is approved by
/// omatether without bothering anyone.
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

/// How much of a tool's input a permission question will show inline.
///
/// Deliberately well under the smallest channel's limit (Photon clips at 3000,
/// Telegram at 3900) so that the question, the input and the pointer to the
/// rest all survive whole. A gate that clips is a gate that gets approved
/// blind.
const PERMISSION_DETAIL_BUDGET: usize = 1500;

struct Thread {
    session: Box<dyn Agent>,
    renderer: TurnRenderer,
    /// The permission question waiting on a human, if any. At most one: the
    /// turn is blocked on it anyway.
    pending: Option<PendingPermission>,
    /// Approve tool calls without asking. Mirrored from the store when the
    /// session starts and kept in step by `/auto`, so the hot path — every
    /// tool call the agent makes — never touches the database. The store stays
    /// the source of truth across restarts.
    auto: bool,
}

struct PendingPermission {
    request_id: String,
    tool: String,
    /// Identifies this question to the channel, so a tap can be matched to the
    /// question it was asked under rather than to whatever is pending now.
    ///
    /// Which *message* carries it is the outbox's business: settling the
    /// question is a job, not something the core waits for an id to be able to
    /// do.
    question: String,
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
    /// One outbound task per thread. Kept separately from `threads` because
    /// they outlive sessions: `/new` and `/cd` end a session and still have
    /// something to say about it.
    outboxes: HashMap<ThreadKey, Outbox>,
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
            outboxes: HashMap::new(),
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
                    self.flush_all();
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
            InboundKind::Decision {
                allow,
                ack,
                question,
            } => {
                self.on_decision(&message.thread, allow, &ack, &question)
                    .await
            }
            InboundKind::Unsupported(note) => self.say(&message.thread, &note),

            InboundKind::Text(text) => match command::parse(&text) {
                Command::Help => self.say(&message.thread, command::HELP),
                Command::New => self.on_new(&message.thread).await,
                Command::Stop => self.on_stop(&message.thread).await,
                Command::Status => self.on_status(&message.thread).await,
                Command::Cd(path) => self.on_cd(&message.thread, &path).await,
                Command::Agent(name) => self.on_agent(&message.thread, &name).await,
                Command::Attach => self.on_attach(&message.thread).await,
                Command::Auto(want) => self.on_auto(&message.thread, want).await,
                Command::Allow => self.decide(&message.thread, Decision::allow(), None).await,
                Command::Deny(why) => {
                    self.decide(&message.thread, Decision::deny(why), None)
                        .await
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
            return self.say(key, &note);
        }

        thread.renderer.reset();

        // A new turn grows a new message rather than continuing the last one's.
        self.queue(key, OutJob::NewTurn);

        // Where the reply cannot arrive progressively, this is the only sign
        // anything is happening.
        let can_edit = self
            .channel(key)
            .map(|channel| channel.can_edit())
            .unwrap_or(false);
        if !can_edit {
            self.queue(key, OutJob::Typing);
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

        self.say(
            key,
            &format!("New {} session in {}.", state.agent, state.cwd),
        )
    }

    async fn on_stop(&mut self, key: &ThreadKey) -> Result<()> {
        match self.threads.get_mut(key) {
            Some(thread) => {
                thread.session.cancel().await?;
                thread.renderer.reset();
                thread.pending = None;
                self.say(key, "Stopped.")
            }
            None => self.say(key, "Nothing running."),
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
            "agent    {}\ndir      {}\nsession  {}\nstate    {}\ntools    {}",
            live_agent.unwrap_or(&state.agent),
            state.cwd,
            state.session_id.as_deref().unwrap_or("(new)"),
            if running { "turn running" } else { "idle" },
            approvals(&state.agent, state.auto)
        );
        if let Some(tool) = awaiting {
            note.push_str(&format!("\nwaiting  decision on {tool}"));
        }
        self.say(key, &note)
    }

    /// Read or change this thread's approval mode.
    ///
    /// Per thread rather than global: the phone that runs errands against a
    /// scratch directory and the one pointed at a repo you care about are the
    /// same product, and only the person holding it knows which is which.
    async fn on_auto(&mut self, key: &ThreadKey, want: Option<bool>) -> Result<()> {
        let mut state = self.state(key)?;

        let want = match want {
            Some(want) => want,
            None => {
                let note = format!(
                    "Tool calls: {}.\n\n/auto on approves them without asking; \
                     /auto off asks first, for Bash, Write and Edit.",
                    approvals(&state.agent, state.auto)
                );
                return self.say(key, &note);
            }
        };

        if state.auto != want {
            state.auto = want;
            self.store.put(&state)?;
            if let Some(thread) = self.threads.get_mut(key) {
                thread.auto = want;
            }
        }

        // Say what it means, not just which way the switch went. This is the
        // one setting that decides whether a message can run a shell command
        // with nobody looking.
        let note = if want {
            "Auto mode on. Tool calls run without asking — you will see each \
             one in the reply as it happens, after it has run. /auto off to be \
             asked first."
                .to_string()
        } else {
            format!(
                "Auto mode off. {}",
                match agent::backend_for(&state.agent) {
                    Some(agent::Backend::Claude) =>
                        "Bash, Write and Edit will wait for Allow or Deny.",
                    _ =>
                        "This agent approves its own tools, though — the gate \
                          only applies to claude. /agent claude to get it back.",
                }
            )
        };
        self.say(key, &note)
    }

    async fn on_cd(&mut self, key: &ThreadKey, path: &str) -> Result<()> {
        if path.is_empty() {
            return self.say(key, "Usage: /cd <path>");
        }

        let expanded = expand_home(path);
        if !expanded.is_dir() {
            let note = format!("Not a directory: {}", expanded.display());
            return self.say(key, &note);
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
        self.say(key, &note)
    }

    /// Switch agents. Each has its own conversation, so this starts a fresh
    /// one rather than pretending a transcript can move between them.
    async fn on_agent(&mut self, key: &ThreadKey, name: &str) -> Result<()> {
        if name.is_empty() {
            let state = self.state(key)?;
            let known: Vec<&str> = agent::AGENTS.iter().map(|(n, _)| *n).collect();
            let note = format!("Using {}. Available: {}", state.agent, known.join(", "));
            return self.say(key, &note);
        }

        if agent::backend_for(name).is_none() {
            let known: Vec<&str> = agent::AGENTS.iter().map(|(n, _)| *n).collect();
            let note = format!("Unknown agent '{name}'. Try: {}", known.join(", "));
            return self.say(key, &note);
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
            Some(agent::Backend::Codex) => note.push_str(
                "\n\nCodex approves its own tools inside a sandbox — no Allow/Deny here.",
            ),
            Some(agent::Backend::Pi) => {
                note.push_str("\n\nPi runs and approves its own tools — no Allow/Deny here.")
            }
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
        self.say(key, &note)
    }

    async fn on_attach(&mut self, key: &ThreadKey) -> Result<()> {
        let state = self.state(key)?;
        let session = crate::tmux::session_name(&key.to_string());
        let host = hostname();

        let note = format!(
            "Take over at a terminal:\n\n  ssh {host} -t tmux attach -t {session}\n\n\
             That session exists only for detached agents ({}). For {} the \
             conversation lives in the agent's own store — resume it with its session id: {}",
            "omp, opencode, crush, grok, gemini, copilot",
            state.agent,
            state.session_id.as_deref().unwrap_or("(none yet)")
        );
        self.say(key, &note)
    }

    /// A button tap. Unlike `/allow` typed as text, this names the question it
    /// was asked under, because the buttons under an old question stay tappable
    /// for as long as the message exists.
    async fn on_decision(
        &mut self,
        key: &ThreadKey,
        allow: bool,
        ack: &str,
        question: &str,
    ) -> Result<()> {
        // Check before consuming anything: a tap that does not name the
        // question we are waiting on must leave that question waiting.
        let answers_the_open_question = self
            .threads
            .get(key)
            .and_then(|thread| thread.pending.as_ref())
            .is_some_and(|pending| pending.question == question);

        if !answers_the_open_question {
            self.queue(
                key,
                OutJob::Ack {
                    ack: ack.to_string(),
                    note: "That question has moved on".to_string(),
                },
            );
            // Say which way it went, because from the chat it looks like the
            // tap did nothing: the buttons are still there under a question
            // that is no longer the one being asked.
            return self.say(
                key,
                "That was a button from an earlier question — it was not \
                 applied. Scroll down for the current one, if there is one.",
            );
        }

        let decision = if allow {
            Decision::allow()
        } else {
            Decision::deny("denied from chat")
        };
        self.decide(key, decision, Some(ack)).await
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
                    self.queue(
                        key,
                        OutJob::Ack {
                            ack: token.to_string(),
                            note: "Nothing pending".to_string(),
                        },
                    );
                }
                return self.say(key, "Nothing waiting for a decision.");
            }
        };

        let allowed = matches!(decision, Decision::Allow { .. });

        let thread = self.threads.get_mut(key).expect("pending implies a thread");
        thread.session.decide(&pending.request_id, decision).await?;

        // What was decided, on what, by whom, and when — the audit line for a
        // service whose whole purpose is running shell commands from a chat.
        let verdict = if allowed { "Allowed" } else { "Denied" };
        tracing::info!(
            thread = %key,
            tool = %pending.tool,
            by = if token.is_some() { "button" } else { "command" },
            "{verdict}"
        );

        self.queue(
            key,
            OutJob::Settle {
                verdict: format!("{} {}", verdict, pending.tool),
                ack: token.map(str::to_string),
            },
        );

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
                    return self.say(key, &note);
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
            self.flush(key);
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
        // Auto mode: approve everything, and leave the audit line behind. The
        // tool call still shows up in the reply as it happens, so the thread
        // reads as a log of what ran rather than a series of questions.
        if self.threads.get(key).is_some_and(|thread| thread.auto) {
            tracing::info!(thread = %key, tool = %tool, by = "auto", "Allowed");
            if let Some(thread) = self.threads.get_mut(key) {
                thread
                    .session
                    .decide(&request_id, Decision::allow())
                    .await?;
            }
            return Ok(());
        }

        // Auto-approve anything that only reads. Asking about every file the
        // agent opens trains you to tap Allow without reading it.
        if !GATED_TOOLS.iter().any(|t| *t == tool) {
            if let Some(thread) = self.threads.get_mut(key) {
                thread
                    .session
                    .decide(&request_id, Decision::allow())
                    .await?;
            }
            return Ok(());
        }

        // Show the pending work before asking, so the question has context.
        // Queued ahead of the question, and the outbox keeps that order.
        self.flush(key);

        let text = self.permission_question(key, &tool, &input);
        let question = question_token();
        if !self.queue(
            key,
            OutJob::Ask {
                text,
                question: question.clone(),
            },
        ) {
            // The question could not even be queued, so nobody will ever be
            // asked. Denying is the only honest answer: the alternative is a
            // turn blocked forever on a prompt that was never posted.
            if let Some(thread) = self.threads.get_mut(key) {
                thread
                    .session
                    .decide(
                        &request_id,
                        Decision::deny("omatether could not deliver the question to the chat"),
                    )
                    .await?;
            }
            return Ok(());
        }

        if let Some(thread) = self.threads.get_mut(key) {
            thread.pending = Some(PendingPermission {
                request_id,
                tool,
                question,
            });
        }
        Ok(())
    }

    /// The text of a permission question.
    ///
    /// This is the one message in the system that must never be silently cut
    /// off. Both channels clip, and an `Edit` input is the whole old file plus
    /// the whole new one — comfortably past every limit — so the naive
    /// "pretty-print the input" produced exactly the wrong thing: a prompt
    /// showing the first few thousand characters of the *old* file, with the
    /// change being approved somewhere below the cut. Approving what you cannot
    /// read is the failure the gate exists to prevent.
    ///
    /// So: show the whole input when the whole input fits, and otherwise say
    /// what the tool is doing in one line and put the complete text in a file.
    /// Never a silent truncation.
    fn permission_question(
        &self,
        key: &ThreadKey,
        tool: &str,
        input: &serde_json::Value,
    ) -> String {
        let detail = serde_json::to_string_pretty(input).unwrap_or_default();

        if detail.chars().count() <= PERMISSION_DETAIL_BUDGET {
            return format!("Run {tool}?\n\n{detail}");
        }

        let headline = crate::render::summarize(input);
        let mut question = format!("Run {tool}?");
        if !headline.is_empty() {
            question.push_str(&format!("\n\n{headline}"));
        }

        match self.spill(key, &detail) {
            Some(path) => question.push_str(&format!(
                "\n\nThe full input is {} characters — too long to show here \
                 without cutting it off, and this is not a message to read half \
                 of. All of it is in:\n\n  ssh {} -t 'cat {}'",
                detail.chars().count(),
                hostname(),
                path.display()
            )),
            // Saying so is the point: the alternative is a prompt that looks
            // complete and is not.
            None => question.push_str(&format!(
                "\n\nThe full input is {} characters and could not be written to \
                 a file to show you. Deny unless you know what this is.",
                detail.chars().count()
            )),
        }
        question
    }

    // ---- output --------------------------------------------------------

    /// The channel a thread belongs to. Absent only if a thread outlives the
    /// channel that created it, which would be a configuration change.
    fn channel(&self, key: &ThreadKey) -> Option<&Arc<dyn Channel>> {
        self.channels.get(key.channel)
    }

    /// This thread's outbound task, started on first use.
    ///
    /// Everything the core says goes through here rather than being awaited
    /// inline, so no chat's latency or rate limit is any other chat's problem.
    fn outbox(&mut self, key: &ThreadKey) -> Option<&Outbox> {
        if !self.outboxes.contains_key(key) {
            let channel = self.channels.get(key.channel)?.clone();
            self.outboxes
                .insert(key.clone(), Outbox::spawn(key.clone(), channel));
        }
        self.outboxes.get(key)
    }

    /// Queue one piece of outbound work, if the thread's channel still exists.
    fn queue(&mut self, key: &ThreadKey, job: OutJob) -> bool {
        match self.outbox(key) {
            Some(outbox) => outbox.queue(job),
            None => false,
        }
    }

    fn flush_all(&mut self) {
        let keys: Vec<ThreadKey> = self.threads.keys().cloned().collect();
        for key in keys {
            self.flush(&key);
        }
    }

    /// Push a thread's pending text.
    ///
    /// On a channel that can edit, this grows one message as the turn runs. On
    /// one that cannot — iMessage — mid-turn flushes are skipped entirely and
    /// the turn arrives as a single finished message, because the alternative
    /// is a stream of fragments nobody wants to read on a phone.
    /// Handing the text over cannot fail slowly: the outbox takes it or says it
    /// is full, and either way the core moves on to the next thread.
    fn flush(&mut self, key: &ThreadKey) {
        let can_edit = match self.channel(key) {
            Some(channel) => channel.can_edit(),
            None => return,
        };

        let text = match self.threads.get(key) {
            Some(thread) => {
                // Two reasons to hold a turn back until it is done: a channel
                // that cannot rewrite a message, and an agent that produces
                // nothing worth showing until the end. Either makes a mid-turn
                // flush a wasted message.
                let deliver_whole = !can_edit || !thread.session.streams();
                if deliver_whole && !thread.renderer.is_finished() {
                    return;
                }
                match thread.renderer.pending() {
                    Some(text) => text,
                    None => return,
                }
            }
            None => return,
        };

        // Spilling rewrites the message into a pointer, but what the renderer
        // has to remember is the text it composed: comparing next time against
        // the pointer would make every tick look like a change and re-send the
        // whole turn.
        let payload = self.spill_if_long(key, text.clone());

        // Only once it has been accepted for delivery is it no longer owed.
        if self.queue(key, OutJob::Turn(payload)) {
            if let Some(thread) = self.threads.get_mut(key) {
                thread.renderer.mark_sent(text);
            }
        }
    }

    /// Write an over-long reply to a file and hand back a pointer to it.
    ///
    /// Falls back to the untouched text if the file cannot be written — a
    /// clipped reply is worse than a whole one, but both beat no reply.
    fn spill_if_long(&self, key: &ThreadKey, text: String) -> String {
        if text.chars().count() <= SPILL_THRESHOLD {
            return text;
        }

        let path = match self.spill(key, &text) {
            Some(path) => path,
            None => return text,
        };

        let head: String = text.chars().take(SPILL_THRESHOLD).collect();
        format!(
            "{head}\n\n… {} characters in all. Read the rest with:\n\n  ssh {} -t 'cat {}'",
            text.chars().count(),
            hostname(),
            path.display()
        )
    }

    /// Write text to a file in the spill directory and report where it went.
    ///
    /// Mode 0600, because what lands here is whatever the agent was about to
    /// say or about to do: file contents, diffs, and whatever secrets those
    /// happen to carry. The process umask would otherwise decide, and the usual
    /// answer is world-readable.
    ///
    /// The name carries nanoseconds as well as seconds: two spills in the same
    /// second are no longer hypothetical now that a permission question can
    /// spill alongside a flush of the same turn, and the loser of that race
    /// would have its pointer left naming someone else's content.
    fn spill(&self, key: &ThreadKey, text: &str) -> Option<PathBuf> {
        use std::io::Write as _;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let name = format!(
            "{}-{}-{:09}.txt",
            key.to_string().replace([':', '/'], "-"),
            now.as_secs(),
            now.subsec_nanos()
        );
        let path = self.spill_dir.join(name);

        if let Err(e) = std::fs::create_dir_all(&self.spill_dir) {
            tracing::warn!("could not create {}: {e}", self.spill_dir.display());
            return None;
        }

        let written = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .and_then(|mut file| file.write_all(text.as_bytes()));

        match written {
            Ok(()) => Some(path),
            Err(e) => {
                tracing::warn!("could not spill to {}: {e}", path.display());
                None
            }
        }
    }

    /// Post a standalone note, outside any turn's message.
    fn say(&mut self, key: &ThreadKey, text: &str) -> Result<()> {
        self.queue(key, OutJob::Say(text.to_string()));
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
                auto: state.auto,
            },
        );
        Ok(())
    }
}

/// How this thread's tool calls are approved, in a phrase.
///
/// An agent that cannot be gated says so regardless of the switch: "gate on"
/// would otherwise read as a promise nothing is keeping.
fn approvals(agent: &str, auto: bool) -> &'static str {
    match agent::backend_for(agent) {
        Some(agent::Backend::Claude) if auto => "run without asking (/auto off to be asked)",
        Some(agent::Backend::Claude) => "Bash, Write and Edit ask first",
        _ => "run without asking — this agent cannot be gated",
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

/// A short handle for one permission question.
///
/// Only ever compared against the one question a thread has outstanding, so it
/// needs to be unpredictable across restarts rather than globally unique — a
/// counter would let a button from before a restart match a question after it.
/// Eight hex characters leave plenty of room inside Telegram's 64-byte
/// callback_data.
fn question_token() -> String {
    uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(8)
        .collect()
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

    /// A `Core` with a scratch spill directory, for the tests that write one.
    fn core_with_spill_dir(dir: &std::path::Path) -> Core {
        Core::new(
            Store::in_memory().unwrap(),
            Vec::new(),
            PathBuf::from("/tmp"),
            "claude".into(),
            dir.to_path_buf(),
        )
    }

    fn test_key() -> ThreadKey {
        ThreadKey {
            channel: "telegram",
            chat_id: "5".into(),
            topic_id: None,
        }
    }

    #[test]
    fn a_short_tool_input_is_shown_whole() {
        let dir = std::env::temp_dir().join(format!("sb-q1-{}", std::process::id()));
        let core = core_with_spill_dir(&dir);

        let question = core.permission_question(
            &test_key(),
            "Bash",
            &serde_json::json!({ "command": "rm -rf ./build" }),
        );

        assert!(question.starts_with("Run Bash?"));
        assert!(question.contains("rm -rf ./build"), "the command itself");
        assert!(
            !question.contains("ssh "),
            "no pointer needed for a short one"
        );
        assert!(!dir.exists(), "nothing spilled for an input that fits");
    }

    #[test]
    fn a_huge_tool_input_is_summarized_and_spilled_never_clipped() {
        let dir = std::env::temp_dir().join(format!("sb-q2-{}", std::process::id()));
        let core = core_with_spill_dir(&dir);

        // The case that motivated this: an Edit carrying a whole file, which
        // every channel would clip — leaving the change being approved below
        // the cut.
        let input = serde_json::json!({
            "file_path": "/home/wy/src/omatether/src/core.rs",
            "old_string": "x".repeat(9000),
            "new_string": "y".repeat(9000),
        });
        let question = core.permission_question(&test_key(), "Edit", &input);

        // Short enough that no channel will clip it.
        assert!(
            question.chars().count() < 3000,
            "must fit the smallest channel, got {}",
            question.chars().count()
        );
        // And it still says what is being touched, and where to read the rest.
        assert!(question.contains("core.rs"), "the file being edited");
        assert!(question.contains("ssh "), "how to read all of it");

        // The whole input really is on disk, not just the part that fit.
        let written: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().collect();
        assert_eq!(written.len(), 1);
        let spilled = std::fs::read_to_string(written[0].path()).unwrap();
        assert!(spilled.contains(&"x".repeat(9000)));
        assert!(spilled.contains(&"y".repeat(9000)));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn spilled_files_are_not_readable_by_anyone_else() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = std::env::temp_dir().join(format!("sb-q3-{}", std::process::id()));
        let core = core_with_spill_dir(&dir);

        // Agent output and tool inputs carry file contents and whatever secrets
        // those contain; the process umask should not be what decides who can
        // read them.
        let path = core.spill(&test_key(), "sk-secret-token").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "group and other must have no access");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_spills_in_the_same_second_do_not_overwrite_each_other() {
        let dir = std::env::temp_dir().join(format!("sb-q4-{}", std::process::id()));
        let core = core_with_spill_dir(&dir);
        let key = test_key();

        // A permission question and a flush of the same turn can land together.
        let first = core.spill(&key, "the first").unwrap();
        let second = core.spill(&key, "the second").unwrap();

        assert_ne!(first, second);
        assert_eq!(std::fs::read_to_string(&first).unwrap(), "the first");
        assert_eq!(std::fs::read_to_string(&second).unwrap(), "the second");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Counts what reached the chat, so a test can tell one message from ten.
    struct CountingChannel {
        sent: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Channel for CountingChannel {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn can_edit(&self) -> bool {
            true
        }
        async fn send(&self, _t: &ThreadKey, text: &str) -> Result<String> {
            let mut sent = self.sent.lock().unwrap();
            sent.push(text.to_string());
            Ok(format!("m{}", sent.len()))
        }
        async fn edit(&self, _t: &ThreadKey, _id: &String, text: &str) -> Result<()> {
            self.sent.lock().unwrap().push(text.to_string());
            Ok(())
        }
        async fn ask_permission(&self, t: &ThreadKey, text: &str, _q: &str) -> Result<String> {
            self.send(t, text).await
        }
    }

    /// An agent that runs no process. Enough for the core to have a session.
    struct StubAgent;

    #[async_trait::async_trait]
    impl Agent for StubAgent {
        fn name(&self) -> &'static str {
            "stub"
        }
        fn gates_tools(&self) -> bool {
            true
        }
        fn streams(&self) -> bool {
            true
        }
        fn is_busy(&self) -> bool {
            false
        }
        async fn prompt(&mut self, _text: &str) -> Result<()> {
            Ok(())
        }
        async fn cancel(&mut self) -> Result<()> {
            Ok(())
        }
        async fn shutdown(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn an_unchanged_turn_is_not_re_sent_on_every_tick() {
        // Spilling rewrites a long reply into a pointer before it goes out. If
        // what the renderer remembers is that pointer rather than the text it
        // composed, every tick compares unequal, and the thread gets the whole
        // turn again every 1.5 seconds — on someone's phone. Verified to fail
        // when that mistake is reintroduced.
        let dir = std::env::temp_dir().join(format!("sb-tick-{}", std::process::id()));
        let channel = Arc::new(CountingChannel {
            sent: std::sync::Mutex::new(Vec::new()),
        });
        let key = ThreadKey {
            channel: "fake",
            chat_id: "1".into(),
            topic_id: None,
        };

        let mut core = Core::new(
            Store::in_memory().unwrap(),
            vec![channel.clone()],
            PathBuf::from("/tmp"),
            "claude".into(),
            dir.clone(),
        );

        let mut renderer = TurnRenderer::new();
        renderer.apply(&AgentEvent::Text {
            text: "x".repeat(SPILL_THRESHOLD + 500),
        });
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent),
                renderer,
                pending: None,
                auto: true,
            },
        );

        // The flush the turn earns, then four ticks that changed nothing.
        for _ in 0..5 {
            core.flush(&key);
        }

        // Let the outbox drain.
        for _ in 0..100 {
            if !channel.sent.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(
            channel.sent.lock().unwrap().len(),
            1,
            "an unchanged turn must cost exactly one message"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn auto_mode_approves_without_asking_and_the_gate_comes_back() {
        // The default a chat thread starts in, and the one it can go back to.
        // Both halves matter: auto mode that cannot be turned off is a product
        // with no gate at all, which is not what the flag was for.
        let dir = std::env::temp_dir().join(format!("sb-auto-{}", std::process::id()));
        let channel = Arc::new(CountingChannel {
            sent: std::sync::Mutex::new(Vec::new()),
        });
        let key = ThreadKey {
            channel: "fake",
            chat_id: "1".into(),
            topic_id: None,
        };

        let mut core = Core::new(
            Store::in_memory().unwrap(),
            vec![channel.clone()],
            PathBuf::from("/tmp"),
            "claude".into(),
            dir.clone(),
        );
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
            },
        );

        let input = serde_json::json!({ "command": "rm -rf ./build" });
        core.on_permission(&key, "r1".into(), "Bash".into(), input.clone())
            .await
            .unwrap();

        assert!(
            core.threads[&key].pending.is_none(),
            "auto mode leaves nothing waiting on a human"
        );

        // Now with the gate back on, the same call has to be asked about.
        core.threads.get_mut(&key).unwrap().auto = false;
        core.on_permission(&key, "r2".into(), "Bash".into(), input)
            .await
            .unwrap();

        assert_eq!(
            core.threads[&key].pending.as_ref().map(|p| p.tool.as_str()),
            Some("Bash"),
        );

        // Let the outbox drain, then look at what the chat actually saw.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let sent = channel.sent.lock().unwrap().clone();
        let questions = sent.iter().filter(|m| m.starts_with("Run Bash?")).count();
        assert_eq!(questions, 1, "exactly one of the two calls was asked about");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_ungatable_agent_says_so_whichever_way_the_switch_is_set() {
        // "gate on" would be a promise nothing is keeping: codex, pi and the
        // tmux tier approve their own tools no matter what /auto says.
        assert!(approvals("claude", false).contains("ask"));
        assert!(!approvals("claude", true).contains("cannot"));
        for agent in ["codex", "pi", "omp"] {
            assert!(
                approvals(agent, false).contains("cannot be gated"),
                "{agent}"
            );
            assert!(
                approvals(agent, true).contains("cannot be gated"),
                "{agent}"
            );
        }
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
