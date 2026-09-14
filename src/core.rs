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
use crate::command::{self, Command, ModelRequest};
use crate::event::{AgentEvent, Decision};
use crate::outbox::{OutJob, Outbox};
use crate::render::TurnRenderer;
use crate::store::Store;

/// How often a streaming turn is pushed to the chat. Comfortably inside
/// Telegram's roughly-one-per-second-per-chat budget, and slow enough that a
/// reader is not watching text reflow constantly.
const FLUSH_INTERVAL: Duration = Duration::from_millis(1500);

/// How often the "working" indicator is refreshed while a turn runs.
///
/// Telegram's chat action expires after about five seconds and cannot be
/// extended, so anything slower leaves the chat looking idle mid-turn. Well
/// inside the per-chat budget even alongside the flushes, and typing is the
/// one job the outbox drops rather than retries.
const TYPING_INTERVAL: Duration = Duration::from_secs(4);

/// Tools that require a human decision. Everything else is approved by
/// omatether without bothering anyone.
///
/// A gate on every tool is unusable from a phone — the agent reads a dozen
/// files before it does anything consequential. Gating what writes or executes
/// keeps the prompts meaningful.
const GATED_TOOLS: &[&str] = &["Bash", "Write", "Edit", "NotebookEdit"];

/// Above this many characters, a reply on a channel that cannot edit is spilled
/// to a file and the chat gets a pointer instead.
///
/// iMessage delivers a turn whole, at the end, and clips a long message — which
/// loses the tail silently. A file plus the command to read it loses nothing,
/// and the tailnet already makes it reachable. A channel that can edit pages
/// instead: see [`PAGE_BUDGET`].
const SPILL_THRESHOLD: usize = 2500;

/// The most a turn puts in one message on a channel that can edit, before it
/// carries on in the next.
///
/// Under Telegram's 4096 — and under the adapter's own clip at 3900, which
/// measures the markdown rather than what Telegram counts, so this is a safe
/// bound on both. The pages are markdown too, and rendering only ever removes
/// characters (fences, `**`, `#`), never adds them.
const PAGE_BUDGET: usize = 3500;

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
    /// Whether a turn is running, as the *chat* sees it: set when a prompt is
    /// accepted, cleared by the event that ends the turn.
    ///
    /// Not a duplicate of `session.is_busy()`, which is the adapter's view of
    /// its own process and clears on its own schedule — Claude's before the
    /// end of the turn goes out, Codex's and pi's a moment after. The working
    /// indicator comes down on whichever of the two says the turn is over
    /// first, so neither a late flag nor a missed event can leave it up.
    turn_running: bool,
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
    /// Where `/new` sends a thread, whatever it was doing before. Held rather
    /// than recomputed per call so a test can point it at a directory that
    /// exists; see [`work_dir`] for why it is `~/Work`.
    fresh_cwd: Option<PathBuf>,
    threads: HashMap<ThreadKey, Thread>,
    /// When each thread's "working" indicator was last sent, and by its
    /// presence, that one is showing at all.
    ///
    /// Kept beside `threads` rather than inside a [`Thread`] for the same
    /// reason `outboxes` is: an indicator has to be turned off after the
    /// session it belonged to is gone, and `/new` and `/cd` drop sessions
    /// mid-turn.
    typing: HashMap<ThreadKey, std::time::Instant>,
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
            fresh_cwd: work_dir(),
            threads: HashMap::new(),
            typing: HashMap::new(),
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
                    // After the flush, so that the first text of a turn to
                    // reach the chat retires the indicator in the same tick.
                    self.refresh_typing_all();
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
                Command::Model(want) => self.on_model(&message.thread, want).await,
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
        thread.turn_running = true;

        // A new turn grows a new message rather than continuing the last one's.
        self.queue(key, OutJob::NewTurn);

        // Straight away rather than on the next tick: the gap between sending
        // a prompt and seeing anything at all is exactly what this is for.
        self.refresh_typing(key);
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
        // And start where an agent launched from Omarchy starts, rather than
        // wherever the last session happened to leave this thread. `/new` is
        // the "begin something else" command, and the directory the previous
        // task ran in is a worse guess at the next task's than the one place
        // work is kept. /cd is still one message away.
        //
        // Checked here rather than at startup so that creating the directory
        // takes effect without a restart — and a machine that has no ~/Work
        // keeps the thread where it was, because sending a session somewhere
        // that is not there fails later, at the spawn, where the reason is
        // much harder to see.
        match self.fresh_cwd.as_ref().filter(|dir| dir.is_dir()) {
            Some(dir) => state.cwd = dir.to_string_lossy().to_string(),
            None => tracing::debug!("no fresh-session directory; staying in {}", state.cwd),
        }
        self.store.put(&state)?;

        self.say(
            key,
            &format!(
                "New {} session in {}.\nmodel {}",
                state.agent,
                state.cwd,
                model_label(&state)
            ),
        )
    }

    async fn on_stop(&mut self, key: &ThreadKey) -> Result<()> {
        match self.threads.get_mut(key) {
            Some(thread) => {
                thread.session.cancel().await?;
                thread.renderer.reset();
                thread.turn_running = false;
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
            "agent    {}\nmodel    {}\ndir      {}\nsession  {}\nstate    {}\ntools    {}",
            live_agent.unwrap_or(&state.agent),
            model_label(&state),
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
        // The remembered model belonged to the agent being left. Reporting
        // claude's model under codex's name would be a confident lie, and
        // "unknown until the first turn" is true. What was *asked* for goes
        // with it: `opus` means nothing to codex, and pi wants a
        // `provider/id` — a model name is one agent's vocabulary, not a
        // setting that travels.
        state.model = None;
        state.requested_model = None;
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

    /// Read or change the model this thread's agent runs.
    ///
    /// The one switch here that keeps the conversation. `/cd` and `/agent`
    /// start a fresh session because a directory is fixed when a process
    /// starts and a transcript cannot move between agents; a model is neither.
    /// Claude Code changes it on the live session, and the two agents that run
    /// a process per turn simply pass a different flag to the next one.
    async fn on_model(&mut self, key: &ThreadKey, want: ModelRequest) -> Result<()> {
        let mut state = self.state(key)?;

        let wanted = match want {
            ModelRequest::Report => return self.report_model(key, &state),
            ModelRequest::Reset => None,
            // Never a valid name in any of the three vocabularies, and the
            // shape of a sentence typed at a command: "/model use opus please".
            ModelRequest::Set(name) if name.split_whitespace().count() > 1 => {
                let note = format!(
                    "Usage: /model <name>\n\nNo model is called '{name}'. \
                     /model on its own says what is running."
                );
                return self.say(key, &note);
            }
            ModelRequest::Set(name) => Some(name),
        };

        if !agent::takes_model(&state.agent) {
            let note = format!(
                "{} runs through omarchy-agent, which takes no model. Set it in \
                 {}'s own config, or /attach and change it there. /agent claude, \
                 codex or pi for one that can be told from here.",
                state.agent, state.agent
            );
            return self.say(key, &note);
        }

        // Tell the session, when one is up. This is also the only place a name
        // is checked: Claude Code answers a bad one with a reason, while
        // `--model` at spawn takes anything and fails a turn later instead.
        let confirmed = match self.threads.get_mut(key) {
            Some(thread) => match thread.session.set_model(wanted.as_deref()).await {
                Ok(resolved) => Some(resolved),
                Err(e) => {
                    let note = format!("{} would not take that: {e}", state.agent);
                    return self.say(key, &note);
                }
            },
            None => None,
        };

        // What the agent resolved it to, else what was typed, else nothing —
        // and nothing is the right answer after a reset nobody confirmed,
        // because the default is the agent's to know, not ours to guess.
        let checked = confirmed.is_some();
        state.model = confirmed.flatten().or_else(|| wanted.clone());
        state.requested_model = wanted.clone();
        self.store.put(&state)?;

        let mut note = match (&wanted, &state.model) {
            (Some(_), Some(model)) => {
                format!("From the next turn, {} runs {model}.", state.agent)
            }
            (None, Some(model)) => format!("{} picks its own model again ({model}).", state.agent),
            (_, None) => format!("{} picks its own model again.", state.agent),
        };

        // Said only when it is true. A name nobody checked is a turn that fails
        // later for a reason nothing here will have mentioned.
        if !checked && wanted.is_some() {
            note.push_str(
                "\n\nNothing is running to check the name against — if the agent \
                 does not know it, the next turn is where that shows up.",
            );
        }
        self.say(key, &note)
    }

    /// What `/model` says when asked nothing: what is running, and what else
    /// this agent has said it will take.
    fn report_model(&mut self, key: &ThreadKey, state: &crate::store::ThreadState) -> Result<()> {
        let mut note = match state.model.as_deref().or(state.requested_model.as_deref()) {
            Some(model) => format!("{} is running {model}.", state.agent),
            None => format!(
                "{} has not said which model it runs; it reports one on the \
                 first turn.",
                state.agent
            ),
        };

        // Worth saying when they differ, and only then: it is the difference
        // between what runs now and what the next session will ask for.
        match &state.requested_model {
            Some(asked) if Some(asked.as_str()) != state.model.as_deref() => {
                note.push_str(&format!(" Asked for: {asked}."));
            }
            _ => {}
        }

        if !agent::takes_model(&state.agent) {
            note.push_str(
                "\n\nThis one launches through omarchy-agent, which takes no \
                 model, so /model cannot change it from here.",
            );
            return self.say(key, &note);
        }

        note.push_str("\n\n/model <name> switches; /model default hands the choice back.");

        // A list the agent volunteered first — for claude that is the one that
        // names what this session actually runs, and it is only there while the
        // session is up. A list hard-coded here would rot against the agent
        // that knows.
        let offered = self
            .threads
            .get(key)
            .map(|thread| thread.session.models())
            .unwrap_or_default();
        if !offered.is_empty() {
            note.push_str(&format!("\nIt offers: {}", offered.join(", ")));
            return self.say(key, &note);
        }

        // Nothing volunteered, so ask the CLI — on a task of its own. Asking
        // means waiting on a process, and this is the core's loop: waiting
        // here stops every other thread's events and flushes for as long as it
        // takes, which on the timeout path is twenty seconds. The finished
        // note goes out through the same outbox everything else does, so it is
        // still one message rather than two.
        let Some(outbox) = self.outbox(key).cloned() else {
            return Ok(());
        };
        let name = state.agent.clone();
        let cwd = PathBuf::from(&state.cwd);
        tokio::spawn(async move {
            match agent::list_models(&name, &cwd).await {
                Ok(list) => note.push_str(&format!("\nIt offers: {}", list.join(", "))),
                // `{e:#}` and not `{e}`: the outermost context is the command
                // that was run, and the reason it failed is behind it.
                Err(e) => note.push_str(&format!("\n{e:#}")),
            }
            outbox.queue(OutJob::Say(note));
        });
        Ok(())
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

        // The agent has the answer and is working again.
        self.refresh_typing(key);
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
            AgentEvent::Ready {
                session_id, model, ..
            } if !session_id.is_empty() => {
                let mut state = self.state(key)?;
                let changed = state.session_id.as_deref() != Some(session_id.as_str())
                    // Only when the agent said something. A `None` here means
                    // "this adapter does not report a model", not "the model
                    // went away", and must not erase what a claude session
                    // already told us.
                    || (model.is_some() && state.model != *model);
                if changed {
                    state.session_id = Some(session_id.clone());
                    if model.is_some() {
                        state.model = model.clone();
                    }
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
                thread.turn_running = false;
            }
            // Queued behind the last flush, so the indicator comes down as the
            // reply lands rather than a tick later.
            self.stop_typing(key);
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
        // What the exchange is waiting on is now a person. Dots under the
        // question would say the opposite.
        self.stop_typing(key);
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

    /// Whether this thread has something an indicator would be telling the
    /// truth about.
    ///
    /// Three ways it does not. The turn is over, so there is nothing to wait
    /// for. A permission question is outstanding, so what the exchange is
    /// waiting on is a person, not the agent — leaving the dots up there says
    /// the opposite. Or the turn's own message is already on screen and
    /// growing, which on a channel that can edit *is* the indicator; a second
    /// one costs rate limit to say what the first is already saying.
    ///
    /// Note the last one does not fire for an agent that does not stream, or a
    /// channel that cannot edit: those hold the whole turn back until it is
    /// finished, so the indicator is all there is for as long as it runs.
    fn is_working(&self, key: &ThreadKey) -> bool {
        let Some(thread) = self.threads.get(key) else {
            return false;
        };
        if !thread.turn_running || !thread.session.is_busy() || thread.pending.is_some() {
            return false;
        }
        match self.channel(key) {
            Some(channel) => !(channel.can_edit() && thread.renderer.has_sent()),
            None => false,
        }
    }

    /// Bring one thread's indicator in line with whether it is working.
    ///
    /// Idempotent and cheap to call often: it re-sends only once the platform
    /// would have expired the last one, and turns the indicator off exactly
    /// once, when it was on.
    fn refresh_typing(&mut self, key: &ThreadKey) {
        match (self.is_working(key), self.typing.get(key).copied()) {
            // Still showing and not stale yet.
            (true, Some(sent)) if sent.elapsed() < TYPING_INTERVAL => {}
            (true, _) => {
                if self.queue(key, OutJob::Typing { on: true }) {
                    self.typing.insert(key.clone(), std::time::Instant::now());
                }
            }
            (false, _) => self.stop_typing(key),
        }
    }

    /// Take the indicator down, if one is up.
    ///
    /// Forgotten before it is queued: an indicator nobody can turn off is a
    /// worse outcome than one turned off twice.
    fn stop_typing(&mut self, key: &ThreadKey) {
        if self.typing.remove(key).is_some() {
            self.queue(key, OutJob::Typing { on: false });
        }
    }

    /// Every thread showing an indicator, plus every thread that might need
    /// one. The two sets differ: a session dropped mid-turn by `/new` or `/cd`
    /// leaves an indicator behind and no thread to notice it.
    fn refresh_typing_all(&mut self) {
        let mut keys: Vec<ThreadKey> = self.threads.keys().cloned().collect();
        keys.extend(
            self.typing
                .keys()
                .filter(|key| !self.threads.contains_key(*key))
                .cloned(),
        );
        for key in keys {
            self.refresh_typing(&key);
        }
    }

    fn flush_all(&mut self) {
        // A completed turn is flushed immediately from `on_agent_event`, then
        // its renderer is reset. Do not let the next debounce tick flush that
        // fresh renderer, or it would edit the Telegram message back to
        // `working…`. The final flush itself is still unaffected: it happens
        // before `turn_running` is cleared.
        let keys: Vec<ThreadKey> = self
            .threads
            .iter()
            .filter_map(|(key, thread)| thread.turn_running.then_some(key.clone()))
            .collect();

        for key in keys {
            self.flush(&key);
        }
    }

    /// Push a thread's pending text.
    ///
    /// On a channel that can edit, this grows the turn's message as it runs,
    /// and carries on in a new one when that fills up. On one that cannot —
    /// iMessage — mid-turn flushes are skipped entirely and the turn arrives as
    /// a single finished message, because the alternative is a stream of
    /// fragments nobody wants to read on a phone.
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
                // Telegram has a typing action for the pre-answer gap. Do not
                // turn the renderer's fallback placeholder into a real
                // message; otherwise the first flush posts `working…` and the
                // answer has to edit over it. The typing action remains active
                // until actual content reaches the chat.
                if can_edit && !thread.renderer.has_content() {
                    return;
                }
                match thread.renderer.pending() {
                    Some(text) => (text, thread.renderer.compose_prose()),
                    None => return,
                }
            }
            None => return,
        };
        let (text, prose) = text;

        if can_edit {
            self.flush_pages(key, text);
            return;
        }

        // Spilling rewrites the message into a pointer, but what the renderer
        // has to remember is the text it composed: comparing next time against
        // the pointer would make every tick look like a change and re-send the
        // whole turn.
        let payload = self.spill_if_long(key, text.clone(), prose);

        // Only once it has been accepted for delivery is it no longer owed.
        if self.queue(key, OutJob::Turn(payload)) {
            if let Some(thread) = self.threads.get_mut(key) {
                thread.renderer.mark_sent(text);
            }
        }
    }

    /// A turn on a channel that can edit, as however many messages it takes.
    ///
    /// This replaced spilling there. The ssh pointer is an answer at a desk and
    /// a non-answer from a phone, and on a channel that streams it was worse
    /// than that: every tick of a long turn wrote another file, and the moment
    /// the turn passed the threshold its progress was replaced by the pointer.
    /// Now a message that fills up is finished and the turn carries on below
    /// it, so nothing is cut and nothing leaves the chat.
    ///
    /// `text` is the whole turn as composed, which is what the renderer compares
    /// against next time — not the current page, which would look unchanged
    /// while a new page was owed.
    fn flush_pages(&mut self, key: &ThreadKey, text: String) {
        let Some(pages) = self
            .threads
            .get(key)
            .map(|thread| thread.renderer.paginate(PAGE_BUDGET))
        else {
            return;
        };

        // One page at a time, and each is remembered only once accepted, so a
        // full queue leaves the rest owed rather than skipped.
        for (page, next) in pages.sealed {
            if !self.queue(key, OutJob::Seal(page)) {
                return;
            }
            if let Some(thread) = self.threads.get_mut(key) {
                thread.renderer.begin_page(next);
            }
        }

        if self.queue(key, OutJob::Turn(pages.current)) {
            if let Some(thread) = self.threads.get_mut(key) {
                thread.renderer.mark_sent(text);
            }
        }
    }

    /// Write an over-long reply to a file and hand back something that fits.
    ///
    /// Three attempts, in order of how much of the answer survives:
    ///
    /// 1. the whole turn, when it fits;
    /// 2. the turn without its tool log, when *that* fits — the log is the bulk
    ///    of a working turn and the least of it, so this is nearly always the
    ///    one that runs, and the answer arrives whole;
    /// 3. the **end** of the prose, plus a pointer. Cutting from the front is
    ///    deliberate: a long answer builds to its conclusion, and a reader who
    ///    can see the file has lost only the run-up.
    ///
    /// Falls back to the untouched text if the file cannot be written — a
    /// clipped reply is worse than a whole one, but both beat no reply.
    fn spill_if_long(&self, key: &ThreadKey, text: String, prose: String) -> String {
        if text.chars().count() <= SPILL_THRESHOLD {
            return text;
        }

        // The file always holds the whole turn, tool log included: it is the
        // record, and the thing a pointer would be lying about if it did not.
        let path = match self.spill(key, &text) {
            Some(path) => path,
            None => return text,
        };
        let pointer = format!("  ssh {} -t 'cat {}'", hostname(), path.display());

        if !prose.is_empty() && prose.chars().count() <= SPILL_THRESHOLD {
            return format!("{prose}\n\nFull turn:\n\n{pointer}");
        }

        // Nothing to prefer between them, so cut whichever is the real answer.
        let long = if prose.is_empty() { &text } else { &prose };
        let cut = long.chars().count() - SPILL_THRESHOLD;
        let tail: String = long.chars().skip(cut).collect();
        format!(
            "[first {cut} characters omitted — {} in all]\n\n{tail}\n\nFull turn:\n\n{pointer}",
            long.chars().count(),
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
            // What was asked for, not what was reported: a thread that never
            // ran /model has to keep letting the agent choose, rather than
            // pinning itself to whatever it happened to default to once.
            model: state.requested_model.clone(),
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
                turn_running: false,
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

/// Where `/new` starts a thread, before checking that it is there.
///
/// The same place `omarchy-agent` steps into before launching an agent from
/// the keybinding or the menu: agents refuse to remember trust for a home
/// directory and re-ask on every session, so a launch starts one level down
/// instead. Spelt the way Omarchy spells it, capital included, since the point
/// is to land in the directory a terminal agent would have.
fn work_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join("Work"))
}

/// What to call this thread's model in a message sent when no agent process
/// exists to ask — which is every `/new`, and `/status` on an idle thread.
///
/// The reported name first: it is the resolved one (`claude-opus-5` for an
/// `opus` that was asked for), and it is what actually ran. What was asked for
/// is the fallback for the agents that never report anything.
fn model_label(state: &crate::store::ThreadState) -> &str {
    state
        .model
        .as_deref()
        .or(state.requested_model.as_deref())
        .unwrap_or("(reported on the first turn)")
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

        assert_eq!(
            core.spill_if_long(&key, "short".into(), "short".into()),
            "short"
        );

        // A turn with no prose to prefer — all of it is the tool log.
        let long = "x".repeat(SPILL_THRESHOLD + 500);
        let pointed = core.spill_if_long(&key, long.clone(), String::new());
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
    fn a_long_turn_keeps_its_answer_and_spills_the_tool_log() {
        // The bug this exists for: a 6,500-character turn was cut at 2,500 from
        // the front, and the front of a working turn is its tool log. What
        // arrived was Bash lines ending mid-command, and every word of the
        // answer was in the part that went to the file. From a phone that is
        // indistinguishable from no reply at all.
        let dir = std::env::temp_dir().join(format!("sb-spill3-{}", std::process::id()));
        let core = core_with_spill_dir(&dir);
        let key = fake_key();

        let log = "▸ Bash  `cargo test`\n".repeat(200);
        let answer = "Here is what I changed and why.";
        let whole = format!("{log}{answer}");
        let prose = answer.to_string();

        let sent = core.spill_if_long(&key, whole, prose);
        assert!(sent.contains(answer), "the answer survives whole: {sent}");
        assert!(!sent.contains('▸'), "the log does not: {sent}");
        assert!(sent.contains("ssh "), "and it is still readable in full");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_answer_too_long_even_alone_is_cut_from_the_front() {
        // A long answer builds to its conclusion, so the end is the half worth
        // keeping — the opposite of what cutting by position gives you.
        let dir = std::env::temp_dir().join(format!("sb-spill4-{}", std::process::id()));
        let core = core_with_spill_dir(&dir);
        let key = fake_key();

        let prose = format!("{}THE CONCLUSION", "preamble. ".repeat(400));
        let sent = core.spill_if_long(&key, prose.clone(), prose);

        assert!(sent.contains("THE CONCLUSION"), "kept the end");
        assert!(
            sent.contains("characters omitted"),
            "and says it cut: {sent}"
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
        /// Every send and edit, in order.
        sent: std::sync::Mutex<Vec<String>>,
        /// Each message as it reads now, by position: what scrolling back
        /// through the chat would show.
        messages: std::sync::Mutex<Vec<String>>,
        /// Every `typing` call, in order, as `on`.
        typing: std::sync::Mutex<Vec<bool>>,
        can_edit: bool,
    }

    impl CountingChannel {
        /// Telegram's shape: a turn grows one message.
        fn editing() -> Arc<Self> {
            Arc::new(Self {
                sent: std::sync::Mutex::new(Vec::new()),
                messages: std::sync::Mutex::new(Vec::new()),
                typing: std::sync::Mutex::new(Vec::new()),
                can_edit: true,
            })
        }

        /// iMessage's shape: nothing arrives until the turn is done.
        fn write_only() -> Arc<Self> {
            Arc::new(Self {
                sent: std::sync::Mutex::new(Vec::new()),
                messages: std::sync::Mutex::new(Vec::new()),
                typing: std::sync::Mutex::new(Vec::new()),
                can_edit: false,
            })
        }

        fn typing(&self) -> Vec<bool> {
            self.typing.lock().unwrap().clone()
        }

        fn messages(&self) -> Vec<String> {
            self.messages.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl Channel for CountingChannel {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn can_edit(&self) -> bool {
            self.can_edit
        }
        async fn typing(&self, _t: &ThreadKey, on: bool) -> Result<()> {
            self.typing.lock().unwrap().push(on);
            Ok(())
        }
        async fn send(&self, _t: &ThreadKey, text: &str) -> Result<String> {
            self.sent.lock().unwrap().push(text.to_string());
            let mut messages = self.messages.lock().unwrap();
            messages.push(text.to_string());
            Ok((messages.len() - 1).to_string())
        }
        async fn edit(&self, _t: &ThreadKey, id: &String, text: &str) -> Result<()> {
            self.sent.lock().unwrap().push(text.to_string());
            let index: usize = id.parse()?;
            self.messages.lock().unwrap()[index] = text.to_string();
            Ok(())
        }
        async fn ask_permission(&self, t: &ThreadKey, text: &str, _q: &str) -> Result<String> {
            self.send(t, text).await
        }
    }

    /// An agent that runs no process. Enough for the core to have a session.
    struct StubAgent {
        busy: bool,
        /// What `set_model` resolves the name to, when it resolves one.
        resolves: Option<String>,
        /// Set to refuse the way a real CLI refuses a name it does not know.
        refuses: Option<String>,
        /// What it offers, and what it was last told, for asserting on both.
        offers: Vec<String>,
        told: Arc<std::sync::Mutex<Option<Option<String>>>>,
    }

    impl StubAgent {
        fn idle() -> Self {
            Self {
                busy: false,
                resolves: None,
                refuses: None,
                offers: Vec::new(),
                told: Arc::new(std::sync::Mutex::new(None)),
            }
        }

        /// A turn in flight, which is the state the working indicator is about.
        fn busy() -> Self {
            Self {
                busy: true,
                ..Self::idle()
            }
        }

        /// Answers `/model` the way Claude Code does: with the id the alias it
        /// was given actually resolves to.
        fn resolving(to: &str) -> Self {
            Self {
                resolves: Some(to.to_string()),
                ..Self::idle()
            }
        }

        /// Refuses it, the way the CLI refuses a name it does not know.
        fn refusing(why: &str) -> Self {
            Self {
                refuses: Some(why.to_string()),
                ..Self::idle()
            }
        }

        fn offering(models: &[&str]) -> Self {
            Self {
                offers: models.iter().map(|m| m.to_string()).collect(),
                ..Self::idle()
            }
        }
    }

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
            self.busy
        }
        async fn prompt(&mut self, _text: &str) -> Result<()> {
            Ok(())
        }
        async fn set_model(&mut self, model: Option<&str>) -> Result<Option<String>> {
            *self.told.lock().unwrap() = Some(model.map(String::from));
            match &self.refuses {
                Some(why) => anyhow::bail!("{why}"),
                None => Ok(self.resolves.clone()),
            }
        }
        fn models(&self) -> Vec<String> {
            self.offers.clone()
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
        // when that mistake is reintroduced. Only a channel that cannot edit
        // spills now, so that is the one this runs against.
        let dir = std::env::temp_dir().join(format!("sb-tick-{}", std::process::id()));
        let channel = CountingChannel::write_only();
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
        renderer.apply(&AgentEvent::TurnEnd {
            ok: true,
            detail: None,
        });
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::idle()),
                renderer,
                pending: None,
                auto: true,
                turn_running: false,
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
        let channel = CountingChannel::editing();
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
                session: Box::new(StubAgent::idle()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: false,
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

    /// Poll until the outbox has drained what the core queued.
    async fn settle() {
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    }

    fn core_with(channel: Arc<CountingChannel>, dir: &std::path::Path) -> Core {
        Core::new(
            Store::in_memory().unwrap(),
            vec![channel],
            PathBuf::from("/tmp"),
            "claude".into(),
            dir.to_path_buf(),
        )
    }

    fn fake_key() -> ThreadKey {
        ThreadKey {
            channel: "fake",
            chat_id: "1".into(),
            topic_id: None,
        }
    }

    #[tokio::test]
    async fn the_working_indicator_stops_once_the_turn_itself_is_on_screen() {
        // On a channel that can edit, the gap the indicator exists to cover is
        // only the one before the first flush: after that the turn's own
        // message is growing, and a second signal costs rate limit to repeat
        // what the first is already saying.
        let dir = std::env::temp_dir().join(format!("sb-typ1-{}", std::process::id()));
        let channel = CountingChannel::editing();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: true,
            },
        );

        core.refresh_typing(&key);
        settle().await;
        assert_eq!(channel.typing(), vec![true], "nothing shown yet, so say so");

        // The agent says something and it reaches the chat.
        core.threads
            .get_mut(&key)
            .unwrap()
            .renderer
            .apply(&AgentEvent::Text {
                text: "working on it".into(),
            });
        core.flush(&key);
        core.refresh_typing_all();
        settle().await;

        assert_eq!(
            channel.typing(),
            vec![true, false],
            "the growing message took over"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_long_turn_on_an_editing_channel_pages_instead_of_spilling() {
        // What a working turn looked like from a phone: its progress replaced
        // by an ssh line the moment it passed 2,500 characters, a new file
        // written on every tick after that, and the answer behind the pointer.
        let dir = std::env::temp_dir().join(format!("sb-pages-{}", std::process::id()));
        let channel = CountingChannel::editing();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: true,
            },
        );

        let paragraphs: Vec<String> = (0..40)
            .map(|i| format!("Finding {i}: {}", "detail ".repeat(30).trim_end()))
            .collect();
        for paragraph in &paragraphs {
            let thread = core.threads.get_mut(&key).unwrap();
            thread.renderer.apply(&AgentEvent::TextDelta {
                text: format!("{paragraph}\n\n"),
            });
            core.flush(&key);
        }
        let thread = core.threads.get_mut(&key).unwrap();
        thread.renderer.apply(&AgentEvent::TurnEnd {
            ok: true,
            detail: None,
        });
        core.flush(&key);
        settle().await;

        let messages = channel.messages();
        assert!(messages.len() >= 3, "{} messages", messages.len());
        for message in &messages {
            assert!(message.chars().count() <= PAGE_BUDGET, "{message:?}");
            assert!(!message.contains("ssh "), "no pointer: {message:?}");
        }
        for paragraph in &paragraphs {
            assert_eq!(
                messages
                    .iter()
                    .filter(|m| m.contains(paragraph.as_str()))
                    .count(),
                1,
                "{paragraph:?} arrives whole, once"
            );
        }
        assert!(!dir.exists(), "nothing spilled");

        // And ticks with nothing new cost nothing, pages or not.
        let before = channel.sent.lock().unwrap().len();
        core.flush(&key);
        core.flush(&key);
        settle().await;
        assert_eq!(channel.sent.lock().unwrap().len(), before);
    }

    #[tokio::test]
    async fn a_completed_editing_turn_is_not_flushed_again_on_the_next_tick() {
        let dir = std::env::temp_dir().join(format!("sb-flush-after-end-{}", std::process::id()));
        let channel = CountingChannel::editing();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: true,
            },
        );

        let thread = core.threads.get_mut(&key).unwrap();
        thread.renderer.apply(&AgentEvent::Text {
            text: "the complete answer".into(),
        });
        thread.renderer.apply(&AgentEvent::TurnEnd {
            ok: true,
            detail: None,
        });
        core.flush(&key);
        let thread = core.threads.get_mut(&key).unwrap();
        thread.renderer.reset();
        thread.turn_running = false;

        // The next debounce tick must not edit the answer back to `working…`.
        core.flush_all();
        settle().await;

        assert_eq!(
            channel.sent.lock().unwrap().clone(),
            vec!["the complete answer"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_completed_non_editing_turn_still_flushes_its_complete_answer() {
        let dir = std::env::temp_dir().join(format!("sb-flush-imessage-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: true,
            },
        );

        let thread = core.threads.get_mut(&key).unwrap();
        thread.renderer.apply(&AgentEvent::Text {
            text: "the complete iMessage answer".into(),
        });
        thread.renderer.apply(&AgentEvent::TurnEnd {
            ok: true,
            detail: None,
        });
        core.flush(&key);
        let thread = core.threads.get_mut(&key).unwrap();
        thread.renderer.reset();
        thread.turn_running = false;
        core.flush_all();
        settle().await;

        assert_eq!(
            channel.sent.lock().unwrap().clone(),
            vec!["the complete iMessage answer"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_channel_that_cannot_edit_keeps_the_indicator_for_the_whole_turn() {
        // iMessage: nothing arrives until the turn is finished, so the
        // indicator is the only sign of life there is, and it has to be
        // refreshed rather than sent once — Telegram's expires in about five
        // seconds, and a turn takes longer than that.
        let dir = std::env::temp_dir().join(format!("sb-typ2-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: true,
            },
        );

        core.refresh_typing(&key);
        // Mid-turn text changes nothing here: it is held back until the end.
        core.threads
            .get_mut(&key)
            .unwrap()
            .renderer
            .apply(&AgentEvent::Text {
                text: "working on it".into(),
            });
        core.flush(&key);
        core.refresh_typing_all();
        settle().await;
        assert_eq!(channel.typing(), vec![true], "still the only sign of life");

        // Ticks inside the refresh interval do not re-send it either.
        core.refresh_typing_all();
        core.refresh_typing_all();
        settle().await;
        assert_eq!(channel.typing(), vec![true]);

        // Once it is stale, it is renewed.
        *core.typing.get_mut(&key).unwrap() =
            std::time::Instant::now() - TYPING_INTERVAL - Duration::from_millis(1);
        core.refresh_typing_all();
        settle().await;
        assert_eq!(channel.typing(), vec![true, true], "renewed before expiry");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn the_indicator_comes_down_with_the_turn_and_while_a_question_waits() {
        // Both are the same mistake: dots that say the agent is working when
        // it is finished, or when what it is waiting for is a person. On
        // iMessage the indicator does not expire on its own, so nothing else
        // takes it down.
        let dir = std::env::temp_dir().join(format!("sb-typ3-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: false,
                turn_running: true,
            },
        );

        core.refresh_typing(&key);
        settle().await;
        assert_eq!(channel.typing(), vec![true]);

        // A gated tool call: the turn is now blocked on someone reading it.
        core.on_permission(
            &key,
            "r1".into(),
            "Bash".into(),
            serde_json::json!({ "command": "ls" }),
        )
        .await
        .unwrap();
        settle().await;
        assert_eq!(
            channel.typing(),
            vec![true, false],
            "a question is waiting on a person, not on the agent"
        );

        // Answering it puts the agent back to work.
        core.decide(&key, Decision::allow(), None).await.unwrap();
        settle().await;
        assert_eq!(channel.typing(), vec![true, false, true]);

        // And the end of the turn takes it down for good, even though this
        // agent has not got round to clearing its busy flag.
        core.on_agent_event(
            &key,
            AgentEvent::TurnEnd {
                ok: true,
                detail: None,
            },
        )
        .await
        .unwrap();
        settle().await;
        assert_eq!(channel.typing(), vec![true, false, true, false]);

        // Nothing left to turn off, so no second one.
        core.refresh_typing_all();
        settle().await;
        assert_eq!(channel.typing().len(), 4);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_session_dropped_mid_turn_does_not_leave_the_dots_up() {
        // `/new` and `/cd` end a session while a turn is running. The thread
        // is gone, so anything that looks for the indicator through `threads`
        // will never find this one to turn it off — which on iMessage means a
        // chat left showing three dots indefinitely.
        let dir = std::env::temp_dir().join(format!("sb-typ4-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::busy()),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: true,
            },
        );
        core.refresh_typing(&key);
        settle().await;
        assert_eq!(channel.typing(), vec![true]);

        core.on_new(&key).await.unwrap();
        core.refresh_typing_all();
        settle().await;

        assert_eq!(channel.typing(), vec![true, false]);
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

    #[tokio::test]
    async fn a_fresh_session_goes_back_to_the_work_directory() {
        // The point of /new is "begin something else", so it starts where a
        // terminal agent would rather than in whatever directory the last task
        // left behind.
        let dir = std::env::temp_dir().join(format!("sb-new1-{}", std::process::id()));
        let work = dir.join("Work");
        std::fs::create_dir_all(&work).unwrap();

        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);
        core.fresh_cwd = Some(work.clone());

        let mut state = core.state(&key).unwrap();
        state.cwd = "/somewhere/else".into();
        state.session_id = Some("old-session".into());
        state.model = Some("claude-opus-5".into());
        core.store.put(&state).unwrap();

        core.on_new(&key).await.unwrap();
        settle().await;

        let after = core.state(&key).unwrap();
        assert_eq!(after.cwd, work.to_string_lossy());
        assert_eq!(after.session_id, None, "the old conversation is let go");

        let sent = channel.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("claude"), "which agent: {}", sent[0]);
        assert!(
            sent[0].contains("claude-opus-5"),
            "which model: {}",
            sent[0]
        );
        assert!(sent[0].contains(&*work.to_string_lossy()), "where");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_missing_work_directory_leaves_the_thread_where_it_was() {
        // Sending a session to a directory that is not there would fail at the
        // spawn, several messages later, where the reason is much less clear.
        let dir = std::env::temp_dir().join(format!("sb-new2-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);
        core.fresh_cwd = Some(dir.join("no-such-Work"));

        let mut state = core.state(&key).unwrap();
        state.cwd = "/somewhere/else".into();
        core.store.put(&state).unwrap();

        core.on_new(&key).await.unwrap();
        settle().await;

        assert_eq!(core.state(&key).unwrap().cwd, "/somewhere/else");

        // And with no session ever run, the model is admitted as unknown
        // rather than guessed at.
        let sent = channel.sent.lock().unwrap().clone();
        assert!(sent[0].contains("first turn"), "{}", sent[0]);
    }

    #[tokio::test]
    async fn the_model_is_remembered_from_the_session_that_reported_it() {
        // It is wanted when no agent process exists to ask — /new answers
        // before anything has spawned — so it has to survive the session.
        let dir = std::env::temp_dir().join(format!("sb-model-{}", std::process::id()));
        let key = fake_key();
        let mut core = core_with(CountingChannel::write_only(), &dir);

        core.on_agent_event(
            &key,
            AgentEvent::Ready {
                session_id: "s1".into(),
                model: Some("claude-opus-5".into()),
                cwd: None,
                tools: Vec::new(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            core.state(&key).unwrap().model.as_deref(),
            Some("claude-opus-5")
        );

        // An adapter that reports no model is saying it does not know, not
        // that the model went away.
        core.on_agent_event(
            &key,
            AgentEvent::Ready {
                session_id: "s2".into(),
                model: None,
                cwd: None,
                tools: Vec::new(),
            },
        )
        .await
        .unwrap();
        let after = core.state(&key).unwrap();
        assert_eq!(after.session_id.as_deref(), Some("s2"));
        assert_eq!(after.model.as_deref(), Some("claude-opus-5"));

        // But it belongs to the agent that said it. Switching agents must not
        // report claude's model under codex's name.
        core.on_agent(&key, "codex").await.unwrap();
        assert_eq!(core.state(&key).unwrap().model, None);
    }

    #[tokio::test]
    async fn a_live_session_is_told_and_answers_with_what_will_run() {
        // The alias is what was typed; the resolved id is what runs. /status
        // should say the second, and the next spawn should ask for the first.
        let dir = std::env::temp_dir().join(format!("sb-model1-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        let agent = StubAgent::resolving("claude-opus-5");
        let told = agent.told.clone();
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(agent),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: false,
            },
        );

        core.on_model(&key, ModelRequest::Set("opus".into()))
            .await
            .unwrap();
        settle().await;

        assert_eq!(
            told.lock().unwrap().clone(),
            Some(Some("opus".to_string())),
            "the session is told, not just the database"
        );

        let after = core.state(&key).unwrap();
        assert_eq!(after.model.as_deref(), Some("claude-opus-5"), "what runs");
        assert_eq!(
            after.requested_model.as_deref(),
            Some("opus"),
            "what to ask"
        );

        let sent = channel.sent.lock().unwrap().clone();
        assert!(sent[0].contains("claude-opus-5"), "{}", sent[0]);
        assert!(
            !sent[0].contains("Nothing is running"),
            "it was checked: {}",
            sent[0]
        );
    }

    #[tokio::test]
    async fn a_refused_model_changes_nothing() {
        // The one place a bad name can be caught before a turn is spent on it,
        // so the agent's own reason has to survive to the phone — and nothing
        // may be remembered, or the next spawn would ask for it again.
        let dir = std::env::temp_dir().join(format!("sb-model2-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::refusing(
                    "Model \"opus-9\" is not a recognized model id",
                )),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: false,
            },
        );

        core.on_model(&key, ModelRequest::Set("opus-9".into()))
            .await
            .unwrap();
        settle().await;

        let after = core.state(&key).unwrap();
        assert_eq!(after.requested_model, None, "nothing is remembered");
        assert_eq!(after.model, None);

        let sent = channel.sent.lock().unwrap().clone();
        assert!(
            sent[0].contains("not a recognized model id"),
            "the agent's own reason: {}",
            sent[0]
        );
    }

    #[tokio::test]
    async fn without_a_session_the_name_is_remembered_but_said_to_be_unchecked() {
        // Nothing is running to reject a typo, and a model the agent does not
        // know fails at the next turn instead — where the reason is much
        // harder to connect to the message that caused it.
        let dir = std::env::temp_dir().join(format!("sb-model3-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.on_model(&key, ModelRequest::Set("haiku".into()))
            .await
            .unwrap();
        settle().await;

        let after = core.state(&key).unwrap();
        assert_eq!(after.requested_model.as_deref(), Some("haiku"));
        assert_eq!(
            after.model.as_deref(),
            Some("haiku"),
            "the best answer there is until something reports one"
        );

        let sent = channel.sent.lock().unwrap().clone();
        assert!(sent[0].contains("Nothing is running"), "{}", sent[0]);
    }

    #[tokio::test]
    async fn resetting_hands_the_choice_back_to_the_agent() {
        // Not the same as setting the model the agent happens to default to:
        // this thread has to keep asking for nothing, so that a later default
        // is picked up rather than frozen out.
        let dir = std::env::temp_dir().join(format!("sb-model4-{}", std::process::id()));
        let key = fake_key();
        let mut core = core_with(CountingChannel::write_only(), &dir);

        let agent = StubAgent::resolving("claude-sonnet-5");
        let told = agent.told.clone();
        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(agent),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: false,
            },
        );

        core.on_model(&key, ModelRequest::Set("opus".into()))
            .await
            .unwrap();
        core.on_model(&key, ModelRequest::Reset).await.unwrap();
        settle().await;

        assert_eq!(
            told.lock().unwrap().clone(),
            Some(None),
            "the agent is told to choose for itself"
        );
        let after = core.state(&key).unwrap();
        assert_eq!(after.requested_model, None, "and nothing is asked for");
        assert_eq!(
            after.model.as_deref(),
            Some("claude-sonnet-5"),
            "what the default resolved to is still worth reporting"
        );
    }

    #[tokio::test]
    async fn an_agent_with_nowhere_to_put_a_model_says_so() {
        // The detached tier launches through omarchy-agent, which takes no
        // model. Remembering one would mean promising a switch that never
        // happens.
        let dir = std::env::temp_dir().join(format!("sb-model5-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.on_agent(&key, "opencode").await.unwrap();
        core.on_model(&key, ModelRequest::Set("gpt-5".into()))
            .await
            .unwrap();
        settle().await;

        assert_eq!(core.state(&key).unwrap().requested_model, None);
        let sent = channel.sent.lock().unwrap().clone();
        let last = sent.last().unwrap();
        assert!(last.contains("omarchy-agent"), "why not: {last}");
        assert!(last.contains("/attach"), "and what to do instead: {last}");
    }

    #[tokio::test]
    async fn asking_for_nothing_reports_what_is_running_and_what_is_on_offer() {
        let dir = std::env::temp_dir().join(format!("sb-model6-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.threads.insert(
            key.clone(),
            Thread {
                session: Box::new(StubAgent::offering(&["default", "opus", "haiku"])),
                renderer: TurnRenderer::new(),
                pending: None,
                auto: true,
                turn_running: false,
            },
        );

        let mut state = core.state(&key).unwrap();
        state.model = Some("claude-opus-5".into());
        state.requested_model = Some("opus".into());
        core.store.put(&state).unwrap();

        core.on_model(&key, ModelRequest::Report).await.unwrap();
        settle().await;

        let sent = channel.sent.lock().unwrap().clone();
        assert!(sent[0].contains("claude-opus-5"), "{}", sent[0]);
        assert!(sent[0].contains("Asked for: opus"), "{}", sent[0]);
        // The list is the agent's, not a copy kept here.
        assert!(sent[0].contains("default, opus, haiku"), "{}", sent[0]);
    }

    #[tokio::test]
    async fn a_listless_agent_is_asked_and_a_listless_answer_said() {
        // No session, so nothing has volunteered a list; the answer is the
        // one the CLI gives when asked, or the reason it does not. Claude
        // has no such command, and its reason says how to get one rather
        // than leaving the question hanging.
        let dir = std::env::temp_dir().join(format!("sb-model9-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.on_model(&key, ModelRequest::Report).await.unwrap();
        settle().await;

        let sent = channel.sent.lock().unwrap().clone();
        let last = sent.last().unwrap();
        assert!(last.contains("live session"), "how to get the list: {last}");
        assert!(!last.contains("It offers"), "nothing was offered: {last}");
    }

    #[tokio::test]
    async fn a_sentence_is_not_a_model_name() {
        let dir = std::env::temp_dir().join(format!("sb-model7-{}", std::process::id()));
        let channel = CountingChannel::write_only();
        let key = fake_key();
        let mut core = core_with(channel.clone(), &dir);

        core.on_model(&key, ModelRequest::Set("use opus please".into()))
            .await
            .unwrap();
        settle().await;

        assert_eq!(core.state(&key).unwrap().requested_model, None);
        assert!(channel.sent.lock().unwrap()[0].contains("Usage:"));
    }

    #[tokio::test]
    async fn switching_agents_forgets_the_model_that_was_asked_for() {
        // A model name is one agent's vocabulary: `opus` means nothing to
        // codex, and carrying it across would spawn a process with a flag its
        // agent rejects.
        let dir = std::env::temp_dir().join(format!("sb-model8-{}", std::process::id()));
        let key = fake_key();
        let mut core = core_with(CountingChannel::write_only(), &dir);

        core.on_model(&key, ModelRequest::Set("opus".into()))
            .await
            .unwrap();
        assert_eq!(
            core.state(&key).unwrap().requested_model.as_deref(),
            Some("opus")
        );

        core.on_agent(&key, "codex").await.unwrap();
        assert_eq!(core.state(&key).unwrap().requested_model, None);
    }

    #[test]
    fn home_is_expanded_only_at_the_start() {
        std::env::set_var("HOME", "/home/test");
        assert_eq!(expand_home("~/src/x"), PathBuf::from("/home/test/src/x"));
        assert_eq!(expand_home("/abs/path"), PathBuf::from("/abs/path"));
        assert_eq!(expand_home("a/~/b"), PathBuf::from("a/~/b"));
    }
}
