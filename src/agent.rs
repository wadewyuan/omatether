//! Seam B: the interface every agent adapter implements.
//!
//! Shaped after ACP's four verbs — prompt, streamed update, permission
//! request, cancel — so a real ACP adapter can slot in beside the hand-written
//! ones without the core noticing.
//!
//! Updates do not appear here: a session hands back an
//! [`mpsc::Receiver<AgentEvent>`] when it is spawned, which keeps the read side
//! independent of the write side and lets the core `select!` over it.
//!
//! Agents differ in ways the core has to know about but must not hard-code, so
//! those differences are capabilities on the trait rather than checks against a
//! name — the same shape as [`crate::channel::Channel::can_edit`].

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::event::{AgentEvent, Decision};

#[async_trait]
pub trait Agent: Send {
    /// The agent's name, as its CLI is called (and as `omarchy default
    /// agent` spells it, on Omarchy).
    fn name(&self) -> &'static str;

    /// Whether tool calls are routed out for a decision.
    ///
    /// False means the agent approves its own tools and no
    /// [`AgentEvent::PermissionRequest`] will ever arrive — true of `codex
    /// exec`, whose only approval mode is automatic. Worth saying out loud when
    /// someone switches to it from a chat thread.
    fn gates_tools(&self) -> bool;

    /// Whether replies arrive progressively or all at once at the end.
    fn streams(&self) -> bool;

    /// Send a prompt to the agent.
    ///
    /// Errors if a turn is already running. One turn at a time per session is
    /// deliberate: from a phone, rejecting a second message is more predictable
    /// than queuing it, and far simpler than interleaving.
    async fn prompt(&mut self, text: &str) -> Result<()>;

    /// Answer an outstanding [`AgentEvent::PermissionRequest`]. A no-op on
    /// agents that do not gate tools.
    async fn decide(&mut self, _request_id: &str, _decision: Decision) -> Result<()> {
        Ok(())
    }

    /// Run a different model from the next turn on. `None` means "whatever you
    /// would have picked yourself".
    ///
    /// Returns the name the agent resolved it to, when it says: `opus` is not
    /// what `/status` should print if `claude-opus-5` is what will run. `None`
    /// means the switch was accepted but unnamed, which is the honest answer
    /// from an agent that takes the flag and never reports back.
    ///
    /// The error is shown to whoever asked, so it has to read as a reason —
    /// this is the one place a bad model name can be caught before a turn is
    /// spent on it.
    async fn set_model(&mut self, _model: Option<&str>) -> Result<Option<String>> {
        bail!("{} cannot be told which model to run", self.name())
    }

    /// Model names this agent has said it accepts, if it ever said.
    ///
    /// Empty is "it did not say", not "there are none". Claude Code
    /// volunteers its catalog in the handshake; codex and pi volunteer
    /// nothing, and the way to ask their CLIs is [`list_models`].
    fn models(&self) -> Vec<String> {
        Vec::new()
    }

    /// Interrupt the running turn. Harmless when nothing is running.
    async fn cancel(&mut self) -> Result<()>;

    /// True while a turn is in flight.
    fn is_busy(&self) -> bool;

    /// Ask the agent to exit, and wait for it briefly.
    async fn shutdown(&mut self) -> Result<()>;
}

/// Which adapter drives an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Bidirectional `stream-json` over one long-lived process.
    Claude,
    /// `codex exec --json`: one process per turn, continuity via `resume`.
    Codex,
    /// `pi -p --mode json`: one process per turn, continuity via `--session`.
    Pi,
    /// The Agent Client Protocol over one long-lived process: streaming,
    /// gated, and a model that can be changed mid-conversation.
    Acp,
    /// A detached tmux session, launched through Omarchy's `omarchy-agent`.
    /// No streaming and no gating, and only where Omarchy is installed — see
    /// [`available`].
    Tmux,
}

/// Every agent omatether knows how to drive, and how.
///
/// The list mirrors `omarchy-default-agent` so that on Omarchy `/agent <name>`
/// accepts whatever the desktop accepts. Off Omarchy the [`Backend::Tmux`]
/// rows are known but not [`available`]: their launcher is Omarchy's.
pub const AGENTS: &[(&str, Backend)] = &[
    ("claude", Backend::Claude),
    ("codex", Backend::Codex),
    ("pi", Backend::Pi),
    ("omp", Backend::Tmux),
    ("opencode", Backend::Acp),
    ("crush", Backend::Tmux),
    ("grok", Backend::Acp),
    ("copilot", Backend::Acp),
];

/// The launcher the detached tier execs. Omarchy's, not ours: it holds each
/// agent's own "don't stop to ask" flags, and a second copy of those here
/// would be the wrong thing to own.
pub const OMARCHY_AGENT: &str = "omarchy-agent";

/// Whether `name` is an executable file in a `$PATH` directory.
pub fn on_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
        .unwrap_or(false)
}

/// Whether this machine can run `name` at all, as far as omatether can tell
/// before trying.
///
/// Only the detached tier is checked: its launcher is part of Omarchy, so on
/// any other machine those agents would fail at every spawn. The others are
/// left to fail at the spawn with their own "is it on PATH?" — they are
/// installed one at a time and checking each here would hide which one is
/// missing behind a shorter list. Checked per call, not once, so installing
/// Omarchy's launcher needs no restart.
pub fn available(name: &str) -> bool {
    match backend_for(name) {
        Some(Backend::Tmux) => on_path(OMARCHY_AGENT),
        Some(_) => true,
        None => false,
    }
}

/// The agents [`available`] on this machine, in [`AGENTS`] order.
pub fn available_agents() -> Vec<&'static str> {
    AGENTS
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| available(name))
        .collect()
}

/// Why `name` cannot be used here, worded for whoever asked for it, or
/// `None` when it can.
pub fn unavailable_reason(name: &str) -> Option<String> {
    match backend_for(name) {
        None => Some(format!(
            "unknown agent '{name}' — try one of: {}",
            available_agents().join(", ")
        )),
        Some(Backend::Tmux) if !on_path(OMARCHY_AGENT) => Some(format!(
            "{name} launches through {OMARCHY_AGENT}, which is part of Omarchy and \
             is not installed here — try one of: {}",
            available_agents().join(", ")
        )),
        Some(_) => None,
    }
}

/// The agent a thread starts on when nothing named one.
///
/// In order: `OMATETHER_AGENT`; on Omarchy, whatever `omarchy default agent`
/// says, so the bridge and the keybinding agree about what "the agent" means;
/// else the first of the streaming agents installed on this machine; else
/// claude, whose spawn error says what is missing.
pub fn default_agent() -> String {
    if let Some(name) = std::env::var("OMATETHER_AGENT")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
    {
        return name;
    }
    if let Some(name) = std::process::Command::new("omarchy-default-agent")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| available(name))
    {
        return name;
    }
    first_installed(on_path).unwrap_or("claude").to_string()
}

/// The first agent omatether drives itself whose CLI `installed` reports.
/// Every one of them is a CLI of the agent's own name.
fn first_installed(installed: impl Fn(&str) -> bool) -> Option<&'static str> {
    AGENTS
        .iter()
        .filter(|(_, backend)| *backend != Backend::Tmux)
        .map(|(name, _)| *name)
        .find(|name| installed(name))
}

pub fn backend_for(name: &str) -> Option<Backend> {
    AGENTS
        .iter()
        .find(|(agent, _)| *agent == name)
        .map(|(_, backend)| *backend)
}

/// Resolve the agent name to a `&'static str` from [`AGENTS`], so adapters can
/// report a name without leaking an owned string through the trait.
pub fn canonical_name(name: &str) -> Option<&'static str> {
    AGENTS
        .iter()
        .find(|(agent, _)| *agent == name)
        .map(|(agent, _)| *agent)
}

pub struct SpawnConfig {
    /// Which agent to run.
    pub agent: String,
    /// Working directory. The "which repo" answer, always explicit.
    pub cwd: PathBuf,
    /// The agent's own handle for this conversation, when there is one to
    /// resume. Claude accepts one we choose; Codex assigns its own and reports
    /// it back through [`AgentEvent::Ready`].
    pub session_id: Option<String>,
    /// The model `/model` asked for, or `None` to let the agent choose.
    pub model: Option<String>,
    /// Label for a detached tmux session, so it can be found again.
    pub label: String,
    /// Said to the agent on top of its own system prompt: where its replies
    /// are read, and what reaches the reader. `None` for the repl, which is a
    /// terminal and needs no telling.
    ///
    /// Each adapter puts it where its CLI keeps instructions rather than in
    /// the prompt, so it is not part of the transcript and cannot be
    /// compacted away. The detached tier ignores it: that agent's replies are
    /// read at the terminal `/attach` names, not in the chat.
    pub brief: Option<String>,
}

/// Whether this agent can be told which model to run at all.
///
/// The detached tier cannot: it launches through Omarchy's `omarchy-agent`, which takes
/// `--inline`, `--pick` and `--prompt`, rejects anything else, and holds each
/// agent's own auto-approve spelling. Passing a model would mean either
/// changing Omarchy or copying that table in here, and a stale copy of the
/// flags that decide whether an agent stops to ask is the wrong thing to own.
pub fn takes_model(agent: &str) -> bool {
    matches!(
        backend_for(agent),
        Some(Backend::Claude) | Some(Backend::Codex) | Some(Backend::Pi) | Some(Backend::Acp)
    )
}

/// Whether this agent's tool calls can be put to a person — the static answer
/// to [`Agent::gates_tools`], for the places that have to say so before any
/// session exists (`/auto`, `/status`).
pub fn gates(agent: &str) -> bool {
    matches!(
        backend_for(agent),
        Some(Backend::Claude) | Some(Backend::Acp)
    )
}

/// Agents that can put a tool call to a person, in [`AGENTS`] order.
///
/// `/auto off` names them. A hardcoded subset would claim the gate for the
/// wrong set the moment another profile arrives.
pub fn gating_agents() -> Vec<&'static str> {
    AGENTS
        .iter()
        .filter(|(name, _)| gates(name))
        .map(|(name, _)| *name)
        .collect()
}

/// Ask the agent's CLI for its model catalog, without a session.
///
/// Where [`Agent::models`] is what an agent volunteered on its own — Claude
/// Code's handshake, which only exists while that session is up — this asks
/// the CLI directly: `codex debug models`, `pi --list-models`, `opencode models`,
/// `grok models`. Each is local and costs no API call, which is what makes a
/// list possible before the first turn or after a `/new`. Claude Code and
/// copilot have no such command, so their answer is how to get one; the error
/// is worded that way because it is shown to the person who asked.
pub async fn list_models(agent: &str, cwd: &Path) -> Result<Vec<String>> {
    match backend_for(agent) {
        Some(Backend::Codex) => crate::codex::list_models(cwd).await,
        Some(Backend::Pi) => crate::pi::list_models(cwd).await,
        Some(Backend::Claude) => bail!(
            "claude's model list comes with a live session — send it a message \
             first, and /model will show it"
        ),
        Some(Backend::Acp) => match crate::acp::Profile::for_agent(agent) {
            Some(profile) => crate::acp::list_models(profile, cwd).await,
            None => bail!("{agent} has no ACP profile"),
        },
        Some(Backend::Tmux) => bail!("{agent} runs through omarchy-agent, which takes no model"),
        None => bail!("unknown agent '{agent}'"),
    }
}

/// Start an agent, whichever backend drives it.
pub async fn spawn(config: SpawnConfig) -> Result<(Box<dyn Agent>, mpsc::Receiver<AgentEvent>)> {
    // A thread may have been switched to a detached agent on a machine that
    // has since lost Omarchy, or the database moved: say why, not "not found".
    if let Some(reason) = unavailable_reason(&config.agent) {
        bail!("{reason}");
    }
    let backend = backend_for(&config.agent).expect("checked just above");

    match backend {
        Backend::Claude => {
            let (session, events) = crate::claude::ClaudeSession::spawn(crate::claude::Config {
                cwd: config.cwd,
                // `None` here means a fresh conversation; `Some` means resume
                // one from an earlier turn or an earlier process.
                session_id: config
                    .session_id
                    .and_then(|id| uuid::Uuid::parse_str(&id).ok()),
                model: config.model,
                brief: config.brief,
                permission_mode: "default".to_string(),
                raw: false,
            })
            .await?;
            Ok((Box::new(session), events))
        }

        Backend::Codex => {
            let (session, events) = crate::codex::CodexSession::new(crate::codex::Config {
                cwd: config.cwd,
                thread_id: config.session_id,
                model: config.model,
                brief: config.brief,
            });
            Ok((Box::new(session), events))
        }

        Backend::Pi => {
            let (session, events) = crate::pi::PiSession::new(crate::pi::Config {
                cwd: config.cwd,
                session_id: config.session_id,
                model: config.model,
                brief: config.brief,
            });
            Ok((Box::new(session), events))
        }

        Backend::Acp => {
            let profile = crate::acp::Profile::for_agent(&config.agent)
                .ok_or_else(|| anyhow::anyhow!("{} has no ACP profile", config.agent))?;
            let (session, events) = crate::acp::AcpSession::spawn(crate::acp::Config {
                profile,
                cwd: config.cwd,
                session_id: config.session_id,
                model: config.model,
                brief: config.brief,
            })
            .await?;
            Ok((Box::new(session), events))
        }

        Backend::Tmux => {
            let (session, events) = crate::tmux::TmuxSession::new(crate::tmux::Config {
                agent: canonical_name(&config.agent).unwrap_or("claude"),
                cwd: config.cwd,
                label: config.label,
            });
            Ok((Box::new(session), events))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_omarchy_agent_has_a_backend() {
        // From `omarchy default agent`'s list, minus gemini: Google refuses
        // the Gemini CLI to individual accounts now ("no longer supported for
        // Gemini Code Assist for individuals", gemini 0.68.0 over ACP), and
        // Omarchy itself maps the name to Antigravity.
        for name in [
            "pi", "omp", "opencode", "claude", "codex", "crush", "grok", "copilot",
        ] {
            assert!(backend_for(name).is_some(), "{name} has no backend");
        }
    }

    #[test]
    fn unknown_agents_are_rejected() {
        assert!(backend_for("emacs").is_none());
        assert!(!available("emacs"));
        assert!(unavailable_reason("emacs")
            .unwrap()
            .contains("unknown agent"));
    }

    #[test]
    fn the_detached_tier_is_offered_only_where_its_launcher_is() {
        // Whatever this machine has, the answer has to agree with it: the
        // tier is there exactly when omarchy-agent is, and nothing else is
        // ever held back.
        let omarchy = on_path(OMARCHY_AGENT);
        for name in ["omp", "crush"] {
            assert_eq!(available(name), omarchy, "{name}");
            match unavailable_reason(name) {
                Some(why) => {
                    assert!(!omarchy);
                    assert!(why.contains("Omarchy"), "say why, not 'unknown': {why}");
                }
                None => assert!(omarchy),
            }
        }
        for name in ["claude", "codex", "pi", "opencode", "grok", "copilot"] {
            assert!(available(name), "{name}");
            assert!(unavailable_reason(name).is_none(), "{name}");
        }
        let listed = available_agents();
        assert!(listed.contains(&"claude"));
        assert_eq!(listed.contains(&"omp"), omarchy);
    }

    #[test]
    fn the_fallback_default_is_an_installed_agent_we_drive_ourselves() {
        assert_eq!(first_installed(|_| true), Some("claude"));
        assert_eq!(
            first_installed(|name| name == "codex" || name == "pi"),
            Some("codex")
        );
        assert_eq!(first_installed(|name| name == "copilot"), Some("copilot"));
        assert_eq!(first_installed(|name| name == "grok"), Some("grok"));
        // A detached agent's CLI being installed is not enough: without
        // Omarchy's launcher it cannot be started, and with it the Omarchy
        // default has already answered.
        assert_eq!(first_installed(|name| name == "omp"), None);
        assert_eq!(first_installed(|_| false), None);
    }

    #[test]
    fn only_the_agents_omatether_spawns_itself_can_be_told_a_model() {
        // The ones with a command line of our own making. The detached tier
        // runs through omarchy-agent, which has nowhere to put one.
        for name in ["claude", "codex", "pi", "opencode", "grok", "copilot"] {
            assert!(takes_model(name), "{name} takes a model");
        }
        for name in ["omp", "crush"] {
            assert!(!takes_model(name), "{name} has no way to be told one");
        }
        assert!(!takes_model("emacs"));
    }

    #[tokio::test]
    async fn asking_an_agent_that_cannot_be_asked_fails_with_a_reason() {
        let dir = std::env::temp_dir();
        let e = list_models("claude", &dir).await.unwrap_err();
        assert!(
            e.to_string().contains("live session"),
            "the claude answer has to say how to get a list: {e}"
        );
        let e = list_models("omp", &dir).await.unwrap_err();
        assert!(e.to_string().contains("omarchy-agent"), "{e}");
        assert!(list_models("emacs", &dir).await.is_err());
    }

    #[test]
    fn claude_and_the_acp_agents_gate_tools() {
        assert_eq!(backend_for("claude"), Some(Backend::Claude));
        assert_eq!(backend_for("opencode"), Some(Backend::Acp));
        assert_eq!(backend_for("copilot"), Some(Backend::Acp));
        assert_eq!(backend_for("grok"), Some(Backend::Acp));
        assert_eq!(gating_agents(), ["claude", "opencode", "grok", "copilot"]);
        for name in gating_agents() {
            assert!(gates(name), "{name}");
        }
        for name in ["codex", "pi", "omp", "crush", "emacs"] {
            assert!(!gates(name), "{name}");
        }
        // codex exec's only approval mode is automatic; pi -p runs its tools
        // as it decides on them. Neither has a callback to route to a human.
        assert_eq!(backend_for("codex"), Some(Backend::Codex));
        assert_eq!(backend_for("pi"), Some(Backend::Pi));
        assert_eq!(backend_for("omp"), Some(Backend::Tmux));
    }
}
