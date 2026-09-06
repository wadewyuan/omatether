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

use std::path::PathBuf;

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::event::{AgentEvent, Decision};

#[async_trait]
pub trait Agent: Send {
    /// The agent's name, as `omarchy default agent` spells it.
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
    /// A detached tmux session. No streaming and no gating, but it works for
    /// every agent Omarchy supports.
    Tmux,
}

/// Every agent Omarchy knows about, and how switchboard drives it.
///
/// The list mirrors `omarchy-default-agent` so that `/agent <name>` accepts
/// whatever the desktop accepts.
pub const AGENTS: &[(&str, Backend)] = &[
    ("claude", Backend::Claude),
    ("codex", Backend::Codex),
    ("pi", Backend::Tmux),
    ("omp", Backend::Tmux),
    ("opencode", Backend::Tmux),
    ("crush", Backend::Tmux),
    ("grok", Backend::Tmux),
    ("gemini", Backend::Tmux),
    ("copilot", Backend::Tmux),
];

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
    /// Label for a detached tmux session, so it can be found again.
    pub label: String,
}

/// Start an agent, whichever backend drives it.
pub async fn spawn(config: SpawnConfig) -> Result<(Box<dyn Agent>, mpsc::Receiver<AgentEvent>)> {
    let backend = match backend_for(&config.agent) {
        Some(backend) => backend,
        None => {
            let known: Vec<&str> = AGENTS.iter().map(|(name, _)| *name).collect();
            bail!("unknown agent '{}' — try one of: {}", config.agent, known.join(", "));
        }
    };

    match backend {
        Backend::Claude => {
            let (session, events) = crate::claude::ClaudeSession::spawn(crate::claude::Config {
                cwd: config.cwd,
                // Claude takes a session id we choose, so a restart resumes.
                session_id: config
                    .session_id
                    .and_then(|id| uuid::Uuid::parse_str(&id).ok())
                    .unwrap_or_else(uuid::Uuid::new_v4),
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
            });
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
        // The list `omarchy default agent` accepts, as of writing.
        for name in [
            "pi", "omp", "opencode", "claude", "codex", "crush", "grok", "gemini", "copilot",
        ] {
            assert!(backend_for(name).is_some(), "{name} has no backend");
        }
    }

    #[test]
    fn unknown_agents_are_rejected() {
        assert!(backend_for("emacs").is_none());
    }

    #[test]
    fn only_claude_gates_tools_today() {
        assert_eq!(backend_for("claude"), Some(Backend::Claude));
        // codex exec's only approval mode is automatic.
        assert_eq!(backend_for("codex"), Some(Backend::Codex));
        assert_eq!(backend_for("pi"), Some(Backend::Tmux));
    }
}
