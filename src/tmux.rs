//! The fallback tier: any agent, in a detached tmux session.
//!
//! Seven of Omarchy's nine agents have no structured output mode, so there is
//! nothing to stream and no way to gate a tool. What they do have is a terminal
//! interface, and `omarchy-agent --inline` execs the agent directly rather than
//! opening a window — so it runs fine under tmux with no compositor in sight.
//!
//! This makes `/agent pi` mean something honest: the agent starts, works, and
//! you take over at a real terminal when you get back. The reply is the tmux
//! session, not a chat message.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::agent::Agent;
use crate::event::AgentEvent;

pub struct Config {
    pub agent: &'static str,
    pub cwd: PathBuf,
    /// Used to name the tmux session, so it can be found again.
    pub label: String,
}

pub struct TmuxSession {
    agent: &'static str,
    cwd: PathBuf,
    session: String,
    events: mpsc::Sender<AgentEvent>,
    busy: Arc<AtomicBool>,
}

impl TmuxSession {
    pub fn new(config: Config) -> (Self, mpsc::Receiver<AgentEvent>) {
        let (tx, rx) = mpsc::channel(32);
        (
            Self {
                agent: config.agent,
                cwd: config.cwd,
                session: session_name(&config.label),
                events: tx,
                busy: Arc::new(AtomicBool::new(false)),
            },
            rx,
        )
    }

    /// The line to type at a terminal to take over.
    pub fn attach_command(&self) -> String {
        format!("tmux attach -t {}", self.session)
    }

    async fn tmux(&self, args: &[&str]) -> Result<std::process::Output> {
        Command::new("tmux")
            .args(args)
            .stdin(Stdio::null())
            .output()
            .await
            .context("running tmux — is it installed?")
    }

    async fn session_exists(&self) -> bool {
        self.tmux(&["has-session", "-t", &self.session])
            .await
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

#[async_trait]
impl Agent for TmuxSession {
    fn name(&self) -> &'static str {
        self.agent
    }

    /// Nothing to gate: the agent runs with its own auto-approval flags and
    /// never asks anyone.
    fn gates_tools(&self) -> bool {
        false
    }

    /// No structured output, so nothing arrives progressively.
    fn streams(&self) -> bool {
        false
    }

    fn is_busy(&self) -> bool {
        self.busy.load(Ordering::SeqCst)
    }

    async fn prompt(&mut self, text: &str) -> Result<()> {
        if self.session_exists().await {
            // Feed an existing session instead of starting a second one: the
            // agent is sitting at its prompt waiting for exactly this.
            self.tmux(&["send-keys", "-t", &self.session, text, "Enter"])
                .await?;

            let _ = self
                .events
                .send(AgentEvent::Text {
                    text: format!(
                        "Sent to the running {} session.\n\n{}",
                        self.agent,
                        self.attach_command()
                    ),
                })
                .await;
            let _ = self
                .events
                .send(AgentEvent::TurnEnd {
                    ok: true,
                    detail: None,
                })
                .await;
            return Ok(());
        }

        // `--inline` execs the agent rather than opening a terminal window, so
        // there is no compositor to find. Whichever of the nine is named, its
        // own auto-approve flags are applied by omarchy-agent.
        let status = Command::new("tmux")
            .args([
                "new-session",
                "-d",
                "-s",
                &self.session,
                "-c",
                &self.cwd.to_string_lossy(),
                "omarchy-agent",
                "--inline",
                "--prompt",
                text,
            ])
            .stdin(Stdio::null())
            .status()
            .await
            .context("starting a tmux session — is tmux installed?")?;

        if !status.success() {
            bail!("tmux refused to start a session for {}", self.agent);
        }

        self.busy.store(false, Ordering::SeqCst);

        let _ = self
            .events
            .send(AgentEvent::Ready {
                session_id: self.session.clone(),
                model: None,
                cwd: Some(self.cwd.to_string_lossy().to_string()),
                tools: Vec::new(),
            })
            .await;

        // Be explicit that this tier is fire-and-forget. Promising a reply that
        // will never arrive is worse than saying there isn't one.
        let _ = self
            .events
            .send(AgentEvent::Text {
                text: format!(
                    "Started {} in a detached session. It does not stream back — \
                     take over with:\n\n{}",
                    self.agent,
                    self.attach_command()
                ),
            })
            .await;
        let _ = self
            .events
            .send(AgentEvent::TurnEnd {
                ok: true,
                detail: None,
            })
            .await;

        Ok(())
    }

    async fn cancel(&mut self) -> Result<()> {
        // Ctrl-C in the pane rather than killing the session: the agent gets to
        // clean up, and the session stays available to attach to.
        self.tmux(&["send-keys", "-t", &self.session, "C-c"])
            .await
            .ok();
        self.busy.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        // Deliberately left running. The whole point of this tier is that the
        // work survives to be picked up at a terminal.
        Ok(())
    }
}

/// tmux session names take neither dots nor colons.
pub fn session_name(label: &str) -> String {
    let cleaned: String = label
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect();
    format!("sb-{cleaned}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_names_are_safe_for_tmux() {
        let name = session_name("telegram:-1001234:77");
        assert!(!name.contains(':'), "colons break tmux targets");
        assert!(!name.contains('.'), "dots break tmux targets");
        assert_eq!(name, "sb-telegram--1001234-77");
    }

    #[test]
    fn attach_command_names_the_session() {
        let (session, _rx) = TmuxSession::new(Config {
            agent: "pi",
            cwd: PathBuf::from("/tmp"),
            label: "telegram:5".into(),
        });
        assert_eq!(session.attach_command(), "tmux attach -t sb-telegram-5");
    }

    #[test]
    fn the_fallback_tier_admits_what_it_cannot_do() {
        let (session, _rx) = TmuxSession::new(Config {
            agent: "pi",
            cwd: PathBuf::from("/tmp"),
            label: "x".into(),
        });
        assert!(!session.streams());
        assert!(!session.gates_tools());
        assert_eq!(session.name(), "pi");
    }
}
