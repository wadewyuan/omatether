//! Seam B: the interface every agent adapter implements.
//!
//! Shaped after ACP's four verbs — prompt, streamed update, permission
//! request, cancel — so that a real ACP adapter can slot in beside the
//! hand-written ones without the core noticing.
//!
//! Updates do not appear here: a session hands back an
//! [`mpsc::Receiver<AgentEvent>`] when it is spawned, which keeps the read
//! side independent of the write side and lets the core `select!` over it.

use anyhow::Result;

use crate::event::Decision;

pub trait Agent {
    /// Send a prompt to the agent.
    ///
    /// Errors if a turn is already running. One turn at a time per session is
    /// a deliberate simplification: from a phone, rejecting a second message
    /// is more predictable than queuing it, and far simpler than interleaving.
    async fn prompt(&mut self, text: &str) -> Result<()>;

    /// Answer an outstanding [`crate::event::AgentEvent::PermissionRequest`].
    async fn decide(&mut self, request_id: &str, decision: Decision) -> Result<()>;

    /// Interrupt the running turn. Harmless when nothing is running.
    async fn cancel(&mut self) -> Result<()>;

    /// Ask the agent to exit, and wait for it briefly.
    async fn shutdown(&mut self) -> Result<()>;
}
