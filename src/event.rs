//! The normalized event model — seam B.
//!
//! Every agent adapter parses its own wire format and emits these. Nothing
//! downstream of this module should ever learn which agent produced an event,
//! and nothing upstream of it should leak a vendor-specific field through.

use serde::Serialize;
use serde_json::{json, Value};

/// Identifies one outstanding permission question, as the agent labelled it.
pub type RequestId = String;

/// One thing that happened inside an agent session.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum AgentEvent {
    /// The session is up and will accept a prompt.
    Ready {
        session_id: String,
        model: Option<String>,
        cwd: Option<String>,
        tools: Vec<String>,
    },

    /// A fragment of assistant text, as it is generated.
    TextDelta { text: String },

    /// A complete assistant text block.
    Text { text: String },

    /// The agent reasoned before answering. A chat channel usually wants to
    /// show that it is working, not the contents.
    Thinking { text: String },

    /// The agent is running a tool. Informational — it has already decided to.
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },

    /// The agent is asking permission and is blocked until answered.
    /// Reply with [`crate::agent::Agent::decide`].
    PermissionRequest {
        request_id: RequestId,
        tool: String,
        input: Value,
        suggestions: Value,
    },

    /// The turn finished. `ok` is false for an error or a turn-limit stop.
    TurnEnd { ok: bool, detail: Option<String> },

    /// Where the account stands against its usage windows, as a 0.0–1.0
    /// fraction. Worth surfacing in a chat bridge before a turn dies mid-reply.
    RateLimit {
        five_hour: Option<f64>,
        seven_day: Option<f64>,
    },

    /// The agent or its transport failed.
    Error { message: String },

    /// A frame this adapter does not model yet.
    ///
    /// Deliberately not dropped. Agent wire formats are unversioned and move
    /// underneath us; surfacing the raw frame is how we find out.
    Unknown { raw: Value },
}

/// The answer to a [`AgentEvent::PermissionRequest`].
#[derive(Debug, Clone)]
pub enum Decision {
    /// Let the tool run, optionally with rewritten arguments.
    Allow { updated_input: Option<Value> },
    /// Refuse, and tell the agent why so it can adapt.
    Deny { message: String },
}

impl Decision {
    pub fn allow() -> Self {
        Decision::Allow {
            updated_input: None,
        }
    }

    pub fn deny(message: impl Into<String>) -> Self {
        Decision::Deny {
            message: message.into(),
        }
    }

    /// The payload for a `can_use_tool` control request. Verified against the
    /// CLI's own validation message:
    ///
    /// > Expected {behavior: 'allow', updatedInput?: object} or
    /// > {behavior: 'deny', message: string}.
    pub fn to_can_use_tool_payload(&self) -> Value {
        match self {
            Decision::Allow { updated_input } => match updated_input {
                Some(input) => json!({ "behavior": "allow", "updatedInput": input }),
                None => json!({ "behavior": "allow" }),
            },
            Decision::Deny { message } => json!({ "behavior": "deny", "message": message }),
        }
    }

    /// The payload for a `PreToolUse` hook callback — the gate that actually
    /// fires in `--print` mode.
    pub fn to_hook_payload(&self) -> Value {
        let (decision, reason) = match self {
            Decision::Allow { .. } => ("allow", String::new()),
            Decision::Deny { message } => ("deny", message.clone()),
        };
        json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": decision,
                "permissionDecisionReason": reason,
            }
        })
    }
}
