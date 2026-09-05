//! Seam A: the interface every messaging channel implements.
//!
//! Kept deliberately small. A channel receives text and taps, sends a message,
//! and edits one it already sent — that is the whole vocabulary a streaming
//! bridge needs, and it is the subset every platform supports.

pub mod telegram;

use anyhow::Result;

/// A message id as the channel understands it, kept as a string so seam A does
/// not inherit Telegram's i64.
pub type MessageId = String;

/// Identifies one conversation. A Telegram forum topic is a distinct thread
/// from the group it lives in, which is what makes topic-per-project work.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadKey {
    pub channel: &'static str,
    pub chat_id: String,
    pub topic_id: Option<String>,
}

impl std::fmt::Display for ThreadKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.topic_id {
            Some(topic) => write!(f, "{}:{}:{}", self.channel, self.chat_id, topic),
            None => write!(f, "{}:{}", self.channel, self.chat_id),
        }
    }
}

/// Something a person did in a chat.
#[derive(Debug, Clone)]
pub struct Inbound {
    pub thread: ThreadKey,
    pub user_id: String,
    pub kind: InboundKind,
}

#[derive(Debug, Clone)]
pub enum InboundKind {
    Text(String),
    /// A tap on one of the permission buttons. `token` is whatever the channel
    /// needs to acknowledge the tap so the client stops showing a spinner.
    Decision { allow: bool, token: String },
}

pub trait Channel {
    /// Post a new message and return its id, so it can be edited later.
    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId>;

    /// Replace the text of a message this channel sent.
    async fn edit(&self, thread: &ThreadKey, id: &MessageId, text: &str) -> Result<()>;

    /// Post a message carrying allow/deny buttons.
    async fn ask_permission(&self, thread: &ThreadKey, text: &str) -> Result<MessageId>;

    /// Acknowledge a button tap. Channels without buttons can do nothing.
    async fn ack_decision(&self, token: &str, note: &str) -> Result<()>;
}
