//! Seam A: the interface every messaging channel implements.
//!
//! Kept deliberately small. A channel receives text and taps, sends a message,
//! and — if the platform allows it — edits one it already sent.
//!
//! That "if" is load-bearing. Telegram edits messages, so a turn can stream
//! into one message that grows. iMessage cannot edit anything, ever, so a turn
//! there has to arrive whole. [`Channel::can_edit`] is how the core learns
//! which world it is in without knowing which channel it is talking to.

mod markup;
pub mod photon;
pub mod photon_setup;
pub mod telegram;

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

/// A channel asking to be tried again in a moment.
///
/// Reported rather than handled: the adapter knows how long the platform said
/// to wait, but only the caller knows whether this message is still worth
/// sending and whose turn is being delayed by the waiting. Waiting inside the
/// adapter meant waiting inside the core, which stalled every other thread on
/// every other channel.
#[derive(Debug, Clone)]
pub struct RateLimited {
    pub retry_after: Duration,
}

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rate limited, retry in {}s", self.retry_after.as_secs())
    }
}

impl std::error::Error for RateLimited {}

/// A message id as the channel understands it, kept as a string so seam A does
/// not inherit Telegram's i64 or Photon's opaque handle.
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
    /// A tap on one of the permission buttons.
    Decision {
        allow: bool,
        /// Whatever the channel needs to acknowledge the tap so the client
        /// stops showing a spinner.
        ack: String,
        /// Which question this answers, as handed to [`Channel::ask_permission`].
        ///
        /// Buttons stay tappable forever in the chat history, so without this
        /// a tap under an old question would be applied to whatever is pending
        /// now — a decision about a tool the person never saw.
        question: String,
    },
    /// Something the channel saw, recognized as addressed to us, and cannot
    /// turn into a prompt — a voice note, a sticker, a photo with no caption.
    ///
    /// Carried through rather than dropped at the edge, because from the phone
    /// a dropped message and a broken bridge look identical: you sent
    /// something and nothing came back. The text is what to tell the sender.
    Unsupported(String),
}

#[async_trait]
pub trait Channel: Send + Sync {
    /// The name this channel puts in a [`ThreadKey`].
    fn name(&self) -> &'static str;

    /// Whether a sent message can be rewritten.
    ///
    /// `true` lets the core stream a turn into one message on a debounce.
    /// `false` means every flush would be a new message, so the core holds the
    /// turn back and delivers it once, complete.
    fn can_edit(&self) -> bool;

    /// Post a new message and return its id.
    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId>;

    /// Replace the text of a message this channel sent. Only called when
    /// [`Channel::can_edit`] is true.
    async fn edit(&self, thread: &ThreadKey, id: &MessageId, text: &str) -> Result<()>;

    /// Ask for a decision. Channels with buttons should use them; channels
    /// without should say how to answer in words.
    ///
    /// `question` identifies this question. A channel whose buttons outlive the
    /// question — all of them — must carry it back in
    /// [`InboundKind::Decision`], so an answer to a question that has already
    /// moved on can be told apart from an answer to this one.
    async fn ask_permission(
        &self,
        thread: &ThreadKey,
        text: &str,
        question: &str,
    ) -> Result<MessageId>;

    /// Acknowledge a button tap. A no-op where there are no buttons.
    async fn ack_decision(&self, _token: &str, _note: &str) -> Result<()> {
        Ok(())
    }

    /// Show that the agent is working.
    ///
    /// Only worth sending on channels that cannot stream: where a message grows
    /// as the turn runs, that *is* the indicator, and an extra one costs rate
    /// limit for nothing.
    async fn typing(&self, _thread: &ThreadKey) -> Result<()> {
        Ok(())
    }
}
