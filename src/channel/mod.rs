//! Seam A: the interface every messaging channel implements.
//!
//! Kept deliberately small. A channel receives text and taps, sends a message,
//! and — if the platform allows it — edits one it already sent.
//!
//! That "if" is load-bearing. Telegram edits messages, so a turn can stream
//! into one message that grows. iMessage cannot edit anything, ever, so a turn
//! there has to arrive whole. [`Channel::can_edit`] is how the core learns
//! which world it is in without knowing which channel it is talking to.

pub mod inbox;
mod markup;
pub mod mcp;
pub mod photon;
pub mod photon_setup;
pub mod telegram;
pub mod telegram_setup;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Result};
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

/// A file someone sent, already on disk.
///
/// The channel adapter fetches the bytes, because that is where the platform's
/// credentials are — the same reason the allowlist is enforced there. By the
/// time the core sees one, the only question left is what to say about it in
/// the prompt.
///
/// `saved` is a `Result` rather than an `Option` because a file that could not
/// be fetched still has to be *said*: a 30 MB video the Bot API refuses to
/// serve must reach the person as "too big for Telegram to send me", not as a
/// message that looks delivered and never mentions their attachment again.
#[derive(Debug, Clone)]
pub struct Attachment {
    pub name: String,
    pub mime: String,
    /// Bytes, or zero where the platform did not say.
    pub size: u64,
    pub saved: Result<PathBuf, String>,
}

#[derive(Debug, Clone)]
pub enum InboundKind {
    /// Words, files, or both. A photo with a caption is one message with both;
    /// a bare screenshot is files with no words, which is a perfectly good
    /// prompt now that the agent can open it.
    Text {
        text: String,
        files: Vec<Attachment>,
    },
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
    /// turn into a prompt at all — a sticker, a tapback, a message whose only
    /// content is a kind nothing can be done with. A photo or a document is no
    /// longer one of these: it arrives as [`InboundKind::Text`] with a file.
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

    /// Whether this channel can carry a file the agent produced.
    ///
    /// The other half of [`InboundKind::Text`]'s files: inbound lands a path in the
    /// prompt, outbound hands the bytes back to the phone. Asked as a capability
    /// rather than assumed, because the core has to be able to say "I can't send
    /// files here" instead of quietly dropping the one the agent asked for.
    fn can_send_files(&self) -> bool {
        false
    }

    /// Hand a file this machine produced to the chat, with an optional caption.
    ///
    /// Only called when [`Channel::can_send_files`] is true. `path` has already been
    /// checked by [`crate::sendfile`] — which files may leave this machine is the
    /// core's policy, not each adapter's — so an adapter's own refusals are about
    /// the platform: a picture too big for `sendPhoto`, a document over the upload
    /// ceiling, a file that vanished between the decision and the send.
    async fn send_file(
        &self,
        _thread: &ThreadKey,
        _path: &Path,
        _caption: &str,
    ) -> Result<MessageId> {
        bail!("{} cannot send files", self.name())
    }

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

    /// Show, or stop showing, that the agent is working.
    ///
    /// Indicators expire: Telegram's lasts about five seconds and cannot be
    /// extended or withdrawn, iMessage's stays until it is turned off. So this
    /// is called repeatedly while a turn runs and once with `on` false when it
    /// is over, and a channel implements whichever half its platform has —
    /// the core does not know which that is.
    ///
    /// Best effort by contract: an indicator that fails to appear must never
    /// cost the reply it was announcing.
    async fn typing(&self, _thread: &ThreadKey, _on: bool) -> Result<()> {
        Ok(())
    }

    /// Who reads this channel. Decides how the agent is briefed.
    fn audience(&self) -> Audience {
        Audience::Person
    }

    /// The core has done all it is going to do about what this thread last
    /// sent: a turn finished, a command answered, or a question is waiting
    /// on an answer. Delivered through the outbox, behind every message that
    /// work produced.
    ///
    /// A person on a phone needs no such signal, since they read the reply.
    /// A program waiting on a request does: it is how it knows the answer it
    /// holds is the whole answer.
    async fn idle(&self, _thread: &ThreadKey) {}

    /// Whether this channel wants [`Channel::progress`] while a turn runs.
    ///
    /// Only a channel that cannot edit needs it: one that can edit is already
    /// showing the turn as it grows. A phone that cannot edit does not want
    /// it either — it gets the answer whole, by design — so this is asked
    /// for, not assumed from `can_edit`.
    fn wants_progress(&self) -> bool {
        false
    }

    /// The running turn so far, as `/log` would show it: every tool call on a
    /// line of its own, and the text between them. Sent when it changes, at
    /// most once a flush interval, and never after the turn's answer.
    async fn progress(&self, _thread: &ThreadKey, _log: &str) {}
}

/// Who is on the other end of a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Audience {
    /// Someone in a chat app on their phone.
    Person,
    /// Another AI agent, calling over MCP.
    Agent,
}
