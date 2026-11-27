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
///
/// A chat can hold several conversations at once. [`ThreadKey::task_id`] is
/// the second one, and the ones after it. Channels leave it empty: a Telegram
/// topic id is sent back as `message_thread_id`, and a made-up one is not a
/// topic, so a task must not be stored there. The core is what fills it in.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadKey {
    pub channel: &'static str,
    pub chat_id: String,
    pub topic_id: Option<String>,
    /// Which conversation inside this chat. `None` is the original one.
    pub task_id: Option<String>,
}

impl ThreadKey {
    /// This chat, with no task selected. What a channel constructs, and the
    /// key focus and `/tasks` are stored under.
    pub fn platform(&self) -> Self {
        Self {
            task_id: None,
            ..self.clone()
        }
    }

    /// The same chat, on this task. `None` is the original conversation.
    pub fn with_task(&self, task_id: Option<String>) -> Self {
        Self {
            task_id,
            ..self.platform()
        }
    }

    /// Same channel, chat and topic. Tasks of one chat are siblings; a forum
    /// topic is a different chat from the group it lives in.
    pub fn same_chat(&self, other: &Self) -> bool {
        self.channel == other.channel
            && self.chat_id == other.chat_id
            && self.topic_id == other.topic_id
    }
}

impl std::fmt::Display for ThreadKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No task keeps the spelling every stored thread already has. A task
        // is a suffix, so `telegram:1` does not prefix-match `telegram:12`,
        // and `#` is not a character a forum topic id or an MCP session name
        // can contain.
        match &self.topic_id {
            Some(topic) => write!(f, "{}:{}:{}", self.channel, self.chat_id, topic)?,
            None => write!(f, "{}:{}", self.channel, self.chat_id)?,
        }
        if let Some(task) = &self.task_id {
            write!(f, "#{task}")?;
        }
        Ok(())
    }
}

/// Something a person did in a chat.
#[derive(Debug, Clone)]
pub struct Inbound {
    pub thread: ThreadKey,
    pub user_id: String,
    pub kind: InboundKind,
    /// The task this message answers, when it was sent as a reply to one of
    /// ours and the channel remembers which task that one came from.
    ///
    /// The one place a channel names a task. It is not where the message is
    /// *from* — `thread` still says that, with no task — but what the person
    /// pointed at. The core decides whether to follow it.
    pub replying_to: Option<ThreadKey>,
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
    /// A tap on a task list: start one, or switch to one. `ack` is what the
    /// channel needs to stop the client's spinner, as with a permission tap.
    Pick { pick: TaskPick, ack: String },
}

/// What a task-list tap asked for.
///
/// A number typed in reply to `/tasks` is the same choice on a channel that
/// has no buttons. The id is the short one omatether assigned (`t1`), never
/// something the person had to invent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskPick {
    /// The next plain message starts a new task.
    New,
    /// Switch to this task. `None` is the original conversation.
    Use(Option<String>),
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

    /// Whether this channel can put tappable choices under a message.
    ///
    /// Telegram can. iMessage cannot, so the same list goes out as numbered
    /// lines and the reply is the number. Asked rather than assumed from
    /// `can_edit`: a button is a different capability from rewriting a message.
    fn offers_choices(&self) -> bool {
        false
    }

    /// Whether replying to one of this channel's messages reaches the task
    /// that sent it ([`Inbound::replying_to`]).
    ///
    /// Where it does, a reply from a task you are not in says so, because a
    /// reply is how you get back to it. Where it does not, that would be
    /// advice that does nothing.
    fn routes_replies(&self) -> bool {
        false
    }

    /// Post a message with choices under it.
    ///
    /// Each choice is a label and the token that comes back in
    /// [`InboundKind::Pick`]. A channel with no buttons sends the text alone;
    /// the text already says how to answer.
    async fn offer(
        &self,
        thread: &ThreadKey,
        text: &str,
        _choices: &[(String, String)],
    ) -> Result<MessageId> {
        self.send(thread, text).await
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn key(topic: Option<&str>) -> ThreadKey {
        ThreadKey {
            channel: "telegram",
            chat_id: "5".into(),
            topic_id: topic.map(str::to_string),
            task_id: None,
        }
    }

    #[test]
    fn a_task_is_a_suffix_and_the_original_key_does_not_change() {
        // Stored threads already use this spelling. Adding tasks must not
        // rename them, or every chat would look new after an upgrade.
        assert_eq!(key(None).to_string(), "telegram:5");
        assert_eq!(key(Some("77")).to_string(), "telegram:5:77");

        let task = key(None).with_task(Some("t1".into()));
        assert_eq!(task.to_string(), "telegram:5#t1");
        assert_eq!(
            key(Some("77")).with_task(Some("t2".into())).to_string(),
            "telegram:5:77#t2"
        );

        // `telegram:1` must not be a prefix of another chat's key, and a task
        // of one chat must not read as a task of the chat next door.
        assert!(!task.to_string().starts_with("telegram:5:"));
        assert!(!format!("telegram:50#t1").starts_with("telegram:5#"));
    }

    #[test]
    fn platform_and_same_chat_ignore_the_task_and_only_the_task() {
        let topic = key(Some("77"));
        let task = topic.with_task(Some("t1".into()));
        assert_eq!(task.platform(), topic);
        assert!(task.same_chat(&topic));
        assert!(!task.same_chat(&key(None)));
        assert_eq!(task.with_task(None), topic);
    }
}
