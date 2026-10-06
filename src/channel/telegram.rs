//! Telegram Bot API channel.
//!
//! A thin client over the four methods a streaming bridge actually needs,
//! rather than a bot framework: the interesting behaviour here is the edit
//! cadence and the rate-limit handling, and both want direct control of the
//! request loop.
//!
//! Inbound uses long polling (`getUpdates`), so the service needs no public
//! URL, no webhook and no inbound port — it stays reachable only over the
//! tailnet.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::{inbox, markup, Attachment, Channel, Inbound, InboundKind, MessageId, ThreadKey};

pub const CHANNEL: &str = "telegram";

/// Telegram rejects messages over 4096 characters. Leave room for the marker.
const MAX_TEXT: usize = 3900;

/// Server-side long-poll window. The request blocks here rather than us
/// spinning; well under any sane proxy timeout.
const POLL_TIMEOUT_SECS: u64 = 25;

/// Callback payloads, as `allow:<question>` / `deny:<question>`.
///
/// The question token is what stops a tap on an old message from answering
/// whichever question happens to be pending now. Telegram allows 64 bytes of
/// callback_data and a verb plus an eight-character token spends fourteen of
/// them.
const CB_ALLOW: &str = "allow";
const CB_DENY: &str = "deny";

pub struct Telegram {
    http: reqwest::Client,
    base: String,
    /// Where a downloaded file is served from. A different host path to the
    /// method endpoint — `/file/bot<token>/` rather than `/bot<token>/` — and
    /// it carries the token too, so it is built once here rather than spelled
    /// out at the call site.
    file_base: String,
    /// Where received files land. See [`super::inbox`].
    inbox_dir: PathBuf,
    /// User ids permitted to talk to this bot. A bot token in a chat is a shell
    /// on this machine, so anyone else is dropped before the core sees them.
    allowed_users: Vec<String>,
    /// Our own @username, learned from `getMe` in [`Self::whoami`]. Telegram
    /// addresses a tapped menu command to its bot in a group as
    /// `/new@omatether_bot`; matching it here is what keeps those taps from
    /// falling through to the agent as prompts.
    username: OnceLock<String>,
}

impl Telegram {
    pub fn new(token: &str, allowed_users: Vec<String>, inbox_dir: PathBuf) -> Result<Self> {
        if allowed_users.is_empty() {
            bail!(
                "refusing to start with an empty allowlist — set OMATETHER_TELEGRAM_ALLOWED_USERS"
            );
        }

        let http = reqwest::Client::builder()
            // Comfortably longer than the long-poll window.
            .timeout(Duration::from_secs(POLL_TIMEOUT_SECS + 15))
            .build()?;

        Ok(Self {
            http,
            base: format!("https://api.telegram.org/bot{token}"),
            file_base: format!("https://api.telegram.org/file/bot{token}"),
            inbox_dir,
            allowed_users,
            username: OnceLock::new(),
        })
    }

    async fn call(&self, method: &str, body: Value) -> Result<Value> {
        let response: Value = self
            .http
            .post(format!("{}/{}", self.base, method))
            .json(&body)
            .send()
            .await
            .map_err(scrub)
            .with_context(|| format!("telegram {method}"))?
            .json()
            .await
            .map_err(scrub)
            .with_context(|| format!("decoding telegram {method}"))?;

        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            let retry_after = response
                .get("parameters")
                .and_then(|p| p.get("retry_after"))
                .and_then(Value::as_u64);

            // 429 is expected traffic on a streaming bridge, not an anomaly.
            // Report how long Telegram said to wait and let the caller do the
            // waiting: this used to sleep here, which meant sleeping inside the
            // core's one loop — every other thread on every other channel
            // stopped too — and then throwing the message away anyway.
            if let Some(seconds) = retry_after {
                return Err(super::RateLimited {
                    // The extra second is Telegram's own advice: retry_after is
                    // when the window opens, not when it is safe to be early.
                    retry_after: Duration::from_secs(seconds + 1),
                }
                .into());
            }

            bail!(
                "telegram {method} failed: {}",
                response
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            );
        }

        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    /// Post or edit a message, rendered as Telegram HTML.
    ///
    /// `text` must already be clipped, and must be the markdown: the fallback
    /// re-sends it unformatted, and a message that failed to parse as HTML is
    /// exactly the one whose raw form has to survive.
    ///
    /// Telegram counts a message as "1-4096 characters after entities
    /// parsing", so the tags and the `&amp;`s cost nothing against the limit —
    /// [`clip`] measures the right string.
    ///
    /// The fallback should never fire: [`markup::to_html`] emits every tag
    /// itself and its tests hold it to balanced output. It is here because the
    /// failure it covers is losing a reply outright, and because the same
    /// insurance on the Photon side has a real error to catch.
    async fn call_rendered(&self, method: &str, mut body: Value, text: &str) -> Result<Value> {
        body["text"] = json!(markup::to_html(text));
        body["parse_mode"] = json!("HTML");

        match self.call(method, body.clone()).await {
            Err(e) if is_parse_failure(&e) => {
                tracing::warn!("telegram refused our html ({e:#}); resending unformatted");
                body["text"] = json!(text);
                if let Some(fields) = body.as_object_mut() {
                    fields.remove("parse_mode");
                }
                self.call(method, body).await
            }
            other => other,
        }
    }

    /// Confirm the token works, and report who we are.
    pub async fn whoami(&self) -> Result<String> {
        let me = self.call("getMe", json!({})).await?;
        let username = me
            .get("username")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        // Whoever asks first — setup and the service both call this before
        // polling starts — records it for `own_command`.
        let _ = self.username.set(username.clone());
        Ok(username)
    }

    /// Register the slash-command menu Telegram shows when someone types `/`.
    ///
    /// Sent on every start rather than configured once in BotFather so the
    /// menu cannot drift from the commands this binary actually parses:
    /// adding a command to `command::parse` and forgetting BotFather would
    /// otherwise leave the old menu claiming otherwise. Telegram rejects
    /// command names over 32 characters or with anything but lowercase
    /// letters, digits and underscores, so none carry the leading slash and
    /// none carry arguments.
    pub async fn set_commands(&self) -> Result<()> {
        const COMMANDS: [(&str, &str); 12] = [
            ("new", "start a fresh session in this thread"),
            ("stop", "interrupt the running turn"),
            ("cd", "set the working directory"),
            ("agent", "switch agent (claude, codex, pi, ...)"),
            ("model", "which model the agent runs"),
            ("attach", "how to take over at a real terminal"),
            ("status", "agent, model, directory, session"),
            ("log", "the last turn in full, every tool call included"),
            ("allow", "approve a pending tool call"),
            ("deny", "refuse a pending tool call"),
            ("auto", "approve tool calls without asking (on by default)"),
            ("help", "list commands"),
        ];
        let commands: Vec<Value> = COMMANDS
            .iter()
            .map(|(command, description)| json!({"command": command, "description": description}))
            .collect();
        self.call("setMyCommands", json!({"commands": commands}))
            .await?;
        Ok(())
    }

    /// Strip our own @username from a tapped menu command, leaving everything
    /// else alone.
    ///
    /// In a group Telegram sends the tap as `/new@omatether_bot` — without
    /// this, `command::parse` sees a head it does not know and the command
    /// reaches the agent as a prompt that silently does nothing. A suffix
    /// naming a *different* bot is left in place on purpose: that command was
    /// meant for someone else in the group, not for us.
    fn own_command<'a>(&self, text: &'a str) -> &'a str {
        let Some(rest) = text.strip_prefix('/') else {
            return text;
        };
        let Some(end) = rest.find([' ', '\t', '\n', '@']) else {
            return text;
        };
        if rest.as_bytes()[end] != b'@' {
            return text;
        }
        let name = rest[end + 1..]
            .split_whitespace()
            .next()
            .unwrap_or_default();
        match self.username.get() {
            Some(me) if me == name => &text[..1 + end],
            _ => text,
        }
    }

    fn target(&self, thread: &ThreadKey) -> Value {
        match &thread.topic_id {
            Some(topic) => json!({
                "chat_id": thread.chat_id,
                "message_thread_id": topic.parse::<i64>().unwrap_or_default(),
            }),
            None => json!({ "chat_id": thread.chat_id }),
        }
    }

    /// Start the long-poll loop. Inbound messages arrive on the receiver.
    ///
    /// A separate task rather than a method the core awaits: `getUpdates`
    /// blocks for up to 25 seconds, and the core must stay responsive to agent
    /// events throughout.
    pub fn start_polling(self: std::sync::Arc<Self>) -> mpsc::Receiver<Inbound> {
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            let mut offset: i64 = 0;
            // Only so that a recovery can be logged. A poll that fails for
            // twenty minutes and then works again is invisible otherwise: the
            // warnings scroll past and nothing ever says inbound is back, so
            // "my message got no reply" has no timeline to sit against.
            let mut failures: u32 = 0;

            loop {
                let body = json!({
                    "offset": offset,
                    "timeout": POLL_TIMEOUT_SECS,
                    "allowed_updates": ["message", "callback_query"],
                });

                let updates = match self.call("getUpdates", body).await {
                    Ok(Value::Array(updates)) => updates,
                    // Not an array: `ok` was true and `result` was something
                    // else. Back off like any other failure rather than
                    // spinning on it — an un-slept `continue` here is a busy
                    // loop against Telegram.
                    Ok(other) => {
                        tracing::warn!("getUpdates returned no update array: {other}");
                        failures += 1;
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                    Err(e) => {
                        // `{e:#}` rather than `{e}`: the outer context is
                        // "telegram getUpdates", which says only which call it
                        // was. The cause — a timeout, a DNS failure, a 409
                        // from a second poller on the same token — is the
                        // whole of the information, and it lives further down
                        // the chain.
                        tracing::warn!("getUpdates failed: {e:#}");
                        failures += 1;
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                };

                if failures > 0 {
                    tracing::info!("getUpdates recovered after {failures} failures");
                    failures = 0;
                }

                for update in updates {
                    if let Some(id) = update.get("update_id").and_then(Value::as_i64) {
                        offset = offset.max(id + 1);
                    }

                    let Some((mut inbound, wanted)) = self.parse_update(&update) else {
                        continue;
                    };

                    // The download happens here, on the poll loop, rather than
                    // on a task of its own. A batch of updates is one person's
                    // messages in the order they sent them, and keeping that
                    // order is worth more than overlapping their own transfers.
                    // What it costs is this bot's polling for the length of the
                    // transfer, bounded by `inbox::MAX_BYTES` and a per-request
                    // timeout — and that is all it costs, because the core and
                    // every other channel are on other tasks.
                    if !wanted.is_empty() {
                        let fetched = self.fetch_all(&inbound.thread, wanted).await;
                        if let InboundKind::Text { files, .. } = &mut inbound.kind {
                            *files = fetched;
                        }
                    }

                    if tx.send(inbound).await.is_err() {
                        return;
                    }
                }
            }
        });

        rx
    }

    /// Turn one update into an [`Inbound`], dropping anything unauthorized or
    /// uninteresting, and report the files it refers to.
    ///
    /// The files come back as ids to fetch rather than fetched, so this stays a
    /// pure function of the update: the tests drive the real parser over real
    /// captured updates without a network in reach, which is what kept the
    /// caption and thread-id rules honest. The caller does the fetching.
    fn parse_update(&self, update: &Value) -> Option<(Inbound, Vec<Want>)> {
        if let Some(callback) = update.get("callback_query") {
            let user_id = user_id(callback.get("from"))?;
            if !self.is_allowed(&user_id) {
                return None;
            }

            // Anything without a question token is from a build that predates
            // them. Dropping it is the safe direction: the alternative is
            // applying it to whatever is pending now.
            let (verb, question) = callback
                .get("data")
                .and_then(Value::as_str)?
                .split_once(':')?;
            let allow = match verb {
                CB_ALLOW => true,
                CB_DENY => false,
                _ => return None,
            };

            return Some((
                Inbound {
                    thread: thread_key(callback.get("message")?)?,
                    user_id,
                    kind: InboundKind::Decision {
                        allow,
                        ack: callback.get("id").and_then(Value::as_str)?.to_string(),
                        question: question.to_string(),
                    },
                },
                Vec::new(),
            ));
        }

        let message = update.get("message")?;
        let user_id = user_id(message.get("from"))?;
        if !self.is_allowed(&user_id) {
            // Silent: replying would confirm the bot exists to whoever found it.
            tracing::info!("dropped message from unauthorized user {user_id}");
            return None;
        }

        // A photo or a document carries its words in `caption`, not `text`.
        // Reading only `text` dropped every screenshot-with-a-question, which
        // is one of the more natural things to send a coding agent from a
        // phone.
        let text = message
            .get("text")
            .or_else(|| message.get("caption"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim();

        let thread = thread_key(message)?;

        // Menu taps in a group arrive addressed to the bot
        // (`/new@omatether_bot`); make those read as what was tapped.
        let text = self.own_command(text);
        let wanted = wants(message);

        if text.is_empty() && wanted.is_empty() {
            // Silence here is the bug this exists to prevent: a message we
            // cannot act on used to vanish with nothing in the chat and nothing
            // in the log, which is indistinguishable from the bridge being
            // down. What is left in here is now narrow — a sticker, a location,
            // a poll — because anything with a file in it has a file to pass on.
            tracing::info!(thread = %thread, "message with nothing to act on");
            return Some((
                Inbound {
                    thread,
                    user_id,
                    kind: InboundKind::Unsupported(
                        "There is nothing in that I can pass on. Send words, a \
                         photo or a file — a caption comes through too."
                            .to_string(),
                    ),
                },
                Vec::new(),
            ));
        }

        Some((
            Inbound {
                thread,
                user_id,
                // Filled in by the caller once the bytes are down.
                kind: InboundKind::Text {
                    text: text.to_string(),
                    files: Vec::new(),
                },
            },
            wanted,
        ))
    }

    /// Fetch every file in one message, in order, and report each outcome.
    ///
    /// One that fails is still reported — as an [`Attachment`] whose `saved` is
    /// the reason — because the person watching their phone attached it, and a
    /// reply that never mentions it reads as an answer to a different message.
    async fn fetch_all(&self, thread: &ThreadKey, wanted: Vec<Want>) -> Vec<Attachment> {
        let mut files = Vec::with_capacity(wanted.len());

        for want in wanted {
            let saved = match self.fetch(thread, &want).await {
                Ok(path) => {
                    tracing::info!(thread = %thread, "received {} -> {}", want.name, path.display());
                    Ok(path)
                }
                Err(e) => {
                    // `{e:#}` for the same reason getUpdates uses it: the outer
                    // context names the call, and the cause is further down.
                    tracing::warn!(thread = %thread, "could not fetch {}: {e:#}", want.name);
                    Err(format!("{e:#}"))
                }
            };
            files.push(Attachment {
                name: want.name,
                mime: want.mime,
                size: want.size,
                saved,
            });
        }

        // On the way past, while we are already doing filesystem work for this
        // thread. An inbox nobody prunes is a disk that fills months later.
        inbox::prune(&self.inbox_dir);
        files
    }

    /// `getFile` for the path, then a plain GET for the bytes.
    ///
    /// The Bot API will not serve a file over 20 MB at all — `getFile` answers
    /// "file is too big" — so the size Telegram already told us in the update is
    /// checked first, to turn that into a sentence naming the actual size
    /// instead of an API error.
    async fn fetch(&self, thread: &ThreadKey, want: &Want) -> Result<PathBuf> {
        if want.size > inbox::MAX_BYTES {
            bail!(
                "it is {} — Telegram will not send a bot anything over {}",
                inbox::human(want.size),
                inbox::human(inbox::MAX_BYTES)
            );
        }

        let file = self
            .call("getFile", json!({ "file_id": want.file_id }))
            .await?;
        let remote = file
            .get("file_path")
            .and_then(Value::as_str)
            .context("getFile returned no file_path")?;

        let bytes = self
            .http
            .get(format!("{}/{}", self.file_base, remote))
            // Overrides the client's poll-shaped default: a download is a
            // transfer, not a long poll, and a stalled one must not wedge
            // inbound for this bot.
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(scrub)
            .context("downloading the file from telegram")?
            .error_for_status()
            .map_err(scrub)
            .context("downloading the file from telegram")?
            .bytes()
            .await
            .map_err(scrub)
            .context("reading the download")?;

        // The update's `file_size` is the only pre-check there is, and it is
        // optional; re-check what actually arrived before it goes to disk.
        if bytes.len() as u64 > inbox::MAX_BYTES {
            bail!(
                "it is {}, which is over the limit",
                inbox::human(bytes.len() as u64)
            );
        }

        inbox::save(&self.inbox_dir, thread, &want.name, &bytes)
            .with_context(|| format!("saving {} to the inbox", want.name))
    }

    fn is_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == user_id)
    }
}

#[async_trait]
impl Channel for Telegram {
    fn name(&self) -> &'static str {
        CHANNEL
    }

    /// Telegram edits messages, so a turn can stream into one that grows.
    fn can_edit(&self) -> bool {
        true
    }

    /// A turn is markdown, because the agents write markdown, and Telegram
    /// renders none of it without a `parse_mode` — this used to arrive as raw
    /// asterisks and visible backticks. See [`markup`] for why the mode is
    /// HTML rather than either of Telegram's markdown dialects.
    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        let body = self.target(thread);
        let result = self.call_rendered("sendMessage", body, &clip(text)).await?;
        Ok(result
            .get("message_id")
            .and_then(Value::as_i64)
            .map(|id| id.to_string())
            .unwrap_or_default())
    }

    async fn edit(&self, thread: &ThreadKey, id: &MessageId, text: &str) -> Result<()> {
        let body = json!({
            "chat_id": thread.chat_id,
            "message_id": id.parse::<i64>().unwrap_or_default(),
        });

        match self
            .call_rendered("editMessageText", body, &clip(text))
            .await
        {
            Ok(_) => Ok(()),
            // Telegram rejects an edit that would not change anything. That is
            // a no-op for us, not a failure.
            Err(e) if e.to_string().contains("message is not modified") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// **Sent unformatted**, and deliberately not through [`Self::call_rendered`].
    ///
    /// This message quotes a tool's own arguments, and any renderer rewrites
    /// them: `rm -rf /tmp/*_cache*` would come out as `rm -rf /tmp/_cache`,
    /// because the asterisks pair into emphasis and are consumed. You would be
    /// reading one command and tapping Allow on another. Same rule as never
    /// truncating a question — it goes out exactly as it is, or not at all.
    async fn ask_permission(
        &self,
        thread: &ThreadKey,
        text: &str,
        question: &str,
    ) -> Result<MessageId> {
        let mut body = self.target(thread);
        body["text"] = json!(clip(text));
        body["reply_markup"] = json!({
            "inline_keyboard": [[
                { "text": "Allow", "callback_data": format!("{CB_ALLOW}:{question}") },
                { "text": "Deny",  "callback_data": format!("{CB_DENY}:{question}")  },
            ]]
        });

        let result = self.call("sendMessage", body).await?;
        Ok(result
            .get("message_id")
            .and_then(Value::as_i64)
            .map(|id| id.to_string())
            .unwrap_or_default())
    }

    /// Telegram has no way to withdraw a chat action — it expires by itself
    /// after about five seconds — so there is nothing to do for `off`, and the
    /// expiry is why the core re-sends this while a turn is running.
    async fn typing(&self, thread: &ThreadKey, on: bool) -> Result<()> {
        if !on {
            return Ok(());
        }
        let mut body = self.target(thread);
        body["action"] = json!("typing");
        self.call("sendChatAction", body).await?;
        Ok(())
    }

    async fn ack_decision(&self, token: &str, note: &str) -> Result<()> {
        self.call(
            "answerCallbackQuery",
            json!({ "callback_query_id": token, "text": note }),
        )
        .await?;
        Ok(())
    }
}

/// Drop the URL from a transport error, because every Bot API URL carries the
/// bot token in its path.
///
/// reqwest's `Display` quotes the URL it was given, and the poller reports its
/// failures with `{e:#}` — so a network outage wrote the token into the journal
/// once per poll, 110 times in the hour this was found. A bot token in a chat
/// is a shell on this machine, and a log is read, shared and pasted far more
/// freely than `~/.config/omatether/env` is. The cause (a timeout, DNS, a 409)
/// survives untouched, and which call it was is already in our own context —
/// that was the useful half of the line.
///
/// Apply it to every reqwest result built from `base` or `file_base`, including
/// `error_for_status` and the body reads: they all carry the request's URL.
pub(super) fn scrub(e: reqwest::Error) -> reqwest::Error {
    e.without_url()
}

/// Telegram refusing to parse what we sent, as opposed to any other failure.
///
/// Matched on the description because that is all the Bot API gives: every
/// error is a 400 with a sentence in it. Only this one is worth retrying
/// unformatted; a 429 or a bad chat id would fail the same way twice.
fn is_parse_failure(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("can't parse entities")
}

/// A file Telegram is holding for us, before anything has been fetched.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Want {
    file_id: String,
    /// What it will be called on disk. Telegram names a document and nothing
    /// else, so the rest get a name describing what they are — which is what
    /// the agent sees in the prompt, and `photo.jpg` reads better there than a
    /// file_unique_id would.
    name: String,
    mime: String,
    size: u64,
}

/// Every file in one message.
///
/// A `Vec` even though Telegram sends one media per message — an album arrives
/// as several updates, not one — because the seam carries a list and a channel
/// that batches would otherwise have nowhere to put the second file.
///
/// Stickers, locations, polls and contacts are deliberately absent: a `.webp`
/// of a cartoon on disk is not something an agent can do anything useful with,
/// and pretending otherwise would spend a turn to be told so.
fn wants(message: &Value) -> Vec<Want> {
    // Largest first is not guaranteed by the docs — "available sizes" — so the
    // biggest is chosen rather than the last. A phone sends a thumbnail and a
    // full-resolution copy in the same array, and the thumbnail is the one an
    // agent cannot read the text in.
    if let Some(sizes) = message.get("photo").and_then(Value::as_array) {
        let largest = sizes.iter().max_by_key(|size| {
            size.get("file_size")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| {
                    let dimension = |name| size.get(name).and_then(Value::as_u64).unwrap_or(0);
                    dimension("width") * dimension("height")
                })
        });
        if let Some(want) = largest.and_then(|size| want(size, "photo.jpg", "image/jpeg")) {
            return vec![want];
        }
    }

    const MEDIA: [(&str, &str, &str); 6] = [
        ("document", "file", "application/octet-stream"),
        ("video", "video.mp4", "video/mp4"),
        ("animation", "animation.mp4", "video/mp4"),
        ("audio", "audio", "audio/mpeg"),
        // A voice note comes through as a file rather than as "I can't read
        // that". Most agents will say they cannot listen to it, which is a
        // true answer the person can act on; one with a transcriber on PATH
        // can do better, and neither outcome is this adapter's call to make.
        ("voice", "voice.ogg", "audio/ogg"),
        ("video_note", "video-note.mp4", "video/mp4"),
    ];

    for (field, name, mime) in MEDIA {
        if let Some(want) = message.get(field).and_then(|m| want(m, name, mime)) {
            return vec![want];
        }
    }

    Vec::new()
}

/// One media object — they all carry `file_id`, and most carry the rest.
fn want(media: &Value, default_name: &str, default_mime: &str) -> Option<Want> {
    let file_id = media.get("file_id").and_then(Value::as_str)?.to_string();
    Some(Want {
        file_id,
        name: media
            .get("file_name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(default_name)
            .to_string(),
        mime: media
            .get("mime_type")
            .and_then(Value::as_str)
            .unwrap_or(default_mime)
            .to_string(),
        size: media.get("file_size").and_then(Value::as_u64).unwrap_or(0),
    })
}

fn user_id(from: Option<&Value>) -> Option<String> {
    from?
        .get("id")
        .and_then(Value::as_i64)
        .map(|i| i.to_string())
}

fn thread_key(message: &Value) -> Option<ThreadKey> {
    let chat_id = message.get("chat")?.get("id").and_then(Value::as_i64)?;

    // Only forum topics count as a distinct thread. `message_thread_id` is also
    // set on plain replies in a group, where treating it as a thread would
    // scatter one conversation across a new session per reply chain.
    let topic_id = message
        .get("is_topic_message")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        .then(|| {
            message
                .get("message_thread_id")
                .and_then(Value::as_i64)
                .map(|t| t.to_string())
        })
        .flatten();

    Some(ThreadKey {
        channel: CHANNEL,
        chat_id: chat_id.to_string(),
        topic_id,
    })
}

/// Insurance, not the way a long turn is handled. The core pages a turn into
/// messages well under this before it gets here (`PAGE_BUDGET` in `core.rs`), so
/// what this still catches is a standalone note — and it says it cut, rather
/// than letting Telegram refuse the message outright.
fn clip(text: &str) -> String {
    if text.chars().count() <= MAX_TEXT {
        return text.to_string();
    }
    let kept: String = text.chars().take(MAX_TEXT).collect();
    format!("{kept}\n\n… truncated")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telegram() -> Telegram {
        // No test in here fetches anything, so the inbox is never written to;
        // `parse_update` is pure by design, and keeping it that way is what
        // lets these run with no network and no scratch directory.
        Telegram::new(
            "test-token",
            vec!["42".into()],
            PathBuf::from("/nonexistent"),
        )
        .unwrap()
    }

    /// The parse, with the files it asked for.
    fn parse(tg: &Telegram, update: &Value) -> Option<(Inbound, Vec<Want>)> {
        tg.parse_update(update)
    }

    #[test]
    fn empty_allowlist_is_refused() {
        assert!(Telegram::new("t", vec![], PathBuf::from("/nonexistent")).is_err());
    }

    /// The token must not survive into a log line. Uses a real transport
    /// failure rather than a hand-built error, because what leaks is reqwest's
    /// own `Display`, and a stub of it would not catch the version that changes
    /// its mind. No network: the port is bound to learn a free one and then
    /// dropped, so the connect is refused locally.
    #[tokio::test]
    async fn a_failed_call_does_not_log_the_token() {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let url = format!("http://127.0.0.1:{port}/bot1234:SECRET-TOKEN/getUpdates");

        let raw = reqwest::Client::new().get(&url).send().await.unwrap_err();
        assert!(
            format!("{raw}").contains("SECRET-TOKEN"),
            "reqwest stopped quoting the url, so this test proves nothing: {raw}"
        );

        let scrubbed = anyhow::Error::new(scrub(raw)).context("telegram getUpdates");
        let line = format!("{scrubbed:#}");
        assert!(!line.contains("SECRET-TOKEN"), "token leaked: {line}");
        // The cause is the half worth keeping.
        assert!(line.contains("telegram getUpdates"), "{line}");
        assert!(line.contains("error sending request"), "{line}");
    }

    #[test]
    fn our_own_suffix_is_stripped_from_menu_taps() {
        let tg = telegram();
        tg.username.set("omatether_bot".into()).unwrap();

        assert_eq!(tg.own_command("/new@omatether_bot"), "/new");
        // Arguments survive: the command word is the only part addressed.
        assert_eq!(tg.own_command("/deny@omatether_bot too risky"), "/deny");
        // Plain commands and commands with arguments are untouched.
        assert_eq!(tg.own_command("/status"), "/status");
        assert_eq!(tg.own_command("/cd ~/src"), "/cd ~/src");
        // A suffix naming a different bot in the group is not ours to eat.
        assert_eq!(
            tg.own_command("/new@someone_elses_bot"),
            "/new@someone_elses_bot"
        );
        // An @ later in the message is prose, not an address.
        assert_eq!(tg.own_command("email a@b.com"), "email a@b.com");
        // Before whoami has run there is nothing to match against.
        let unknown = telegram();
        assert_eq!(
            unknown.own_command("/new@omatether_bot"),
            "/new@omatether_bot"
        );
    }

    #[test]
    fn a_menu_tap_in_a_group_reaches_the_command_parser() {
        let tg = telegram();
        tg.username.set("omatether_bot".into()).unwrap();
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "text": "/status@omatether_bot"
            }
        });
        match parse(&tg, &update).unwrap().0.kind {
            InboundKind::Text { text, .. } => assert_eq!(text, "/status"),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn messages_from_strangers_are_dropped() {
        let update = json!({
            "update_id": 1,
            "message": { "from": { "id": 999 }, "chat": { "id": 5 }, "text": "hi" }
        });
        assert!(parse(&telegram(), &update).is_none());
    }

    #[test]
    fn allowed_message_parses() {
        let update = json!({
            "update_id": 1,
            "message": { "from": { "id": 42 }, "chat": { "id": 5 }, "text": "  hello  " }
        });
        let (inbound, wanted) = parse(&telegram(), &update).unwrap();
        assert_eq!(inbound.thread.to_string(), "telegram:5");
        assert!(wanted.is_empty(), "no files to fetch for a plain message");
        match inbound.kind {
            InboundKind::Text { text, .. } => assert_eq!(text, "hello"),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn a_photo_caption_is_the_prompt_and_the_photo_comes_with_it() {
        // Sending a screenshot with a question is a natural thing to do from a
        // phone, and the words are in `caption`, not `text`.
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "photo": [{ "file_id": "x" }], "caption": "what is wrong here?"
            }
        });
        let (inbound, wanted) = parse(&telegram(), &update).unwrap();
        match inbound.kind {
            InboundKind::Text { text, .. } => assert_eq!(text, "what is wrong here?"),
            other => panic!("expected text, got {other:?}"),
        }
        assert_eq!(wanted.len(), 1);
        assert_eq!(wanted[0].file_id, "x");
    }

    #[test]
    fn the_biggest_copy_of_a_photo_is_the_one_fetched() {
        // A phone sends the same picture several times over. The thumbnail is
        // the one whose text an agent cannot read, and the array is documented
        // as "available sizes" rather than sorted, so size decides.
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "photo": [
                    { "file_id": "thumb", "file_size": 1200, "width": 90, "height": 60 },
                    { "file_id": "full", "file_size": 480_000, "width": 1280, "height": 960 },
                    { "file_id": "middle", "file_size": 42_000, "width": 320, "height": 240 }
                ]
            }
        });
        let (_, wanted) = parse(&telegram(), &update).unwrap();
        assert_eq!(wanted[0].file_id, "full");
        assert_eq!(wanted[0].name, "photo.jpg");
        assert_eq!(wanted[0].size, 480_000);
    }

    #[test]
    fn a_bare_photo_is_a_prompt_on_its_own() {
        // It used to be "I can only read text". There is now a file to hand
        // over, so the turn runs with the picture and no words.
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "photo": [{ "file_id": "x", "file_size": 900 }]
            }
        });
        let (inbound, wanted) = parse(&telegram(), &update).unwrap();
        assert_eq!(wanted.len(), 1);
        match inbound.kind {
            InboundKind::Text { text, .. } => assert!(text.is_empty()),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn a_document_keeps_the_name_and_type_telegram_gave_it() {
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "document": {
                    "file_id": "d1", "file_name": "trace.log",
                    "mime_type": "text/plain", "file_size": 2048
                },
                "caption": "why does this end here?"
            }
        });
        let (_, wanted) = parse(&telegram(), &update).unwrap();
        assert_eq!(wanted[0].name, "trace.log");
        assert_eq!(wanted[0].mime, "text/plain");
        assert_eq!(wanted[0].size, 2048);
    }

    #[test]
    fn a_voice_note_is_handed_over_rather_than_refused() {
        // Most agents will answer that they cannot listen to it — which is a
        // true answer the person can act on, and not this adapter's call to
        // pre-empt.
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "voice": { "file_id": "v1", "duration": 3, "mime_type": "audio/ogg" }
            }
        });
        let (inbound, wanted) = parse(&telegram(), &update).unwrap();
        assert_eq!(wanted[0].name, "voice.ogg");
        assert!(matches!(inbound.kind, InboundKind::Text { .. }));
    }

    #[test]
    fn a_message_we_cannot_read_still_gets_an_answer() {
        // A sticker carries no words and nothing worth putting on disk. It used
        // to be dropped in silence, which from the phone looks exactly like the
        // bridge being down.
        let update = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "sticker": { "file_id": "s1", "emoji": "🎉" }
            }
        });
        let (inbound, wanted) = parse(&telegram(), &update).unwrap();
        assert_eq!(inbound.thread.to_string(), "telegram:5");
        assert!(wanted.is_empty());
        match inbound.kind {
            InboundKind::Unsupported(note) => assert!(note.contains("Send words")),
            other => panic!("expected unsupported, got {other:?}"),
        }
    }

    #[test]
    fn strangers_get_no_answer_at_all() {
        // Not even the "nothing in that I can pass on" note: replying would
        // confirm the bot exists to whoever found it.
        let update = json!({
            "update_id": 1,
            "message": { "from": { "id": 999 }, "chat": { "id": 5 }, "voice": {} }
        });
        assert!(parse(&telegram(), &update).is_none());
    }

    #[test]
    fn only_a_parse_failure_is_worth_resending_unformatted() {
        // The trigger for dropping parse_mode, matched on Telegram's own
        // sentence because a 400 is all the Bot API ever reports.
        let parse = anyhow::anyhow!(
            "telegram sendMessage failed: Bad Request: can't parse entities: \
             Unsupported start tag \"x\" at byte offset 0"
        );
        assert!(is_parse_failure(&parse));

        // Anything else would fail the same way twice.
        assert!(!is_parse_failure(&anyhow::anyhow!(
            "telegram sendMessage failed: Bad Request: chat not found"
        )));
        assert!(!is_parse_failure(&anyhow::Error::new(
            super::super::RateLimited {
                retry_after: Duration::from_secs(3)
            }
        )));
    }

    #[test]
    fn only_forum_topics_become_separate_threads() {
        let reply = json!({
            "update_id": 1,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "message_thread_id": 77, "text": "hi"
            }
        });
        assert_eq!(
            parse(&telegram(), &reply).unwrap().0.thread.to_string(),
            "telegram:5"
        );

        let topic = json!({
            "update_id": 2,
            "message": {
                "from": { "id": 42 }, "chat": { "id": 5 },
                "message_thread_id": 77, "is_topic_message": true, "text": "hi"
            }
        });
        assert_eq!(
            parse(&telegram(), &topic).unwrap().0.thread.to_string(),
            "telegram:5:77"
        );
    }

    #[test]
    fn button_taps_carry_the_question_they_were_asked_under() {
        let update = json!({
            "update_id": 1,
            "callback_query": {
                "id": "cb-1", "from": { "id": 42 }, "data": "deny:a1b2c3d4",
                "message": { "chat": { "id": 5 } }
            }
        });
        match parse(&telegram(), &update).unwrap().0.kind {
            InboundKind::Decision {
                allow,
                ack,
                question,
            } => {
                assert!(!allow);
                assert_eq!(ack, "cb-1", "so the spinner can be stopped");
                assert_eq!(question, "a1b2c3d4", "so a stale tap can be spotted");
            }
            other => panic!("expected decision, got {other:?}"),
        }
    }

    #[test]
    fn a_tap_with_no_question_token_is_dropped() {
        // A button from a build that predates question tokens. Dropping it is
        // the safe direction — the alternative is applying someone's old tap to
        // whatever tool is waiting now.
        let update = json!({
            "update_id": 1,
            "callback_query": {
                "id": "cb-1", "from": { "id": 42 }, "data": "allow",
                "message": { "chat": { "id": 5 } }
            }
        });
        assert!(parse(&telegram(), &update).is_none());
    }

    #[test]
    fn the_buttons_carry_the_question_into_their_callback_data() {
        // The two halves have to agree, and they are written in different
        // places: this is the seam where a rename would go unnoticed.
        let sent = json!({
            "inline_keyboard": [[
                { "text": "Allow", "callback_data": format!("{CB_ALLOW}:{}", "a1b2c3d4") },
                { "text": "Deny",  "callback_data": format!("{CB_DENY}:{}", "a1b2c3d4")  },
            ]]
        });
        let allow = sent["inline_keyboard"][0][0]["callback_data"]
            .as_str()
            .unwrap();
        assert!(allow.len() <= 64, "Telegram's callback_data limit");

        let update = json!({
            "update_id": 1,
            "callback_query": {
                "id": "cb-1", "from": { "id": 42 }, "data": allow,
                "message": { "chat": { "id": 5 } }
            }
        });
        match parse(&telegram(), &update).unwrap().0.kind {
            InboundKind::Decision {
                allow, question, ..
            } => {
                assert!(allow);
                assert_eq!(question, "a1b2c3d4");
            }
            other => panic!("expected decision, got {other:?}"),
        }
    }

    #[test]
    fn long_text_is_clipped_not_split() {
        let clipped = clip(&"x".repeat(MAX_TEXT + 500));
        assert!(clipped.ends_with("… truncated"));
        assert!(clipped.chars().count() < MAX_TEXT + 20);
    }

    #[test]
    fn short_text_is_untouched() {
        assert_eq!(clip("hello"), "hello");
    }
}
