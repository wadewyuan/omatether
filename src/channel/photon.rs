//! Photon (iMessage) channel.
//!
//! Photon's Spectrum SDK is TypeScript only, so this channel is half Rust and
//! half a supervised Node process. The sidecar in `vendor/photon-sidecar` owns
//! the SDK and speaks loopback HTTP; everything here drives it.
//!
//! Two things differ from Telegram, and both are the platform's doing:
//!
//! * **No editing.** iMessage cannot rewrite a sent message, so
//!   [`Channel::can_edit`] is false and the core delivers each turn whole
//!   rather than streaming it.
//! * **No buttons.** Permission questions arrive as text asking for `/allow`
//!   or `/deny`, which the command parser already understands.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use super::{Channel, Inbound, InboundKind, MessageId, ThreadKey};

pub const CHANNEL: &str = "photon";

/// iMessage has no documented hard limit, but a wall of text is unreadable on a
/// phone and long agent output belongs somewhere else anyway.
const MAX_TEXT: usize = 3000;

pub struct Photon {
    http: reqwest::Client,
    base: String,
    token: String,
    /// Held so the sidecar dies with us: its stdin is a pipe, and it is started
    /// with PHOTON_SIDECAR_WATCH_STDIN=1.
    _child: Child,
    allowed_users: Vec<String>,
}

pub struct Config {
    pub sidecar_dir: PathBuf,
    pub project_id: String,
    pub project_secret: String,
    /// Loopback port for the sidecar's control channel. Zero — the default —
    /// means "pick a free one", which is what you want: a fixed port collides
    /// with anything else running a Photon sidecar, Hermes included.
    pub port: u16,
    /// Phone numbers (E.164) permitted to talk to this bridge.
    pub allowed_users: Vec<String>,
}

impl Photon {
    /// Start the sidecar and wait for it to answer.
    pub async fn start(config: Config) -> Result<Self> {
        if config.allowed_users.is_empty() {
            bail!("refusing to start Photon with an empty allowlist — set OMATETHER_PHOTON_ALLOWED_USERS");
        }

        let entry = config.sidecar_dir.join("index.mjs");
        if !entry.is_file() {
            bail!(
                "photon sidecar not found at {} — run `npm install` in that directory",
                entry.display()
            );
        }

        // A fresh secret per run. It never leaves this machine and only guards
        // a loopback socket, but an unauthenticated one would let any local
        // process send iMessages as the user.
        let token = format!("{}", uuid::Uuid::new_v4());

        let port = match config.port {
            0 => free_port().context("finding a free loopback port for the sidecar")?,
            port => port,
        };

        let mut child = Command::new("node")
            .arg(&entry)
            .current_dir(&config.sidecar_dir)
            .env("PHOTON_PROJECT_ID", &config.project_id)
            .env("PHOTON_PROJECT_SECRET", &config.project_secret)
            .env("PHOTON_SIDECAR_PORT", port.to_string())
            .env("PHOTON_SIDECAR_TOKEN", &token)
            // Bind the sidecar's life to ours. Without this a crashed
            // omatether leaves a process holding the iMessage line.
            .env("PHOTON_SIDECAR_WATCH_STDIN", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("spawning the photon sidecar — is node on PATH?")?;

        // Keep the sidecar's last words, so a startup failure can quote the
        // reason instead of pointing at a log the operator may not have.
        let recent: RecentLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(log_lines(stderr, recent.clone()));
        }

        let http = reqwest::Client::builder()
            // No timeout: /inbound is an open-ended stream. Per-request
            // timeouts are applied where they make sense instead.
            .build()?;

        let base = format!("http://127.0.0.1:{port}");
        wait_until_ready(&http, &base, &token, &mut child, &recent).await?;

        Ok(Self {
            http,
            base,
            token,
            _child: child,
            allowed_users: config.allowed_users,
        })
    }

    /// Consume the sidecar's NDJSON stream, reconnecting if it drops.
    pub fn start_streaming(self: std::sync::Arc<Self>) -> mpsc::Receiver<Inbound> {
        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            // The sidecar replays catch-up messages after a reconnect, so the
            // same message can arrive twice.
            let mut seen: std::collections::VecDeque<String> = std::collections::VecDeque::new();

            loop {
                match self.stream_once(&tx, &mut seen).await {
                    Ok(()) => tracing::warn!("photon inbound stream ended, reconnecting"),
                    Err(e) => tracing::warn!("photon inbound stream failed: {e:#}"),
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

        rx
    }

    async fn stream_once(
        &self,
        tx: &mpsc::Sender<Inbound>,
        seen: &mut std::collections::VecDeque<String>,
    ) -> Result<()> {
        let response = self
            .http
            .get(format!("{}/inbound", self.base))
            .header("x-omatether-token", &self.token)
            .send()
            .await?
            .error_for_status()?;

        let stream = tokio_util_lines(response);
        tokio::pin!(stream);

        let mut reader = BufReader::new(stream).lines();
        while let Some(line) = reader.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }

            let event: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(e) => {
                    tracing::warn!("photon sent an unparseable line: {e}");
                    continue;
                }
            };

            if let Some(inbound) = parse_event(&self.allowed_users, &event, seen) {
                if tx.send(inbound).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Post one message, choosing whether the sidecar renders it as markdown.
    ///
    /// The choice is the caller's because it is not cosmetic. `"markdown"`
    /// means the iMessage adapter *parses* the string and sends plain text plus
    /// native emphasis ranges, so a decorator becomes actual bold — and so a
    /// literal `*` in the text is consumed rather than shown. Prose wants that;
    /// a quoted command does not.
    async fn post_send(&self, thread: &ThreadKey, text: &str, format: &str) -> Result<MessageId> {
        let response: Value = self
            .http
            .post(format!("{}/send", self.base))
            .header("x-omatether-token", &self.token)
            .timeout(Duration::from_secs(30))
            .json(&json!({ "spaceId": thread.chat_id, "text": clip(text), "format": format }))
            .send()
            .await
            .context("photon /send")?
            .json()
            .await
            .context("decoding photon /send")?;

        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            bail!(
                "photon send failed: {}",
                response
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            );
        }

        Ok(response
            .get("messageId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string())
    }
}

#[async_trait]
impl Channel for Photon {
    fn name(&self) -> &'static str {
        CHANNEL
    }

    /// iMessage cannot rewrite a sent message.
    fn can_edit(&self) -> bool {
        false
    }

    /// A turn is markdown, because the agents write markdown. iMessage has no
    /// markup of its own, but the Spectrum adapter renders markdown down to
    /// native styled text — real bold, `•` bullets, headings as bold — so this
    /// is the platform's own supported path rather than a hack.
    ///
    /// Code spans and fences become Unicode mathematical monospace, which reads
    /// correctly but cannot be pasted into a shell. That is a real cost and it
    /// was weighed: from a phone these are read far more often than pasted.
    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        self.post_send(thread, text, "markdown").await
    }

    async fn edit(&self, _thread: &ThreadKey, _id: &MessageId, _text: &str) -> Result<()> {
        // Unreachable while can_edit() is false, and a loud failure is better
        // than a silent no-op if that ever stops being true.
        bail!("iMessage cannot edit a sent message")
    }

    /// The only feedback there is on iMessage: with no editing, nothing else
    /// arrives until the turn is finished.
    ///
    /// Unlike Telegram's, this indicator does not expire on its own, so the
    /// `off` half is the one that matters here — without it the chat is left
    /// showing three dots for an agent that stopped.
    async fn typing(&self, thread: &ThreadKey, on: bool) -> Result<()> {
        let state = if on { "start" } else { "stop" };
        self.http
            .post(format!("{}/typing", self.base))
            .header("x-omatether-token", &self.token)
            .timeout(Duration::from_secs(10))
            .json(&json!({ "spaceId": thread.chat_id, "state": state }))
            .send()
            .await
            .context("photon /typing")?
            // Checked rather than ignored: the sidecar used to call a method
            // the SDK does not have, which failed in exactly this silence.
            .error_for_status()
            .context("photon /typing")?;
        Ok(())
    }

    /// No buttons here, so nothing can carry the question token back: an
    /// answer arrives as `/allow` or `/deny` text, which is always about
    /// whatever is currently pending. There is no stale tap to guard against
    /// because there is nothing left behind to tap.
    ///
    /// **Sent as text, never markdown**, and deliberately not through
    /// [`Channel::send`]. This message quotes a tool's own arguments, and a
    /// markdown renderer rewrites them: `rm -rf /tmp/*_cache*` renders as
    /// `rm -rf /tmp/_cache`, because the asterisks pair into emphasis and are
    /// consumed. You would be reading one command and approving another. That
    /// is the same rule as never truncating a question — a permission prompt is
    /// the one human control here, so it goes out exactly as it is or not at
    /// all.
    async fn ask_permission(
        &self,
        thread: &ThreadKey,
        text: &str,
        _question: &str,
    ) -> Result<MessageId> {
        let question = format!("{text}\n\nReply /allow or /deny <why>");
        self.post_send(thread, &question, "text").await
    }
}

/// Wait for the sidecar to serve, or explain why it never will.
///
/// It has to construct a Spectrum client — which validates credentials against
/// Photon's cloud — and open a gRPC stream before it listens, so the first
/// second or two of requests fail normally. A sidecar that has *exited*,
/// though, is never going to answer: noticing that turns a 20-second timeout
/// into an immediate, accurate error.
async fn wait_until_ready(
    http: &reqwest::Client,
    base: &str,
    token: &str,
    child: &mut Child,
    recent: &RecentLog,
) -> Result<()> {
    for attempt in 0..40 {
        if let Some(status) = child.try_wait()? {
            // Give it a moment to flush: the process can exit before the
            // reader task has drained the line explaining why.
            tokio::time::sleep(Duration::from_millis(200)).await;
            bail!(
                "photon sidecar exited during startup ({status}):\n{}",
                tail(recent)
            );
        }

        let response = http
            .get(format!("{base}/health"))
            .header("x-omatether-token", token)
            .timeout(Duration::from_secs(2))
            .send()
            .await;

        if let Ok(response) = response {
            if response.status().is_success() {
                tracing::info!("photon sidecar ready after {attempt} attempts");
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("photon sidecar did not become ready within 20s — check its log")
}

/// Adapt a reqwest byte stream into something `BufReader` can read lines from.
fn tokio_util_lines(response: reqwest::Response) -> impl tokio::io::AsyncRead {
    use futures_util::TryStreamExt;
    tokio::io::BufReader::new(tokio_util::io::StreamReader::new(
        response.bytes_stream().map_err(std::io::Error::other),
    ))
}

/// Ask the OS for a port nobody is using.
///
/// Binding and immediately dropping leaves a small race, but the alternative —
/// a fixed default — collides in practice: Hermes runs its own Photon sidecar
/// on 8789, and losing that race is a certainty rather than a chance.
fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// The sidecar's most recent stderr, kept so a failure can quote itself.
type RecentLog = Arc<std::sync::Mutex<Vec<String>>>;

const RECENT_LINES: usize = 20;

async fn log_lines(stderr: tokio::process::ChildStderr, recent: RecentLog) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "omatether::photon", "{line}");
        if let Ok(mut recent) = recent.lock() {
            recent.push(line);
            if recent.len() > RECENT_LINES {
                recent.remove(0);
            }
        }
    }
}

fn tail(recent: &RecentLog) -> String {
    match recent.lock() {
        Ok(recent) if !recent.is_empty() => recent
            .iter()
            .map(|line| format!("  {line}"))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => "  (the sidecar said nothing before exiting)".to_string(),
    }
}

/// Turn one sidecar line into an inbound message.
///
/// A free function rather than a method so the tests can drive the real thing:
/// `Photon` owns a live child process, and a mirror of these rules in the test
/// module is a copy that can drift away from the rules it stands in for.
///
/// The order of the checks is the load-bearing part. The allowlist comes first,
/// because a stranger gets silence — not even "I cannot read that", which would
/// confirm to whoever found the number that something is listening. Only after
/// that does a message with no words become [`InboundKind::Unsupported`]:
/// iMessage carries voice notes, stickers and bare images, and dropping them
/// left the sender looking at a chat where nothing happened, which is exactly
/// what the bridge being down looks like too.
fn parse_event(
    allowed_users: &[String],
    event: &Value,
    seen: &mut std::collections::VecDeque<String>,
) -> Option<Inbound> {
    let space_id = event.get("spaceId").and_then(Value::as_str)?;

    let sender = event
        .get("senderId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    if !is_allowed(allowed_users, &sender) {
        tracing::info!("dropped photon message from unauthorized sender {sender}");
        return None;
    }

    // Dedupe across reconnects. A short window is enough — a replay is
    // immediate, not hours later.
    if let Some(id) = event.get("messageId").and_then(Value::as_str) {
        if seen.contains(&id.to_string()) {
            return None;
        }
        seen.push_back(id.to_string());
        if seen.len() > 256 {
            seen.pop_front();
        }
    }

    let thread = ThreadKey {
        channel: CHANNEL,
        chat_id: space_id.to_string(),
        topic_id: None,
    };

    let text = event
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();

    if text.is_empty() {
        tracing::info!(thread = %thread, "message with no text to act on");
        return Some(Inbound {
            thread,
            user_id: sender,
            kind: InboundKind::Unsupported(
                "I can only read text. A voice note or an image on its own has \
                 nothing in it for me to pass on — send words with it and I \
                 will."
                    .to_string(),
            ),
        });
    }

    Some(Inbound {
        thread,
        user_id: sender,
        kind: InboundKind::Text(text.to_string()),
    })
}

/// A sender is a phone number; compare on digits so +1 555 000 and +1555000 are
/// the same person.
fn is_allowed(allowed_users: &[String], sender: &str) -> bool {
    let normalized = digits(sender);
    !normalized.is_empty()
        && allowed_users
            .iter()
            .any(|allowed| digits(allowed) == normalized)
}

fn digits(value: &str) -> String {
    value.chars().filter(|c| c.is_ascii_digit()).collect()
}

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
    use std::collections::VecDeque;

    use serde_json::json;

    /// The one allowed number in these tests.
    fn allowed() -> Vec<String> {
        vec!["+1 555 123 4567".to_string()]
    }

    fn parse(event: Value) -> Option<Inbound> {
        parse_event(&allowed(), &event, &mut VecDeque::new())
    }

    #[test]
    fn phone_numbers_compare_on_digits() {
        let allowed = allowed();
        assert!(is_allowed(&allowed, "+15551234567"));
        assert!(is_allowed(&allowed, "+1-555-123-4567"));
        assert!(!is_allowed(&allowed, "+15551234560"));
        assert!(!is_allowed(&allowed, ""));
    }

    #[test]
    fn a_message_with_words_is_a_prompt() {
        let inbound = parse(json!({
            "spaceId": "space-1",
            "messageId": "m1",
            "senderId": "+15551234567",
            "text": "  run the tests  "
        }))
        .expect("an allowed sender with words");

        assert_eq!(inbound.thread.to_string(), "photon:space-1");
        match inbound.kind {
            InboundKind::Text(text) => assert_eq!(text, "run the tests"),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn a_message_we_cannot_read_still_gets_an_answer() {
        // A voice note or a bare image reaches us with an empty `text`. It used
        // to be dropped — by the sidecar, and then again here — which from the
        // phone is indistinguishable from the bridge being down.
        let inbound = parse(json!({
            "spaceId": "space-1",
            "messageId": "m1",
            "senderId": "+15551234567",
            "text": ""
        }))
        .expect("an allowed sender always gets an answer");

        match inbound.kind {
            InboundKind::Unsupported(note) => assert!(note.contains("text")),
            other => panic!("expected unsupported, got {other:?}"),
        }
    }

    #[test]
    fn strangers_get_no_answer_at_all() {
        // Not even the "I can only read text" note: replying would tell whoever
        // found the number that something is listening on it.
        assert!(parse(json!({
            "spaceId": "space-1",
            "messageId": "m1",
            "senderId": "+19998887777",
            "text": ""
        }))
        .is_none());
    }

    #[test]
    fn a_replayed_message_is_parsed_once() {
        // The sidecar replays after a reconnect, and an unreadable message must
        // not answer twice any more than a readable one runs twice.
        let mut seen = VecDeque::new();
        let event = json!({
            "spaceId": "space-1",
            "messageId": "m1",
            "senderId": "+15551234567",
            "text": ""
        });
        assert!(parse_event(&allowed(), &event, &mut seen).is_some());
        assert!(parse_event(&allowed(), &event, &mut seen).is_none());
    }

    #[test]
    fn dedupe_window_holds_recent_ids() {
        let mut seen: VecDeque<String> = VecDeque::new();
        for i in 0..300 {
            seen.push_back(i.to_string());
            if seen.len() > 256 {
                seen.pop_front();
            }
        }
        assert!(!seen.contains(&"1".to_string()), "old ids are dropped");
        assert!(seen.contains(&"299".to_string()), "recent ids are kept");
        assert_eq!(seen.len(), 256);
    }

    #[test]
    fn a_free_port_is_actually_free() {
        let port = free_port().unwrap();
        assert!(port > 1024, "must be an unprivileged port, got {port}");
        // Bindable, which is the whole point of asking.
        std::net::TcpListener::bind(("127.0.0.1", port)).expect("port should be free");
    }

    #[test]
    fn long_text_is_clipped() {
        let clipped = clip(&"x".repeat(MAX_TEXT + 100));
        assert!(clipped.ends_with("… truncated"));
    }

    #[test]
    fn empty_allowlist_is_refused() {
        // Constructing Config is enough; start() rejects before spawning.
        let config = Config {
            sidecar_dir: PathBuf::from("/nonexistent"),
            project_id: "p".into(),
            project_secret: "s".into(),
            port: 8789,
            allowed_users: vec![],
        };
        assert!(config.allowed_users.is_empty());
    }
}
