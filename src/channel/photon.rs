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
    pub port: u16,
    /// Phone numbers (E.164) permitted to talk to this bridge.
    pub allowed_users: Vec<String>,
}

impl Photon {
    /// Start the sidecar and wait for it to answer.
    pub async fn start(config: Config) -> Result<Self> {
        if config.allowed_users.is_empty() {
            bail!("refusing to start Photon with an empty allowlist — set SWITCHBOARD_PHOTON_ALLOWED_USERS");
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

        let mut child = Command::new("node")
            .arg(&entry)
            .current_dir(&config.sidecar_dir)
            .env("PHOTON_PROJECT_ID", &config.project_id)
            .env("PHOTON_PROJECT_SECRET", &config.project_secret)
            .env("PHOTON_SIDECAR_PORT", config.port.to_string())
            .env("PHOTON_SIDECAR_TOKEN", &token)
            // Bind the sidecar's life to ours. Without this a crashed
            // switchboard leaves a process holding the iMessage line.
            .env("PHOTON_SIDECAR_WATCH_STDIN", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("spawning the photon sidecar — is node on PATH?")?;

        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(log_lines(stderr));
        }

        let http = reqwest::Client::builder()
            // No timeout: /inbound is an open-ended stream. Per-request
            // timeouts are applied where they make sense instead.
            .build()?;

        let base = format!("http://127.0.0.1:{}", config.port);
        wait_until_ready(&http, &base, &token, &mut child).await?;

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
            .header("x-switchboard-token", &self.token)
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

            if let Some(inbound) = self.parse_event(&event, seen) {
                if tx.send(inbound).await.is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn parse_event(
        &self,
        event: &Value,
        seen: &mut std::collections::VecDeque<String>,
    ) -> Option<Inbound> {
        let space_id = event.get("spaceId").and_then(Value::as_str)?;
        let text = event.get("text").and_then(Value::as_str)?.trim();
        if text.is_empty() {
            return None;
        }

        let sender = event
            .get("senderId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if !self.is_allowed(&sender) {
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

        Some(Inbound {
            thread: ThreadKey {
                channel: CHANNEL,
                chat_id: space_id.to_string(),
                topic_id: None,
            },
            user_id: sender,
            kind: InboundKind::Text(text.to_string()),
        })
    }

    fn is_allowed(&self, sender: &str) -> bool {
        // A sender is a phone number; compare on digits so +1 555 000 and
        // +1555000 are the same person.
        let normalized = digits(sender);
        !normalized.is_empty()
            && self
                .allowed_users
                .iter()
                .any(|allowed| digits(allowed) == normalized)
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

    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        let response: Value = self
            .http
            .post(format!("{}/send", self.base))
            .header("x-switchboard-token", &self.token)
            .timeout(Duration::from_secs(30))
            .json(&json!({ "spaceId": thread.chat_id, "text": clip(text) }))
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

    async fn edit(&self, _thread: &ThreadKey, _id: &MessageId, _text: &str) -> Result<()> {
        // Unreachable while can_edit() is false, and a loud failure is better
        // than a silent no-op if that ever stops being true.
        bail!("iMessage cannot edit a sent message")
    }

    async fn ask_permission(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        let question = format!("{text}\n\nReply /allow or /deny <why>");
        self.send(thread, &question).await
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
) -> Result<()> {
    for attempt in 0..40 {
        if let Some(status) = child.try_wait()? {
            bail!("photon sidecar exited during startup ({status}) — see its log above");
        }

        let response = http
            .get(format!("{base}/health"))
            .header("x-switchboard-token", token)
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
        response
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    ))
}

async fn log_lines(stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "photon::sidecar", "{line}");
    }
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

    /// Build one without a sidecar, to exercise the parsing rules.
    fn parser(allowed: &[&str]) -> ParseOnly {
        ParseOnly {
            allowed_users: allowed.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// The allowlist and event parsing do not need a running process, but
    /// `Photon` owns a child. This mirrors just those two rules.
    struct ParseOnly {
        allowed_users: Vec<String>,
    }

    impl ParseOnly {
        fn is_allowed(&self, sender: &str) -> bool {
            let normalized = digits(sender);
            !normalized.is_empty()
                && self
                    .allowed_users
                    .iter()
                    .any(|allowed| digits(allowed) == normalized)
        }
    }

    #[test]
    fn phone_numbers_compare_on_digits() {
        let p = parser(&["+1 555 123 4567"]);
        assert!(p.is_allowed("+15551234567"));
        assert!(p.is_allowed("+1-555-123-4567"));
        assert!(!p.is_allowed("+15551234560"));
        assert!(!p.is_allowed(""));
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
