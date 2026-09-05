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

use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::{Channel, Inbound, InboundKind, MessageId, ThreadKey};

pub const CHANNEL: &str = "telegram";

/// Telegram rejects messages over 4096 characters. Leave room for the marker.
const MAX_TEXT: usize = 3900;

/// Server-side long-poll window. The request blocks here rather than us
/// spinning; well under any sane proxy timeout.
const POLL_TIMEOUT_SECS: u64 = 25;

/// Callback payloads. At most one permission is outstanding per thread, so
/// these need carry no request id — which also keeps them inside Telegram's
/// 64-byte callback_data limit without any encoding scheme.
const CB_ALLOW: &str = "allow";
const CB_DENY: &str = "deny";

pub struct Telegram {
    http: reqwest::Client,
    base: String,
    /// User ids permitted to talk to this bot. A bot token in a chat is a shell
    /// on this machine, so anyone else is dropped before the core sees them.
    allowed_users: Vec<String>,
}

impl Telegram {
    pub fn new(token: &str, allowed_users: Vec<String>) -> Result<Self> {
        if allowed_users.is_empty() {
            bail!("refusing to start with an empty allowlist — set SWITCHBOARD_TELEGRAM_ALLOWED_USERS");
        }

        let http = reqwest::Client::builder()
            // Comfortably longer than the long-poll window.
            .timeout(Duration::from_secs(POLL_TIMEOUT_SECS + 15))
            .build()?;

        Ok(Self {
            http,
            base: format!("https://api.telegram.org/bot{token}"),
            allowed_users,
        })
    }

    async fn call(&self, method: &str, body: Value) -> Result<Value> {
        let response: Value = self
            .http
            .post(format!("{}/{}", self.base, method))
            .json(&body)
            .send()
            .await
            .with_context(|| format!("telegram {method}"))?
            .json()
            .await
            .with_context(|| format!("decoding telegram {method}"))?;

        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            let retry_after = response
                .get("parameters")
                .and_then(|p| p.get("retry_after"))
                .and_then(Value::as_u64);

            // 429 is expected traffic on a streaming bridge, not an anomaly —
            // wait exactly as long as told, then let the caller try again.
            if let Some(seconds) = retry_after {
                tracing::warn!("telegram rate limited on {method}, waiting {seconds}s");
                tokio::time::sleep(Duration::from_secs(seconds + 1)).await;
                bail!("rate limited on {method}, retry");
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

    /// Confirm the token works, and report who we are.
    pub async fn whoami(&self) -> Result<String> {
        let me = self.call("getMe", json!({})).await?;
        Ok(me
            .get("username")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string())
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

            loop {
                let body = json!({
                    "offset": offset,
                    "timeout": POLL_TIMEOUT_SECS,
                    "allowed_updates": ["message", "callback_query"],
                });

                let updates = match self.call("getUpdates", body).await {
                    Ok(Value::Array(updates)) => updates,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!("getUpdates failed: {e}");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                };

                for update in updates {
                    if let Some(id) = update.get("update_id").and_then(Value::as_i64) {
                        offset = offset.max(id + 1);
                    }

                    match self.parse_update(&update) {
                        Some(inbound) => {
                            if tx.send(inbound).await.is_err() {
                                return;
                            }
                        }
                        None => continue,
                    }
                }
            }
        });

        rx
    }

    /// Turn one update into an [`Inbound`], dropping anything unauthorized or
    /// uninteresting.
    fn parse_update(&self, update: &Value) -> Option<Inbound> {
        if let Some(callback) = update.get("callback_query") {
            let user_id = user_id(callback.get("from"))?;
            if !self.is_allowed(&user_id) {
                return None;
            }

            let data = callback.get("data").and_then(Value::as_str)?;
            let allow = match data {
                CB_ALLOW => true,
                CB_DENY => false,
                _ => return None,
            };

            return Some(Inbound {
                thread: thread_key(callback.get("message")?)?,
                user_id,
                kind: InboundKind::Decision {
                    allow,
                    token: callback.get("id").and_then(Value::as_str)?.to_string(),
                },
            });
        }

        let message = update.get("message")?;
        let user_id = user_id(message.get("from"))?;
        if !self.is_allowed(&user_id) {
            // Silent: replying would confirm the bot exists to whoever found it.
            tracing::info!("dropped message from unauthorized user {user_id}");
            return None;
        }

        let text = message.get("text").and_then(Value::as_str)?.trim();
        if text.is_empty() {
            return None;
        }

        Some(Inbound {
            thread: thread_key(message)?,
            user_id,
            kind: InboundKind::Text(text.to_string()),
        })
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

    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        let mut body = self.target(thread);
        body["text"] = json!(clip(text));

        let result = self.call("sendMessage", body).await?;
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
            "text": clip(text),
        });

        match self.call("editMessageText", body).await {
            Ok(_) => Ok(()),
            // Telegram rejects an edit that would not change anything. That is
            // a no-op for us, not a failure.
            Err(e) if e.to_string().contains("message is not modified") => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn ask_permission(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        let mut body = self.target(thread);
        body["text"] = json!(clip(text));
        body["reply_markup"] = json!({
            "inline_keyboard": [[
                { "text": "Allow", "callback_data": CB_ALLOW },
                { "text": "Deny",  "callback_data": CB_DENY  },
            ]]
        });

        let result = self.call("sendMessage", body).await?;
        Ok(result
            .get("message_id")
            .and_then(Value::as_i64)
            .map(|id| id.to_string())
            .unwrap_or_default())
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

fn user_id(from: Option<&Value>) -> Option<String> {
    from?.get("id").and_then(Value::as_i64).map(|i| i.to_string())
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

/// Chat is a bad place for a 500-line diff. Truncate rather than split: a
/// stream of continuation messages is worse to read on a phone than a clipped
/// one, and the full transcript is a `/attach` away.
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
        Telegram::new("test-token", vec!["42".into()]).unwrap()
    }

    #[test]
    fn empty_allowlist_is_refused() {
        assert!(Telegram::new("t", vec![]).is_err());
    }

    #[test]
    fn messages_from_strangers_are_dropped() {
        let update = json!({
            "update_id": 1,
            "message": { "from": { "id": 999 }, "chat": { "id": 5 }, "text": "hi" }
        });
        assert!(telegram().parse_update(&update).is_none());
    }

    #[test]
    fn allowed_message_parses() {
        let update = json!({
            "update_id": 1,
            "message": { "from": { "id": 42 }, "chat": { "id": 5 }, "text": "  hello  " }
        });
        let inbound = telegram().parse_update(&update).unwrap();
        assert_eq!(inbound.thread.to_string(), "telegram:5");
        match inbound.kind {
            InboundKind::Text(t) => assert_eq!(t, "hello"),
            other => panic!("expected text, got {other:?}"),
        }
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
            telegram().parse_update(&reply).unwrap().thread.to_string(),
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
            telegram().parse_update(&topic).unwrap().thread.to_string(),
            "telegram:5:77"
        );
    }

    #[test]
    fn button_taps_parse_as_decisions() {
        let update = json!({
            "update_id": 1,
            "callback_query": {
                "id": "cb-1", "from": { "id": 42 }, "data": "deny",
                "message": { "chat": { "id": 5 } }
            }
        });
        match telegram().parse_update(&update).unwrap().kind {
            InboundKind::Decision { allow, token } => {
                assert!(!allow);
                assert_eq!(token, "cb-1");
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
