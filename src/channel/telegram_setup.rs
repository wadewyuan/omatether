//! One-time Telegram bot setup.
//!
//! The Bot API calls `omatether setup` makes before the channel adapter can
//! run: proving a token is real, and reading the operator's user id off a
//! message they send the bot. Separate from the adapter because this one
//! reports rather than retries, and runs before an allowlist exists.
//!
//! The questions asked around these calls live in `src/setup.rs`; nothing
//! here reads stdin.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

const TELEGRAM_API: &str = "https://api.telegram.org";

/// getMe, the cheapest possible proof that a token is real. Returns the bot's
/// username.
pub async fn get_me(token: &str) -> Result<String> {
    let response = call(token, Method::GetMe, json!({})).await?;
    response
        .get("username")
        .and_then(Value::as_str)
        .map(String::from)
        .context("Telegram's getMe named no username")
}

/// Who sent a private message to the bot.
#[derive(Debug, PartialEq)]
pub struct Sender {
    pub update_id: u64,
    pub id: String,
    pub name: String,
}

/// How waiting for that message ended.
pub enum Wait {
    Found(Sender),
    /// Another poller holds getUpdates on this token — in practice a running
    /// omatether service. Detection cannot work while it is up, and fighting
    /// it for the poll would steal its messages.
    Conflict,
    TimedOut,
}

/// Short-poll for the newest private message, for up to `within`. Does not
/// mark it read — see `acknowledge`, which the caller owes once it has
/// decided what the message was.
pub async fn wait_for_private_sender(token: &str, within: Duration) -> Result<Wait> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match call(
            token,
            Method::GetUpdates,
            json!({ "offset": -1, "timeout": 0 }),
        )
        .await
        {
            Ok(updates) => {
                if let Some(sender) = updates
                    .as_array()
                    .and_then(|all| all.iter().find_map(private_sender))
                {
                    return Ok(Wait::Found(sender));
                }
            }
            Err(e) if e.to_string().contains("Conflict") => return Ok(Wait::Conflict),
            Err(e) => return Err(e).context("asking Telegram for updates"),
        }

        if tokio::time::Instant::now() > deadline {
            return Ok(Wait::TimedOut);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Mark the detection message read, and anything sent after it. getUpdates
/// with offset -1 hands back the newest update *without* confirming it, and
/// the service polls from offset 0 — so left alone, the message sent to be
/// detected is replayed to the agent as a prompt the moment the service
/// starts. An update is confirmed by a call whose offset is past its id; the
/// loop ends on the call that comes back empty, which confirms the rest.
pub async fn acknowledge(token: &str, update_id: u64) -> Result<()> {
    let mut offset = update_id + 1;
    loop {
        let updates = call(
            token,
            Method::GetUpdates,
            json!({ "offset": offset, "timeout": 0 }),
        )
        .await?;
        match updates
            .as_array()
            .and_then(|all| all.last())
            .and_then(|last| last.get("update_id"))
            .and_then(Value::as_u64)
        {
            Some(last) => offset = last + 1,
            None => return Ok(()),
        }
    }
}

/// The sender of a private-chat message in one update, if this update is one.
fn private_sender(update: &Value) -> Option<Sender> {
    let update_id = update.get("update_id")?.as_u64()?;
    let message = update.get("message")?;
    if message.get("chat")?.get("type")?.as_str()? != "private" {
        return None;
    }
    let from = message.get("from")?;
    let id = from.get("id")?.as_u64()?.to_string();
    let name = from
        .get("first_name")
        .and_then(Value::as_str)
        .unwrap_or("someone")
        .to_string();
    Some(Sender {
        update_id,
        id,
        name,
    })
}

/// The Bot API calls made here. Not strings: the wire names are camelCase,
/// and a snake_case guess is a 404 "Not Found" — which rendered as "Telegram
/// refused it" for every token, valid ones included, and called a working
/// saved token dead.
#[derive(Clone, Copy)]
enum Method {
    GetMe,
    GetUpdates,
}

impl Method {
    fn as_str(self) -> &'static str {
        match self {
            Method::GetMe => "getMe",
            Method::GetUpdates => "getUpdates",
        }
    }
}

async fn call(token: &str, method: Method, body: Value) -> Result<Value> {
    let method = method.as_str();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let response: Value = http
        .post(format!("{TELEGRAM_API}/bot{token}/{method}"))
        .json(&body)
        .send()
        .await
        .with_context(|| format!("reaching Telegram for {method}"))?
        .json()
        .await
        .with_context(|| format!("decoding Telegram's {method} reply"))?;

    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        bail!(
            "{}",
            response
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        );
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_names_are_the_bot_api_spelling() {
        // Checked against the live API: getMe answers, get_me is a 404.
        assert_eq!(Method::GetMe.as_str(), "getMe");
        assert_eq!(Method::GetUpdates.as_str(), "getUpdates");
    }

    #[test]
    fn the_sender_comes_from_a_private_message_only() {
        let private = json!({
            "update_id": 1,
            "message": {
                "chat": {"id": 7, "type": "private"},
                "from": {"id": 424242, "first_name": "Ada"},
                "text": "hi"
            }
        });
        // update_id is carried so the message can be marked read — otherwise
        // the service replays it to the agent as a prompt.
        assert_eq!(
            private_sender(&private),
            Some(Sender {
                update_id: 1,
                id: "424242".to_string(),
                name: "Ada".to_string(),
            })
        );

        // A group message must not become the allowlist entry — anyone can
        // add a bot to a group.
        let group = json!({
            "update_id": 2,
            "message": {
                "chat": {"id": -9, "type": "group"},
                "from": {"id": 424242, "first_name": "Ada"},
                "text": "hi"
            }
        });
        assert_eq!(private_sender(&group), None);
    }
}
