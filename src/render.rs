//! Turns a stream of agent events into one chat message, edited in place.
//!
//! Telegram allows roughly one message per second to a chat and counts edits
//! against the same budget, so streaming cannot be token-by-token. Instead the
//! renderer accumulates events and the core flushes it on a timer, editing a
//! single message rather than posting a wall of fragments.
//!
//! The debounce lives here rather than in the channel adapter because every
//! channel needs the same discipline for its own reasons.

use crate::event::AgentEvent;

/// One piece of the reply, kept in the order it happened so tool calls appear
/// between the paragraphs they belong to rather than collected at the end.
#[derive(Debug, Clone, PartialEq)]
enum Segment {
    Text(String),
    Tool { name: String, summary: String },
}

#[derive(Debug, Default)]
pub struct TurnRenderer {
    segments: Vec<Segment>,
    /// What was last actually sent, so an unchanged turn costs no API call.
    ///
    /// Which *message* that went into is the outbox's business, not this
    /// type's: the core never waits for a send, so it never learns an id.
    sent: String,
    thinking: bool,
    finished: bool,
}

impl TurnRenderer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one agent event into the message being built.
    pub fn apply(&mut self, event: &AgentEvent) {
        match event {
            AgentEvent::TextDelta { text } => {
                self.thinking = false;
                match self.segments.last_mut() {
                    Some(Segment::Text(existing)) => existing.push_str(text),
                    _ => self.segments.push(Segment::Text(text.clone())),
                }
            }

            // The complete block repeats what the deltas already delivered.
            // Only use it if no deltas arrived for this segment.
            AgentEvent::Text { text } => {
                self.thinking = false;
                if !matches!(self.segments.last(), Some(Segment::Text(_))) {
                    self.segments.push(Segment::Text(text.clone()));
                }
            }

            AgentEvent::Thinking { .. } => self.thinking = true,

            AgentEvent::ToolCall { name, input, .. } => {
                self.thinking = false;
                self.segments.push(Segment::Tool {
                    name: name.clone(),
                    summary: summarize(input),
                });
            }

            AgentEvent::TurnEnd { ok, detail } => {
                self.finished = true;
                if !ok {
                    self.segments.push(Segment::Text(format!(
                        "\n\n[turn failed: {}]",
                        detail.as_deref().unwrap_or("unknown")
                    )));
                }
            }

            AgentEvent::Error { message } => {
                self.segments
                    .push(Segment::Text(format!("\n\n[error: {message}]")));
            }

            // Handled by the core, or deliberately not shown.
            AgentEvent::Ready { .. }
            | AgentEvent::PermissionRequest { .. }
            | AgentEvent::RateLimit { .. }
            | AgentEvent::Unknown { .. } => {}
        }
    }

    /// The message as it should currently read.
    pub fn compose(&self) -> String {
        let mut out = String::new();

        for segment in &self.segments {
            match segment {
                Segment::Text(text) => out.push_str(text),
                Segment::Tool { name, summary } if summary.is_empty() => {
                    push_line(&mut out, &format!("▸ {name}"));
                }
                Segment::Tool { name, summary } => {
                    push_line(&mut out, &format!("▸ {name}  {summary}"));
                }
            }
        }

        let out = out.trim().to_string();

        if out.is_empty() {
            return if self.thinking {
                "thinking…".to_string()
            } else {
                "working…".to_string()
            };
        }

        if self.thinking && !self.finished {
            return format!("{out}\n\nthinking…");
        }
        out
    }

    /// The text to send now, or `None` when nothing changed since last time.
    ///
    /// Peeks rather than consumes, because handing the text to the outbox can
    /// fail: the two phases mean a message that was not accepted for delivery
    /// is still pending on the next flush instead of quietly vanishing.
    pub fn pending(&self) -> Option<String> {
        let next = self.compose();
        (next != self.sent).then_some(next)
    }

    /// Record that [`Self::pending`]'s text is on its way.
    pub fn mark_sent(&mut self, text: String) {
        self.sent = text;
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Begin a new turn, keeping nothing from the last one.
    pub fn reset(&mut self) {
        *self = Self::new();
    }
}

fn push_line(out: &mut String, line: &str) {
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(line);
    out.push('\n');
}

/// A one-line gist of a tool's arguments — the command for Bash, the path for
/// a file tool, nothing for anything we do not recognize.
///
/// Also used as the headline of a permission question, where the full input is
/// too long to show: see `Core::permission_question`.
pub fn summarize(input: &serde_json::Value) -> String {
    for key in ["command", "file_path", "path", "pattern", "url"] {
        if let Some(value) = input.get(key).and_then(|v| v.as_str()) {
            let one_line = value.replace('\n', " ");
            return if one_line.chars().count() > 80 {
                format!("{}…", one_line.chars().take(80).collect::<String>())
            } else {
                one_line
            };
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn delta(text: &str) -> AgentEvent {
        AgentEvent::TextDelta {
            text: text.to_string(),
        }
    }

    #[test]
    fn deltas_accumulate_into_one_segment() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("Hello"));
        r.apply(&delta(" world"));
        assert_eq!(r.compose(), "Hello world");
    }

    #[test]
    fn complete_text_does_not_duplicate_streamed_deltas() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("Hello world"));
        r.apply(&AgentEvent::Text {
            text: "Hello world".into(),
        });
        assert_eq!(r.compose(), "Hello world");
    }

    #[test]
    fn complete_text_is_used_when_no_deltas_arrived() {
        let mut r = TurnRenderer::new();
        r.apply(&AgentEvent::Text {
            text: "no streaming here".into(),
        });
        assert_eq!(r.compose(), "no streaming here");
    }

    #[test]
    fn tools_appear_in_order_between_text() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("checking"));
        r.apply(&AgentEvent::ToolCall {
            id: "1".into(),
            name: "Bash".into(),
            input: json!({ "command": "ls" }),
        });
        r.apply(&delta("done"));

        let out = r.compose();
        assert!(out.starts_with("checking"));
        assert!(out.contains("▸ Bash  ls"));
        assert!(out.ends_with("done"));
    }

    #[test]
    fn unchanged_content_produces_no_second_flush() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("hi"));
        let text = r.pending().expect("first flush");
        assert_eq!(text, "hi");
        r.mark_sent(text);
        assert_eq!(r.pending(), None, "no edit when nothing changed");

        r.apply(&delta(" there"));
        assert_eq!(r.pending().as_deref(), Some("hi there"));
    }

    #[test]
    fn text_not_accepted_for_delivery_is_still_pending() {
        // The outbox can refuse a job when a thread's channel is backed up.
        // Peeking rather than consuming is what keeps that from silently
        // eating the turn: nothing is marked sent until it has been handed over.
        let mut r = TurnRenderer::new();
        r.apply(&delta("important"));

        assert_eq!(r.pending().as_deref(), Some("important"));
        // ... delivery refused, so no mark_sent ...
        assert_eq!(r.pending().as_deref(), Some("important"), "still owed");

        r.mark_sent("important".to_string());
        assert_eq!(r.pending(), None);
    }

    #[test]
    fn empty_turn_still_says_something() {
        let mut r = TurnRenderer::new();
        assert_eq!(r.compose(), "working…");
        r.apply(&AgentEvent::Thinking { text: String::new() });
        assert_eq!(r.compose(), "thinking…");
    }

    #[test]
    fn failed_turn_is_reported_in_the_message() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("partial"));
        r.apply(&AgentEvent::TurnEnd {
            ok: false,
            detail: Some("error_max_turns".into()),
        });
        assert!(r.compose().contains("error_max_turns"));
        assert!(r.is_finished());
    }

    #[test]
    fn long_tool_arguments_are_summarized() {
        let mut r = TurnRenderer::new();
        r.apply(&AgentEvent::ToolCall {
            id: "1".into(),
            name: "Bash".into(),
            input: json!({ "command": "x".repeat(200) }),
        });
        assert!(r.compose().contains('…'));
        assert!(r.compose().len() < 150);
    }

    #[test]
    fn reset_clears_the_turn() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("old"));
        r.mark_sent("old".to_string());
        r.reset();
        assert_eq!(r.compose(), "working…");
        assert_eq!(r.pending().as_deref(), Some("working…"), "a fresh turn");
    }
}
