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
    /// A *run* of tool calls with no prose between them, not a single call.
    ///
    /// A turn's worth of one-line-per-call is most of its length and almost
    /// none of its meaning — read from a phone it buries the answer, and on a
    /// channel that cannot edit, where the whole turn lands at once at the end,
    /// it buried it past the length limit. So a run collapses to one line: how
    /// many, and what the most recent one was, which is the useful half while
    /// the turn is still streaming and a fair summary once it is not.
    Tool {
        name: String,
        summary: String,
        count: usize,
    },
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
                match self.segments.last_mut() {
                    // Still in the same run: keep the count and the newest
                    // call, and drop the one it replaces.
                    Some(Segment::Tool {
                        name: last,
                        summary,
                        count,
                    }) => {
                        *last = name.clone();
                        *summary = summarize(input);
                        *count += 1;
                    }
                    _ => self.segments.push(Segment::Tool {
                        name: name.clone(),
                        summary: summarize(input),
                        count: 1,
                    }),
                }
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
                Segment::Tool {
                    name,
                    summary,
                    count,
                } => {
                    // The name of the newest call when it is the only one, and
                    // the size of the run when it is not: "▸ Bash `ls`" reads
                    // as what just happened, "▸ 9 tools · Bash `ls`" as what
                    // has been happening.
                    let head = if *count == 1 {
                        format!("▸ {name}")
                    } else {
                        format!("▸ {count} tools · {name}")
                    };
                    let line = if summary.is_empty() {
                        head
                    } else {
                        format!("{head}  {}", code_span(summary))
                    };
                    push_line(&mut out, &line);
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

    /// The turn with the tool log left out — what the agent actually said.
    ///
    /// For the one case where the whole turn will not fit in a chat message.
    /// Cutting by position keeps the beginning, and the beginning of a working
    /// turn is its tool log; the answer is at the end, so a length-cut message
    /// delivers the transcript and drops the conclusion. That is not a
    /// truncated reply, it is a missing one. Dropping the log instead cuts the
    /// part that was never the point.
    pub fn compose_prose(&self) -> String {
        let mut out = String::new();
        let mut tools = 0;

        for segment in &self.segments {
            match segment {
                Segment::Text(text) => out.push_str(text),
                Segment::Tool { count, .. } => tools += count,
            }
        }

        let out = out.trim();
        if out.is_empty() {
            return String::new();
        }

        // Say that something was left out. A reply that silently omits every
        // command it ran reads as if it ran none.
        match tools {
            0 => out.to_string(),
            1 => format!("{out}\n\n[1 tool call not shown]"),
            n => format!("{out}\n\n[{n} tool calls not shown]"),
        }
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

    /// Whether any of this turn has reached the chat yet.
    ///
    /// What the working indicator is for: until the first flush lands there is
    /// nothing on screen at all, and on a channel that can edit, the turn's
    /// own growing message takes over from there.
    pub fn has_sent(&self) -> bool {
        !self.sent.is_empty()
    }

    /// Whether the turn has real content to show, as opposed to the temporary
    /// `working…`/`thinking…` placeholder used by non-editing channels.
    pub fn has_content(&self) -> bool {
        !self.segments.is_empty()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Begin a new turn, keeping nothing from the last one.
    pub fn reset(&mut self) {
        *self = Self::new();
    }
}

/// Wrap literal text in a markdown code span, so a channel that renders
/// markdown shows it exactly as it is.
///
/// What comes out of [`summarize`] is a command or a path, not prose, and it
/// travels inside a message the agent wrote in markdown. Left bare it gets
/// parsed with everything else: `/a/_b_/c.rs` arrives as `/a/b/c.rs`, a path
/// that is not the one the tool touched. A code span is the one construct that
/// suppresses all inline parsing.
///
/// The fence has to be longer than any backtick run inside — command
/// substitution puts real backticks in commands — and content that begins or
/// ends with one needs padding, which CommonMark strips back off.
fn code_span(text: &str) -> String {
    let longest_run = text
        .split(|c| c != '`')
        .map(str::len)
        .max()
        .unwrap_or_default();
    let fence = "`".repeat(longest_run + 1);
    let pad = if text.starts_with('`') || text.ends_with('`') {
        " "
    } else {
        ""
    };
    format!("{fence}{pad}{text}{pad}{fence}")
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
        assert!(out.contains("▸ Bash  `ls`"));
        assert!(out.ends_with("done"));
    }

    /// The composed turn is markdown — the agents write markdown, and a channel
    /// that renders it must not silently rewrite a path we quoted.
    #[test]
    fn tool_summaries_survive_a_markdown_renderer() {
        let mut r = TurnRenderer::new();
        r.apply(&AgentEvent::ToolCall {
            id: "1".into(),
            name: "Read".into(),
            input: json!({ "file_path": "/a/_b_/c.rs" }),
        });
        assert!(
            r.compose().contains("`/a/_b_/c.rs`"),
            "underscores would pair into emphasis and vanish: {}",
            r.compose()
        );
    }

    #[test]
    fn a_command_containing_backticks_still_closes_its_span() {
        // Command substitution is not exotic, and a fence the content also
        // contains ends the span early — the rest of the turn then renders as
        // code.
        // Padding is symmetric: CommonMark strips a leading and a trailing
        // space as a pair, so a span ending in a backtick is padded at both
        // ends or the spaces stay in the output.
        assert_eq!(code_span("echo `date`"), "`` echo `date` ``");
        assert_eq!(code_span("`x`"), "`` `x` ``");
        assert_eq!(code_span("a ``b`` c"), "```a ``b`` c```");
        assert_eq!(code_span("plain"), "`plain`");
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
        r.apply(&AgentEvent::Thinking {
            text: String::new(),
        });
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

    fn tool(name: &str, command: &str) -> AgentEvent {
        AgentEvent::ToolCall {
            id: "1".into(),
            name: name.to_string(),
            input: json!({ "command": command }),
        }
    }

    #[test]
    fn a_run_of_tool_calls_is_one_line_not_one_line_each() {
        // Forty tool calls is forty lines of a turn whose answer is one
        // paragraph. On a channel that cannot edit, that pushed the answer past
        // the length limit and it was dropped outright.
        let mut r = TurnRenderer::new();
        r.apply(&delta("looking"));
        for i in 0..9 {
            r.apply(&tool("Bash", &format!("cmd{i}")));
        }
        r.apply(&delta("\n\ndone"));

        let out = r.compose();
        assert_eq!(out.lines().filter(|l| l.starts_with('▸')).count(), 1);
        assert!(out.contains("▸ 9 tools · Bash  `cmd8`"), "{out}");
        assert!(out.starts_with("looking"));
        assert!(out.ends_with("done"), "the answer still lands last");
    }

    #[test]
    fn a_lone_tool_call_still_reads_as_itself() {
        // The collapsed form is for runs. One call is not a run, and "1 tools"
        // would be noise where the plain line was already right.
        let mut r = TurnRenderer::new();
        r.apply(&tool("Bash", "ls"));
        assert!(r.compose().contains("▸ Bash  `ls`"), "{}", r.compose());
    }

    #[test]
    fn prose_between_tool_calls_separates_the_runs() {
        // The interleaving is the point of segments: a run belongs to the
        // paragraph that introduced it.
        let mut r = TurnRenderer::new();
        r.apply(&tool("Read", "a"));
        r.apply(&tool("Read", "b"));
        r.apply(&delta("now editing"));
        r.apply(&tool("Edit", "c"));

        let out = r.compose();
        assert!(out.contains("▸ 2 tools · Read"), "{out}");
        assert!(out.contains("▸ Edit"), "{out}");
    }

    #[test]
    fn prose_only_drops_the_log_but_admits_it() {
        // Used when the whole turn will not fit. A reply that silently omits
        // every command it ran reads as if it ran none.
        let mut r = TurnRenderer::new();
        r.apply(&delta("here is what I found"));
        r.apply(&tool("Bash", "ls"));
        r.apply(&tool("Bash", "pwd"));

        let prose = r.compose_prose();
        assert!(prose.starts_with("here is what I found"));
        assert!(!prose.contains('▸'), "the log is gone: {prose}");
        assert!(prose.contains("2 tool calls not shown"), "{prose}");
    }

    #[test]
    fn a_turn_that_is_only_tool_calls_has_no_prose_to_prefer() {
        // The caller uses this to decide; an empty string says "nothing here
        // is better than what you have".
        let mut r = TurnRenderer::new();
        r.apply(&tool("Bash", "ls"));
        assert_eq!(r.compose_prose(), "");
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
