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
    /// Where the message this turn is growing begins, once the messages before
    /// it have filled up. See [`Self::paginate`].
    page: PageStart,
    thinking: bool,
    finished: bool,
}

/// Where the message a turn is currently growing begins.
///
/// A turn too long for one chat message carries on in the next, and every
/// message before that one is finished. This is the boundary: a byte offset
/// into [`TurnRenderer::compose`], plus the code fence the last page was cut
/// inside, when it was — the next page has to reopen it, or the rest of the
/// block renders as prose.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct PageStart {
    at: usize,
    reopen: Option<String>,
}

/// A turn cut into chat messages that each fit.
#[derive(Debug, PartialEq)]
pub struct Pages {
    /// Messages that are full, in order: each one's final text, and where the
    /// page after it begins.
    pub sealed: Vec<(String, PageStart)>,
    /// The message still growing.
    pub current: String,
}

/// What closes a fence a page was cut inside.
const FENCE_CLOSE: &str = "\n```";

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
        self.composed().0
    }

    /// The turn cut into messages of at most `budget` characters, starting
    /// from the page the last flush left off in.
    ///
    /// A page is only ever sealed where the text in front of the cut can no
    /// longer change. Prose grows at the end and nowhere else, but the newest
    /// tool line is rewritten with every call in its run, and a message that has
    /// been left behind is never edited again — a count sealed mid-run would sit
    /// there wrong for good.
    pub fn paginate(&self, budget: usize) -> Pages {
        let (text, stable) = self.composed();
        paginate(&text, stable, &self.page, budget)
    }

    /// Record that the pages before `start` are on their way and finished.
    pub fn begin_page(&mut self, start: PageStart) {
        self.page = start;
    }

    /// [`Self::compose`], and how many bytes at the front of it no later event
    /// can change.
    fn composed(&self) -> (String, usize) {
        let mut out = String::new();
        // Where the newest tool line begins, when it is the last thing in the
        // turn: the one part of the text that is rewritten rather than grown.
        let mut settled = None;

        for (i, segment) in self.segments.iter().enumerate() {
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
                    // The start of the line itself, past any newline pushed to
                    // separate it: a cut right in front of it is a safe one.
                    if i + 1 == self.segments.len() {
                        settled = Some(out.len() - line.len() - 1);
                    }
                }
            }
        }

        let lead = out.len() - out.trim_start().len();
        let body = out.trim();

        if body.is_empty() {
            let placeholder = if self.thinking {
                "thinking…"
            } else {
                "working…"
            };
            return (placeholder.to_string(), 0);
        }

        // Trimming only ever takes whitespace off the end, which the next word
        // puts back, so the whole trimmed body is as settled as the segments.
        let stable = settled.map_or(body.len(), |at| at.saturating_sub(lead).min(body.len()));

        if self.thinking && !self.finished {
            return (format!("{body}\n\nthinking…"), stable);
        }
        (body.to_string(), stable)
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

/// Cut `text` into pages of at most `budget` characters, the first beginning
/// at `start`, cutting only inside the first `stable` bytes.
///
/// The last page can come back over budget when the only place to cut is in
/// the part that can still change; the next flush, once it has settled, cuts
/// it then.
fn paginate(text: &str, stable: usize, start: &PageStart, budget: usize) -> Pages {
    let mut start = start.clone();
    let mut sealed = Vec::new();
    loop {
        let current = page_text(text, &start);
        if current.chars().count() <= budget {
            return Pages { sealed, current };
        }
        match page_break(text, stable, &start, budget) {
            Some((page, next)) => {
                sealed.push((page, next.clone()));
                start = next;
            }
            None => return Pages { sealed, current },
        }
    }
}

/// Everything from `start` on, as one message.
fn page_text(text: &str, start: &PageStart) -> String {
    let body = text.get(start.at..).unwrap_or(text);
    match &start.reopen {
        Some(fence) => format!("{fence}\n{body}"),
        None => body.to_string(),
    }
}

/// Where to end the page that begins at `start`: its final text, and where the
/// next page begins. `None` when nowhere settled will do.
///
/// Preference, among cuts that leave the page at least half full: a paragraph
/// break, then a line break, both outside a code block; then a line break
/// inside one, closing the fence here and reopening it on the next page. Below
/// half full the order stops mattering — a page of two lines followed by a
/// page of forty is worse than a code block that continues overleaf. Only when
/// a single line is longer than a page is a line cut, at a space if there is
/// one in its second half.
fn page_break(
    text: &str,
    stable: usize,
    start: &PageStart,
    budget: usize,
) -> Option<(String, PageStart)> {
    struct Cut {
        at: usize,
        fence: Option<String>,
        rank: u8,
        cost: usize,
    }

    let body = text.get(start.at..)?;
    let limit = stable.checked_sub(start.at)?;
    let opener = start.reopen.as_ref().map_or(0, |f| f.chars().count() + 1);

    let mut cuts = Vec::new();
    let mut fence = start.reopen.clone();
    let mut offset = 0;
    // Characters of `body[..offset]`, and of it with trailing whitespace off.
    let mut chars = 0;
    let mut kept = 0;
    let mut blank_before = false;

    for line in body.split_inclusive('\n') {
        if offset > limit {
            break;
        }
        let blank = line.trim().is_empty();
        let is_fence = is_fence(line);

        // Never between a fence's last line and its closing one: that page
        // would end on a block reopened only to be closed again.
        if offset > 0 && !blank && kept > 0 && !(fence.is_some() && is_fence) {
            let close = if fence.is_some() {
                FENCE_CLOSE.len()
            } else {
                0
            };
            let cost = opener + kept + close;
            if cost > budget {
                break;
            }
            let rank = match (&fence, blank_before) {
                (None, true) => 2,
                (None, false) => 1,
                (Some(_), _) => 0,
            };
            cuts.push(Cut {
                at: offset,
                fence: fence.clone(),
                rank,
                cost,
            });
        }

        if is_fence {
            fence = match fence {
                Some(_) => None,
                None => Some(line.trim().to_string()),
            };
        }
        if !blank {
            kept = chars + line.trim_end().chars().count();
        }
        chars += line.chars().count();
        blank_before = blank;
        offset += line.len();
    }

    let cut = cuts
        .iter()
        .filter(|cut| cut.cost >= budget / 2)
        .max_by_key(|cut| (cut.rank, cut.at))
        .or_else(|| cuts.last());

    let (at, fence) = match cut {
        Some(cut) => (cut.at, cut.fence.clone()),
        None => {
            // One line longer than a whole page, at the top of this one.
            let close = if start.reopen.is_some() {
                FENCE_CLOSE.len()
            } else {
                0
            };
            let room = budget.checked_sub(opener + close)?;
            let line_end = body.find('\n').unwrap_or(body.len()).min(limit);
            let hard = body[..line_end].char_indices().nth(room)?.0;
            let at = body[..hard]
                .rfind(' ')
                .filter(|space| *space >= hard / 2)
                .map_or(hard, |space| space + 1);
            (at, start.reopen.clone())
        }
    };

    let kept = body[..at].trim_end();
    if kept.is_empty() {
        return None;
    }
    let mut page = String::new();
    if let Some(reopen) = &start.reopen {
        page.push_str(reopen);
        page.push('\n');
    }
    page.push_str(kept);
    if fence.is_some() {
        page.push_str(FENCE_CLOSE);
    }

    Some((
        page,
        PageStart {
            at: start.at + at,
            reopen: fence,
        },
    ))
}

/// A line that opens or closes a code block — the rule `channel::markup` reads
/// them by.
fn is_fence(line: &str) -> bool {
    line.trim_start().starts_with("```")
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

    fn fences(page: &str) -> usize {
        page.lines().filter(|line| is_fence(line)).count()
    }

    /// Pages until nothing more can be sealed, the way successive flushes would.
    fn seal_all(r: &mut TurnRenderer, budget: usize) -> Vec<String> {
        let pages = r.paginate(budget);
        let mut sealed = Vec::new();
        for (page, next) in pages.sealed {
            sealed.push(page);
            r.begin_page(next);
        }
        sealed
    }

    #[test]
    fn a_turn_that_fits_is_one_page() {
        let mut r = TurnRenderer::new();
        r.apply(&delta("short"));
        assert_eq!(
            r.paginate(100),
            Pages {
                sealed: Vec::new(),
                current: "short".into()
            }
        );
    }

    #[test]
    fn a_long_turn_breaks_between_paragraphs_and_loses_nothing() {
        let paragraphs: Vec<String> = (0..12)
            .map(|i| format!("Paragraph {i} {}", "word ".repeat(15).trim_end()))
            .collect();
        let mut r = TurnRenderer::new();
        r.apply(&delta(&paragraphs.join("\n\n")));

        let pages = r.paginate(300);
        assert!(pages.sealed.len() >= 2, "{pages:?}");
        let mut all: Vec<&str> = pages.sealed.iter().map(|(p, _)| p.as_str()).collect();
        all.push(&pages.current);

        for page in &all {
            assert!(page.chars().count() <= 300, "over budget: {page:?}");
            assert!(!page.starts_with('\n') && !page.ends_with('\n'), "{page:?}");
        }
        // Every paragraph whole, on exactly one page.
        for paragraph in &paragraphs {
            assert_eq!(
                all.iter()
                    .filter(|page| page.contains(paragraph.as_str()))
                    .count(),
                1,
                "{paragraph:?} in {all:?}"
            );
        }
    }

    #[test]
    fn a_code_block_cut_across_pages_is_closed_and_reopened() {
        // Left open, the rest of the block on the next page renders as prose —
        // and on Telegram `*` and `_` in it become emphasis and vanish.
        let code: String = (0..60).map(|i| format!("let x{i} = *p_{i};\n")).collect();
        let mut r = TurnRenderer::new();
        r.apply(&delta(&format!("Here:\n\n```rust\n{code}```\n\nDone.")));

        let pages = r.paginate(400);
        assert!(!pages.sealed.is_empty());
        let mut all: Vec<String> = pages.sealed.iter().map(|(p, _)| p.clone()).collect();
        all.push(pages.current.clone());

        for page in &all[1..all.len() - 1] {
            assert!(
                page.starts_with("```rust\n"),
                "reopened with its language: {page:?}"
            );
        }
        for page in &all {
            assert!(page.chars().count() <= 400, "over budget: {page:?}");
            assert_eq!(fences(page) % 2, 0, "balanced fences: {page:?}");
        }
        for i in 0..60 {
            let line = format!("let x{i} = *p_{i};");
            assert_eq!(
                all.iter().filter(|p| p.contains(&line)).count(),
                1,
                "{line}"
            );
        }
    }

    #[test]
    fn a_line_longer_than_a_page_is_still_cut_to_fit() {
        let mut r = TurnRenderer::new();
        r.apply(&delta(&"token ".repeat(200)));

        let pages = r.paginate(250);
        assert!(!pages.sealed.is_empty());
        for (page, _) in &pages.sealed {
            assert!(page.chars().count() <= 250, "{page:?}");
        }
        assert!(pages.current.chars().count() <= 250);
        let rejoined: usize = pages
            .sealed
            .iter()
            .map(|(p, _)| p.matches("token").count())
            .sum::<usize>()
            + pages.current.matches("token").count();
        assert_eq!(rejoined, 200, "no word lost at a cut");
    }

    #[test]
    fn a_sealed_page_is_never_contradicted_by_what_the_turn_says_later() {
        // A message left behind is never edited again, so whatever it says has
        // to still be true once the turn is over. The tool line is the part
        // that moves: sealed with "2 tools" in it, it would say 2 for good.
        // Across a spread of budgets, because where the cuts fall depends on
        // it, and a single one can happen never to put a cut anywhere risky.
        for budget in (250..=450).step_by(10) {
            let mut r = TurnRenderer::new();
            let mut sealed = Vec::new();

            for round in 0..8 {
                r.apply(&delta(&format!(
                    "\n\nRound {round}: {}",
                    "some findings ".repeat(12)
                )));
                for call in 0..5 {
                    r.apply(&tool("Bash", &format!("step {round}.{call}")));
                    // "thinking…" under the run is a line start after it — the
                    // one place a careless cut would take the run's line along.
                    r.apply(&AgentEvent::Thinking {
                        text: String::new(),
                    });
                    sealed.extend(seal_all(&mut r, budget));
                }
            }
            r.apply(&AgentEvent::TurnEnd {
                ok: true,
                detail: None,
            });
            sealed.extend(seal_all(&mut r, budget));
            let last = r.paginate(budget).current;

            assert!(sealed.len() >= 2, "budget {budget}: {sealed:?}");
            for page in &sealed {
                for line in page.lines().filter(|l| l.starts_with('▸')) {
                    assert!(
                        line.contains("5 tools"),
                        "budget {budget}: a run sealed half-counted: {page:?}"
                    );
                }
            }
            let mut rejoined = sealed.join("\n\n");
            rejoined.push_str("\n\n");
            rejoined.push_str(&last);
            for round in 0..8 {
                assert!(
                    rejoined.contains(&format!("Round {round}:")),
                    "budget {budget}"
                );
            }
            assert_eq!(
                rejoined.matches('▸').count(),
                8,
                "budget {budget}: each run exactly once"
            );
        }
    }

    #[test]
    fn a_tool_line_still_counting_stays_on_the_page_that_can_still_change() {
        // 95 + 1 + "▸ Bash  `ls`" (12) + "\n\nthinking…" (11) = 119. Under a
        // budget of 115 the best-looking cut is the paragraph break before
        // "thinking…" — which would seal the tool line while its run is open.
        let mut r = TurnRenderer::new();
        r.apply(&delta(&"x".repeat(95)));
        r.apply(&tool("Bash", "ls"));
        r.apply(&AgentEvent::Thinking {
            text: String::new(),
        });

        let pages = r.paginate(115);
        assert_eq!(pages.sealed.len(), 1, "{pages:?}");
        assert!(!pages.sealed[0].0.contains('▸'), "{pages:?}");
        assert!(pages.current.starts_with("▸ Bash"), "{pages:?}");

        // Which is what lets the run go on counting where it is shown.
        r.begin_page(pages.sealed[0].1.clone());
        r.apply(&tool("Bash", "pwd"));
        assert!(r.paginate(115).current.starts_with("▸ 2 tools"));
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
