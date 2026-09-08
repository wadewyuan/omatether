//! Markdown, rendered into the small HTML subset Telegram parses.
//!
//! A composed turn is markdown: the agents write it, and the renderer adds a
//! little of its own (a code span around every tool summary, so a path with
//! underscores in it is not quietly rewritten). Telegram renders none of that
//! unless the message carries a `parse_mode`, so it used to arrive as raw
//! `**asterisks**` and visible backticks.
//!
//! **HTML rather than either markdown mode, and that is the whole design.**
//! `MarkdownV2` demands that `.`, `-`, `(`, `!` and eleven more characters be
//! escaped everywhere they are not markup — ordinary prose trips it, and
//! Telegram answers a message it cannot parse with a 400, which on this bridge
//! means a lost reply rather than an ugly one. Legacy `Markdown` is laxer but
//! still fails on the unbalanced `*` that streaming produces constantly, since
//! a turn is flushed mid-sentence. HTML has three characters to escape and no
//! way to be unbalanced, because we emit every tag ourselves.
//!
//! What Telegram accepts is a fixed list — `b i u s code pre a blockquote` and
//! a few more — not a subset of HTML with unknown tags ignored. Everything
//! here stays inside it, and anything not recognized is escaped down to text.

/// Render `md` as Telegram HTML.
///
/// Lossy on purpose: what markdown cannot become one of Telegram's tags (a
/// table, a nested list's structure) is left as legible text rather than
/// dropped.
pub fn to_html(md: &str) -> String {
    let lines: Vec<&str> = md.lines().collect();
    let mut out = String::new();
    let mut i = 0;

    while i < lines.len() {
        if let Some(info) = fence(lines[i]) {
            i += 1;
            let start = i;
            while i < lines.len() && fence(lines[i]).is_none() {
                i += 1;
            }
            let body = lines[start..i].join("\n");
            // Past the closing fence, if there was one. There often is not: a
            // turn is composed and flushed while the agent is still writing, so
            // half a code block is the normal case rather than a malformed
            // message, and it closes itself here.
            if i < lines.len() {
                i += 1;
            }

            out.push_str("<pre>");
            match language(info) {
                Some(lang) => out.push_str(&format!("<code class=\"language-{}\">", escape(lang))),
                None => out.push_str("<code>"),
            }
            out.push_str(&escape(&body));
            out.push_str("</code></pre>\n");
            continue;
        }

        out.push_str(&block(lines[i]));
        out.push('\n');
        i += 1;
    }

    out.trim_end().to_string()
}

/// The info string of a fence line, or `None` if this is not one.
fn fence(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    trimmed
        .strip_prefix("```")
        .map(|rest| rest.trim_matches('`').trim())
}

/// A fence's language, when it is one bare word. Telegram wants it as a class
/// on the `<code>`, and anything else there is a hint we cannot use.
fn language(info: &str) -> Option<&str> {
    (!info.is_empty()
        && info
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-'))
    .then_some(info)
}

/// One line's block-level shape: a heading, a bullet, or ordinary text.
fn block(line: &str) -> String {
    let indent: String = line.chars().take_while(|c| *c == ' ').collect();
    let rest = &line[indent.len()..];

    // Headings become bold. Telegram has no heading of its own, and leaving the
    // hashes in reads like the markdown leaked.
    if let Some(text) = rest.strip_prefix('#') {
        let text = text.trim_start_matches('#');
        if let Some(text) = text.strip_prefix(' ') {
            return format!("{indent}<b>{}</b>", inline(text.trim_end()));
        }
    }

    // Bullets become a real bullet, keeping their indentation so a nested list
    // still reads as one.
    for marker in ["- ", "* ", "+ "] {
        if let Some(text) = rest.strip_prefix(marker) {
            return format!("{indent}• {}", inline(text));
        }
    }

    format!("{indent}{}", inline(rest))
}

/// Inline markup within one line.
///
/// Line at a time on purpose: an emphasis run that never closes stays literal
/// instead of swallowing the rest of the message, which is what streaming
/// produces on nearly every flush.
fn inline(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // A backslash escape is markdown's way of saying "this one is
        // literal", so the backslash itself must not survive into the output.
        if c == '\\' {
            if let Some(next) = chars.get(i + 1).filter(|n| n.is_ascii_punctuation()) {
                push_escaped(&mut out, *next);
                i += 2;
                continue;
            }
        }

        if c == '`' {
            let run = run_len(&chars, i, '`');
            if let Some(close) = find_run(&chars, i + run, '`', run) {
                let inner: String = chars[i + run..close].iter().collect();
                out.push_str("<code>");
                out.push_str(&escape(unpad(&inner)));
                out.push_str("</code>");
                i = close + run;
                continue;
            }
        }

        if c == '[' {
            if let Some((html, next)) = link(&chars, i) {
                out.push_str(&html);
                i = next;
                continue;
            }
        }

        if c == '*' || c == '_' || c == '~' {
            if let Some((html, next)) = emphasis(&chars, i, c) {
                out.push_str(&html);
                i = next;
                continue;
            }
        }

        push_escaped(&mut out, c);
        i += 1;
    }

    out
}

/// `[text](url)` as an anchor, if it really is one.
///
/// The scheme is checked rather than passed through: Telegram rejects a message
/// whose href it does not like, and a link is not worth losing a reply over.
fn link(chars: &[char], start: usize) -> Option<(String, usize)> {
    let close = find_char(chars, start + 1, ']')?;
    if chars.get(close + 1) != Some(&'(') {
        return None;
    }
    let end = find_char(chars, close + 2, ')')?;

    let url: String = chars[close + 2..end].iter().collect();
    let url = url.trim();
    let allowed = ["https://", "http://", "mailto:", "tg://"];
    if !allowed.iter().any(|scheme| url.starts_with(scheme)) {
        return None;
    }

    let label: String = chars[start + 1..close].iter().collect();
    let label = if label.trim().is_empty() {
        escape(url)
    } else {
        inline(&label)
    };

    Some((
        format!(
            "<a href=\"{}\">{label}</a>",
            escape(url).replace('"', "&quot;")
        ),
        end + 1,
    ))
}

/// `**bold**`, `*italic*`, `~~struck~~`.
///
/// Stricter than CommonMark, on purpose, and the reason is the same one the
/// renderer wraps tool summaries in code spans: **markup that fires by accident
/// deletes its own delimiters**, and here the delimiters are part of a name.
/// CommonMark renders `ls /tmp/*_cache*` as `ls /tmp/_cache` in italics and
/// `/a/_b_/c.rs` as `/a/b/c.rs` — a path that is not the one the agent touched,
/// shown with nothing to say it was changed. Reading it back off the phone you
/// cannot tell.
///
/// So emphasis has to look like prose: it opens at the start of a word, closes
/// at the end of one, and `_` is never emphasis at all. Underscores in a chat
/// about code are `file_path`, `__init__` and `MAX_TEXT` far more often than
/// they are italics, and the agents write `*` for emphasis anyway.
///
/// The closer is looked for as text, so a delimiter inside a code span on the
/// same line can close one early. That is a cosmetic mistake in a rare line,
/// and the alternative — a second parsing pass to find the spans first — is a
/// lot of machinery for it.
fn emphasis(chars: &[char], start: usize, delim: char) -> Option<(String, usize)> {
    if delim == '_' {
        return None;
    }

    let run = run_len(chars, start, delim);
    let width = if run >= 2 { 2 } else { 1 };

    // `~` only ever means strikethrough, and only doubled: a single one is a
    // home directory or an approximation, not markup.
    if delim == '~' && width == 1 {
        return None;
    }

    // What sits either side of the run. A glob (`/tmp/*`), a multiplication
    // (`3*4`) and a footnote marker all fail here, which is the point.
    let before = start.checked_sub(1).and_then(|i| chars.get(i).copied());
    if !before.is_none_or(|c| c.is_whitespace() || "([{\"'“‘—–".contains(c)) {
        return None;
    }

    // An opener is followed by content, not by a space: `2 * 3 * 4`.
    if chars.get(start + width)?.is_whitespace() {
        return None;
    }

    let close = find_run(chars, start + width, delim, width)?;
    if chars[close - 1].is_whitespace() {
        return None;
    }
    let after = chars.get(close + width).copied();
    if !after.is_none_or(|c| c.is_whitespace() || ")]}.,;:!?\"'—–".contains(c)) {
        return None;
    }

    let inner: String = chars[start + width..close].iter().collect();
    let tag = match (delim, width) {
        ('~', _) => "s",
        (_, 2) => "b",
        _ => "i",
    };

    Some((format!("<{tag}>{}</{tag}>", inline(&inner)), close + width))
}

/// How many of `c` start at `from`.
fn run_len(chars: &[char], from: usize, c: char) -> usize {
    chars[from..].iter().take_while(|ch| **ch == c).count()
}

/// The start of the next run of exactly `len` `c`s at or after `from`.
fn find_run(chars: &[char], from: usize, c: char, len: usize) -> Option<usize> {
    let mut i = from;
    while i < chars.len() {
        if chars[i] == c {
            let run = run_len(chars, i, c);
            if run == len {
                return Some(i);
            }
            i += run;
        } else {
            i += 1;
        }
    }
    None
}

fn find_char(chars: &[char], from: usize, c: char) -> Option<usize> {
    chars[from..]
        .iter()
        .position(|ch| *ch == c)
        .map(|i| i + from)
}

/// CommonMark strips one leading and one trailing space from a code span, which
/// is how a span holding a backtick at either end is written.
fn unpad(text: &str) -> &str {
    match text.strip_prefix(' ').and_then(|t| t.strip_suffix(' ')) {
        Some(stripped) if !stripped.trim().is_empty() => stripped,
        _ => text,
    }
}

fn push_escaped(out: &mut String, c: char) {
    match c {
        '&' => out.push_str("&amp;"),
        '<' => out.push_str("&lt;"),
        '>' => out.push_str("&gt;"),
        other => out.push(other),
    }
}

/// The three characters Telegram's HTML parser reads as markup. Everything else
/// — including the `<` in `Vec<String>` — travels as itself once these are out
/// of the way.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        push_escaped(&mut out, c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_summaries_become_code() {
        // What the renderer emits for every tool call.
        assert_eq!(
            to_html("▸ Read  `/a/_b_/c.rs`"),
            "▸ Read  <code>/a/_b_/c.rs</code>"
        );
    }

    #[test]
    fn markup_the_agent_wrote_is_rendered() {
        assert_eq!(to_html("**done**"), "<b>done</b>");
        assert_eq!(to_html("*maybe*"), "<i>maybe</i>");
        assert_eq!(to_html("~~gone~~"), "<s>gone</s>");
        assert_eq!(to_html("## Findings"), "<b>Findings</b>");
        assert_eq!(to_html("- one\n- two"), "• one\n• two");
        assert_eq!(to_html("  - nested"), "  • nested");
        // Emphasis mid-sentence, and with the punctuation that surrounds it in
        // real prose.
        assert_eq!(
            to_html("it is **done**, mostly"),
            "it is <b>done</b>, mostly"
        );
        assert_eq!(to_html("(**note**)"), "(<b>note</b>)");
    }

    #[test]
    fn a_glob_or_a_path_is_never_quietly_rewritten() {
        // The failure this module is stricter than CommonMark to avoid: both of
        // these render as emphasis under the real rules, and both come out
        // showing a name that is not the one the agent used.
        assert_eq!(to_html("ls /tmp/*_cache*"), "ls /tmp/*_cache*");
        assert_eq!(to_html("/a/_b_/c.rs"), "/a/_b_/c.rs");
        assert_eq!(to_html("3*4*5"), "3*4*5");
        // Underscores are identifiers here, never italics.
        assert_eq!(to_html("_maybe_"), "_maybe_");
        assert_eq!(to_html("__init__ and file_path"), "__init__ and file_path");
    }

    #[test]
    fn html_in_the_agents_prose_is_shown_not_obeyed() {
        // Rust and TypeScript both produce angle brackets constantly, and an
        // unescaped one is a 400 from Telegram — a lost reply, not a stray tag.
        assert_eq!(
            to_html("returns Vec<String> & a <b>tag</b>"),
            "returns Vec&lt;String&gt; &amp; a &lt;b&gt;tag&lt;/b&gt;"
        );
    }

    #[test]
    fn arithmetic_is_not_emphasis() {
        assert_eq!(to_html("2 * 3 * 4"), "2 * 3 * 4");
    }

    #[test]
    fn an_unclosed_run_stays_literal() {
        // Every flush mid-stream ends in the middle of something.
        assert_eq!(to_html("**half a bo"), "**half a bo");
        assert_eq!(to_html("a `command that"), "a `command that");
    }

    #[test]
    fn an_unclosed_fence_closes_itself() {
        // A code block is flushed to the chat long before the agent finishes
        // writing it, and `<pre><code>` left open is a message Telegram
        // refuses.
        assert_eq!(
            to_html("```rust\nfn main() {"),
            "<pre><code class=\"language-rust\">fn main() {</code></pre>"
        );
        assert_eq!(
            to_html("```\nls -la\n```"),
            "<pre><code>ls -la</code></pre>"
        );
    }

    #[test]
    fn code_is_never_parsed_as_markup() {
        assert_eq!(
            to_html("`rm -rf /tmp/*_cache*`"),
            "<code>rm -rf /tmp/*_cache*</code>"
        );
        assert_eq!(to_html("`` `x` ``"), "<code>`x`</code>");
        assert_eq!(
            to_html("```\n<b>x</b>\n```"),
            "<pre><code>&lt;b&gt;x&lt;/b&gt;</code></pre>"
        );
    }

    #[test]
    fn links_are_rendered_only_for_schemes_telegram_takes() {
        assert_eq!(
            to_html("[docs](https://example.com/a_b)"),
            "<a href=\"https://example.com/a_b\">docs</a>"
        );
        // A relative path is not a link, and a href Telegram rejects loses the
        // whole message.
        assert_eq!(to_html("[src](./src/main.rs)"), "[src](./src/main.rs)");
        assert_eq!(
            to_html("[x](javascript:alert(1))"),
            "[x](javascript:alert(1))"
        );
    }

    #[test]
    fn a_backslash_escape_does_not_survive_as_a_backslash() {
        assert_eq!(to_html(r"\*not bold\*"), "*not bold*");
    }

    #[test]
    fn omatethers_own_messages_come_out_as_themselves() {
        // Everything the bridge says goes through here too, not just the
        // agent's prose: `/help` is a plain-text listing and must not sprout
        // formatting or lose an angle bracket.
        let help = to_html(crate::command::HELP);
        assert!(help.contains("/deny &lt;why&gt;"), "{help}");
        assert!(help.contains("/auto [on|off]"), "{help}");
        assert!(!help.contains("<b>"), "nothing here is a heading: {help}");
        assert!(!help.contains("<i>"), "nothing here is emphasis: {help}");
    }

    #[test]
    fn every_tag_we_emit_is_balanced() {
        // The property the fallback exists for, and the one that should keep
        // it from ever firing: whatever goes in, the tags come out matched.
        let inputs = [
            "**a *b* c**",
            "```\nunclosed",
            "`a` **b** _c_ ~~d~~ [e](https://x.example)",
            "***",
            "____",
            "~~~",
            "# ",
            "*a **b** c*",
            "<script>alert(1)</script>",
        ];
        for input in inputs {
            let html = to_html(input);
            let opens = html.matches('<').count();
            let closes = html.matches('>').count();
            assert_eq!(opens, closes, "unbalanced brackets in {html:?}");
        }
    }
}
