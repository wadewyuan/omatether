//! Slash commands.
//!
//! Two kinds live in the same namespace: omatether's own commands, which the
//! agent could never provide, and everything else, which is passed through
//! untouched. Many Claude Code slash commands are prompt expansions, so
//! `/review` reaching the agent verbatim does the right thing.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Abandon the session and start a fresh one in the same thread.
    New,
    /// Interrupt the running turn.
    Stop,
    /// Set the working directory for this thread.
    Cd(String),
    /// Switch which agent this thread talks to.
    Agent(String),
    /// Get the line to type to take over at a real terminal.
    Attach,
    /// Report agent, directory, session and whether a turn is running.
    Status,
    /// Answer a pending permission request without tapping a button.
    Allow,
    Deny(String),
    /// Turn the permission gate off (`on`) or back on (`off`), or report it.
    ///
    /// `None` means "just tell me": a mode that decides whether a phone can
    /// run `rm -rf` unattended is worth being able to read without changing.
    Auto(Option<bool>),
    Help,
    /// Anything else — hand it to the agent as typed.
    Prompt(String),
}

pub fn parse(text: &str) -> Command {
    let text = text.trim();
    let (head, rest) = match text.split_once(char::is_whitespace) {
        Some((head, rest)) => (head, rest.trim()),
        None => (text, ""),
    };

    match head {
        "/new" => Command::New,
        "/stop" | "/cancel" => Command::Stop,
        "/status" => Command::Status,
        "/allow" | "/yes" => Command::Allow,
        "/deny" | "/no" => Command::Deny(if rest.is_empty() {
            "denied from chat".to_string()
        } else {
            rest.to_string()
        }),
        "/help" | "/start" => Command::Help,
        "/cd" => Command::Cd(rest.to_string()),
        "/agent" => Command::Agent(rest.to_string()),
        "/attach" => Command::Attach,
        "/auto" | "/yolo" => Command::Auto(match rest.to_ascii_lowercase().as_str() {
            "" => None,
            "on" | "yes" | "true" => Some(true),
            // Anything else is treated as "off" rather than guessed at: the
            // safe direction for a typo is more prompts, not fewer.
            _ => Some(false),
        }),
        _ => Command::Prompt(text.to_string()),
    }
}

pub const HELP: &str = "\
omatether — your coding agent, over chat

/new           start a fresh session in this thread
/stop          interrupt the running turn
/cd <path>     set the working directory (starts a fresh session)
/agent <name>  switch agent (claude, codex, pi, ...)
/attach        how to take over at a real terminal
/status        agent, directory, session, whether a turn is running
/allow         approve a pending tool call
/deny <why>    refuse it, and tell the agent why
/auto [on|off] approve tool calls without asking (on by default)

Anything else is sent to the agent as typed, including its own \
slash commands.

Threads start in auto mode: tool calls run without asking, and you \
see each one in the reply as it happens. /auto off puts the gate \
back for this thread — Allow/Deny before every Bash, Write and Edit.

Not every agent can be gated at all, even with /auto off:
  claude       can ask you before Bash, Write and Edit
  codex        approves its own tools inside a sandbox
  pi           runs and approves its own tools
  everything else (omp, opencode, crush, grok, gemini, copilot)
               runs detached with its own auto-approve flags, unsandboxed,
               and nothing here can stop a tool call";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omatether_commands_are_recognized() {
        assert_eq!(parse("/new"), Command::New);
        assert_eq!(parse("  /stop  "), Command::Stop);
        assert_eq!(parse("/status"), Command::Status);
        assert_eq!(parse("/cd ~/src/foo"), Command::Cd("~/src/foo".into()));
    }

    #[test]
    fn deny_carries_a_reason_and_has_a_default() {
        assert_eq!(parse("/deny too risky"), Command::Deny("too risky".into()));
        assert_eq!(parse("/deny"), Command::Deny("denied from chat".into()));
    }

    #[test]
    fn agent_and_attach_are_omatether_commands() {
        assert_eq!(parse("/agent codex"), Command::Agent("codex".into()));
        assert_eq!(parse("/attach"), Command::Attach);
        assert_eq!(parse("/agent"), Command::Agent(String::new()));
    }

    #[test]
    fn unknown_slash_commands_go_to_the_agent() {
        assert_eq!(parse("/review"), Command::Prompt("/review".into()));
        assert_eq!(
            parse("/compact keep the plan"),
            Command::Prompt("/compact keep the plan".into())
        );
    }

    #[test]
    fn help_admits_which_tiers_have_no_gate() {
        // The gate is the one safety feature this product has. A tier without
        // it has to say so somewhere a person reads before switching, not only
        // in the reply to /agent after they already have.
        assert!(HELP.contains("unsandboxed"), "the tmux tier's terms");
        assert!(HELP.contains("sandbox"), "codex's terms");
    }

    #[test]
    fn auto_reads_as_well_as_writes() {
        assert_eq!(parse("/auto"), Command::Auto(None));
        assert_eq!(parse("/auto on"), Command::Auto(Some(true)));
        assert_eq!(parse("/auto OFF"), Command::Auto(Some(false)));
        assert_eq!(parse("/yolo on"), Command::Auto(Some(true)));
        // A typo must not silently disarm the gate.
        assert_eq!(parse("/auto onn"), Command::Auto(Some(false)));
    }

    #[test]
    fn help_says_tool_calls_are_not_gated_by_default() {
        // Auto mode is the default, so /help is where someone finds out that
        // nothing is going to ask them before it runs.
        assert!(HELP.contains("auto mode"), "the default, stated");
        assert!(HELP.contains("/auto off"), "and how to undo it");
    }

    #[test]
    fn plain_text_is_a_prompt() {
        assert_eq!(
            parse("fix the flaky test"),
            Command::Prompt("fix the flaky test".into())
        );
    }

    #[test]
    fn cd_without_an_argument_is_still_cd() {
        assert_eq!(parse("/cd"), Command::Cd(String::new()));
    }
}
