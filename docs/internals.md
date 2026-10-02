# omatether internals

How it is built, for whoever changes it. Using it is in the [README](../README.md);
the hard-won facts about each agent and channel — the ones that cost real
time to find — are in [CLAUDE.md](../CLAUDE.md).

## Architecture

```
channel adapters  →  core  →  agent adapters
   telegram            │         claude   (one long-lived stream-json process)
   photon ── node      │         codex    (one process per turn)
     sidecar         seam A      pi       (one process per turn)
                     seam B      tmux     (the detached tier)
```

Both seams are Rust traits: `Channel` (`src/channel/mod.rs`) and `Agent`
(`src/agent.rs`). Only Photon crosses a process boundary on the channel
side, because its SDK is TypeScript-only — `vendor/photon-sidecar/` is a
small Node server the Photon adapter supervises over loopback HTTP. Every
agent is a subprocess, since that is how each one is shipped.

Seam B is shaped after **ACP** (Agent Client Protocol), whose four verbs —
prompt, streamed update, permission request, cancel — are exactly what a chat
bridge needs.

The core never asks which channel or agent it is talking to. It branches on
capabilities instead — `Channel::can_edit`, `Agent::streams`,
`Agent::gates_tools` — and every channel call goes through a per-thread
outbox (`src/outbox.rs`), so a rate-limited chat waits on its own and no one
else's.

### Layout

| Path | Role |
|---|---|
| `src/main.rs` | The CLI: `serve`, `setup`, `photon-setup`, `repl`. |
| `src/setup.rs` | Guided first run: credentials, env file, service. |
| `src/core.rs` | The router: threads ↔ sessions, commands, flushing. |
| `src/command.rs` | Slash commands, and what passes through to the agent. |
| `src/render.rs` | Agent events → one chat message, debounced and paged. |
| `src/outbox.rs` | Per-thread outbound queue, rate-limit waits included. |
| `src/store.rs` | SQLite thread state and its migrations. |
| `src/event.rs` | The normalized event model — seam B's vocabulary. |
| `src/agent.rs` | Seam B: the trait every agent adapter implements. |
| `src/claude/mod.rs` | Claude Code: process lifetime, control requests. |
| `src/claude/wire.rs` | Claude's `stream-json` frames → normalized events. |
| `src/codex.rs` | `codex exec --json`, `resume` for continuity. |
| `src/pi.rs` | `pi -p --mode json`, `--session` for continuity. |
| `src/tmux.rs` | The detached tier, for agents with no structured output. |
| `src/channel/mod.rs` | Seam A: the trait every channel implements. |
| `src/channel/telegram.rs` | Bot API client and the long-poll loop. |
| `src/channel/markup.rs` | Markdown → the HTML subset Telegram parses. |
| `src/channel/photon.rs` | The iMessage channel and its sidecar supervisor. |
| `src/channel/inbox.rs` | Where a file someone sent lands on disk. |
| `src/channel/*_setup.rs` | The live API calls behind `omatether setup`. |

### The loosely-typed boundary

Agent wire formats are unversioned and will move underneath us. Frames are
parsed as `serde_json::Value` at the adapter edge and normalized inward, and
anything unrecognized becomes `AgentEvent::Unknown` rather than a parse error.
A change upstream should cost one adapter, not the whole binary.

This is the single most important implementation decision in the project. The
risk here is integration churn, not concurrency correctness.

## The Claude adapter

`claude` runs in bidirectional `stream-json` mode — one long-lived process per
session:

```
claude --print --verbose \
       --output-format stream-json \
       --input-format stream-json \
       --include-partial-messages \
       --session-id <uuid>
```

`--input-format stream-json` is what makes this a session rather than a series
of one-shots: prompts are written to stdin as they arrive.

### Gating tools

**`--permission-mode manual` does not survive `--print`.** Pass it and the CLI
reports `"permissionMode":"default"` in its `system/init` frame and approves
tools itself — silently. If permission requests stop arriving, check that
frame first.

The gate that fires is a **`PreToolUse` hook registered in the `initialize`
handshake** — no MCP server, no `--permission-prompt-tool`. Each tool call
then arrives as a `hook_callback` control request and blocks until answered:

```jsonc
// initialize — shape per the CLI's own validation error
{"type":"control_request","request_id":"omatether-1","request":{
  "subtype":"initialize",
  "hooks":{"PreToolUse":[{"matcher":"*","hookCallbackIds":["omatether-pretooluse"]}]}}}

// …then, per tool call
{"type":"control_request","request_id":"…","request":{
  "subtype":"hook_callback","callback_id":"omatether-pretooluse",
  "input":{"tool_name":"Bash","tool_input":{"command":"echo hi"}}}}

// answered with
{"type":"control_response","response":{"subtype":"success","request_id":"…","response":{
  "hookSpecificOutput":{"hookEventName":"PreToolUse",
    "permissionDecision":"deny","permissionDecisionReason":"…"}}}}
```

A denial's reason reaches the model, which explains it back rather than
retrying blindly — verified end to end. The older `can_use_tool` control
request is also accepted and answered in kind; that bookkeeping stays inside
the adapter so seam B remains vendor-neutral.

### Environment scrubbing

The agent is spawned with `CLAUDECODE`, `CLAUDE_CODE_SESSION_ID`,
`CLAUDE_CODE_CHILD_SESSION` and friends removed. Otherwise, when omatether is
itself launched from inside a Claude Code session, the child decides it is a
nested session and resolves its configuration from the parent.

## Decisions taken

Deliberately the simplest option in each case; revisit when something hurts.

| Question | Answer | Why |
|---|---|---|
| Session scope | One per chat thread | Matches chat intuition; per-repo lets two threads collide in one working tree. |
| Concurrent turns | Rejected while one runs | More predictable from a phone than queuing, and much simpler than interleaving. |
| One send, several messages | Held ~600ms and joined | iMessage splits a captioned photo in two; Telegram splits an album per photo. |
| Restart with a turn in flight | Kill the child, resume the conversation | The turn is lost; the conversation is not. |
| Long output | Paged into further messages | Never cut, never a file to read over ssh from a phone. |
| Files sent in chat | A path in the prompt | Every agent here has filesystem tools; one representation serves all nine. |
| Whether tools ask at all | No, by default (`/auto off` per thread) | A gate on every `Bash` from a phone becomes a tap-Allow reflex. |
| Which tools ask, with the gate on | `Bash`/`Write`/`Edit`/`NotebookEdit` | A prompt per file read trains you to tap Allow without reading it. |
| `/cd` on a live thread | Starts a fresh session | `cwd` is fixed when the agent process starts. |
| Telegram reply threads | Not separate threads | Only forum topics are; otherwise one conversation scatters. |
| Switching agents | Starts a fresh conversation | Transcripts do not move between agents. |

## The repl

The fastest way to see what a backend actually emits, with no channel in the
way:

```bash
omatether repl --dir .                              # interactive
omatether repl --agent codex --dir . "run tests"    # one-shot, runs to completion
omatether repl --agent claude --model haiku --dir . # start on a given model
```

## What is verified, and what is not

Checked against the real tool or service rather than inferred:

- **Telegram**, in daily use, and **Photon/iMessage** end to end.
- **The Claude adapter**, including the gate blocking a tool call and a
  denial's reason reaching the model.
- **The detached tier**: a real tmux session, the exact `omarchy-agent
  --inline --prompt …` invocation, and a second prompt via `send-keys`.
- **The Codex adapter's lifecycle** up to the model call.
- **The store migration**, on a live database.

Not yet:

- **A successful Codex turn** — `codex login` has never been run on the
  machine this was built on.
- **A real photo through either channel end to end.** Parsers, compose step
  and file modes are tested; a screenshot landing and an agent opening it has
  not been watched.
- **Multi-page Telegram turns** against the live bot.
