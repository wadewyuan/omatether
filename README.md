# Switchboard

Bridge between messaging channels and the coding agent already installed in
your repo. Text it from a phone; it runs Claude Code (or Codex, or whichever
agent `omarchy default agent` points at) in your working tree and streams the
answer back to the thread it came from.

Not to be confused with a general chat assistant: the thing on the far end of
the pipe is *your* agent, with your `CLAUDE.md`, your MCP servers, and your
working tree.

## Status: milestone 2

Telegram end to end. Text the bot, the agent runs in your working tree, and the
reply streams back into a single message edited in place.

```bash
switchboard serve --dir ~/src     # the bridge
switchboard repl  --dir .         # one session, driven from this terminal
```

### Setup

1. Create a bot with [@BotFather](https://t.me/BotFather). **Mint a new one** —
   `getUpdates` is exclusive, so sharing a token with another bot means the two
   steal each other's messages.
2. Find your numeric Telegram user id (message [@userinfobot](https://t.me/userinfobot)).
3. Write `~/.config/switchboard/env`, `chmod 600`:

   ```
   SWITCHBOARD_TELEGRAM_TOKEN=123456:AA...
   SWITCHBOARD_TELEGRAM_ALLOWED_USERS=<your telegram user id>
   ```

4. Install the service:

   ```bash
   cargo build --release
   cp contrib/switchboard.service ~/.config/systemd/user/
   systemctl --user enable --now switchboard
   ```

The service needs no inbound port: long polling means it reaches out to
Telegram, so it stays behind the tailnet with nothing exposed.

### Commands

| Command | Effect |
|---|---|
| `/new` | Fresh session in this thread |
| `/stop` | Interrupt the running turn |
| `/cd <path>` | Set the working directory (starts a fresh session) |
| `/status` | Agent, directory, session, whether a turn is running |
| `/allow`, `/deny <why>` | Answer a permission request without tapping |
| `/help` | The above |

Anything else goes to the agent as typed — including its own slash commands,
many of which are prompt expansions, so `/review` just works.

### How a turn looks

Tool calls appear inline as the agent makes them, and permission requests for
consequential tools arrive as a message with **Allow** / **Deny** buttons that
block the turn until tapped.

Not every tool asks. Gating all of them is unusable from a phone — the agent
reads a dozen files before doing anything consequential, and a prompt per read
trains you to tap Allow without looking. Only `Bash`, `Write`, `Edit` and
`NotebookEdit` ask; the rest are approved by switchboard itself
(`GATED_TOOLS` in `src/core.rs`).

### The repl

The milestone-1 instrument, still the fastest way to see the wire protocol:

```bash
switchboard repl --raw --dir . "list the files here"
```

`--raw` echoes every frame to stderr, which is how you find out what changed
when a Claude Code release moves the format.

## How the Claude adapter works

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

### Gating tools: what actually works

This took some finding, so it is written down.

**`--permission-mode manual` does not survive `--print`.** Pass it and the CLI
reports `"permissionMode":"default"` in its `system/init` frame and approves
tools itself — silently, with no warning. If permission requests stop arriving,
check that frame first. Setting `CLAUDE_CODE_ENTRYPOINT=sdk-cli` does not
change it either.

**Permissions need no MCP server, and `--permission-prompt-tool` is not used
anywhere in this codebase.** The gate that does fire is a **`PreToolUse` hook
registered in the `initialize` handshake**, which is what the CLI's own
diagnostics recommend ("To gate every tool call, use a PreToolUse hook"). Each
tool call then arrives as a `hook_callback` control request and blocks until
answered:

```jsonc
// initialize — shape per the CLI's own validation error
{"type":"control_request","request_id":"switchboard-1","request":{
  "subtype":"initialize",
  "hooks":{"PreToolUse":[{"matcher":"*","hookCallbackIds":["switchboard-pretooluse"]}]}}}

// …then, per tool call
{"type":"control_request","request_id":"…","request":{
  "subtype":"hook_callback","callback_id":"switchboard-pretooluse",
  "input":{"tool_name":"Bash","tool_input":{"command":"echo hi"}}}}

// answered with
{"type":"control_response","response":{"subtype":"success","request_id":"…","response":{
  "hookSpecificOutput":{"hookEventName":"PreToolUse",
    "permissionDecision":"deny","permissionDecisionReason":"…"}}}}
```

A denial's reason reaches the model, which explains it back rather than
retrying blindly — verified end to end.

The older `can_use_tool` control request is also accepted (payload:
`{behavior: 'allow', updatedInput?: object}` / `{behavior: 'deny', message}`,
per the CLI's validation message), but it is not what arrives in practice. The
adapter remembers which dialect each question was asked in and answers in kind;
that bookkeeping stays inside the adapter so seam B remains vendor-neutral.

### Environment scrubbing

The agent is spawned with `CLAUDECODE`, `CLAUDE_CODE_SESSION_ID`,
`CLAUDE_CODE_CHILD_SESSION` and friends removed. Otherwise, when switchboard is
itself launched from inside a Claude Code session, the child inherits them,
decides it is a nested session, and resolves its configuration from the parent.
A service's agent environment should be deterministic regardless of what
started it.

## Architecture

Three layers, two out-of-process seams, both speaking JSON:

```
channel adapters  →  core  →  agent adapters
   (telegram)         │         (claude, codex, acp)
   (photon/node)      │
                   seam A     seam B
```

Photon's SDK is TypeScript-only, so its adapter must be a separate process
regardless of what the core is written in. Rather than treat that as a wart,
both seams are out-of-process by design — each adapter can live in whatever
language its SDK does, and the core never learns which channel or agent it is
talking to.

Seam B is shaped after **ACP** (Agent Client Protocol), whose four verbs —
prompt, streamed update, permission request, cancel — are exactly what a chat
bridge needs. See `src/agent.rs`.

### Layout

| Path | Role |
|---|---|
| `src/event.rs` | The normalized event model. Seam B's vocabulary. |
| `src/agent.rs` | The trait every agent adapter implements. |
| `src/claude/wire.rs` | Claude's `stream-json` frames → normalized events. |
| `src/claude/mod.rs` | Process lifetime, stdin writes, stdout reader task. |
| `src/channel/mod.rs` | Seam A: send, edit, ask, acknowledge. |
| `src/channel/telegram.rs` | Bot API client and the long-poll loop. |
| `src/core.rs` | The router: threads ↔ sessions, dispatch, flushing. |
| `src/render.rs` | Debounced edit-in-place message building. |
| `src/command.rs` | Slash commands, and what passes through. |
| `src/store.rs` | SQLite thread state. |
| `src/main.rs` | `serve` and `repl`. |

### The loosely-typed boundary

Agent wire formats are unversioned and will move underneath us. Frames are
parsed as `serde_json::Value` at the adapter edge and normalized inward, and
anything unrecognized becomes `AgentEvent::Unknown` rather than a parse error.
A change upstream should cost one adapter, not the whole binary.

This is the single most important implementation decision in the project. The
risk here is integration churn, not concurrency correctness.

## Decisions taken

Deliberately the simplest option in each case; revisit when something hurts.

| Question | Answer | Why |
|---|---|---|
| Session scope | One per chat thread | Matches chat intuition; per-repo lets two threads collide in one working tree. |
| Concurrent turns | Rejected while one runs | More predictable from a phone than queuing, and much simpler than interleaving. |
| Restart with a turn in flight | Kill the child, resume the conversation | `kill_on_drop` plus `--session-id`. The turn is lost; the conversation is not. |
| Long output | Truncate | Revisit with a paste file served over the tailnet — chat is a bad place for a 500-line diff. |
| Which tools ask | Only `Bash`/`Write`/`Edit`/`NotebookEdit` | A prompt per file read trains you to tap Allow without reading it. |
| `/cd` on a live thread | Starts a fresh session | `cwd` is fixed when the agent process starts; the old session stays resumable by id. |
| Telegram reply threads | Not separate threads | Only forum topics are; otherwise one conversation scatters into a session per reply chain. |

## Roadmap

1. ~~Claude driver, no channels~~
2. ~~Telegram end to end~~ ← you are here
3. Photon channel — vendored Node sidecar, `GET /inbound` NDJSON, `POST /send`
4. Second agent — Codex via `exec --json`, plus a tmux fallback tier for the rest
5. Polish — attachments, tapbacks, forum-topic-per-project, `/attach` handoff over ssh

### Notes for milestone 3

- Vendor the Photon sidecar rather than pointing at the copy inside the Hermes
  install tree — an update there would break us, and depending on that path is
  not Hermes-free.
- `PHOTON_SIDECAR_WATCH_STDIN=1` makes the sidecar exit on stdin EOF, which
  gives parent-death binding for free when spawned with a piped stdin.
