# Switchboard

Bridge between messaging channels and the coding agent already installed in
your repo. Text it from a phone; it runs Claude Code (or Codex, or whichever
agent `omarchy default agent` points at) in your working tree and streams the
answer back to the thread it came from.

Not to be confused with a general chat assistant: the thing on the far end of
the pipe is *your* agent, with your `CLAUDE.md`, your MCP servers, and your
working tree.

## Status: complete

Two channels — Telegram and iMessage (via Photon) — and every agent Omarchy
supports, over one core. Text either one, the agent runs in your working tree,
and the reply comes back to the thread it came from.

```bash
switchboard serve --dir ~/src     # the bridge
switchboard repl  --dir .         # one session, driven from this terminal
```

### Setup

At least one channel must be configured; both is fine.

**Telegram.** Create a bot with [@BotFather](https://t.me/BotFather). **Mint a
new one** — `getUpdates` is exclusive, so sharing a token with another bot means
the two steal each other's messages. Get your numeric user id from
[@userinfobot](https://t.me/userinfobot).

**iMessage (Photon).** Photon is a managed service — no Mac relay. You need a
project id and secret from [app.photon.codes](https://app.photon.codes/), and
the sidecar's dependencies:

```bash
cd vendor/photon-sidecar && npm install
```

Then write `~/.config/switchboard/env`, `chmod 600`:

```
SWITCHBOARD_TELEGRAM_TOKEN=123456:AA...
SWITCHBOARD_TELEGRAM_ALLOWED_USERS=<your telegram user id>

SWITCHBOARD_PHOTON_PROJECT_ID=...
SWITCHBOARD_PHOTON_PROJECT_SECRET=...
SWITCHBOARD_PHOTON_ALLOWED_USERS=+15551234567
```

Install the service:

   ```bash
   cargo build --release
   cp contrib/switchboard.service ~/.config/systemd/user/
   systemctl --user enable --now switchboard
   ```

The service needs no inbound port. Telegram long-polling reaches out rather
than being called, and the Photon sidecar binds to loopback only — so the whole
thing stays behind the tailnet with nothing exposed.

### Commands

| Command | Effect |
|---|---|
| `/new` | Fresh session in this thread |
| `/stop` | Interrupt the running turn |
| `/cd <path>` | Set the working directory (starts a fresh session) |
| `/agent <name>` | Switch agent — claude, codex, pi, omp, opencode, crush, grok, gemini, copilot |
| `/attach` | The ssh line to take over at a real terminal |
| `/status` | Agent, directory, session, whether a turn is running |
| `/allow`, `/deny <why>` | Answer a permission request without tapping |
| `/help` | The above |

Anything else goes to the agent as typed — including its own slash commands,
many of which are prompt expansions, so `/review` just works.

### How a turn looks

Tool calls appear inline as the agent makes them, and permission requests for
consequential tools block the turn until answered — with **Allow** / **Deny**
buttons on Telegram, and as a message asking for `/allow` or `/deny` on
iMessage, which has no buttons.

Streaming differs by channel, and the core learns which world it is in from
`Channel::can_edit` rather than from the channel's name:

| | Telegram | iMessage |
|---|---|---|
| Editing | yes | **never** |
| A turn arrives as | one message, growing on a 1.5s debounce | one complete message at the end |

iMessage cannot rewrite a sent message under any circumstances, so streaming
there would mean a stream of fragments. Holding the turn back and delivering it
whole is the only readable option.

Not every tool asks. Gating all of them is unusable from a phone — the agent
reads a dozen files before doing anything consequential, and a prompt per read
trains you to tap Allow without looking. Only `Bash`, `Write`, `Edit` and
`NotebookEdit` ask; the rest are approved by switchboard itself
(`GATED_TOOLS` in `src/core.rs`).

### Agents

All nine of Omarchy's agents work, in three tiers. `/agent` says what changes
when you switch, because waiting for an approval prompt that will never arrive
is a bad way to find out.

| Tier | Agents | Streams | Gates tools | How |
|---|---|---|---|---|
| Structured | `claude` | yes | **yes** | Bidirectional `stream-json`, one long-lived process |
| Structured | `codex` | yes | no | `codex exec --json`, one process per turn, `resume` for continuity |
| Detached | the other seven | no | no | `omarchy-agent --inline` in a tmux session |

Two honest limits, both the agent's rather than ours:

* **Codex cannot gate tools.** `codex exec`'s only non-interactive approval mode
  is `--approve-for-me`, which reviews automatically inside a workspace-write
  sandbox. There is no callback to route to a human, so no Allow/Deny appears.
* **The detached tier does not stream.** Those agents have no structured output,
  so the reply is the tmux session, not a chat message. Switchboard says so and
  gives you the `/attach` line rather than pretending otherwise.

Codex assigns its own conversation id on the first turn and reports it back
through `AgentEvent::Ready`; Claude takes one we choose. Either way it
round-trips through the store, so a restart resumes.

### Long replies

Above ~2500 characters a reply is written to
`$XDG_STATE_HOME/switchboard/out/` and the chat gets the head plus the command
to read the rest:

```
… 12431 characters in all. Read the rest with:

  ssh omarchy -t 'cat /home/wy/.local/state/switchboard/out/telegram-5-1788.txt'
```

Both channels clip long messages anyway, which loses the tail silently. A file
plus a way to read it loses nothing, and the tailnet already makes it
reachable.

### The repl

The milestone-1 instrument, still the fastest way to see the wire protocol:

```bash
switchboard repl --dir .                            # interactive
switchboard repl --agent codex --dir . "run tests"  # one-shot; runs to completion
```

It drives any agent through the same seam, which makes it the fastest way to
see what a backend actually emits.

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
| `src/codex.rs` | `codex exec --json`: per-turn process, `resume` for continuity. |
| `src/tmux.rs` | The detached tier, for agents with no structured output. |
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
| Switching agents | Starts a fresh conversation | Transcripts do not move between agents; pretending otherwise would lose context silently. |
| Typing indicators | Only on channels that cannot edit | Where a message grows as the turn runs, that *is* the indicator. |

## Roadmap

1. ~~Claude driver, no channels~~
2. ~~Telegram end to end~~
3. ~~Photon channel~~
4. ~~Second agent, plus the detached tier~~
5. ~~Polish~~ — typing indicators, long-reply spill, `/attach`, forum topics

Deliberately not built: **attachments** (sending and receiving files) and
**Photon tapbacks**. Both need live channel credentials to exercise at all, and
neither addresses a problem the long-reply spill does not already solve.

## What is verified, and what is not

Verified against the real thing:

- The Claude adapter end to end, including the permission gate blocking a tool
  call and a denial's reason reaching the model.
- The Codex adapter's process lifecycle: a real `thread.started` id captured,
  real JSONL parsed, errors surfaced, turn failure reported. Its event
  vocabulary was read out of codex-cli 0.152.1 itself.
- The detached tier: a real tmux session, the exact
  `omarchy-agent --inline --prompt …` invocation, and a second prompt reaching
  the running session through `send-keys`.
- Telegram's configuration and error paths against the live API, plus the
  service running live.
- The Photon sidecar's startup, credential validation, and supervision — a dead
  sidecar is reported in 2s with its real reason.

Not verified, and why:

- **A successful Codex turn.** `codex login` has not been run on this machine,
  so every request 401s. Everything up to the model call is exercised.
- **The live Photon path.** Testing it would have meant pointing a second client
  at the Photon project the Hermes bridge is using, which could have intercepted
  its messages.

## Notes kept from the build

- Vendor the Photon sidecar rather than pointing at the copy inside the Hermes
  install tree — an update there would break us, and depending on that path is
  not Hermes-free.
- `PHOTON_SIDECAR_WATCH_STDIN=1` makes the sidecar exit on stdin EOF, which
  gives parent-death binding for free when spawned with a piped stdin.
- Telegram allows roughly one message per second to a chat, and
  `editMessageText` draws on the same budget.
- `getUpdates` is exclusive: two pollers on one bot token steal each other's
  messages. Outbound `sendMessage` on a shared token is fine.
