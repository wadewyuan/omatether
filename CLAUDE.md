# switchboard — notes for whoever works on this next

A bridge between chat channels and the coding agent installed in your repo.
Read `README.md` for what it is and how to run it. This file is only the things
you cannot work out from the code, and that cost real time to find.

Everything below was verified against the live tool or service, not inferred.
Where something is unverified, it says so.

## Running state

Deployed as a systemd **user** service: `systemctl --user {status,restart} switchboard`,
`journalctl --user -fu switchboard`. Config is `~/.config/switchboard/env`
(mode 600), read by the unit as `EnvironmentFile`. Both channels are live and
working end to end.

Two things about that unit are load-bearing. `PATH` is set explicitly, because
a user service does not source `.bashrc` and would otherwise find neither
`claude` nor the mise-installed agents nor `node`. And the log filter is
`switchboard=info`, which is why every subprocess logs under a
`switchboard::` target — see "diagnostics" below.

## Claude Code adapter

**`--permission-mode manual` does not survive `--print`.** Pass it and the CLI
reports `"permissionMode":"default"` in its `system/init` frame and approves
tools itself, silently. If permission requests stop arriving, read that frame
first — it reports what the CLI actually settled on. Setting
`CLAUDE_CODE_ENTRYPOINT=sdk-cli` does not change it.

**The gate that works is a `PreToolUse` hook registered in the `initialize`
control request.** Not `--permission-prompt-tool`, and no MCP server. Each tool
call then arrives as a `hook_callback` control request and blocks until
answered. The hook schema came from the CLI's own validation error, and the
shape is fussy — matchers carrying `hookCallbackIds` arrays, not nested hook
objects:

```jsonc
"hooks": {"PreToolUse": [{"matcher": "*", "hookCallbackIds": ["switchboard-pretooluse"]}]}
```

Answer with `hookSpecificOutput.permissionDecision` (`allow`/`deny`) plus
`permissionDecisionReason`. The reason reaches the model, which explains it
back rather than retrying blindly.

The older `can_use_tool` control request is also accepted (`{behavior:
'allow'|'deny'}`), but it is not what arrives in practice. The adapter
remembers which dialect each question came in and answers in kind; that
bookkeeping stays inside the adapter so seam B stays vendor-neutral.

**Claude Code exports session-identity env vars to its children**
(`CLAUDECODE`, `CLAUDE_CODE_SESSION_ID`, `CLAUDE_CODE_CHILD_SESSION`, …). A
spawned agent inherits them, decides it is a nested session, and resolves
config from the parent. They are scrubbed by a fixed list, deliberately not a
`CLAUDE*` sweep — `CLAUDE_CODE_USE_BEDROCK` and friends are legitimate
operator config.

## Codex adapter

One process per turn, not a session: `codex exec --json`, with continuity
through `codex exec resume <thread_id>`. **Codex assigns the thread id itself**
on the first turn and reports it back through `AgentEvent::Ready`, which is why
`ThreadState::session_id` is an `Option<String>` the agent fills in rather than
a UUID we choose.

**Codex cannot gate tools.** `--approve-for-me` is the only non-interactive
approval mode and reviews automatically inside a workspace-write sandbox. There
is no callback for a human, so `gates_tools()` is false and `/agent codex` says
so on switch.

Event vocabulary, read out of codex-cli 0.152.1 rather than guessed:
`thread.started`, `turn.{started,completed,failed}`,
`item.{started,updated,completed}`, with item types `agent_message`,
`reasoning`, `command_execution`, `file_change`, `mcp_tool_call`, `web_search`,
`todo_list`, `error`. `command_execution` is normalized onto the same tool name
Claude uses (`Bash`) so the renderer and the gate list need no per-agent
knowledge.

**Unverified:** a successful Codex turn. `codex login` has never been run on
this machine, so every request 401s. Everything up to the model call is
exercised, including a real captured thread id. Run `codex login` and then
`switchboard repl --agent codex --dir . "say hi"` to close this.

## Detached tier (the other seven agents)

`omarchy-agent --inline` execs the agent directly instead of opening a terminal
window, so it runs under tmux with no compositor. That is the whole trick. No
structured output means no streaming and no gating, and the code says so to the
user rather than pretending.

A second prompt to a live session goes in via `send-keys` rather than starting
a new one.

## Telegram

**`getUpdates` is exclusive.** Two pollers on one bot token steal each other's
messages — this is why switchboard needs its own bot, separate from Hermes.
Outbound `sendMessage` on a shared token is fine.

Roughly one message per second per chat, and `editMessageText` draws on the
same budget, hence the 1.5s debounce. 429s carry `parameters.retry_after` and
are expected traffic, not an anomaly.

**Only forum topics are separate threads.** Plain replies in a group also set
`message_thread_id`; treating those as threads scatters one conversation into a
session per reply chain.

## Photon (iMessage)

**A shared line cannot initiate a conversation.** Sending to a phone number
with no existing space is refused with `AuthenticationError: [spectrum-imessage]
Target not allowed for this project`. This is not a bug and does not affect
normal use — switchboard only ever replies into a space that messaged it first
— but it means you cannot test outbound before texting the line.

**Hermes runs its own Photon sidecar on port 8789.** Ours defaults to port 0,
meaning "ask the OS for a free one". Do not reintroduce a fixed default; losing
that race is a certainty, not a chance.

**Spectrum envelopes its replies**: `{"succeed":true,"data":{"users":[…]}}`, so
lists are two levels down, and application errors arrive as **HTTP 200 with
`succeed:false`**. Checking the status alone reports success for a failure —
that bug once claimed a correctly configured project had zero users and nearly
led to re-registering it.

The sidecar in `vendor/photon-sidecar/` is **ours**, not a copy of the Hermes
plugin — depending on a path inside the Hermes install tree is the opposite of
this project's point. The SDK usage was learned from it. `npm install` there is
required; `node_modules` is gitignored.

`PHOTON_SIDECAR_WATCH_STDIN=1` makes the sidecar exit on stdin EOF, which binds
its life to ours for free when spawned with a piped stdin.

**iMessage can never edit a sent message**, which is why `Channel::can_edit`
exists and why a turn on Photon arrives whole at the end with only a typing
indicator meanwhile.

## Storage

**Databases exist in the wild. Changing `CREATE TABLE` is not a migration.**
`session_id` began `NOT NULL` and later had to accept null; only the fresh-database
path was updated, so every older database dropped the first message from every
new thread. SQLite cannot drop `NOT NULL` in place — it needs a table rebuild.
`migrate()` runs on open, is idempotent, and has tests that build the previously
shipped schema and migrate it. Add to it rather than editing `CREATE TABLE`
alone.

## Invariants worth preserving

- **Capabilities, not names.** The core branches on `Channel::can_edit`,
  `Agent::streams` and `Agent::gates_tools`, never on which channel or agent it
  is talking to. Both seams grew a capability the moment a second
  implementation arrived; expect the third to do it again.
- **Parse loosely at the edges, strictly inside.** Adapter frames are
  `serde_json::Value`; unrecognized ones become `AgentEvent::Unknown` and are
  never dropped. Agent wire formats are unversioned and will move. This is what
  surfaced the hook-schema error that unblocked the permission gate.
- **The allowlist is enforced in the channel adapter**, before the core sees a
  message, and an empty allowlist refuses to start. A bot token in a chat is a
  shell on this machine.
- **One turn per thread.** A second message while one runs is rejected, not
  queued. From a phone that is more predictable, and far simpler.

## Diagnostics

Subprocess output logs under `switchboard::{photon,claude,codex}` so the
default `switchboard=info` filter catches it. If you add a subprocess, use a
`switchboard::` target — a target outside that tree is silently dropped, which
once produced an error saying "see its log above" when there was no log above.

Failures should quote the reason, not point at a log: the Photon supervisor
keeps the sidecar's last 20 stderr lines and includes them in the startup
error, and notices a child that has exited instead of waiting out its
readiness timeout.

## How to verify a change

`cargo test` covers the wire parsers, the renderer's debounce, command parsing,
the store and its migration, using frames captured from the real tools.

Beyond that, the repl drives any agent without a channel:

```bash
switchboard repl --dir .                              # interactive
switchboard repl --agent codex --dir . "run tests"    # one-shot, runs to completion
```

To exercise the detached tier without launching a real agent, put a stub
`omarchy-agent` earlier on `PATH` and point `TMUX_TMPDIR` at a scratch
directory so the test server is isolated from your own tmux.

`switchboard photon-setup --phone +…` is a live, idempotent check of the Photon
credentials, registration and assigned line.

For the permission loop, the repl needs stdin held open — feed it from a file
with `tail -n +1 -f`, since a closed stdin ends the session once the turn does.
