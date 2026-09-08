# omatether — notes for whoever works on this next

A bridge between chat channels and the coding agent installed in your repo.
Read `README.md` for what it is and how to run it. This file is only the things
you cannot work out from the code, and that cost real time to find.

Everything below was verified against the live tool or service, not inferred.
Where something is unverified, it says so.

## Running state

Deployed as a systemd **user** service: `systemctl --user {status,restart} omatether`,
`journalctl --user -fu omatether`. Config is `~/.config/omatether/env`
(mode 600), read by the unit as `EnvironmentFile`. Both channels are live and
working end to end.

Two things about that unit are load-bearing. `PATH` is set explicitly, because
a user service does not source `.bashrc` and would otherwise find neither
`claude` nor the mise-installed agents nor `node`. And the log filter is
`omatether=info`, which is why every subprocess logs under an
`omatether::` target — see "diagnostics" below.

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
"hooks": {"PreToolUse": [{"matcher": "*", "hookCallbackIds": ["omatether-pretooluse"]}]}
```

Answer with `hookSpecificOutput.permissionDecision` (`allow`/`deny`) plus
`permissionDecisionReason`. The reason reaches the model, which explains it
back rather than retrying blindly.

**The handshake is answered, and `spawn` blocks on the answer.** The reply is
`{"type":"control_response","response":{"subtype":"success","request_id":…}}`,
it carries the request id we sent, and it is the *first* frame out — ahead of
`system/init` — about 900ms in, nearly all of it node starting. A session whose
hook registration is refused is a session with no gate that looks exactly like
a working one (no `PermissionRequest` ever arrives, which reads as "the agent
didn't need permission"), so registration failing is now a startup error
quoting the CLI's reason rather than something only the logs know.

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
`omatether repl --agent codex --dir . "say hi"` to close this.

## Pi adapter

Same shape as Codex: `pi -p --mode json`, one process per turn, continuity
through `--session <id>`. **Pi assigns the session id itself** and reports it in
the first frame (`{"type":"session","id":…}`), so like Codex it fills in
`ThreadState::session_id` rather than taking one we choose. Verified: a second
process started with `--session <id>` still knows what the first one was told.

**Pi cannot gate tools.** `-p` runs its tools as it decides on them; there is no
approval callback, so `gates_tools()` is false and `/agent pi` says so.

**`turn_end` is not the end of the exchange — `agent_end` is.** This one cost
real time and looks like nothing in the frame log. Pi has two nested
lifecycles, `agent_*` around `turn_*`, and a *turn* is one model round-trip: a
prompt answered with a tool call emits `turn_end` twice, once when the model
stops to call the tool and again when it has read the result and replied.
Reading the first as the end is not a cosmetic error — the core flushes and
*resets the renderer* on `TurnEnd`, so the answer that comes after it is
discarded and the user gets the tool call alone, reported as a completed turn.
The A/B is worth keeping in mind: with `turn_end` closing the turn, `omatether
repl --agent pi` prints the `[tool]` line and `[turn end] ok` and never prints
the reply at all.

**A failed turn is quiet.** Pi exits 0 and still emits a well-formed `agent_end`
on an API error; the only sign is the last message's `stopReason` of `"error"`,
with the reason next to it in `errorMessage` (`401 … "API key is invalid."`).
Read the status alone and a failure renders as an empty successful answer —
the same trap as Spectrum's `succeed:false` below.

`tool_execution_{start,update,end}` re-report a tool call that the preceding
`message_end` already announced, so they are dropped rather than rendered
twice. Vocabulary is confirmed against both a live run and pi's own
`docs/json.md` (pi 0.84.4) — note there is **no** `turn_aborted` frame, despite
it being an obvious guess.

## Detached tier (the other six agents)

`omarchy-agent --inline` execs the agent directly instead of opening a terminal
window, so it runs under tmux with no compositor. That is the whole trick. No
structured output means no streaming and no gating, and the code says so to the
user rather than pretending — in `/agent`'s reply, in `/help` and in the
README, because "no gate" is a security property and inferring it from "does
not stream" is not reasonable to ask of anyone.

A second prompt to a live session goes in via `send-keys` rather than starting
a new one. **It must be `send-keys -l -- <text>`**, with the newline sent
separately. Without `-l`, tmux reads key *names*: a message of exactly `C-c` is
not typed but pressed, which interrupts the agent it was meant for — the probe
that found this killed its own test session, because `cat` took the SIGINT and
the pane closed. `Enter`, `Space` and `BSpace` are the same trap. Without `--`,
a message starting with `-` is parsed as flags. Verified against tmux 3.7c.

The `new-session` path is fine as it is: tmux passes the prompt to
`omarchy-agent` as argv with no shell in between, so `foo; rm -rf ~` stays a
string. Verified, not assumed.

## Telegram

**`getUpdates` is exclusive.** Two pollers on one bot token steal each other's
messages — this is why omatether needs its own bot, separate from Hermes.
Outbound `sendMessage` on a shared token is fine.

Roughly one message per second per chat, and `editMessageText` draws on the
same budget, hence the 1.5s debounce. 429s carry `parameters.retry_after` and
are expected traffic, not an anomaly.

**The waiting happens in the thread's outbox, never in the adapter.** A 429
becomes a `RateLimited` error the adapter reports; `src/outbox.rs` decides
whether to wait and retry. It used to sleep inside `Telegram::call`, which is
inside the core's one `select!` loop — so one rate-limited chat stopped every
other chat on every channel — and then bailed anyway, losing the message it had
just waited for. If you add a channel, report the platform's retry hint rather
than acting on it.

**Buttons outlive their question**, so `callback_data` carries a per-question
token (`allow:<token>`) and a tap naming a question that is no longer open is
refused rather than applied to whatever is pending now.

**A message is not always `text`.** A photo or a document carries its words in
`caption`, and a voice note carries none at all. Reading only `text` dropped
both — silently, with nothing in the log and nothing in the chat, which from a
phone is indistinguishable from the bridge being down. Captions are now read as
prompts, and anything with no words at all comes through as
`InboundKind::Unsupported` so the sender is told rather than ignored. If you add
a channel, the rule is: an allowed user who sent something always gets an
answer, even if the answer is "I cannot read that".

**A failing poller used to say nothing useful.** `getUpdates failed: telegram
getUpdates` was the whole warning — `{e}` prints only anyhow's outermost
context, which here is just the name of the call. The cause (a timeout, DNS, a
409 from a second poller on the same token) is in the chain and needs `{e:#}`.
Recovery is logged too: without it, a poll that fails for twenty minutes and
then works again leaves no timeline for "my message got no reply" to sit
against.

**Telegram renders nothing without a `parse_mode`, and the mode has to be
HTML.** A turn is markdown — the agents write it — so it used to arrive as raw
`**asterisks**` and visible backticks. `MarkdownV2` is not the fix: it requires
`.`, `-`, `(`, `!` and eleven more characters to be escaped everywhere they are
not markup, so ordinary prose fails to parse, and a message Telegram cannot
parse is a 400 — a *lost* reply, not an ugly one. Legacy `Markdown` still trips
on the unbalanced `*` that streaming produces on nearly every flush, because a
turn is sent mid-sentence. HTML has three characters to escape and cannot be
unbalanced, since `src/channel/markup.rs` emits every tag itself.

That converter is deliberately stricter than CommonMark about emphasis, and it
is the same rule as never truncating a permission question: **markup that fires
by accident deletes its own delimiters.** CommonMark renders `ls /tmp/*_cache*`
as `ls /tmp/_cache` and `/a/_b_/c.rs` as `/a/b/c.rs` — a name that is not the
one the agent used, with nothing on screen to say so. So `_` is never emphasis
(in a chat about code it is `file_path` and `__init__`), and `*` only opens at
the start of a word and closes at the end of one. Permission questions skip the
renderer entirely, as they do on Photon.

**Telegram validates entities before it resolves the chat**, which is a free
test rig: `sendMessage` with `chat_id: 0` answers `can't parse entities: …` for
HTML it rejects and `chat not found` for HTML it accepts, and nothing is
delivered to anyone either way. Every rendered string in `markup.rs`'s tests,
plus a realistic turn, was checked against the live parser that way — the
unformatted fallback in `call_rendered` has never had to fire.

**Only forum topics are separate threads.** Plain replies in a group also set
`message_thread_id`; treating those as threads scatters one conversation into a
session per reply chain.

## Photon (iMessage)

**A shared line cannot initiate a conversation.** Sending to a phone number
with no existing space is refused with `AuthenticationError: [spectrum-imessage]
Target not allowed for this project`. This is not a bug and does not affect
normal use — omatether only ever replies into a space that messaged it first
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

**`format: "markdown"` on the sidecar's `/send` is not a hint — the adapter
*parses* the string** and sends plain text plus native emphasis ranges, so
`**bold**` becomes real bold and a literal `*` is consumed rather than shown.
Prose wants that; a permission question quoting a tool's own arguments does not,
so it asks for `"text"` and the field defaults to `"text"` — the direction that
cannot silently rewrite anything is the one you get by forgetting. Code spans
come out as Unicode mathematical monospace: readable, not pasteable, and worth
it from a phone. The renderer also refuses text that renders to nothing at all
(`**`, an HTML comment), which is a bad reason to lose a message, so the sidecar
falls back to plain text on that one error — it is raised before anything is
sent, so the fallback cannot double-send.

**A textless message was dropped twice over.** The sidecar only broadcast events
with a non-empty `text`, and `parse_event` dropped them again, so a voice note
or a bare image vanished with nothing in the chat and nothing in the log — the
Telegram bug above, in a second place. Both now carry it through as
`InboundKind::Unsupported`. The order in `parse_event` is load-bearing: the
allowlist runs *first*, because a stranger gets silence rather than a reply that
confirms the number is a bridge.

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
- **Nothing slow happens on the core's task.** Every channel call goes through
  the thread's outbox (`src/outbox.rs`); the core queues and moves on. This is
  what keeps "one turn per thread" from quietly meaning "one *anything* at a
  time, globally" — which is what it meant when a 429 slept in the shared loop.
  If you find yourself awaiting a `Channel` method from `core.rs`, that is the
  regression.
- **The gate must be confirmed, not assumed.** An agent that claims to gate
  tools has to prove it at startup; a gate that silently failed to install is
  indistinguishable from an agent that had nothing to ask about. This holds
  even though threads now start with the gate *open* — see below. Auto mode is
  a decision the core makes about a question it received; a handshake that
  failed means the question never arrives, and those are not the same state.
- **Auto mode is the default, and it is a product decision.** A thread approves
  its own tool calls unless someone types `/auto off`; the bit lives in
  `threads.auto` and is mirrored into `Thread.auto` so the hot path — every
  tool call the agent makes — never touches sqlite. It was turned on because
  gating every `Bash` from a phone produced a tap-Allow reflex within a day,
  and a prompt nobody reads is worse than no prompt because it looks like
  review. Two things follow: the gate must stay working (`/auto off` is the
  reason the whole `PreToolUse` apparatus is still here), and every place that
  describes the product has to say the default out loud — `/help`, `/status`
  and the README all do.
- **Never show a question you had to cut off.** A permission prompt is the one
  human control here, so the full tool input goes out whole or goes to a file
  with a pointer — never clipped by the channel with "… truncated".

## Diagnostics

Subprocess output logs under `omatether::{photon,claude,codex,pi}` so the
default `omatether=info` filter catches it. If you add a subprocess, use an
`omatether::` target — a target outside that tree is silently dropped, which
once produced an error saying "see its log above" when there was no log above.

Failures should quote the reason, not point at a log: the Photon supervisor
keeps the sidecar's last 20 stderr lines and includes them in the startup
error, and notices a child that has exited instead of waiting out its
readiness timeout.

## How to verify a change

`cargo test` covers the wire parsers, the renderer's debounce, command parsing,
the store and its migration, using frames captured from the real tools. It also
covers the outbox against a fake channel, including the property it exists for:
a thread waiting out a rate limit does not delay another thread's message.

Two things there are worth knowing before you trust them. The permission-gate
handshake was verified in both directions against the installed CLI — accepted,
and deliberately broken — not just unit tested. And the "unchanged turn is not
re-sent" test was confirmed to fail when its bug is reintroduced; a test for a
spam-the-phone bug is worth that much.

Beyond that, the repl drives any agent without a channel:

```bash
omatether repl --dir .                              # interactive
omatether repl --agent codex --dir . "run tests"    # one-shot, runs to completion
```

To exercise the detached tier without launching a real agent, put a stub
`omarchy-agent` earlier on `PATH` and point `TMUX_TMPDIR` at a scratch
directory so the test server is isolated from your own tmux.

`omatether photon-setup --phone +…` is a live, idempotent check of the Photon
credentials, registration and assigned line.

For the permission loop, the repl needs stdin held open — feed it from a file
with `tail -n +1 -f`, since a closed stdin ends the session once the turn does.
