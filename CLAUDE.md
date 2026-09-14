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

**`set_model` is a control request, and it is the only place a model name gets
checked.** `{"subtype":"set_model","model":"opus"}` on the live session's stdin
comes back `success`; an unknown name comes back
`{"subtype":"error","error":"Model \"x\" is not a recognized model id…"}`, and
`"model":null` resets to the session default. Verified against 2.1.235, all
three. **`--model` at spawn checks nothing**: `--model definitely-not-a-model`
starts fine, reports that name back in its own `system/init` frame, and only
falls over when a turn is asked of it. So a name typed at `/model` goes through
the control request when a session is up, and `--model` is for replaying a name
that was already accepted.

Two things fall out of that. `ClaudeSession::ask` waits for the answer through a
`RepliesMap` keyed by request id — and a frame claimed that way is *not* passed
to `wire::normalize`, or the rejection reported to whoever asked would also
arrive a second time as an `Error` event in the middle of somebody's turn. And
the handshake reply carries a **`models` array** (`value`, `resolvedModel`),
which is the only catalog anywhere in reach — `claude` has no "list models"
command — so `/model` lists what that array said and resolves `opus` to
`claude-opus-5` with it rather than with a table kept here.

**`/model` on its own shows a list, and each agent answers from a different
place.** Claude's is the handshake array, so it exists only while a session
is up — `/model` before the first message or after `/new` says "send it a
message first" rather than inventing one. Codex and pi volunteer nothing, so
the list comes from asking their CLI, both local and no API call: `codex
debug models` (JSON catalog, works **without** `codex login` on this
machine; `-m` takes the `slug`; entries with `visibility:"hide"` are the
ones codex keeps out of its own picker and are left off) and `pi
--list-models` (a table; `provider/model` is the name `--model` takes —
the very command pi's "Model … not found" error points at). Both CLIs
print the mise activation line to *stdout* first, which is why the parsers
find the payload rather than assume line one. The detached tier still
answers "takes no model", which is the honest answer there.

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

`-m <model>` is accepted by both `codex exec` and `codex exec resume <id>`
(checked against 0.153.4's own `--help`), so the flag goes on every turn's
process and a `/model` needs no session restart. Nothing validates the name
here — there is no process between turns to ask — so a bad one surfaces as a
failed turn.

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

**Pi checks the model itself, and says so outside the JSON.** `--model
definitely-not-a-model` exits before any turn with `Error: Model "…" not found.
Use --list-models to see available models.` on a plain line — which this adapter
logs as non-JSON and drops, so what reaches the chat is the generic "pi exited
without completing the turn". The reason is in the `omatether::pi` log. Worth
fixing by carrying the last non-JSON line into that detail; it would improve
every pi startup failure, not just this one.

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

**No model can be passed through this tier.** `omarchy-agent` takes `--inline`,
`--pick` and `--prompt`, exits on anything else, and holds each agent's own
spelling of "don't stop to ask". Getting a model in would mean either changing
Omarchy or building these command lines here — and a stale second copy of the
flags that decide whether an agent pauses for permission is the wrong thing to
own. `agent::takes_model` is false for the tier and `/model` says why.

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

**A chat action expires after about five seconds and cannot be withdrawn.**
So the working indicator is a heartbeat, not a switch: the core re-sends it
every 4s while a turn is running and there is nothing else on screen yet, and
`typing(_, false)` is a no-op here because there is nothing to withdraw. It is
also the one job `src/outbox.rs` does *not* retry through a rate limit —
waiting out a 429 would park the thread's queue, with the turn's actual text
behind an indicator that is stale by the time it lands.

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

**`space.typing(...)` is not a method on a Space, and optional chaining hid
that.** The sidecar called `await space.typing?.("start")`, which is
`undefined`, so the call evaluated to nothing: no request, no error, no log,
and a `{ok:true}` back to Rust. The typing indicator had never once appeared.
The names the SDK does have are `space.startTyping()` / `space.stopTyping()`
(sugar over `space.send(typing("start"|"stop"))`) — verified by reading the
space wrapper in the installed `@spectrum-ts/core` 12.7.0 bundle, which builds
exactly those two. `?.` on a method you believe exists buys nothing and costs
the error that would have told you.

**Unlike Telegram's, this indicator does not expire**, so `/typing` takes a
`state` and the stop half is load-bearing: without it a chat is left showing
three dots for an agent that finished, was interrupted, or had its session
dropped by `/cd`. The Rust side checks the HTTP status on this call for the
same reason the `succeed:false` lesson below exists — silence was the failure
mode last time.

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

**Order inside `migrate()` is load-bearing.** The rebuild is last because it
copies columns by name — every `ADD COLUMN` above it has to have run before its
`SELECT` can name them. A new column added *after* the rebuild would be dropped
on any database old enough to take that path, and only on those, which is the
worst kind of bug to find.

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
- **`/new` starts in `~/Work`, not where the last session was.** It matches
  `omarchy-agent`, which steps out of `$HOME` into `~/Work` before launching
  because agents refuse to remember trust for a home directory. So a session
  begun from chat lands where one begun from the keybinding does. The
  directory's existence is checked per call, not at startup, and a machine
  without it keeps the thread where it was — a cwd that is not there fails at
  the spawn, several messages later, where the reason is much harder to see.
  `/cd` is still the way to point a thread somewhere specific, and it does not
  survive `/new` on purpose.
- **The model is remembered, never asked for.** `/new` and `/status` both name
  it at moments when no agent process exists, so `threads.model` holds whatever
  the last `AgentEvent::Ready` reported. Two consequences: a `Ready` with
  `model: None` means "this adapter does not report one" and must not erase
  what claude already told us (codex, pi and the tmux tier all send `None`
  today), and `/agent` clears it, because claude's model under codex's name is
  a confident lie. Unknown prints as unknown.
- **`threads.model` and `threads.requested_model` are different facts, and
  collapsing them is a bug waiting to happen.** The first is what came back —
  resolved, `claude-opus-5` — and is only ever for saying out loud. The second
  is what `/model` asked for, spelt as typed (`opus`), and is the only one
  passed to the next process. Pass the reported one to a spawn and a thread
  that never ran `/model` pins itself for good to whatever the agent defaulted
  to on the day it was first asked; show the requested one in preference to the
  reported one and an alias hides the id that is really running. `/agent`
  clears both, because a model name is one agent's vocabulary — `opus` means
  nothing to codex, and pi wants a `provider/id`.
- **`/model` keeps the session; `/cd` and `/agent` do not.** A directory is
  fixed when a process starts and a transcript cannot move between agents, but
  a model can change underneath a live conversation — Claude Code takes a
  `set_model` control request, and the per-turn agents just get a different
  flag next time. Do not copy the shutdown-and-forget shape from the commands
  either side of it.
- **Nothing slow happens on the core's task.** Every channel call goes through
  the thread's outbox (`src/outbox.rs`); the core queues and moves on. This is
  what keeps "one turn per thread" from quietly meaning "one *anything* at a
  time, globally" — which is what it meant when a 429 slept in the shared loop.
  If you find yourself awaiting a `Channel` method from `core.rs`, that is the
  regression — and a `Channel` method is not the only shape it takes. `/model`
  with no argument asks codex or pi's CLI for a catalog, which is a process to
  wait on; it runs on a task of its own holding a cloned `Outbox`, because
  waiting for it here would stop every other thread's events and flushes for as
  long as it took. Twelve milliseconds in the normal case is exactly what makes
  this kind of thing invisible until the day it is not.
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
- **An indicator must never outlive what it is claiming.** The working
  indicator comes down on the first signal that the turn is over — the event
  that ends it, or the adapter's busy flag — never the last, because the two
  clear in different orders per agent (Claude's before the end of the turn goes
  out, Codex's and pi's a moment after). It also comes down while a permission
  question is outstanding: what the exchange is waiting for there is a person,
  and dots under the question say the opposite. The bookkeeping lives beside
  `outboxes` rather than inside a `Thread` for the same reason that one does —
  `/new` and `/cd` drop a session mid-turn, and something still has to turn the
  indicator off afterwards.
- **Never show a question you had to cut off.** A permission prompt is the one
  human control here, so the full tool input goes out whole or goes to a file
  with a pointer — never clipped by the channel with "… truncated".
- **A length cut must never land on the answer.** `spill_if_long` used to keep
  the first 2500 characters of a long turn, and the first 2500 characters of a
  working turn are its tool log: a 6,500-character reply arrived as `▸ Bash`
  lines ending mid-command, with every word of the answer in the spill file.
  It reads as *no reply*, not a truncated one, and on Photon — where the whole
  turn lands at once at the end, unedited — it is the normal case for any real
  piece of work, not an edge one. So the order is: whole turn, else the turn
  without its tool log (`TurnRenderer::compose_prose`), else the *end* of the
  prose. Related, and the reason the first two nearly always suffice: a run of
  consecutive tool calls renders as one line naming the count and the newest
  call, rather than a line each.

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
omatether repl --agent claude --model haiku --dir . # start on a given model
```

`/model` works in the repl, and that is how the live switch was checked against
the real CLI without spending a turn: `/model` lists, `/model haiku` answers
`now claude-haiku-4-5-20251001`, an unknown name comes back refused in the
CLI's own words, and `/model default` returns to `claude-sonnet-5`. None of
that costs an API call, which makes it a cheap thing to re-run when the CLI
moves under us.

**Do not run a second `omatether serve` while the user unit is active.** A
scratch `--state` keeps the databases apart but not the bot: `getUpdates` is
exclusive, so for as long as the second one runs the two steal each other's
messages and some arrive nowhere. Check `systemctl --user is-active omatether`
first, and use the repl — which touches no channel — for anything that does not
specifically need one.

To exercise the detached tier without launching a real agent, put a stub
`omarchy-agent` earlier on `PATH` and point `TMUX_TMPDIR` at a scratch
directory so the test server is isolated from your own tmux.

`omatether photon-setup --phone +…` is a live, idempotent check of the Photon
credentials, registration and assigned line.

For the permission loop, the repl needs stdin held open — feed it from a file
with `tail -n +1 -f`, since a closed stdin ends the session once the turn does.
