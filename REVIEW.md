# Switchboard — brutal code review

Scope: architecture, multi-channel support, abstraction, extensibility, security.
Verdict first, evidence after. Tests pass (62), the code is readable, and the
docs are unusually honest about what's verified — which makes the gaps below
all the more fixable, because nothing here is hidden.

---

## TL;DR

The *shape* of this project is right: two capability-driven seams, a tolerant
event model, one loop, good tests at the parsers. The *execution* has three
real problems:

1. **The core is single-threaded and every chat on every channel shares it.**
   One rate-limited Telegram chat stalls iMessage. This is the biggest flaw.
2. **The security posture is inconsistent across agent tiers** — Claude users
   get a tool gate, tmux-tier users get an unsandboxed auto-approving shell,
   and permission prompts are truncated to the point of being approvable blind.
3. **The README's architecture diagram lies.** Both seams are claimed to be
   out-of-process JSON; only the Photon sidecar is. Everything else is an
   in-process trait object. The abstraction story oversells.

---

## 1. Architecture

### 1.1 The core is a single point of serialization (high)

`Core::run` is one `select!` loop, and every handler is an `await` on that
task. Consequences:

- `Telegram::call` **sleeps inside the core** on a 429 (`retry_after + 1`
  seconds, `channel/telegram.rs`), then bails. While it sleeps, no inbound
  message on *any* channel is processed, no agent event on *any* thread is
  processed, no flush fires. One chat hitting Telegram's rate limit freezes
  the entire bridge.
- The sleep-then-bail is doubly wrong: **the callers don't retry.** `flush()`
  propagates the error; the message is lost after a pointlessly long wait. So
  the 429 path is *both* blocking *and* lossy.
- Channel sends (`send`, `edit`, `ask_permission`) are awaited inline. A slow
  `api.telegram.org` adds latency to every other thread's turn.

The fan-in (`forward`/`merge`) is pointless if everything downstream is
serialized. Either run per-thread state machines as separate tasks with the
core as a supervisor, or at minimum move all channel I/O onto a bounded
worker pool with per-thread mailboxes. "One turn per thread" is a fine
invariant; "one *anything* at a time globally" is not the same thing and
wasn't the stated design goal.

### 1.2 `Core` is a god object (medium)

`core.rs` mixes routing, slash-command dispatch, permission bookkeeping,
rendering, spilling, session lifecycle, and hostname reading. The
`/new`/`/cd`/`/agent` handlers are copy-paste triplicates (remove thread →
mutate state → `store.put` → `say`). Command dispatch is a 12-arm match.
This survived to 700 lines because the project is small; it won't survive a
third channel or a fourth command without seams of its own. Extract
`CommandHandler` or at least a `reset_thread(key, mutate)` helper now.

### 1.3 Errors are swallowed exactly where the user is looking (medium)

`decide()` does `channel.edit(...).await.ok()` and `channel.send(...).await.ok()`.
The user tapped **Allow**, the agent *did* run the tool, and if the "Allowed
Bash" confirmation fails to post, nobody knows. Same pattern in
`ack_decision`, `typing`, and the `flush` error paths. A bridge whose only UI
is a chat cannot afford to drop UI errors silently. Log them at `warn` with
thread context, minimum.

### 1.4 The handshake never verifies the gate is armed (high, security)

`ClaudeSession::initialize()` writes the `PreToolUse` hook registration and
**never checks the response**. If registration fails (error-shaped
`control_response` — which `wire.rs` diligently models as `AgentEvent::Error`
and then nothing acts on), every tool runs ungated and no
`PermissionRequest` ever arrives. The user believes they approved a gate;
there is no gate. For a service whose pitch is "shell on request, gated,"
"did the gate install?" is a startup assertion, not a hope. Block `spawn()` on
the initialize response and fail the session if hook registration errors.

### 1.5 Codex: `busy` stuck forever on spawn failure (medium, bug)

`CodexSession::prompt` sets `busy` with `swap(true)` and then
`command.spawn()` — on error, the `?` returns **without clearing `busy`**
(`src/codex.rs`). Compare `ClaudeSession::prompt`, which explicitly resets the
flag on write failure. After one transient `codex` spawn failure, the thread
is permanently "Busy — a turn is already running" until restart. The
CLAUDE.md notes a *successful Codex turn was never verified* — this is the
kind of bug that hides in exactly that blind spot.

### 1.6 Dead config knobs carried as if they were live

`agent::spawn` hard-codes `permission_mode: "default"` and `raw: false`;
`Config.permission_mode`'s doc comment ("`manual` … is what exercises the
permission round-trip") describes a path that is never taken. Either wire them
or delete them — right now they advertise a control that doesn't exist.

---

## 2. Multi-channel support

### 2.1 The claimed architecture doesn't match the code (high, docs)

README: "both seams are out-of-process by design … each adapter can live in
whatever language its SDK does." Reality: seam B is `Box<dyn Agent>` trait
objects in-process; seam A is `Arc<dyn Channel>` in-process; Telegram is a
reqwest client in the same binary. Only Photon's *sidecar* is out of process,
and it's out of process because the SDK is TypeScript, not because of a
design rule. A new channel or agent requires a recompile — which is a
perfectly fine design! It's just not the one the diagram describes.
`ThreadKey.channel: &'static str` makes it structurally impossible for a
dynamically loaded channel to exist. Fix the docs or build the registry; the
current text sets reviewers' expectations you don't meet.

### 2.2 Stale buttons approve the *wrong* tool (high, security)

Telegram's callback payloads are literally `CB_ALLOW`/`CB_DENY` with no
request binding — "at most one permission is outstanding per thread" is doing
all the safety work. Scenario: question A posts with buttons; the user
doesn't tap; the turn somehow moves on (interrupt, error, restart with a
different pending question); question B posts. User taps **Deny** under
question A's text. Core denies **B**. The user just made a decision about a
tool they never saw, based on a message about a different command. Bind the
request id (or a nonce) into `callback_data` — you have 64 bytes and an
empty turn; a short hash fits — and treat a mismatched tap as "nothing
pending" instead of applying it to whatever is current.

### 2.3 Capability model won't survive a third channel (medium)

The channel capability surface is `can_edit`. Real channels differ on more
axes: per-chat rate budget, max message length, edit counting against the
send budget, threading model, reaction-only channels. Right now those live as
scattered magic numbers: `MAX_TEXT` in `telegram.rs`, `MAX_TEXT` in
`photon.rs`, `SPILL_THRESHOLD` in `core.rs`, `FLUSH_INTERVAL` in `core.rs` —
three independent places clip text, none derived from the channel's actual
limit. When channel three arrives (and per the project's own invariant, "the
third implementation grows a capability"), promote these to trait methods:
`max_message_chars()`, `edit_budget()`, maybe `rate_profile()`. Do it *with*
the third channel, but know that today's layout hard-codes two-channel
thinking.

### 2.4 Permission prompts are truncated blind (high, security)

`on_permission` does `serde_json::to_string_pretty(&input)` into the question
and `Telegram::send` clips at 3900 chars with "… truncated". An `Edit` tool
input is the *entire file content plus the new content* — exactly the case
that gets clipped. So the flow is: agent proposes a 200-line rewrite; user
sees the first 3.9k chars (the old file) and taps **Allow**; the diff they
approved is invisible. `render.rs` already has `summarize()` that understands
`command`/`file_path`/`pattern`. The gate — the one human control in the
system — should use it, or spill the full input to a file and link it, the
same treatment long replies get. Approving what you cannot read is the
failure mode the whole gate exists to prevent.

### 2.5 Dedupe window is fragile (low)

Photon's `seen` VecDeque caps at 256 message ids and is shared across
reconnects. If the sidecar replays more than 256 messages on reconnect
(plausible after an hour offline), old ids fall out of the window and replay
dupes reach the core — which then *rejects them as "busy"* if a turn runs, or
re-runs a prompt. Size the window by time, or have the sidecar not replay to
re-subscribers.

### 2.6 Spill filenames can collide (low)

`spill_if_long` names files `{thread}-{unix_secs}.txt`. Two flushes of the
same thread in the same second overwrite; the earlier chat message's pointer
now names a file with different content and a stale character count. Use a
counter or the nanos. Also: spill files are never cleaned up — see 4.4.

---

## 3. Abstraction & extensibility

### 3.1 What genuinely works (credit where due)

- `AgentEvent::Unknown` instead of parse errors, frames as
  `serde_json::Value` at the edge — this is the right call and the tests prove
  it pays (the hook-schema discovery story validates the whole approach).
- Capabilities on traits (`can_edit`, `streams`, `gates_tools`) rather than
  name checks in the core. The core genuinely doesn't know which channel or
  agent it's talking to. Good.
- Codex's `command_execution` normalized onto `Bash` so the gate list and
  renderer stay agent-agnostic. Good.
- The store treats `session_id` as an agent-owned string; the Codex flow
  (agent assigns, core persists) is modeled honestly.
- Tests are real: wire frames are verbatim captures, migration tests build the
  legacy schema. This is the strongest part of the repo.

### 3.2 The seams are untested at the trait level (medium)

Every test is a parser test, a renderer test, or a store test. There is no
`MockChannel`/`MockAgent`, so `Core` — the thing all the abstraction exists
to serve — has tests only for `spill_if_long`, `GATED_TOOLS`, and
`expand_home`. The `decide`/`on_permission`/flush paths, which is where the
security-relevant behavior lives, have zero coverage. Two hundred lines of
mocks would give you: busy-rejection, auto-approve of non-gated tools,
stale-decision handling, flush-on-finished, spill-on-long — the actual
contract of the core. A claim of "capabilities, not names" deserves a test
that swaps a fake and the world still works.

### 3.3 `Agent` trait leaks session shape into the core (low)

`prompt(&mut self)` erroring when busy pushes the one-turn invariant into
every adapter (Codex and Claude both re-implement the same `busy` atomic +
same error string). Fine at two adapters; by four it's copy-paste. A small
`TurnGate` owned by the core would keep the invariant in one place.

### 3.4 `Decision::Allow { updated_input }` is dead air (low)

`updated_input` is constructed nowhere and honored nowhere downstream; the
chat UI has no way to produce it. Carried for ACP symmetry — note it as
aspirational or cut it; the serializer suggests capability that the rest of
the binary ignores.

### 3.5 Error-message string matching (low)

`edit()` treats failure as success if `e.to_string().contains("message is not
modified")`. Telegram localizes and rewords these; the match will silently
break. It's the *correct* place for a typed check (`error_code == 400 &&
description contains …` at least). Low severity, emblematic of stringly-typed
edge handling.

---

## 4. Security

The allowlists are enforced in the adapters before the core, empty allowlists
refuse startup, the sidecar token is per-run and loopback-only, and the env
scrub list is a deliberate fixed list rather than a `CLAUDE*` sweep. Those
are right. Now the buts, roughly in order of how much I'd worry:

### 4.1 The tmux tier silently removes the only control (high)

For claude, a phone user approves every `Bash`/`Write`/`Edit`. For the seven
tmux-tier agents, the agent "runs with its own auto-approve flags"
(`tmux.rs`) — i.e. **unsandboxed, self-approving, unaudited** — and
switchboard hands the phone a `tmux attach` line. The same allowlisted user
who gets a permission prompt on `/agent claude` gets a free remote root shell
on `/agent pi`, in the same thread, one command apart. `/agent` *mentions*
"no Allow/Deny" for codex; the tmux note says "does not stream" but not
"runs ungated and unsandboxed." If this is a deliberate trust decision, say
so loudly — in `/agent`'s reply, in the README, and in `HELP`. Right now the
product's central safety feature is per-agent optional, and the user can't
tell from the UI when it's gone.

### 4.2 `send-keys` is a keystroke injection surface (medium)

`TmuxSession::prompt` passes raw user text as a tmux argument. Two issues:
arguments beginning with `-` are parsed as tmux flags (message `-H` does not
type `-H`), and without `-l`, tmux interprets key *names* — a user message of
exactly `Enter`, `Space`, `BSpace`, `C-c` is not typed but *pressed*. So a
user trying to send the text "C-c" interrupts the agent. Use `send-keys -l`
and a `--` separator equivalent (pass the text after a literal `-l`), and
consider what "type arbitrary text into a live REPL" should do with control
characters. The allowlist makes this an authenticated footgun rather than a
vulnerability — but it's exactly the kind that bites the operator, not the
attacker.

### 4.3 No audit trail for decisions (medium)

`on_inbound` logs the message; `decide()` logs nothing. A service whose
entire purpose is executing shell commands from a chat should record: tool
name, gate decision, who made it (button token vs `/allow` text), and when.
Today, reconstructing "who ran `rm -rf` in my tree at 02:00" means grepping
agent transcripts. Journald is right there; three `tracing::info!` lines fix
this.

### 4.4 Spill files leak and accumulate (medium)

`spill_if_long` writes agent output — which routinely contains secrets,
tokens, and file contents — to `~/.local/state/switchboard/out/` with the
process umask (typically 0644, world-readable on a multi-user box) and never
deletes it. The README pitches this as the answer to long output. Set 0600
explicitly, and either expire files (mtime sweep on startup) or state plainly
that out/ is a permanent transcript. Also the suggested reader is
`ssh host -t 'cat …'` — which will mangle anything with quotes/backslashes
and assumes tailnet ssh; a `switchboard read <file>` subcommand would be both
safer and correct.

### 4.5 Token in error chains (low)

`Telegram::call` builds URLs as `bot{token}/method` and uses
`with_context(|| format!("telegram {method}"))` — but `{:e}`-style error
chains from reqwest can include the full URL, token included, into journald.
`whoami`'s failure context wraps the reqwest error. Scrub the token from the
base URL before attaching it to errors, or build the client with a header
instead of an in-URL credential. Same class of leak: `start_telegram` logs
`allowed` but not the token — good — keep it that way everywhere.

### 4.6 `/cd` path handling (low)

`expand_home` handles `~/` but not a bare `~`; relative paths resolve against
switchboard's cwd, not the thread's current cwd (surprising); and nothing
prevents `/cd /etc` — the agent then runs in `/etc` with the phone user's
full environment. That's by design ("explicit state") but there's no
allowlist on *directories*, only on people. One wrong paste of `/cd /` from a
phone keyboard and the next "run tests" eats the whole disk. Consider
confining to a root (`--dir` hierarchy) unless overridden by a flag the
operator sets.

### 4.7 Sidecar nits (low)

Token comparison isn't constant-time (fine on loopback), the `spaces` cache
is unbounded (fine until it isn't), and `PHOTON_SIDECAR_TOKEN` on the child
env is visible to any same-user process — all acceptable, none documented as
considered. The loopback binding is the real defense and it is correct.

---

## 5. Smaller correctness notes (unfiled, still real)

- **`/stop` leaves the adapter's pending map dirty.** Core clears
  `thread.pending` but `ClaudeSession.pending` still holds the unanswered
  request id. Harmless today (interrupt supersedes), but the two "pending"
  concepts can disagree across a cancel/decide race.
- **repl one-shot hang:** a non-slash line that `prompt()` rejects
  ("[rejected]") still sets `turn_running = true` in `repl()` — with stdin
  closed the loop then waits forever for a turn that never started.
- **`hostname()` fallback `"localhost"`** produces a confidently broken ssh
  line. Fail the spill pointer instead.
- **`on_decision` denies with the fixed reason "denied from chat"** even when
  tapped via button; the Deny button gives the model nothing to work with.
  Accept the tradeoff, or ask for a reason on deny.
- **Empty `message_id` from Telegram** (`unwrap_or_default`) is stored and
  later `parse::<i64>("") → 0`, so every edit of that message fails
  permanently for the turn. Treat empty as an error at send time.
- **`default_sidecar_dir` uses `CARGO_MANIFEST_DIR`** — the *build machine's*
  source path baked into the binary. Works here; breaks for anyone who
  installs the binary without the source tree. `--photon-sidecar` exists;
  make the installed path the default story.

---

## 6. What I'd do first (priority order)

1. Verify the hook handshake at `ClaudeSession::spawn` — no session without a
   confirmed gate. (Security, small diff.)
2. Move channel I/O off the core task; fix the 429 sleep-then-lose path.
   (Architecture, the one that hurts in production.)
3. Summarize or spill permission-prompt input; never let the gate clip what
   it's gating. (Security.)
4. Bind nonce into Telegram callback_data; reject stale taps. (Security.)
5. Clear `busy` on Codex spawn failure; say "ungated, unsandboxed" out loud in
   `/agent` and the README for the tmux tier. (Correctness + honesty.)
6. Mode 0600 + expiry on spill files; audit-log every decision.
7. MockChannel/MockAgent integration tests for `Core`'s contract.
8. Fix the architecture diagram to match the in-process reality.

The bones are good. The two things this project is *for* — bridging channels
and gating a shell — are precisely the two places where the current code
blocks globally and gates blind. Fix those and the rest is polish.

---

# Part 2 — Design decisions

Code-level findings above; this part argues with the decision record itself.
Section 1.1 covered the serialized core as an implementation defect — here it
is treated as the *consequence* of a design choice, alongside the decisions
that have no code-level symptom yet but constrain everything built on top.

## D1. "Reject the second message" is the right default and the wrong only option

The invariant is enforced at the session boundary (`Agent::prompt` errors when
busy) rather than at the product boundary. That means the architecture cannot
later express "queue one, reject the third" — or any per-thread policy — without
renegotiating the trait contract with every adapter. From a phone, a message
sent two seconds after the first while the agent is still reading files gets
an error. Rejecting *interruptions* mid-turn is defensible; rejecting *arrivals*
is an error message where the user expected to be heard. A queue of depth one
per thread costs almost nothing in the core and changes the invariant from
"one turn ever in flight" to "one turn running" — the former is a phone UX
guess baked into a protocol seam, the latter is a real constraint.

## D2. Sessions are keyed by chat thread, not by working tree

One thread ↔ one session matches chat intuition, but the collision the design
table claims to avoid — "two threads collide in one working tree" — still
happens; the chosen design just cannot see it. Two Telegram topics, or
Telegram plus iMessage, each get a live agent session in the *same* `cwd`,
sharing one git index and one build cache, with no awareness of each other
and nothing to stop interleaved writes to the same files. The core has no
concept of a working tree at all — only of threads — so the fix (a per-cwd
lock, or at minimum a "another thread is working here" warning) is not
expressible without a new axis of identity. That is the architectural tell:
the domain's natural unit of concurrency (the tree) was modeled as a
per-thread string attribute.

## D3. Continuity is one string, with three fragilities

The entire resume story is `session_id` in SQLite handed back to the agent:

- **It is a composite value smuggled in as two columns.** `session_id` is
  only meaningful in the context of the `agent` column, but nothing in the
  schema or the type says so. `/agent` clears it (silently discarding the
  previous agent's conversation — and switching *back* starts cold again).
- **The store has no notion of a turn in flight.** `kill_on_drop` + resume
  preserves the conversation but not the turn. On iMessage, where a turn
  arrives whole at the end, a service restart means the user's question gets
  no answer *and no error* — the systemd unit restarts, the session resumes,
  and the vanished turn is indistinguishable from the agent being slow. A
  "turn in flight" marker would at least let the bridge apologize on boot.
- **Codex's id assignment has a loss window.** The thread id is captured from
  `thread.started`, forwarded as `Ready`, and persisted by the core — three
  hops, any of which a crash can interrupt, after which Codex's own record of
  the conversation is orphaned. Claude doesn't have this race (id chosen
  upfront), which is likely why the design didn't account for it. The
  generalizable fix: adapters that don't own their id should persist it
  themselves, or the `Ready` event should be a blocking ack.

## D4. Seam B has four verbs; the domain needs five

Prompt, streamed update, permission request, cancel. Missing: **any way to
move a file in either direction.** `AgentEvent` cannot carry an attachment,
`Channel` cannot receive one, `InboundKind` has no variant for it. The
roadmap defers attachments — fine as prioritization — but this is an
*architectural* omission, not a feature flag: adding the verb touches every
layer of the normalized model at once. That is what cutting a protocol one
verb short looks like two years later.

Related: `Decision::Allow { updated_input }` was imported from ACP's surface
without ACP's power. An ACP client can rewrite the tool call; a chat user
cannot, and no UI path produces `updated_input`. The seam advertises a
capability the product cannot deliver — and reviewer suspicion should always
attach to exactly that shape.

## D5. The event fan-in is elegant and is the cause of finding 1.1

Per-session `forward()` tasks merging into one `mpsc<(ThreadKey,
AgentEvent)>` let the core `select!` over a single receiver — and thereby
serialize all *handling*, which is finding 1.1. The fan-in solved "N
receivers," but N receivers only existed because the core insists on owning
the sessions. Hand each thread a task that selects on its own session
receiver (with the core as registry and inbound router) and the concurrency
model finally matches the unit of isolation, which was always the thread.
The current design multiplexes the transport and then refuses to multiplex
everything after it.

## D6. Three stores, no source of truth, no reconciliation

Thread state (memory), `ThreadState` (SQLite), and the agent's own
transcript store — maintained by convention:

- `Thread.session_id` (live) and `ThreadState.session_id` (persisted) are
  updated independently; a crash between the agent reporting `Ready` and the
  core writing the row leaves them diverged.
- The outstanding permission lives only in memory. Restart mid-question and
  the user's chat still shows a question with buttons that now mean nothing;
  nothing on boot reconciles UI state against session state.
- The renderer's `message_id` is memory-only; after a restart mid-turn the
  partially grown Telegram message is orphaned and the resumed turn starts a
  new one.

`Core::run` simply starts trusting itself. The design assumes crashes happen
at turn boundaries, which is precisely the assumption a long-running service
with auto-restart violates. A boot-time reconcile pass — "for each stored
thread, is there a partial turn or an unanswered question? say so" — would
acknowledge the multiplicity instead of papering over it.

## D7. The "no name checks" invariant leaks at the edges it was meant to protect

The core checks `can_edit()`/`streams()` and genuinely doesn't know its
channels. But the same core also hard-codes Claude Code's tool vocabulary
(`GATED_TOOLS` — matched by display name, so any MCP tool that happens to
share a name with a builtin is gated while its namespaced siblings sail
through), hard-codes the seven-agent tmux list *inside the `/attach` help
text* (a format string in `core.rs`), and matches on `Backend` to write the
per-agent switching notes. All three are the name-checking the invariants
forbid, in the two places where the trait surface ran out: "what vocabulary
do your tools speak" and "how do I describe your limits to a user." Two more
capability methods — or a tool-classification hook owned by the adapter —
would close the seam properly. The invariant held exactly until the second
feature needed it.

## D8. The detached tier manufactures events, and the type system doesn't know

`TmuxSession::prompt()` sends keystrokes, then immediately emits a synthetic
`Text` and `TurnEnd`. `is_busy()` is always false; `cancel` injects Ctrl-C
into a pane. This is honest in the docs and dishonest in the type: every
consumer of `AgentEvent` must silently tolerate that one backend's entire
event stream is fabricated, and the only marker is `streams() == false` — a
statement about the *agent*, not about the event stream's provenance. The
event model's contract ("these describe what the agent did") is violated in
exactly one implementation, with no way to distinguish it. A separate
`DetachedHandle` type — or an explicit `Synthetic` marker — would keep the
lie out of the vocabulary that structured agents share.

## D9. Decisions that hold up — recorded so the next refactor doesn't undo them

- **Parse-loose/normalize-strict at the wire** is validated by the project's
  own debugging history (the hook schema was recovered from a validation
  error, not documentation). Most likely decision to still be right in two
  years.
- **`session_id` as a plain agent-owned string** in the store — resisting a
  UUID column — is what let Codex arrive without a migration. Underrated call.
- **Capabilities on the seam traits** for the two axes that matter
  (editability, streaming, gating) genuinely kept the core vendor-blind; the
  leak (D7) is at the periphery, not the seam.

## The one-sentence version

The architecture isolates vendors correctly but too little above them: one
global event loop where there should be per-thread actors, one session slot
per thread where there should be a working-tree view, and a four-verb
protocol where the domain needs five.
