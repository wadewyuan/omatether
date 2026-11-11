# omatether

Your coding agent, over chat. Text it from your phone on Telegram or iMessage,
or let another AI agent on your tailnet hand it work over MCP. It runs the
agent installed on your machine (Claude Code, Codex, or whichever one
`omarchy default agent` points at) in your working tree, and sends the answer
back to whoever asked.

This isn't a general chat assistant. What answers is *your* agent, with your
`CLAUDE.md`, your MCP servers and your files.

Three channels, all optional, all running the same sessions and commands:

| Channel | Who uses it | How it reaches this machine |
|---|---|---|
| Telegram | You, from your phone | Polled from here; opens no port |
| iMessage | You, from your phone | Via [Photon](https://app.photon.codes/); helper on localhost |
| MCP | Other AI agents on your tailnet | HTTP on this machine's Tailscale address only, API key required |

## Install

On Omarchy, or any Arch:

```bash
yay -S omatether-bin    # prebuilt
yay -S omatether        # or build from source
omatether setup
```

You need at least one agent CLI on the machine (`claude`, `codex`, `pi`, or
any agent Omarchy can launch), plus `node` if you want iMessage and
[Tailscale](https://tailscale.com) if you want MCP.

## Setup

`omatether setup` walks you through it. It checks each credential the moment
you paste it, writes `~/.config/omatether/env` (mode 600), then starts the user
service and confirms it stayed up. Running it again is safe: anything already
configured is kept.

**Telegram.** Create a **new** bot with [@BotFather](https://t.me/BotFather)
and paste its token. Don't reuse a bot another program uses: only one program
can read a bot's messages, and two would steal each other's. Setup then asks
you to message the bot once, and takes your user id from that message.

**iMessage (optional).** Uses [Photon](https://app.photon.codes/), a hosted
service, so no Mac is needed. Create a project there, and give setup its id,
its secret and your phone number. Setup registers your number and prints the
number you text. Each project gets its own line, so a new project means a new
number to text.

Only the Telegram user and phone number you give setup can use the bridge. A
message from anyone else is ignored and never answered.

**MCP** is not part of `omatether setup`; it is off until you turn it on. See
[Other agents on your tailnet](#other-agents-on-your-tailnet-mcp).

## Using it

Send a message and the agent works on it. On Telegram its reply builds up in
place as it works. iMessage can't edit a sent message, so there you see typing
dots, then the answer arrives whole. A long answer continues in the next
message rather than being cut.

| Command | Effect |
|---|---|
| `/new` | Fresh session, starting in `~/Work` |
| `/stop` | Interrupt the running turn |
| `/cd <path>` | Change the working directory (starts a fresh session) |
| `/agent <name>` | Switch agent: claude, codex, pi, omp, opencode, crush, grok, gemini, copilot |
| `/model [name]` | List models, or switch without losing the conversation |
| `/status` | Agent, model, directory, and whether a turn is running |
| `/log` | The last turn in full, with every tool call |
| `/send <path>` | Send a file from this machine to the chat |
| `/attach` | The ssh command to take over at a real terminal |
| `/auto [on\|off]` | Whether tool calls run without asking. **On by default** |
| `/allow`, `/deny <why>` | Answer a permission question |
| `/help` | This list |

Anything else goes to the agent as typed, including its own slash commands,
so `/review` works.

**Photos and files** are saved to `~/.local/state/omatether/in/` and passed to
the agent as a file path, with your caption as the prompt. The limit is 20 MB.
Files are private to you and deleted after a week.

**Files come back too.** Ask the agent for one, or type `/send <path>`. Only
files in the thread's working directory, omatether's own inbox, or the temp
directory are sent, up to 50 MB, and nothing hidden, so `~/.ssh` and `.env`
stay put even when the agent is working in your home directory.

## Agents, and what they may do without asking

**Tool calls run without asking by default.** Asking about every command from
a phone quickly turns into tapping Allow without reading, which only looks
like review. Use `/auto off` in a thread to be asked before Claude runs
`Bash`, `Write`, `Edit` or `NotebookEdit`. On Telegram you get Allow / Deny
buttons; on iMessage you reply `/allow` or `/deny`.

Only Claude can be asked at all:

| Agent | Streams replies | Can ask before a tool call | Model choice |
|---|---|---|---|
| `claude` | yes | **yes** | yes |
| `codex` | yes | no | yes |
| `pi` | yes | no | yes |
| omp, opencode, crush, grok, gemini, copilot | no | no | no |

> **Codex, Pi and the last six run their tools as they decide, and omatether
> cannot stop them.** Switching to one is one `/agent` away for anyone on the
> allowlist. `/agent` says so when you switch.

The last six run in a tmux session through `omarchy-agent`. Their replies
appear in that terminal rather than in the chat; `/attach` gives you the
command to open it.

## Other agents on your tailnet (MCP)

omatether can also be an [MCP](https://modelcontextprotocol.io) server, so an
AI agent on another machine on your Tailscale network (a cloud agent's
sandbox, a laptop, a CI runner) can hand tasks to the agents on this one and
get the answers back. To the agents here it is just another channel: each
caller gets sessions with their own conversation, directory and agent, the
same commands work, and auto mode applies as it does everywhere.

### Turn it on

Add to `~/.config/omatether/env`, then `systemctl --user restart omatether`:

```
OMATETHER_MCP=on
# OMATETHER_MCP_PORT=8742                      # the default
# OMATETHER_MCP_ALLOWED=laptop,ci-runner       # tailnet device or login names; unset = any
```

It listens on this machine's Tailscale addresses only, never on the LAN or
localhost, and starts once Tailscale is up. Every caller has to get past
three checks:

1. **On your tailnet.** Anything else cannot even connect, and every request's
   device is looked up with `tailscale whois`.
2. **An API key.** Each agent gets its own, sent as
   `Authorization: Bearer omt_...` (or `X-API-Key: omt_...`). No key, or no
   keys made yet, means the request is refused.
3. **Not a browser.** A request carrying an `Origin` header is refused, so a
   web page open on one of your devices cannot reach in.

### Give an agent a key

Let the agent collect its key itself, so it is never shown anywhere else: not
in your terminal, not in a chat, not to the agent you asked to set it up.

```bash
omatether mcp-key offer my-agent --node <its tailnet device>   # open 15 min; --minutes N
```

Then tell that agent to run, once:

```bash
curl -sS -X POST http://<this-machine>:8742/claim
```

and keep the output as a secret. The key is created at that moment and is in
that response only. Only the named device can claim it (Tailscale says which
device is connecting, not the request), only once, and only before the offer
expires. `omatether mcp-key withdraw my-agent` cancels an offer.

```bash
omatether mcp-key list               # keys and open offers, by name
omatether mcp-key revoke my-agent    # takes effect on the next request
omatether mcp-key add my-agent       # prints a key here instead, once
```

Only a SHA-256 of each key is kept, in `~/.config/omatether/mcp-keys`. Use
`add` only at a terminal you are sitting at: a key printed in a chat or a log
is a key someone else can use.

### Connect the agent

The endpoint is `http://<this-machine>:8742/mcp`, Streamable HTTP. For Claude
Code:

```bash
claude mcp add --transport http omatether http://<this-machine>:8742/mcp \
  --header "Authorization: Bearer $OMATETHER_MCP_KEY"
```

Any MCP client that can send a header works the same way.

### What it can do

| Tool | Does |
|---|---|
| `run_task` | Send a prompt, and any files, to a session's agent and wait for its answer |
| `get_result` | Keep waiting on a task that came back `running` |
| `stop_task` | Interrupt the running task; the conversation is kept |
| `session_command` | Any chat command: `/status`, `/new`, `/cd`, `/agent`, `/model`, `/auto`, `/allow`, `/deny`, `/log`, `/send`, `/help` |
| `list_sessions` | This key's sessions, and which are running |

- **Sessions.** Named by the caller (`default` if not), one conversation each,
  kept across restarts. They belong to a key, so two agents never share one.
  Sessions run in parallel; a session runs one task at a time, and a second is
  refused until the first finishes or is stopped. New sessions start in the
  service's `--dir`; `/cd` moves them.
- **Long tasks.** A call waits up to `wait_seconds` (default 45, at most 600)
  and then returns `running`, so it stays inside the client's timeout. That
  reply shows how far the agent has got (each tool call and what it has written
  so far), so the caller can keep waiting with `get_result`, redirect it or
  stop it. Only the final answer comes back when it finishes; the working is
  in `/log`. Nothing is streamed.
- **Files in.** `run_task` takes `files`: each a `name` and its contents as
  base64 `data`, or plain `text`, up to 20 MB each. They land in the same
  private inbox as a photo from your phone, deleted after a week, and the agent
  gets their paths.
- **Files out.** Whatever the agent hands over (or `/send <path>`) comes back
  inline in the result, images as images, up to 20 MB, under the same rules as
  on the chats: only from the working directory, the inbox or the temp
  directory, and nothing hidden.
- **The agent here knows who is asking.** It is told an AI agent is calling
  and that only its final message is returned, so it writes that message to
  stand on its own: what it did, the result, exact paths and values.

> **Auto mode is on here too.** An agent holding a key can run commands on
> this machine as you, without asking. `/auto off` in a session makes Claude
> ask first, and the question comes back as the result for the caller to
> answer with `/allow` or `/deny`. Give each agent its own key so one can be
> revoked without the others, and use `OMATETHER_MCP_ALLOWED` or your tailnet
> ACLs to narrow which devices may connect at all.

## Running it

```bash
systemctl --user status omatether
journalctl --user -fu omatether
```

It opens no ports unless you turn on MCP, and then only on the tailnet.
Telegram is polled from this machine, and the iMessage helper listens on
localhost only. Every MCP request is logged with the key and device it came
from (`journalctl --user -u omatether | grep mcp`).

**Manual setup**, if you'd rather not use `omatether setup`: write
`~/.config/omatether/env` with mode 600:

```
OMATETHER_TELEGRAM_TOKEN=123456:AA...
OMATETHER_TELEGRAM_ALLOWED_USERS=<your numeric telegram id>

OMATETHER_PHOTON_PROJECT_ID=...
OMATETHER_PHOTON_PROJECT_SECRET=...
OMATETHER_PHOTON_ALLOWED_USERS=<your phone, e.g. +15551234567>

OMATETHER_MCP=on
```

Any one channel is enough; leave out the ones you don't use.

then `systemctl --user enable --now omatether`. A source build runs
`omatether setup` the same way. It writes a unit for the binary you ran it
from, and the iMessage helper needs `npm install` in `vendor/photon-sidecar`
once (setup offers to run it).

## Uninstall

```bash
systemctl --user disable --now omatether
rm -f ~/.config/systemd/user/omatether.service   # only exists for source builds
rm -rf ~/.config/omatether ~/.local/state/omatether
yay -R omatether-bin                             # or omatether
```

Also revoke the bot with @BotFather if you won't use it again, and remove the
omatether entry from any agent you gave an MCP key (the keys die with
`~/.config/omatether`).

## More

[docs/internals.md](docs/internals.md) covers how it's built: architecture,
how Claude Code's tools are gated, decisions taken, and what is verified.
[CLAUDE.md](CLAUDE.md) has the per-agent and per-channel details learned the
hard way.

MIT licensed.
