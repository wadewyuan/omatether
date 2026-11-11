# omatether

Your coding agent, over chat. Text it from your phone on Telegram or iMessage.
It runs the agent installed on your machine (Claude Code, Codex, or whichever
one `omarchy default agent` points at) in your working tree, and sends the
answer back to the chat.

This isn't a general chat assistant. What answers is *your* agent, with your
`CLAUDE.md`, your MCP servers and your files.

## Install

On Omarchy, or any Arch:

```bash
yay -S omatether-bin    # prebuilt
yay -S omatether        # or build from source
omatether setup
```

You need at least one agent CLI on the machine (`claude`, `codex`, `pi`, or
any agent Omarchy can launch), plus `node` if you want iMessage.

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

omatether can also be an MCP server, so an AI agent on another machine on your
[Tailscale](https://tailscale.com) network can hand tasks to the agents on
this one. Add to `~/.config/omatether/env`:

```
OMATETHER_MCP=on
# OMATETHER_MCP_PORT=8742                      # the default
# OMATETHER_MCP_ALLOWED=macbook-air,ci-runner  # node or login names; unset = whole tailnet
```

and restart the service. It listens on this machine's Tailscale addresses
only, not on the LAN or localhost, and every caller is identified with
`tailscale whois`.

**Every agent needs its own API key.** Without one, a request is refused;
with no keys made at all, every request is.

```bash
omatether mcp-key add grokbot     # prints the key, once
omatether mcp-key list
omatether mcp-key revoke grokbot  # takes effect at once, no restart
```

Better, so the key is never shown anywhere but on the agent's machine: offer
it to that device and let the agent collect it.

```bash
omatether mcp-key offer grokbot --node grokbot-user-518630511   # open 15 minutes
# then, on that device, once:
curl -sS -X POST http://omarchy:8742/claim
```

The key is made at the claim and returned only in that response. Only the
named tailnet device can claim it (checked with `tailscale whois`, not by
anything the request says), only once, and only before the offer expires.

Only a hash of each key is stored, in `~/.config/omatether/mcp-keys`. Point
the agent's client at `http://<this-machine>:8742/mcp` (Streamable HTTP) with
the key as a bearer token, for example:

```bash
claude mcp add --transport http omatether http://omarchy:8742/mcp \
  --header "Authorization: Bearer omt_..."
```

`X-API-Key: omt_...` works too, for clients that only take a plain header.

| Tool | Does |
|---|---|
| `run_task` | Send a prompt, and any files, to a session's agent and wait for its answer |
| `get_result` | Keep waiting on a task that returned `running` |
| `stop_task` | Interrupt it |
| `session_command` | `/status`, `/new`, `/cd`, `/agent`, `/model`, `/auto`, `/allow`, `/deny`, `/log`, `/send` |
| `list_sessions` | The caller's sessions and which are running |

Each key gets its own sessions, named by its agent (`default` if not), and each
is a thread like a chat: its own conversation, directory and agent, kept
across restarts. Sessions run in parallel; one session runs one task at a
time. A call waits up to `wait_seconds` (default 45) and then returns
`running`, so a long task never trips the client's timeout. A `running` reply
says how far the agent has got (each tool call it ran and what it has written
so far), so the caller can keep waiting, redirect it or stop it.

**Files go both ways.** `run_task` takes `files`, each a `name` with its
contents as base64 `data` (or plain `text`), up to 20 MB each. They land in the
same inbox as a photo from your phone, private and deleted after a week, and the
agent gets their paths. Files the agent hands back arrive inline in the result,
images as images, under the same rules as `/send`.

> **Auto mode is on here too**, as on every channel: an agent holding a key
> can run commands on this machine as you. Give each agent its own key, so
> one can be revoked without the others. `OMATETHER_MCP_ALLOWED` or your
> tailnet ACLs can also narrow which devices may connect at all.

## Running it

```bash
systemctl --user status omatether
journalctl --user -fu omatether
```

It opens no ports unless you turn on MCP, and then only on the tailnet.
Telegram is polled from this machine, and the iMessage helper listens on
localhost only.

**Manual setup**, if you'd rather not use `omatether setup`: write
`~/.config/omatether/env` with mode 600:

```
OMATETHER_TELEGRAM_TOKEN=123456:AA...
OMATETHER_TELEGRAM_ALLOWED_USERS=<your numeric telegram id>

OMATETHER_PHOTON_PROJECT_ID=...
OMATETHER_PHOTON_PROJECT_SECRET=...
OMATETHER_PHOTON_ALLOWED_USERS=<your phone, e.g. +15551234567>
```

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

Also revoke the bot with @BotFather if you won't use it again.

## More

[docs/internals.md](docs/internals.md) covers how it's built: architecture,
how Claude Code's tools are gated, decisions taken, and what is verified.
[CLAUDE.md](CLAUDE.md) has the per-agent and per-channel details learned the
hard way.

MIT licensed.
