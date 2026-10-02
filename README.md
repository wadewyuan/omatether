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
| `/attach` | The ssh command to take over at a real terminal |
| `/auto [on\|off]` | Whether tool calls run without asking. **On by default** |
| `/allow`, `/deny <why>` | Answer a permission question |
| `/help` | This list |

Anything else goes to the agent as typed, including its own slash commands,
so `/review` works.

**Photos and files** are saved to `~/.local/state/omatether/in/` and passed to
the agent as a file path, with your caption as the prompt. The limit is 20 MB.
Files are private to you and deleted after a week.

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

## Running it

```bash
systemctl --user status omatether
journalctl --user -fu omatether
```

It opens no ports. Telegram is polled from this machine, and the iMessage
helper listens on localhost only.

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
