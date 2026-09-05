//! switchboard — your coding agent, reachable from a chat thread.
//!
//! `serve` runs the bridge. `repl` is the milestone-1 instrument: the same
//! agent driver with a terminal on the front, useful for watching the wire
//! protocol when a Claude Code release changes it.

mod agent;
mod channel;
mod claude;
mod command;
mod core;
mod event;
mod render;
mod store;

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, BufReader};
use uuid::Uuid;

use crate::agent::Agent;
use crate::channel::photon::{self, Photon};
use crate::channel::telegram::Telegram;
use crate::channel::{Channel, Inbound};
use crate::claude::{ClaudeSession, Config};
use crate::core::Core;
use crate::event::{AgentEvent, Decision};
use crate::store::Store;

#[derive(Parser, Debug)]
#[command(name = "switchboard", about = "Your coding agent, over chat")]
struct Args {
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Run the bridge: Telegram in, agent out.
    Serve {
        /// Working directory for threads that have not set one.
        #[arg(long, default_value = ".")]
        dir: PathBuf,

        /// State database. Defaults to $XDG_STATE_HOME/switchboard/state.db.
        #[arg(long)]
        state: Option<PathBuf>,

        /// The Photon sidecar directory. Defaults to the copy vendored beside
        /// this binary's source.
        #[arg(long)]
        photon_sidecar: Option<PathBuf>,
    },

    /// Drive one agent session from this terminal.
    Repl {
        #[arg(long, default_value = ".")]
        dir: PathBuf,

        #[arg(long, default_value = "default")]
        permission_mode: String,

        /// Resume an existing session instead of starting a new one.
        #[arg(long)]
        session_id: Option<Uuid>,

        /// Echo every raw frame from the agent to stderr.
        #[arg(long)]
        raw: bool,

        /// Send this prompt immediately on start.
        prompt: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "switchboard=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    match Args::parse().command {
        Mode::Serve {
            dir,
            state,
            photon_sidecar,
        } => serve(dir, state, photon_sidecar).await,
        Mode::Repl {
            dir,
            permission_mode,
            session_id,
            raw,
            prompt,
        } => repl(dir, permission_mode, session_id, raw, prompt).await,
    }
}

// ---- serve -------------------------------------------------------------

async fn serve(
    dir: PathBuf,
    state: Option<PathBuf>,
    photon_sidecar: Option<PathBuf>,
) -> Result<()> {
    let dir = dir.canonicalize().unwrap_or(dir);
    if !dir.is_dir() {
        bail!("--dir is not a directory: {}", dir.display());
    }

    let state_path = state.unwrap_or_else(default_state_path);
    let store = Store::open(&state_path)?;

    // Every channel's inbound stream funnels into one receiver, so the core
    // selects over a single source no matter how many channels are configured.
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel::<Inbound>(64);
    let mut channels: Vec<Arc<dyn Channel>> = Vec::new();

    if let Some(telegram) = start_telegram().await? {
        merge(telegram.clone().start_polling(), inbound_tx.clone());
        channels.push(telegram);
    }

    if let Some(photon) = start_photon(photon_sidecar).await? {
        merge(photon.clone().start_streaming(), inbound_tx.clone());
        channels.push(photon);
    }

    if channels.is_empty() {
        bail!(
            "no channels configured. Set SWITCHBOARD_TELEGRAM_TOKEN (with \
             SWITCHBOARD_TELEGRAM_ALLOWED_USERS), or SWITCHBOARD_PHOTON_PROJECT_ID \
             (with _SECRET and SWITCHBOARD_PHOTON_ALLOWED_USERS), or both."
        );
    }

    tracing::info!("default dir {}", dir.display());
    tracing::info!("state {}", state_path.display());

    Core::new(store, channels, dir).run(inbound_rx).await
}

/// Telegram is configured when a token is present; absent is not an error.
async fn start_telegram() -> Result<Option<Arc<Telegram>>> {
    let token = match std::env::var("SWITCHBOARD_TELEGRAM_TOKEN") {
        Ok(token) => token,
        Err(_) => return Ok(None),
    };

    let allowed = allowlist("SWITCHBOARD_TELEGRAM_ALLOWED_USERS").context(
        "SWITCHBOARD_TELEGRAM_ALLOWED_USERS is not set. A bot token in a chat is \
         a shell on this machine — list the Telegram user ids allowed to use it, \
         comma separated.",
    )?;

    let telegram = Arc::new(Telegram::new(&token, allowed.clone())?);
    let username = telegram
        .whoami()
        .await
        .context("could not reach Telegram — check the token")?;

    tracing::info!("telegram @{username}, allowed: {}", allowed.join(", "));
    Ok(Some(telegram))
}

/// Photon is configured when a project id is present.
async fn start_photon(sidecar: Option<PathBuf>) -> Result<Option<Arc<Photon>>> {
    let project_id = match std::env::var("SWITCHBOARD_PHOTON_PROJECT_ID") {
        Ok(id) => id,
        Err(_) => return Ok(None),
    };

    let project_secret = std::env::var("SWITCHBOARD_PHOTON_PROJECT_SECRET")
        .context("SWITCHBOARD_PHOTON_PROJECT_SECRET is not set")?;

    let allowed = allowlist("SWITCHBOARD_PHOTON_ALLOWED_USERS").context(
        "SWITCHBOARD_PHOTON_ALLOWED_USERS is not set — list the phone numbers \
         allowed to message this bridge, comma separated.",
    )?;

    let port: u16 = std::env::var("SWITCHBOARD_PHOTON_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8789);

    let sidecar_dir = sidecar.unwrap_or_else(default_sidecar_dir);

    let photon = Arc::new(
        Photon::start(photon::Config {
            sidecar_dir,
            project_id,
            project_secret,
            port,
            allowed_users: allowed.clone(),
        })
        .await?,
    );

    tracing::info!("photon ready, allowed: {}", allowed.join(", "));
    Ok(Some(photon))
}

fn allowlist(var: &str) -> Result<Vec<String>> {
    let raw = std::env::var(var)?;
    let entries: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if entries.is_empty() {
        bail!("{var} is empty");
    }
    Ok(entries)
}

/// Pump one channel's inbound stream into the shared receiver.
fn merge(mut source: tokio::sync::mpsc::Receiver<Inbound>, sink: tokio::sync::mpsc::Sender<Inbound>) {
    tokio::spawn(async move {
        while let Some(message) = source.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });
}

fn default_sidecar_dir() -> PathBuf {
    // Beside the source tree this binary was built from, so a `cargo run`
    // during development and an installed binary both find it.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("vendor/photon-sidecar")
}

fn default_state_path() -> PathBuf {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("switchboard/state.db")
}

// ---- repl --------------------------------------------------------------

struct Pending {
    request_id: String,
    tool: String,
}

async fn repl(
    dir: PathBuf,
    permission_mode: String,
    session_id: Option<Uuid>,
    raw: bool,
    prompt: Option<String>,
) -> Result<()> {
    let config = Config {
        cwd: dir.clone(),
        session_id: session_id.unwrap_or_else(Uuid::new_v4),
        permission_mode,
        raw,
    };

    println!("session  {}", config.session_id);
    println!("dir      {}", dir.display());
    println!("commands /allow  /deny <why>  /cancel  /status  /quit");
    println!();

    let (mut session, mut events) = ClaudeSession::spawn(config).await?;

    if let Some(prompt) = prompt.as_deref() {
        session.prompt(prompt).await?;
        println!("> {prompt}");
    }

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut pending: Option<Pending> = None;
    let mut streamed = false;

    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Some(event) => if render(&event, &mut streamed, &mut pending) { break },
                    None => break,
                }
            }

            line = stdin.next_line() => {
                match line? {
                    Some(line) => {
                        if handle_input(line.trim(), &mut session, &mut pending).await? {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    session.shutdown().await?;
    Ok(())
}

/// Render one event. Returns true when the session is over.
fn render(event: &AgentEvent, streamed: &mut bool, pending: &mut Option<Pending>) -> bool {
    match event {
        AgentEvent::Ready {
            session_id,
            model,
            tools,
            ..
        } => println!(
            "[ready] {} · {} · {} tools",
            session_id,
            model.as_deref().unwrap_or("?"),
            tools.len()
        ),

        AgentEvent::TextDelta { text } => {
            *streamed = true;
            print!("{text}");
            let _ = std::io::stdout().flush();
        }

        AgentEvent::Text { text } => {
            if !*streamed {
                println!("{text}");
            }
        }

        AgentEvent::Thinking { .. } => println!("\n[thinking]"),

        AgentEvent::ToolCall { name, input, .. } => {
            println!("\n[tool] {name} {}", compact(input));
        }

        AgentEvent::PermissionRequest {
            request_id,
            tool,
            input,
            ..
        } => {
            println!("\n[permission] {tool} {}", compact(input));
            println!("             /allow or /deny <why>");
            *pending = Some(Pending {
                request_id: request_id.clone(),
                tool: tool.clone(),
            });
        }

        AgentEvent::TurnEnd { ok, detail } => {
            *streamed = false;
            println!(
                "\n[turn end] {}{}",
                if *ok { "ok" } else { "failed" },
                detail
                    .as_deref()
                    .filter(|d| *d != "success")
                    .map(|d| format!(" ({d})"))
                    .unwrap_or_default()
            );
        }

        AgentEvent::RateLimit {
            five_hour,
            seven_day,
        } => {
            if five_hour.unwrap_or(0.0) > 0.8 || seven_day.unwrap_or(0.0) > 0.8 {
                println!(
                    "\n[rate limit] 5h {:.0}% · 7d {:.0}%",
                    five_hour.unwrap_or(0.0) * 100.0,
                    seven_day.unwrap_or(0.0) * 100.0
                );
            }
        }

        AgentEvent::Error { message } => {
            println!("\n[error] {message}");
            if message == "agent exited" {
                return true;
            }
        }

        AgentEvent::Unknown { raw } => println!("\n[unknown] {}", compact(raw)),
    }
    false
}

/// Handle one line of operator input. Returns true to quit.
async fn handle_input(
    line: &str,
    session: &mut ClaudeSession,
    pending: &mut Option<Pending>,
) -> Result<bool> {
    if line.is_empty() {
        return Ok(false);
    }

    match command::parse(line) {
        command::Command::Prompt(text) if text == "/quit" => return Ok(true),

        command::Command::Stop => {
            session.cancel().await?;
            println!("[cancelled]");
        }

        command::Command::Status => println!(
            "[status] {}{}",
            if session.is_busy() {
                "turn running"
            } else {
                "idle"
            },
            pending
                .as_ref()
                .map(|p| format!(", awaiting decision on {}", p.tool))
                .unwrap_or_default()
        ),

        command::Command::Allow => match pending.take() {
            Some(p) => {
                session.decide(&p.request_id, Decision::allow()).await?;
                println!("[allowed {}]", p.tool);
            }
            None => println!("[nothing pending]"),
        },

        command::Command::Deny(why) => match pending.take() {
            Some(p) => {
                session.decide(&p.request_id, Decision::deny(why)).await?;
                println!("[denied {}]", p.tool);
            }
            None => println!("[nothing pending]"),
        },

        command::Command::Help => println!("{}", command::HELP),

        // /new and /cd are thread concepts; the repl drives a single session.
        command::Command::New | command::Command::Cd(_) => {
            println!("[not available in repl — use serve]")
        }

        command::Command::Prompt(text) => {
            if let Err(e) = session.prompt(&text).await {
                println!("[rejected] {e}");
            }
        }
    }

    Ok(false)
}

/// One-line JSON, clipped, for terminal rendering.
fn compact(value: &serde_json::Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".into());
    if text.chars().count() > 160 {
        format!("{}…", text.chars().take(160).collect::<String>())
    } else {
        text
    }
}
