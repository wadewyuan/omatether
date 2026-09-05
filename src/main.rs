//! Milestone 1: a Claude Code driver with no channels attached.
//!
//! This is the instrument that proves the seam-B event model and the
//! permission round-trip before any Telegram or Photon code exists. Type a
//! prompt, watch normalized events, answer permission questions by hand.

mod agent;
mod claude;
mod event;

use std::io::Write as _;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use uuid::Uuid;

use crate::agent::Agent;
use crate::claude::{ClaudeSession, Config};
use crate::event::{AgentEvent, Decision};

#[derive(Parser, Debug)]
#[command(
    name = "switchboard",
    about = "Drive the Claude Code stream-json protocol from a terminal"
)]
struct Args {
    /// Working directory for the agent session.
    #[arg(long, default_value = ".")]
    dir: PathBuf,

    /// Permission mode. `manual` asks before every tool, which is the point.
    #[arg(long, default_value = "manual")]
    permission_mode: String,

    /// Resume an existing session instead of starting a new one.
    #[arg(long)]
    session_id: Option<Uuid>,

    /// Echo every raw frame from the agent to stderr.
    #[arg(long)]
    raw: bool,

    /// Send this prompt immediately on start.
    prompt: Option<String>,
}

/// What the REPL is waiting for the operator to answer.
struct Pending {
    request_id: String,
    tool: String,
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

    let args = Args::parse();

    let config = Config {
        cwd: args.dir.clone(),
        session_id: args.session_id.unwrap_or_else(Uuid::new_v4),
        permission_mode: args.permission_mode.clone(),
        raw: args.raw,
    };

    println!("session  {}", config.session_id);
    println!("dir      {}", args.dir.display());
    println!("mode     {}", config.permission_mode);
    println!("commands /allow  /deny <why>  /cancel  /status  /quit");
    println!();

    let (mut session, mut events) = ClaudeSession::spawn(config).await?;

    if let Some(prompt) = args.prompt.as_deref() {
        session.prompt(prompt).await?;
        println!("> {prompt}");
    }

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut pending: Option<Pending> = None;
    // Deltas and the completed assistant block both arrive; render one of them.
    let mut streamed = false;

    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Some(event) => {
                        if render(&event, &mut streamed, &mut pending) {
                            break;
                        }
                    }
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
        } => {
            println!(
                "[ready] {} · {} · {} tools",
                session_id,
                model.as_deref().unwrap_or("?"),
                tools.len()
            );
        }

        AgentEvent::TextDelta { text } => {
            *streamed = true;
            print!("{text}");
            let _ = std::io::stdout().flush();
        }

        AgentEvent::Text { text } => {
            // Only print the assembled block if we never saw the deltas.
            if !*streamed {
                println!("{text}");
            }
        }

        AgentEvent::Thinking { .. } => {
            println!("\n[thinking]");
        }

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
            // Only worth interrupting the reader for once it is close.
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

        AgentEvent::Unknown { raw } => {
            println!("\n[unknown] {}", compact(raw));
        }
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

    match line.split_once(' ') {
        _ if line == "/quit" => return Ok(true),

        _ if line == "/cancel" => {
            session.cancel().await?;
            println!("[cancelled]");
        }

        _ if line == "/status" => {
            println!(
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
            );
        }

        _ if line == "/allow" => match pending.take() {
            Some(p) => {
                session.decide(&p.request_id, Decision::allow()).await?;
                println!("[allowed {}]", p.tool);
            }
            None => println!("[nothing pending]"),
        },

        Some(("/deny", why)) => match pending.take() {
            Some(p) => {
                session.decide(&p.request_id, Decision::deny(why)).await?;
                println!("[denied {}]", p.tool);
            }
            None => println!("[nothing pending]"),
        },

        _ if line == "/deny" => match pending.take() {
            Some(p) => {
                session
                    .decide(&p.request_id, Decision::deny("denied by operator"))
                    .await?;
                println!("[denied {}]", p.tool);
            }
            None => println!("[nothing pending]"),
        },

        _ => {
            if let Err(e) = session.prompt(line).await {
                println!("[rejected] {e}");
            }
        }
    }

    Ok(false)
}

/// One-line JSON, clipped, for terminal rendering.
fn compact(value: &serde_json::Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".into());
    if text.len() > 160 {
        format!("{}…", &text[..160])
    } else {
        text
    }
}
