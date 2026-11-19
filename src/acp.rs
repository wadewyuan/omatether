//! ACP adapter: any agent that speaks the Agent Client Protocol.
//!
//! JSON-RPC 2.0 over the agent's stdio, one object per line, one long-lived
//! process per session — the same shape as the Claude adapter, but a
//! published protocol rather than one CLI's private dialect. Seam B was drawn
//! after ACP's verbs in the first place, so the mapping is direct:
//!
//! | seam B            | ACP                                                   |
//! |-------------------|-------------------------------------------------------|
//! | spawn             | `initialize`, then `session/resume`, `load` or `new`  |
//! | `prompt`          | `session/prompt` (its *reply* is the end of the turn) |
//! | events            | `session/update` notifications                        |
//! | `PermissionRequest` / `decide` | `session/request_permission`, from the agent |
//! | `cancel`          | `session/cancel`                                      |
//! | `set_model`       | `session/set_config_option` (or older `set_model`)    |
//!
//! What ACP leaves to each agent is how to start it in ACP mode, how to make
//! it *ask* (the protocol carries the question; whether one is sent is the
//! agent's own permission config), and where extra instructions go (ACP has
//! no system-prompt field). That is [`Profile`], one per agent, and it is the
//! only per-agent knowledge in this file.
//!
//! Verified against opencode 2.3.2 live: a full turn with a tool call, a
//! permission request answered both ways, `session/resume` keeping the
//! conversation with nothing replayed, `set_config_option` refusing an unknown
//! model with a reason, `session/cancel` answered with `stopReason:
//! "cancelled"` in ~50ms, and the brief obeyed from an `instructions` file.
//! Copilot 1.0.114 likewise, except `/cancel`; its differences (no
//! `in_progress`, `load` rather than `resume`, an `allow_all` option, a brief
//! that has to be laid out as a `.instructions.md`) are noted where handled.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

use crate::agent::Agent;
use crate::event::{AgentEvent, Decision};

const EVENT_BUFFER: usize = 256;

/// How long `initialize` and the session call may take. Generous: these are
/// node and bun processes starting cold, and opencode's first `session/new`
/// on a machine also loads its provider catalog.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a request sent mid-session may take — someone is waiting on a
/// chat message for the answer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Stderr lines kept for the error that reports a dead agent, so the reason
/// is in the chat and not only in the journal.
const STDERR_TAIL: usize = 20;

/// The per-agent half: everything ACP does not standardize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// `opencode acp`. Asks through `session/request_permission` only for
    /// what its permission config says `ask`, and its default config asks for
    /// nothing — so the gate is turned on with `OPENCODE_CONFIG_CONTENT`
    /// (`permission: {"*": "ask"}`), and the brief goes in the same object as
    /// an `instructions` file.
    Opencode,
    /// `copilot --acp`. Asks before shell commands and edits by default
    /// (`copilot help permissions`); `COPILOT_ALLOW_ALL` is scrubbed so an
    /// inherited one cannot quietly turn that off, and a session reporting
    /// `allow_all: on` is switched off before use. The brief goes in as a
    /// `.github/instructions/*.instructions.md` under a directory named by
    /// `COPILOT_CUSTOM_INSTRUCTIONS_DIRS`.
    Copilot,
}

impl Profile {
    pub fn for_agent(name: &str) -> Option<Self> {
        match name {
            "opencode" => Some(Profile::Opencode),
            "copilot" => Some(Profile::Copilot),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Profile::Opencode => "opencode",
            Profile::Copilot => "copilot",
        }
    }

    fn command(self) -> (&'static str, &'static [&'static str]) {
        match self {
            Profile::Opencode => ("opencode", &["acp"]),
            Profile::Copilot => ("copilot", &["--acp"]),
        }
    }

    /// Put the gate and the brief where this agent reads them.
    fn configure(self, command: &mut Command, brief: Option<&Brief>) {
        match self {
            Profile::Opencode => {
                let existing = std::env::var("OPENCODE_CONFIG_CONTENT").ok();
                command.env(
                    "OPENCODE_CONFIG_CONTENT",
                    opencode_config(existing.as_deref(), brief.map(|b| b.file.as_path())),
                );
            }
            Profile::Copilot => {
                command.env_remove("COPILOT_ALLOW_ALL");
                if let Some(dir) = brief.map(|b| &b.root) {
                    let mut dirs = dir.to_string_lossy().to_string();
                    if let Ok(existing) = std::env::var("COPILOT_CUSTOM_INSTRUCTIONS_DIRS") {
                        if !existing.is_empty() {
                            dirs = format!("{existing},{dirs}");
                        }
                    }
                    command.env("COPILOT_CUSTOM_INSTRUCTIONS_DIRS", dirs);
                }
            }
        }
    }

    /// Where under its directory the brief is written, and what goes in it.
    ///
    /// opencode takes any path. Copilot does not: an `AGENTS.md` in a custom
    /// directory is *listed* by `copilot instruction list` — as a "nested"
    /// one — and then not applied, which cost a wrong claim in the docs to
    /// find. `.github/instructions/*.instructions.md` there is listed as a
    /// repository instruction and obeyed (copilot 1.0.114, three runs of
    /// three). `applyTo: "**"` is what that format uses for "everywhere";
    /// copilot obeyed it without, but it costs nothing to say.
    fn brief_layout(self, brief: &str) -> (&'static str, String) {
        match self {
            Profile::Opencode => ("brief.md", brief.to_string()),
            Profile::Copilot => (
                ".github/instructions/omatether.instructions.md",
                format!("---\napplyTo: \"**\"\n---\n{brief}"),
            ),
        }
    }
}

/// A brief on disk: the file, and the directory it was laid out under.
struct Brief {
    root: PathBuf,
    file: PathBuf,
}

/// `OPENCODE_CONFIG_CONTENT` with the gate on and the brief attached, on top
/// of whatever the operator already put there.
///
/// `"*": "ask"` is the whole gate. It does not override a more specific rule
/// in opencode's own config files — someone who wrote `"bash": "allow"` there
/// has made that call for every client, this one included.
pub fn opencode_config(existing: Option<&str>, brief: Option<&Path>) -> String {
    let mut config = existing
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| match value {
            Value::Object(map) => Some(map),
            _ => None,
        })
        .unwrap_or_default();

    let mut permission = match config.remove("permission") {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    permission.insert("*".into(), json!("ask"));
    config.insert("permission".into(), Value::Object(permission));

    if let Some(brief) = brief {
        let mut instructions = match config.remove("instructions") {
            Some(Value::Array(list)) => list,
            _ => Vec::new(),
        };
        instructions.push(json!(brief.to_string_lossy()));
        config.insert("instructions".into(), Value::Array(instructions));
    }

    Value::Object(config).to_string()
}

pub struct Config {
    pub profile: Profile,
    pub cwd: PathBuf,
    /// The agent's own session id, when resuming a conversation.
    pub session_id: Option<String>,
    /// A model `/model` asked for and the agent accepted earlier.
    pub model: Option<String>,
    /// See [`crate::agent::SpawnConfig::brief`].
    pub brief: Option<String>,
}

/// What a reply to one of our requests is for.
enum Waiting {
    /// Someone is awaiting it.
    Reply(oneshot::Sender<Result<Value, String>>),
    /// A `session/prompt`: its reply is the end of the turn. `silenced` once
    /// the turn has been cancelled, so a late answer cannot end the next one.
    Prompt { silenced: bool },
}

/// One question the agent is blocked on, keyed by the id we gave the core.
struct Question {
    /// The JSON-RPC id the agent sent it under — a number or a string.
    rpc_id: Value,
    options: Vec<Value>,
}

/// State the reader task and the session share.
struct Shared {
    agent: &'static str,
    out: mpsc::UnboundedSender<String>,
    next_id: AtomicU64,
    waiting: Mutex<HashMap<u64, Waiting>>,
    questions: Mutex<HashMap<String, Question>>,
    busy: AtomicBool,
    /// Per tool call id: what has been learned of it so far, and whether it
    /// has been announced. ACP sends a call as `tool_call` and then any
    /// number of `tool_call_update`s, each carrying some of the fields.
    tools: Mutex<HashMap<String, ToolState>>,
    stderr: Mutex<VecDeque<String>>,
    /// `session/update`s are swallowed while true: `session/load` replays the
    /// whole conversation as updates, and a chat is not the place to read it
    /// again.
    replaying: AtomicBool,
    /// Whether a tool call was refused during this turn. Opencode answers a
    /// rejection by ending the whole turn as `cancelled` — and "cancelled"
    /// with nobody having pressed stop reads as a fault.
    denied: AtomicBool,
    /// Inside a run of `agent_thought_chunk`s. Reasoning streams a few words
    /// at a time, and one `Thinking` per run is all the core uses — it shows
    /// that the agent is working, never what it thought.
    thinking: AtomicBool,
}

#[derive(Default)]
struct ToolState {
    frame: Map<String, Value>,
    announced: bool,
}

impl Shared {
    fn send(&self, frame: Value) -> Result<()> {
        self.out
            .send(frame.to_string())
            .map_err(|_| anyhow!("{} is not running", self.agent))
    }

    fn notify(&self, method: &str, params: Value) -> Result<()> {
        self.send(json!({"jsonrpc": "2.0", "method": method, "params": params}))
    }

    /// Send a request and wait for its answer. An error reply comes back as
    /// the agent's own message, since that is what gets shown.
    async fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.waiting.lock().unwrap().insert(id, Waiting::Reply(tx));
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))?;

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(message))) => Err(anyhow!(message)),
            Ok(Err(_)) => Err(anyhow!("{} exited{}", self.agent, self.stderr_tail())),
            Err(_) => {
                self.waiting.lock().unwrap().remove(&id);
                bail!(
                    "{} did not answer {method} within {}s",
                    self.agent,
                    timeout.as_secs()
                )
            }
        }
    }

    fn stderr_tail(&self) -> String {
        let tail = self.stderr.lock().unwrap();
        if tail.is_empty() {
            String::new()
        } else {
            format!(": {}", tail.iter().cloned().collect::<Vec<_>>().join(" | "))
        }
    }
}

pub struct AcpSession {
    profile: Profile,
    shared: Arc<Shared>,
    session_id: String,
    child: tokio::process::Child,
    /// Model ids the agent offered, and how to switch between them.
    models: Vec<String>,
    model_switch: ModelSwitch,
    /// What the agent ran before anyone asked, so `/model default` can go
    /// back to it — ACP has no "unset".
    default_model: Option<String>,
}

/// How this agent takes a model change, learned from its session reply.
#[derive(Debug, Clone, PartialEq)]
enum ModelSwitch {
    /// `session/set_config_option` with this config id — current ACP.
    ConfigOption(String),
    /// `session/set_model` — the older unstable method.
    SetModel,
    /// The agent offered no way.
    None,
}

impl AcpSession {
    pub async fn spawn(config: Config) -> Result<(Self, mpsc::Receiver<AgentEvent>)> {
        let profile = config.profile;
        let agent = profile.name();
        let (program, args) = profile.command();

        let brief_path = match &config.brief {
            Some(brief) => Some(write_brief(profile, brief).context("writing the brief")?),
            None => None,
        };

        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for var in crate::claude::INHERITED_SESSION_VARS {
            command.env_remove(var);
        }
        profile.configure(&mut command, brief_path.as_ref());

        let mut child = command
            .spawn()
            .with_context(|| format!("spawning `{program}` — is it on PATH?"))?;
        let mut stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take().expect("stderr was piped");

        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
        let (tx, rx) = mpsc::channel(EVENT_BUFFER);
        let shared = Arc::new(Shared {
            agent,
            out: out_tx,
            next_id: AtomicU64::new(1),
            waiting: Mutex::new(HashMap::new()),
            questions: Mutex::new(HashMap::new()),
            busy: AtomicBool::new(false),
            tools: Mutex::new(HashMap::new()),
            stderr: Mutex::new(VecDeque::new()),
            replaying: AtomicBool::new(false),
            denied: AtomicBool::new(false),
            thinking: AtomicBool::new(false),
        });

        // The writer owns stdin; dropping every sender closes it, which is
        // how `shutdown` asks the agent to exit.
        tokio::spawn(async move {
            while let Some(line) = out_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err()
                    || stdin.write_all(b"\n").await.is_err()
                    || stdin.flush().await.is_err()
                {
                    break;
                }
            }
        });
        tokio::spawn(read_stderr(stderr, shared.clone()));
        tokio::spawn(read_frames(stdout, shared.clone(), tx.clone()));

        let init = shared
            .request(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    // Nothing offered: no file or terminal access through us.
                    // The agent uses its own tools, which is what the gate is
                    // in front of.
                    "clientCapabilities": {
                        "fs": {"readTextFile": false, "writeTextFile": false},
                        "terminal": false,
                    },
                    "clientInfo": {"name": "omatether", "version": env!("CARGO_PKG_VERSION")},
                }),
                STARTUP_TIMEOUT,
            )
            .await
            .with_context(|| format!("{agent} did not start an ACP session"))?;

        let cwd = config.cwd.to_string_lossy().to_string();
        let (session_id, session, resumed) =
            open_session(&shared, &init, &cwd, config.session_id.as_deref())
                .await
                .map_err(|e| anyhow!("{agent}: {e:#}{}", login_hint(&init, &e)))?;

        // The one gate fact ACP lets us read. Turned off if it is on, and a
        // session that will not turn it off does not start: a gate that is
        // claimed and not there is worse than an agent that admits it has none.
        if allow_all_on(&session) {
            shared
                .request(
                    "session/set_config_option",
                    json!({"sessionId": session_id, "configId": "allow_all", "value": "off"}),
                    STARTUP_TIMEOUT,
                )
                .await
                .map_err(|e| {
                    anyhow!("{agent} has allow-all on and would not turn it off: {e:#}")
                })?;
            tracing::info!(target: "omatether::acp", agent, "turned allow_all off");
        }

        let (models, current, model_switch) = model_catalog(&session);
        let mut this = Self {
            profile,
            shared,
            session_id: session_id.clone(),
            child,
            models,
            model_switch,
            default_model: current.clone(),
        };

        // A model asked for earlier and accepted then. Not fatal if it is
        // refused now — the conversation is worth more than the setting, and
        // the reason goes to the chat.
        let mut model = current;
        if let Some(wanted) = &config.model {
            if model.as_deref() != Some(wanted.as_str()) {
                match this.switch_model(wanted).await {
                    Ok(resolved) => model = resolved.or(Some(wanted.clone())),
                    Err(e) => {
                        let _ = tx
                            .send(AgentEvent::Error {
                                message: format!("{agent} would not take model {wanted}: {e:#}"),
                            })
                            .await;
                    }
                }
            }
        }

        if let Some(failed) = resumed.err() {
            let _ = tx
                .send(AgentEvent::Error {
                    message: format!(
                        "could not pick up the earlier {agent} conversation ({failed}); \
                         this is a new one"
                    ),
                })
                .await;
        }

        let _ = tx
            .send(AgentEvent::Ready {
                session_id,
                model,
                cwd: Some(cwd),
                tools: Vec::new(),
            })
            .await;

        Ok((this, rx))
    }

    /// Switch the model, returning what the agent says it now runs.
    async fn switch_model(&mut self, model: &str) -> Result<Option<String>> {
        let reply = match &self.model_switch {
            ModelSwitch::ConfigOption(id) => {
                self.shared
                    .request(
                        "session/set_config_option",
                        json!({"sessionId": self.session_id, "configId": id, "value": model}),
                        REQUEST_TIMEOUT,
                    )
                    .await?
            }
            ModelSwitch::SetModel => {
                self.shared
                    .request(
                        "session/set_model",
                        json!({"sessionId": self.session_id, "modelId": model}),
                        REQUEST_TIMEOUT,
                    )
                    .await?
            }
            ModelSwitch::None => {
                bail!("{} offered no way to change its model", self.profile.name())
            }
        };
        // set_config_option answers with the whole option set; set_model with
        // nothing. Either way, the catalog is refreshed where it can be.
        let (models, current, _) = model_catalog(&reply);
        if !models.is_empty() {
            self.models = models;
        }
        Ok(current)
    }
}

/// Resume, load or start a session, in that order of preference.
///
/// Returns the session id, the session reply (where the model catalog is), and
/// whether a requested resume worked — a failure is the reason, reported
/// rather than silently starting over.
async fn open_session(
    shared: &Shared,
    init: &Value,
    cwd: &str,
    resume: Option<&str>,
) -> Result<(String, Value, std::result::Result<(), String>)> {
    let caps = init.get("agentCapabilities");
    let can_resume = caps
        .and_then(|c| c.pointer("/sessionCapabilities/resume"))
        .is_some();
    let can_load = caps
        .and_then(|c| c.get("loadSession"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut resumed = Ok(());
    if let Some(id) = resume {
        let params = json!({"sessionId": id, "cwd": cwd, "mcpServers": []});
        let attempt = if can_resume {
            shared
                .request("session/resume", params, STARTUP_TIMEOUT)
                .await
        } else if can_load {
            // `load` replays the transcript as updates before it answers.
            shared.replaying.store(true, Ordering::SeqCst);
            let reply = shared
                .request("session/load", params, STARTUP_TIMEOUT)
                .await;
            shared.replaying.store(false, Ordering::SeqCst);
            reply
        } else {
            Err(anyhow!("this agent cannot resume a session"))
        };
        match attempt {
            Ok(reply) => return Ok((id.to_string(), reply, Ok(()))),
            Err(e) => resumed = Err(format!("{e:#}")),
        }
    }

    let reply = shared
        .request(
            "session/new",
            json!({"cwd": cwd, "mcpServers": []}),
            STARTUP_TIMEOUT,
        )
        .await?;
    let id = reply
        .get("sessionId")
        .and_then(Value::as_str)
        .context("session/new answered without a sessionId")?
        .to_string();
    Ok((id, reply, resumed))
}

/// "Authentication required" is the whole of copilot's answer before a login,
/// and the agent's `authMethods` say what to run. Worth adding, because the
/// reason is going to a phone, not a terminal where the next step is obvious.
fn login_hint(init: &Value, error: &anyhow::Error) -> String {
    if !format!("{error:#}").to_lowercase().contains("auth") {
        return String::new();
    }
    init.get("authMethods")
        .and_then(Value::as_array)
        .and_then(|methods| methods.first())
        .and_then(|m| m.get("description"))
        .and_then(Value::as_str)
        .map(|how| format!(" — {how} on that machine"))
        .unwrap_or_default()
}

/// The models a session reply offers, the current one, and how to switch.
///
/// Current ACP reports them as a `configOptions` entry of category `model`;
/// earlier versions as `models: {availableModels, currentModelId}`. Both are
/// read, the newer preferred.
fn model_catalog(reply: &Value) -> (Vec<String>, Option<String>, ModelSwitch) {
    if let Some(option) = reply
        .get("configOptions")
        .and_then(Value::as_array)
        .and_then(|options| {
            options
                .iter()
                .find(|o| o.get("category").and_then(Value::as_str) == Some("model"))
        })
    {
        let models = dedupe(
            option
                .get("options")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter_map(|o| o.get("value").and_then(Value::as_str))
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default(),
        );
        let current = option
            .get("currentValue")
            .and_then(Value::as_str)
            .map(String::from);
        let id = option
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("model")
            .to_string();
        return (models, current, ModelSwitch::ConfigOption(id));
    }

    if let Some(state) = reply.get("models") {
        let models = dedupe(
            state
                .get("availableModels")
                .and_then(Value::as_array)
                .map(|list| {
                    list.iter()
                        .filter_map(|m| m.get("modelId").and_then(Value::as_str))
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default(),
        );
        let current = state
            .get("currentModelId")
            .and_then(Value::as_str)
            .map(String::from);
        return (models, current, ModelSwitch::SetModel);
    }

    (Vec::new(), None, ModelSwitch::None)
}

/// Copilot lists `auto` twice (copilot 1.0.114, an account offered only that).
fn dedupe(list: Vec<String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    list.into_iter()
        .filter(|m| seen.insert(m.clone()))
        .collect()
}

/// A session setting that would answer every question before it is asked:
/// copilot reports `allow_all` among its `configOptions`, and "on" there is
/// the gate gone with nothing in the protocol to say so. The id and the "on"
/// value are both read off a live copilot 1.0.114 session reply.
fn allow_all_on(reply: &Value) -> bool {
    reply
        .get("configOptions")
        .and_then(Value::as_array)
        .is_some_and(|options| {
            options.iter().any(|o| {
                o.get("id").and_then(Value::as_str) == Some("allow_all")
                    && o.get("currentValue").and_then(Value::as_str) == Some("on")
            })
        })
}

/// Write the brief where the agent will read it: a 0600 file in a 0700
/// directory under the state dir, named by its content so threads that share
/// a brief share a file and nothing is rewritten under a running agent.
fn write_brief(profile: Profile, brief: &str) -> Result<Brief> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    let digest = ring::digest::digest(&ring::digest::SHA256, brief.as_bytes());
    let hex: String = digest.as_ref()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let root = crate::dirs_state_home()
        .join("omatether/brief")
        .join(hex)
        .join(profile.name());
    let (relative, text) = profile.brief_layout(brief);
    let path = root.join(relative);
    let dir = path.parent().expect("a layout names a file");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    // Chmod what is already there, not just what is created — see Storage
    // in CLAUDE.md.
    for step in dir.ancestors().take_while(|d| d.starts_with(&root)) {
        std::fs::set_permissions(step, std::fs::Permissions::from_mode(0o700))?;
    }

    if std::fs::read_to_string(&path).ok().as_deref() != Some(text.as_str()) {
        let tmp = dir.join(".brief.tmp");
        std::fs::write(&tmp, &text)?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&tmp, &path)?;
    }
    Ok(Brief { root, file: path })
}

async fn read_stderr(stderr: tokio::process::ChildStderr, shared: Arc<Shared>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::info!(target: "omatether::acp", agent = shared.agent, "stderr: {line}");
        let mut tail = shared.stderr.lock().unwrap();
        if tail.len() == STDERR_TAIL {
            tail.pop_front();
        }
        tail.push_back(line);
    }
}

async fn read_frames(
    stdout: tokio::process::ChildStdout,
    shared: Arc<Shared>,
    tx: mpsc::Sender<AgentEvent>,
) {
    let mut lines = BufReader::new(stdout).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        // A mise shim's activation line, or anything else that is not a
        // frame, goes to the log rather than into serde.
        if !trimmed.starts_with('{') {
            if !trimmed.is_empty() {
                tracing::info!(target: "omatether::acp", agent = shared.agent, "non-json line: {trimmed}");
            }
            continue;
        }
        let frame: Value = match serde_json::from_str(trimmed) {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!(target: "omatether::acp", agent = shared.agent, "unparseable frame: {e}: {trimmed}");
                continue;
            }
        };
        for event in handle_frame(&shared, frame) {
            if tx.send(event).await.is_err() {
                return;
            }
        }
    }

    // The process is gone. Anyone awaiting a reply learns it from the dropped
    // sender; a running turn has to be told, or the chat waits forever.
    let waiting: Vec<Waiting> = shared
        .waiting
        .lock()
        .unwrap()
        .drain()
        .map(|(_, w)| w)
        .collect();
    let turn_open = waiting
        .iter()
        .any(|w| matches!(w, Waiting::Prompt { silenced: false }));
    drop(waiting);
    shared.questions.lock().unwrap().clear();
    shared.busy.store(false, Ordering::SeqCst);
    if turn_open {
        let _ = tx
            .send(AgentEvent::TurnEnd {
                ok: false,
                detail: Some(format!(
                    "{} exited mid-turn{}",
                    shared.agent,
                    shared.stderr_tail()
                )),
            })
            .await;
    }
}

/// One frame in, zero or more events out — and any reply the frame needs.
fn handle_frame(shared: &Shared, frame: Value) -> Vec<AgentEvent> {
    let method = frame.get("method").and_then(Value::as_str);
    let id = frame.get("id").cloned();

    match (method, id) {
        // A reply to something we sent.
        (None, Some(id)) => {
            let Some(id) = id.as_u64() else {
                return vec![AgentEvent::Unknown { raw: frame }];
            };
            let Some(waiting) = shared.waiting.lock().unwrap().remove(&id) else {
                return Vec::new();
            };
            let outcome = match (frame.get("result"), frame.get("error")) {
                (_, Some(error)) => Err(rpc_error(error)),
                (Some(result), None) => Ok(result.clone()),
                (None, None) => Ok(Value::Null),
            };
            match waiting {
                Waiting::Reply(tx) => {
                    let _ = tx.send(outcome);
                    Vec::new()
                }
                Waiting::Prompt { silenced } => {
                    shared.busy.store(false, Ordering::SeqCst);
                    shared.questions.lock().unwrap().clear();
                    shared.tools.lock().unwrap().clear();
                    if silenced {
                        return Vec::new();
                    }
                    let denied = shared.denied.swap(false, Ordering::SeqCst);
                    vec![turn_end(shared.agent, outcome, denied)]
                }
            }
        }

        // The agent asking us something.
        (Some(method), Some(rpc_id)) => match method {
            "session/request_permission" => {
                let params = frame.get("params").cloned().unwrap_or(Value::Null);
                let options = params
                    .get("options")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let call = params.get("toolCall").cloned().unwrap_or(Value::Null);
                // Fold in what earlier updates said about this call — the
                // request may carry only its id and a title.
                let merged = merge_tool(shared, &call);
                let (tool, input) = describe_tool(&merged);
                let request_id = format!("acp-{}", rpc_id);
                shared
                    .questions
                    .lock()
                    .unwrap()
                    .insert(request_id.clone(), Question { rpc_id, options });
                vec![AgentEvent::PermissionRequest {
                    request_id,
                    tool,
                    input,
                    suggestions: Value::Null,
                }]
            }
            // `fs/*` and `terminal/*`: not offered in `initialize`, so an
            // agent asking anyway is told no rather than left hanging.
            _ => {
                let _ = shared.send(json!({
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {"code": -32601, "message": format!("omatether does not provide {method}")},
                }));
                Vec::new()
            }
        },

        (Some("session/update"), None) => {
            if shared.replaying.load(Ordering::SeqCst) {
                return Vec::new();
            }
            let update = frame
                .pointer("/params/update")
                .cloned()
                .unwrap_or(Value::Null);
            normalize_update(shared, update)
        }

        _ => vec![AgentEvent::Unknown { raw: frame }],
    }
}

fn rpc_error(error: &Value) -> String {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    // `data` is where opencode says which model was not found; worth having
    // when it is a plain string, noise when it is a structure.
    match error.get("data").and_then(Value::as_str) {
        Some(data) if !data.is_empty() => format!("{message}: {data}"),
        _ => message.to_string(),
    }
}

/// The `session/prompt` reply, as the end of a turn.
pub fn turn_end(
    agent: &str,
    outcome: std::result::Result<Value, String>,
    denied: bool,
) -> AgentEvent {
    match outcome {
        Err(message) => AgentEvent::TurnEnd {
            ok: false,
            detail: Some(message),
        },
        Ok(result) => match result.get("stopReason").and_then(Value::as_str) {
            Some("end_turn") | None => AgentEvent::TurnEnd {
                ok: true,
                detail: None,
            },
            Some("cancelled") if denied => AgentEvent::TurnEnd {
                ok: false,
                detail: Some(format!("{agent} stopped at the tool call that was denied")),
            },
            Some(reason) => AgentEvent::TurnEnd {
                ok: false,
                detail: Some(
                    match reason {
                        "max_tokens" => "the reply hit the model's token limit",
                        "max_turn_requests" => "the agent hit its limit on model requests",
                        "refusal" => "the model refused",
                        "cancelled" => "cancelled",
                        other => other,
                    }
                    .to_string(),
                ),
            },
        },
    }
}

/// One `session/update` notification into events.
fn normalize_update(shared: &Shared, update: Value) -> Vec<AgentEvent> {
    let kind = update
        .get("sessionUpdate")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let was_thinking = shared
        .thinking
        .swap(kind == "agent_thought_chunk", Ordering::SeqCst);
    match kind {
        "agent_message_chunk" => match chunk_text(&update) {
            Some(text) => vec![AgentEvent::TextDelta { text }],
            None => Vec::new(),
        },
        "agent_thought_chunk" if was_thinking => Vec::new(),
        "agent_thought_chunk" => match chunk_text(&update) {
            Some(text) => vec![AgentEvent::Thinking { text }],
            None => Vec::new(),
        },
        "tool_call" | "tool_call_update" => {
            let merged = merge_tool(shared, &update);
            let status = merged
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("pending");
            // Announced once, as soon as there is something to say about it.
            // A bare `pending` is too early — opencode's first frame for a
            // shell call carries only the directory, and the command arrives
            // in the next. But waiting for `in_progress` is too late on
            // copilot, which never sends one: its calls go `pending` (with
            // the command) straight to `completed`, and a long command would
            // show nothing until it had finished.
            let says_something = merged
                .get("rawInput")
                .and_then(Value::as_object)
                .is_some_and(|input| input.keys().any(|k| k != "cwd" && k != "workdir"));
            if status == "pending" && !says_something {
                return Vec::new();
            }
            let id = merged
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            {
                let mut tools = shared.tools.lock().unwrap();
                let state = tools.entry(id.clone()).or_default();
                if state.announced {
                    return Vec::new();
                }
                state.announced = true;
            }
            let (name, input) = describe_tool(&merged);
            vec![AgentEvent::ToolCall { id, name, input }]
        }
        // Known and deliberately not shown: echoes of our own prompt, the
        // agent's slash commands, plan and mode bookkeeping, token counts.
        "user_message_chunk"
        | "available_commands_update"
        | "config_option_update"
        | "current_mode_update"
        | "plan"
        | "usage_update"
        | "session_info_update" => Vec::new(),
        _ => vec![AgentEvent::Unknown { raw: update }],
    }
}

fn chunk_text(update: &Value) -> Option<String> {
    let content = update.get("content")?;
    if content.get("type").and_then(Value::as_str) != Some("text") {
        return None;
    }
    content
        .get("text")
        .and_then(Value::as_str)
        .map(String::from)
}

/// Fold a tool frame into what is known of that call, and return the result.
/// Later fields win; fields a frame leaves out are kept.
fn merge_tool(shared: &Shared, frame: &Value) -> Value {
    let Some(id) = frame.get("toolCallId").and_then(Value::as_str) else {
        return frame.clone();
    };
    let mut tools = shared.tools.lock().unwrap();
    let state = tools.entry(id.to_string()).or_default();
    if let Value::Object(fields) = frame {
        for (key, value) in fields {
            // `rawInput` is merged too: opencode's first frame has only `cwd`
            // in it, the next adds `command`.
            if key == "rawInput" {
                if let (Some(Value::Object(old)), Value::Object(new)) =
                    (state.frame.get_mut("rawInput"), value)
                {
                    for (k, v) in new {
                        old.insert(k.clone(), v.clone());
                    }
                    continue;
                }
            }
            state.frame.insert(key.clone(), value.clone());
        }
    }
    Value::Object(state.frame.clone())
}

/// A tool call in the shared vocabulary: Claude's tool names, so the renderer
/// and [`crate::core`]'s gate list need no per-agent knowledge, and an input
/// that keeps every field the agent sent — a permission question shows it
/// whole — plus the one key [`crate::render::summarize`] looks for.
pub fn describe_tool(call: &Value) -> (String, Value) {
    let kind = call.get("kind").and_then(Value::as_str).unwrap_or("other");
    let title = call
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut input = match call.get("rawInput") {
        Some(Value::Object(map)) => map.clone(),
        _ => Map::new(),
    };
    let first = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| input.get(*k).and_then(Value::as_str).map(String::from))
    };
    let location = call
        .pointer("/locations/0/path")
        .and_then(Value::as_str)
        .map(String::from);

    let (name, key, value) = match kind {
        "execute" => (
            "Bash",
            "command",
            first(&["command", "cmd"]).or(Some(title.to_string())),
        ),
        "read" => (
            "Read",
            "file_path",
            first(&["filePath", "file_path", "path"]).or(location),
        ),
        "edit" => (
            "Edit",
            "file_path",
            first(&["filePath", "file_path", "path"]).or(location),
        ),
        "delete" => (
            "Delete",
            "file_path",
            first(&["filePath", "file_path", "path"]).or(location),
        ),
        "move" => (
            "Move",
            "file_path",
            first(&["filePath", "file_path", "path", "source"]).or(location),
        ),
        "search" => (
            "Grep",
            "pattern",
            first(&["pattern", "query"]).or(Some(title.to_string())),
        ),
        "fetch" => (
            "WebFetch",
            "url",
            first(&["url"]).or(Some(title.to_string())),
        ),
        _ => {
            let name = call
                .get("name")
                .and_then(Value::as_str)
                .filter(|n| !n.is_empty())
                .unwrap_or(if title.is_empty() { "tool" } else { title });
            return (name.to_string(), Value::Object(input));
        }
    };
    if let Some(value) = value.filter(|v| !v.is_empty()) {
        input.entry(key.to_string()).or_insert(json!(value));
    }
    (name.to_string(), Value::Object(input))
}

#[async_trait]
impl Agent for AcpSession {
    fn name(&self) -> &'static str {
        self.profile.name()
    }

    /// Questions come through `session/request_permission`, and each profile
    /// configures its agent to send them.
    fn gates_tools(&self) -> bool {
        true
    }

    fn streams(&self) -> bool {
        true
    }

    fn is_busy(&self) -> bool {
        self.shared.busy.load(Ordering::SeqCst)
    }

    async fn prompt(&mut self, text: &str) -> Result<()> {
        if self.shared.busy.swap(true, Ordering::SeqCst) {
            bail!("a turn is already running — cancel it first");
        }
        self.shared.denied.store(false, Ordering::SeqCst);
        let id = self.shared.next_id.fetch_add(1, Ordering::SeqCst);
        self.shared
            .waiting
            .lock()
            .unwrap()
            .insert(id, Waiting::Prompt { silenced: false });
        let sent = self.shared.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "session/prompt",
            "params": {
                "sessionId": self.session_id,
                "prompt": [{"type": "text", "text": text}],
            },
        }));
        if let Err(e) = sent {
            self.shared.waiting.lock().unwrap().remove(&id);
            self.shared.busy.store(false, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    /// ACP has no field for a reason: a denial reaches the model as a
    /// rejected call, without the "why" that Claude would read back.
    async fn decide(&mut self, request_id: &str, decision: Decision) -> Result<()> {
        let Some(question) = self.shared.questions.lock().unwrap().remove(request_id) else {
            bail!("no open question {request_id}");
        };
        if matches!(decision, Decision::Deny { .. }) {
            self.shared.denied.store(true, Ordering::SeqCst);
        }
        let wanted: &[&str] = match decision {
            Decision::Allow { .. } => &["allow_once", "allow_always"],
            Decision::Deny { .. } => &["reject_once", "reject_always"],
        };
        let option = wanted
            .iter()
            .find_map(|kind| {
                question
                    .options
                    .iter()
                    .find(|o| o.get("kind").and_then(Value::as_str) == Some(kind))
            })
            .and_then(|o| o.get("optionId").cloned());
        let outcome = match option {
            Some(option_id) => json!({"outcome": "selected", "optionId": option_id}),
            // No option of the kind asked for. Withdrawing the question is the
            // only answer that cannot be read as consent.
            None => json!({"outcome": "cancelled"}),
        };
        self.shared.send(json!({
            "jsonrpc": "2.0",
            "id": question.rpc_id,
            "result": {"outcome": outcome},
        }))
    }

    async fn set_model(&mut self, model: Option<&str>) -> Result<Option<String>> {
        let target = match model
            .map(String::from)
            .or_else(|| self.default_model.clone())
        {
            Some(target) => target,
            None => bail!(
                "{} did not say which model it started on",
                self.profile.name()
            ),
        };
        self.switch_model(&target).await
    }

    fn models(&self) -> Vec<String> {
        self.models.clone()
    }

    async fn cancel(&mut self) -> Result<()> {
        if !self.shared.busy.load(Ordering::SeqCst) {
            return Ok(());
        }
        // The protocol requires open questions be answered `cancelled` once a
        // turn is called off.
        let questions: Vec<Question> = self
            .shared
            .questions
            .lock()
            .unwrap()
            .drain()
            .map(|(_, q)| q)
            .collect();
        for question in questions {
            let _ = self.shared.send(json!({
                "jsonrpc": "2.0",
                "id": question.rpc_id,
                "result": {"outcome": {"outcome": "cancelled"}},
            }));
        }
        // The turn's own reply still comes, `stopReason: "cancelled"`, and by
        // then the core has moved on — possibly into the next turn.
        for waiting in self.shared.waiting.lock().unwrap().values_mut() {
            if let Waiting::Prompt { silenced } = waiting {
                *silenced = true;
            }
        }
        self.shared.busy.store(false, Ordering::SeqCst);
        self.shared
            .notify("session/cancel", json!({"sessionId": self.session_id}))
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.cancel().await.ok();
        // Killed, not asked: the conversation is already on disk on the
        // agent's side (that is what `session/resume` reads), and nothing an
        // agent does between a cancel and its exit is worth waiting for.
        self.child.kill().await.ok();
        Ok(())
    }
}

/// The models opencode will take, from `opencode models` — local, no session
/// needed. Copilot has no such command, so its list comes with a session.
pub async fn list_models(profile: Profile, cwd: &Path) -> Result<Vec<String>> {
    match profile {
        Profile::Opencode => {
            let mut command = Command::new("opencode");
            command.arg("models").current_dir(cwd).kill_on_drop(true);
            let output = match tokio::time::timeout(Duration::from_secs(20), command.output()).await
            {
                Ok(output) => output.context("running `opencode models`")?,
                Err(_) => bail!("`opencode models` did not answer within 20s"),
            };
            if !output.status.success() {
                bail!(
                    "`opencode models` exited {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Ok(parse_model_lines(&String::from_utf8_lossy(&output.stdout)))
        }
        Profile::Copilot => bail!(
            "copilot's model list comes with a live session — send it a message \
             first, and /model will show it"
        ),
    }
}

/// `provider/model`, one per line. Anything with a space in it is not a model
/// name — the mise shim's activation line, most likely.
pub fn parse_model_lines(out: &str) -> Vec<String> {
    out.lines()
        .map(str::trim)
        .filter(|line| line.contains('/') && !line.contains(char::is_whitespace))
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> (Arc<Shared>, mpsc::UnboundedReceiver<String>) {
        let (out, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Shared {
                agent: "opencode",
                out,
                next_id: AtomicU64::new(1),
                waiting: Mutex::new(HashMap::new()),
                questions: Mutex::new(HashMap::new()),
                busy: AtomicBool::new(false),
                tools: Mutex::new(HashMap::new()),
                stderr: Mutex::new(VecDeque::new()),
                replaying: AtomicBool::new(false),
                denied: AtomicBool::new(false),
                thinking: AtomicBool::new(false),
            }),
            rx,
        )
    }

    fn update(update: Value) -> Value {
        json!({"jsonrpc": "2.0", "method": "session/update",
               "params": {"sessionId": "ses_1", "update": update}})
    }

    // Frames below are trimmed from a live opencode 2.3.2 run.

    #[test]
    fn message_chunks_stream_and_thoughts_do_not_render_as_text() {
        let (s, _rx) = shared();
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "agent_message_chunk", "messageId": "m",
            "content": {"type": "text", "text": "I'll run the command"}})),
        );
        assert!(
            matches!(&events[..], [AgentEvent::TextDelta { text }] if text == "I'll run the command")
        );
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "agent_thought_chunk", "messageId": "m:reasoning:0",
            "content": {"type": "text", "text": "The output shows"}})),
        );
        assert!(matches!(&events[..], [AgentEvent::Thinking { .. }]));
        // The rest of the run adds nothing; a new run after text announces again.
        let thought = update(json!({"sessionUpdate": "agent_thought_chunk",
            "content": {"type": "text", "text": " one"}}));
        assert!(handle_frame(&s, thought.clone()).is_empty());
        handle_frame(
            &s,
            update(json!({"sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "x"}})),
        );
        assert_eq!(handle_frame(&s, thought).len(), 1);
    }

    #[test]
    fn a_tool_call_is_announced_once_with_the_command_it_ran() {
        let (s, _rx) = shared();
        // `pending` carries only the directory — too early to announce.
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "tool_call", "toolCallId": "t1", "name": "shell", "title": "shell",
            "kind": "execute", "status": "pending",
            "rawInput": {"cwd": "/w"}})),
        );
        assert!(events.is_empty());
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": "in_progress",
            "kind": "execute", "title": "ls -la",
            "rawInput": {"command": "ls -la", "workdir": "/w"}})),
        );
        match &events[..] {
            [AgentEvent::ToolCall { name, input, .. }] => {
                assert_eq!(name, "Bash");
                assert_eq!(input["command"], "ls -la");
                // Merged, not replaced: the field from the first frame is kept.
                assert_eq!(input["cwd"], "/w");
            }
            other => panic!("{other:?}"),
        }
        // The repeat and the completion say nothing new.
        for status in ["in_progress", "completed"] {
            let events = handle_frame(
                &s,
                update(json!({
                "sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": status})),
            );
            assert!(events.is_empty(), "{status}");
        }
    }

    #[test]
    fn a_pending_call_that_names_its_command_is_announced_at_once() {
        // Live copilot 1.0.114: no `in_progress` ever; `pending` then
        // `completed`, and the pending frame already has the command.
        let (s, _rx) = shared();
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "tool_call", "toolCallId": "c1", "title": "Create or update b.txt",
            "kind": "execute", "status": "pending",
            "rawInput": {"command": "touch b.txt", "description": "Create or update b.txt"}})),
        );
        assert!(
            matches!(&events[..], [AgentEvent::ToolCall { name, input, .. }]
            if name == "Bash" && input["command"] == "touch b.txt")
        );
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "tool_call_update", "toolCallId": "c1", "status": "completed"})),
        );
        assert!(events.is_empty());
    }

    #[test]
    fn a_permission_request_is_a_question_and_the_answer_names_an_option() {
        let (s, mut rx) = shared();
        let events = handle_frame(
            &s,
            json!({
            "jsonrpc": "2.0", "id": 0, "method": "session/request_permission",
            "params": {"sessionId": "ses_1",
                "toolCall": {"toolCallId": "t2", "name": "shell", "title": "ls", "kind": "execute",
                             "status": "pending", "rawInput": {"command": "ls", "cwd": "/w"}},
                "options": [
                    {"optionId": "once", "kind": "allow_once", "name": "Allow once"},
                    {"optionId": "always", "kind": "allow_always", "name": "Always allow"},
                    {"optionId": "reject", "kind": "reject_once", "name": "Reject"}]}}),
        );
        let request_id = match &events[..] {
            [AgentEvent::PermissionRequest {
                request_id,
                tool,
                input,
                ..
            }] => {
                assert_eq!(tool, "Bash");
                assert_eq!(input["command"], "ls");
                request_id.clone()
            }
            other => panic!("{other:?}"),
        };

        // Answer it through a session built around the same shared state.
        let question = s.questions.lock().unwrap().remove(&request_id).unwrap();
        assert_eq!(question.rpc_id, json!(0));
        let deny = ["reject_once", "reject_always"]
            .iter()
            .find_map(|k| question.options.iter().find(|o| o["kind"] == *k));
        assert_eq!(deny.unwrap()["optionId"], "reject");
        assert!(rx.try_recv().is_err(), "nothing is sent until decided");
    }

    #[test]
    fn the_prompt_reply_ends_the_turn_and_a_cancelled_one_ends_nothing() {
        let (s, _rx) = shared();
        s.busy.store(true, Ordering::SeqCst);
        s.waiting
            .lock()
            .unwrap()
            .insert(3, Waiting::Prompt { silenced: false });
        let events = handle_frame(
            &s,
            json!({"jsonrpc": "2.0", "id": 3,
            "result": {"stopReason": "end_turn", "usage": {}}}),
        );
        assert!(matches!(
            &events[..],
            [AgentEvent::TurnEnd { ok: true, .. }]
        ));
        assert!(!s.busy.load(Ordering::SeqCst));

        s.waiting
            .lock()
            .unwrap()
            .insert(4, Waiting::Prompt { silenced: true });
        let events = handle_frame(
            &s,
            json!({"jsonrpc": "2.0", "id": 4,
            "result": {"stopReason": "cancelled"}}),
        );
        assert!(
            events.is_empty(),
            "a late answer must not end the next turn"
        );
    }

    #[test]
    fn stop_reasons_other_than_end_turn_are_failures_with_a_reason() {
        assert!(matches!(
            turn_end("opencode", Ok(json!({"stopReason": "end_turn"})), false),
            AgentEvent::TurnEnd { ok: true, .. }
        ));
        match turn_end("opencode", Ok(json!({"stopReason": "max_tokens"})), false) {
            AgentEvent::TurnEnd {
                ok: false,
                detail: Some(d),
            } => assert!(d.contains("token")),
            other => panic!("{other:?}"),
        }
        // Live opencode 2.3.2: rejecting a call ends the turn as cancelled.
        match turn_end("opencode", Ok(json!({"stopReason": "cancelled"})), true) {
            AgentEvent::TurnEnd {
                ok: false,
                detail: Some(d),
            } => assert!(d.contains("denied"), "{d}"),
            other => panic!("{other:?}"),
        }
        match turn_end("opencode", Err("Authentication required".into()), false) {
            AgentEvent::TurnEnd {
                ok: false,
                detail: Some(d),
            } => assert_eq!(d, "Authentication required"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn updates_during_a_load_replay_are_swallowed() {
        let (s, _rx) = shared();
        s.replaying.store(true, Ordering::SeqCst);
        let events = handle_frame(
            &s,
            update(json!({
            "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "old"}})),
        );
        assert!(events.is_empty());
    }

    #[test]
    fn requests_we_did_not_offer_are_refused_not_ignored() {
        let (s, mut rx) = shared();
        handle_frame(
            &s,
            json!({"jsonrpc": "2.0", "id": 9, "method": "fs/read_text_file",
            "params": {"path": "/etc/passwd"}}),
        );
        let reply: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(reply["id"], 9);
        assert_eq!(reply["error"]["code"], -32601);
    }

    #[test]
    fn unknown_updates_are_surfaced() {
        let (s, _rx) = shared();
        let events = handle_frame(&s, update(json!({"sessionUpdate": "something_new"})));
        assert!(matches!(&events[..], [AgentEvent::Unknown { .. }]));
    }

    #[test]
    fn the_model_catalog_is_read_from_config_options_or_the_older_field() {
        let reply = json!({"sessionId": "s", "configOptions": [
            {"id": "mode", "category": "mode", "currentValue": "build", "options": []},
            {"id": "model", "category": "model", "type": "select",
             "currentValue": "opencode/big-pickle",
             "options": [{"value": "opencode/beam-free"}, {"value": "opencode/big-pickle"}]}]});
        let (models, current, switch) = model_catalog(&reply);
        assert_eq!(models, ["opencode/beam-free", "opencode/big-pickle"]);
        assert_eq!(current.as_deref(), Some("opencode/big-pickle"));
        assert_eq!(switch, ModelSwitch::ConfigOption("model".into()));

        let older = json!({"models": {"currentModelId": "gpt-5",
            "availableModels": [{"modelId": "gpt-5"}, {"modelId": "claude-sonnet-5"}]}});
        let (models, current, switch) = model_catalog(&older);
        assert_eq!(models, ["gpt-5", "claude-sonnet-5"]);
        assert_eq!(current.as_deref(), Some("gpt-5"));
        assert_eq!(switch, ModelSwitch::SetModel);

        assert_eq!(model_catalog(&json!({})).2, ModelSwitch::None);

        // Copilot's duplicated `auto`.
        let copilot = json!({"configOptions": [{"id": "model", "category": "model",
            "currentValue": "auto", "options": [{"value": "auto"}, {"value": "auto"}]}]});
        assert_eq!(model_catalog(&copilot).0, ["auto"]);
    }

    #[test]
    fn copilot_gets_its_brief_where_it_applies_one() {
        let (path, text) = Profile::Copilot.brief_layout("Be brief.");
        assert_eq!(path, ".github/instructions/omatether.instructions.md");
        assert!(text.starts_with("---\napplyTo: \"**\"\n---\n"));
        assert!(text.ends_with("Be brief."));
        assert_eq!(Profile::Opencode.brief_layout("Be brief.").1, "Be brief.");
    }

    #[test]
    fn copilot_allow_all_is_noticed() {
        let reply = |value: &str| {
            json!({"configOptions": [
            {"id": "allow_all", "category": "other", "currentValue": value,
             "options": [{"value": "on"}, {"value": "off"}]}]})
        };
        assert!(allow_all_on(&reply("on")));
        assert!(!allow_all_on(&reply("off")));
        assert!(!allow_all_on(&json!({})));
    }

    #[test]
    fn opencode_is_told_to_ask_and_keeps_the_operators_config() {
        let config: Value = serde_json::from_str(&opencode_config(
            Some(
                r#"{"model":"x/y","permission":{"bash":{"rm *":"deny"}},"instructions":["a.md"]}"#,
            ),
            Some(Path::new("/s/brief/AGENTS.md")),
        ))
        .unwrap();
        assert_eq!(config["permission"]["*"], "ask");
        assert_eq!(config["permission"]["bash"]["rm *"], "deny");
        assert_eq!(config["model"], "x/y");
        assert_eq!(
            config["instructions"],
            json!(["a.md", "/s/brief/AGENTS.md"])
        );

        let bare: Value = serde_json::from_str(&opencode_config(None, None)).unwrap();
        assert_eq!(bare, json!({"permission": {"*": "ask"}}));
        // Garbage in the operator's variable is not a reason to run ungated.
        let junk: Value = serde_json::from_str(&opencode_config(Some("not json"), None)).unwrap();
        assert_eq!(junk["permission"]["*"], "ask");
    }

    #[test]
    fn tools_map_onto_the_shared_vocabulary() {
        let (name, input) = describe_tool(&json!({"kind": "edit",
            "locations": [{"path": "/w/a.rs"}], "rawInput": {"oldString": "x"}}));
        assert_eq!(name, "Edit");
        assert_eq!(input["file_path"], "/w/a.rs");
        assert_eq!(input["oldString"], "x");
        let (name, _) =
            describe_tool(&json!({"kind": "other", "name": "todowrite", "title": "todo"}));
        assert_eq!(name, "todowrite");
        let (name, input) = describe_tool(&json!({"kind": "execute", "title": "ls"}));
        assert_eq!(
            (name.as_str(), input["command"].as_str()),
            ("Bash", Some("ls"))
        );
    }

    #[test]
    fn model_lines_skip_anything_that_is_not_a_name() {
        let out =
            "mise WARN  some activation noise/here\nopencode/beam-free\n\nopencode/big-pickle\n";
        assert_eq!(
            parse_model_lines(out),
            ["opencode/beam-free", "opencode/big-pickle"]
        );
    }

    /// The brief, through this adapter's own spawn, against the real CLI.
    /// Spends a turn on each agent, so opt in:
    /// `cargo test -- --ignored live_brief`.
    #[tokio::test]
    #[ignore = "runs opencode and copilot; spends a model turn on each"]
    async fn live_brief_is_obeyed() {
        for profile in [Profile::Opencode, Profile::Copilot] {
            let (mut session, mut events) = AcpSession::spawn(Config {
                profile,
                cwd: std::env::temp_dir(),
                session_id: None,
                model: None,
                brief: Some(
                    "Always end every reply with the single word PINEAPPLE on its own line.".into(),
                ),
            })
            .await
            .unwrap();
            session.prompt("Say hello in five words.").await.unwrap();
            let mut reply = String::new();
            while let Some(event) = events.recv().await {
                match event {
                    AgentEvent::TextDelta { text } => reply.push_str(&text),
                    AgentEvent::TurnEnd { ok, detail } => {
                        assert!(ok, "{}: {detail:?}", profile.name());
                        break;
                    }
                    _ => {}
                }
            }
            session.shutdown().await.unwrap();
            assert!(reply.contains("PINEAPPLE"), "{}: {reply}", profile.name());
        }
    }

    #[test]
    fn only_opencode_and_copilot_have_a_profile() {
        assert_eq!(Profile::for_agent("opencode"), Some(Profile::Opencode));
        assert_eq!(Profile::for_agent("copilot"), Some(Profile::Copilot));
        assert_eq!(Profile::for_agent("crush"), None);
    }
}
