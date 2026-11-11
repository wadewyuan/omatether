//! The MCP channel: other AI agents on the tailnet, driving the agents here.
//!
//! A third channel beside Telegram and iMessage, and deliberately the same
//! shape: a caller's request becomes an [`Inbound`], the core runs it exactly
//! as it would a chat message — sessions, `/cd`, `/agent`, `/model`, auto mode
//! and all — and what the core sends back is collected here and returned as
//! the tool result. Nothing in the core knows a program is on the other end
//! except [`Audience::Agent`], which changes how the agent is briefed.
//!
//! **Transport.** MCP's Streamable HTTP, the request/response half only: a
//! POST to `/mcp` carries JSON-RPC and gets JSON back. No SSE stream, since
//! nothing here is pushed unasked; a turn longer than one request is picked up
//! with `get_result`. No session header either: a caller is identified by its
//! tailnet node, not by anything it says about itself.
//!
//! **Who may call: the tailnet, and only the tailnet.** Three things hold that
//! line. The listener binds to this machine's Tailscale addresses alone, so it
//! is not on the LAN or the internet at all. A connection whose source is not
//! a Tailscale address is dropped anyway, in case a packet for the 100.x
//! address arrived over another interface. And every request's peer is put
//! through `tailscale whois`, which only answers for a node on this tailnet
//! (or shared into it) — that is also where the caller's name comes from. An
//! optional allowlist narrows it further. There is no token: a tailnet node is
//! already an authenticated device, and that is the trust boundary asked for.
//!
//! **Approvals.** Threads here start in auto mode like every other channel's.
//! `/auto off` through `session_command` turns the gate on, and a question then
//! comes back as the result, answered with `/allow` or `/deny`.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use super::{inbox, Attachment, Audience, Channel, Inbound, InboundKind, MessageId, ThreadKey};

/// The name this channel puts in a [`ThreadKey`].
const NAME: &str = "mcp";

/// Where the MCP endpoint is served when no port is configured.
pub const DEFAULT_PORT: u16 = 8742;

/// Largest request body accepted.
///
/// Sized for files, not prompts: one attachment may be [`inbox::MAX_BYTES`],
/// base64 grows that by a third, and a caller may send a few at once.
const MAX_REQUEST: usize = 64 * 1024 * 1024;

/// Largest file handed back inline. Base64 in a JSON response grows it by a
/// third and the caller has to hold all of it in context, so well under what
/// the chat channels carry.
const MAX_FILE: u64 = 20 * 1024 * 1024;

/// How long a tool call waits for the turn before returning what it has.
///
/// Under the 60 seconds many MCP clients give a request by default, so the
/// common case — a caller that never set a timeout — gets an answer or a
/// "still running" rather than a timeout error that loses both.
const DEFAULT_WAIT: u64 = 45;

/// The longest a caller may ask one request to wait.
const MAX_WAIT: u64 = 600;

/// How much of a running turn's progress a "still running" reply carries: the
/// newest end of it, which is what says where the agent is now.
const PROGRESS_SHOWN: usize = 3000;

/// How long a `tailscale whois` answer is trusted.
const WHOIS_TTL: Duration = Duration::from_secs(600);

/// The protocol revisions this server speaks, newest first. It uses nothing
/// that differs between them.
const PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

pub struct Config {
    pub port: u16,
    /// Where files callers send are saved: the same inbox the chats use.
    pub inbox_dir: std::path::PathBuf,
    /// The API key file; see [`crate::mcp_keys`].
    pub keys_file: std::path::PathBuf,
    /// Node names or login names allowed to call. `None` is the whole tailnet.
    pub allowed: Option<Vec<String>>,
}

/// One caller's view of one session.
struct Session {
    /// Everything the core has sent since the caller's last request, in order.
    items: Vec<(MessageId, Item)>,
    /// Whether the core still owes this session something.
    busy: bool,
    /// When the current request was made.
    since: Instant,
    notify: Arc<Notify>,
    /// The running turn so far, as the core last reported it.
    progress: String,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            busy: false,
            since: Instant::now(),
            notify: Arc::new(Notify::new()),
            progress: String::new(),
        }
    }
}

enum Item {
    Text(String),
    File {
        name: String,
        mime: String,
        data: String,
        caption: String,
    },
}

/// Who is calling, as the tailnet knows it.
#[derive(Debug, Clone)]
struct Caller {
    /// The name of the API key it presented: which agent this is. Sessions
    /// belong to a key, so two agents on one machine do not share them.
    key: String,
    /// The node's short name, e.g. `macbook-air`.
    node: String,
    /// The owning user's login, or `tagged-devices`.
    login: String,
}

pub struct Mcp {
    sessions: Mutex<HashMap<String, Session>>,
    inbound: mpsc::Sender<Inbound>,
    allowed: Option<Vec<String>>,
    whois: Mutex<HashMap<IpAddr, (Caller, Instant)>>,
    next_id: std::sync::atomic::AtomicU64,
    inbox_dir: std::path::PathBuf,
    keys_file: std::path::PathBuf,
}

impl Mcp {
    /// The channel, and the inbound stream its callers produce. Serving starts
    /// with [`Mcp::serve`].
    pub fn new(config: &Config) -> (Arc<Self>, mpsc::Receiver<Inbound>) {
        let (tx, rx) = mpsc::channel(64);
        let mcp = Arc::new(Self {
            sessions: Mutex::new(HashMap::new()),
            inbound: tx,
            allowed: config.allowed.clone(),
            whois: Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            inbox_dir: config.inbox_dir.clone(),
            keys_file: config.keys_file.clone(),
        });
        (mcp, rx)
    }

    /// Bind to this machine's Tailscale addresses and serve, retrying in the
    /// background until Tailscale is up. Returns at once.
    ///
    /// At boot this service can start before tailscaled has an address, and
    /// failing the whole bridge for that would take Telegram down with it.
    pub fn serve(self: Arc<Self>, port: u16) {
        tokio::spawn(async move {
            let mut warned = false;
            loop {
                match bind(port).await {
                    Ok(listeners) => {
                        for listener in listeners {
                            tracing::info!(
                                "mcp listening on http://{}/mcp",
                                listener
                                    .local_addr()
                                    .map(|a| a.to_string())
                                    .unwrap_or_default()
                            );
                            tokio::spawn(self.clone().accept(listener));
                        }
                        return;
                    }
                    Err(e) => {
                        if !warned {
                            tracing::warn!("mcp not serving yet, retrying: {e:#}");
                            warned = true;
                        }
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }
                }
            }
        });
    }

    async fn accept(self: Arc<Self>, listener: tokio::net::TcpListener) {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!("mcp accept: {e}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            if !in_tailnet(peer.ip()) {
                tracing::warn!(%peer, "mcp: refused a connection from outside the tailnet");
                continue;
            }
            let this = self.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| {
                    let this = this.clone();
                    async move { Ok::<_, std::convert::Infallible>(this.handle(request, peer).await) }
                });
                let io = hyper_util::rt::TokioIo::new(stream);
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::debug!(%peer, "mcp connection: {e}");
                }
            });
        }
    }

    async fn handle(&self, request: Request<Incoming>, peer: SocketAddr) -> Response<Full<Bytes>> {
        if request.uri().path() != "/mcp" {
            return plain(StatusCode::NOT_FOUND, "not found; the endpoint is /mcp");
        }
        // A browser sends Origin; an agent's MCP client does not. Refusing it
        // is what stops a web page open on a tailnet device from reaching in
        // by DNS rebinding, which the spec asks every HTTP server to prevent.
        if request.headers().contains_key(hyper::header::ORIGIN) {
            return plain(StatusCode::FORBIDDEN, "browser requests are not accepted");
        }
        if request.method() != Method::POST {
            let mut response = plain(StatusCode::METHOD_NOT_ALLOWED, "POST only");
            response
                .headers_mut()
                .insert(hyper::header::ALLOW, "POST".parse().unwrap());
            return response;
        }

        let mut caller = match self.identify(peer.ip()).await {
            Ok(caller) => caller,
            Err(e) => {
                tracing::warn!(%peer, "mcp: refused: {e:#}");
                return plain(StatusCode::FORBIDDEN, &format!("{e:#}"));
            }
        };
        match self.authenticate(request.headers()) {
            Ok(key) => caller.key = key,
            Err(e) => {
                tracing::warn!(%peer, node = %caller.node, "mcp: refused: {e:#}");
                // No WWW-Authenticate: that header sends MCP clients off on
                // OAuth discovery, and there is no OAuth here to find.
                return plain(StatusCode::UNAUTHORIZED, &format!("{e:#}"));
            }
        }

        let body = match Limited::new(request.into_body(), MAX_REQUEST)
            .collect()
            .await
        {
            Ok(body) => body.to_bytes(),
            Err(_) => return plain(StatusCode::PAYLOAD_TOO_LARGE, "request too large"),
        };
        let message: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &rpc_error(Value::Null, -32700, &format!("parse error: {e}")),
                )
            }
        };

        // A batch is an array of messages; the 2025-03-26 revision allows one.
        let replies: Vec<Value> = match message {
            Value::Array(messages) => {
                let mut replies = Vec::new();
                for message in messages {
                    if let Some(reply) = self.dispatch(&caller, message).await {
                        replies.push(reply);
                    }
                }
                replies
            }
            message => self.dispatch(&caller, message).await.into_iter().collect(),
        };

        match replies.len() {
            // Only notifications and responses: accepted, nothing to say.
            0 => {
                let mut response = Response::new(Full::new(Bytes::new()));
                *response.status_mut() = StatusCode::ACCEPTED;
                response
            }
            1 if !body.starts_with(b"[") => json_response(StatusCode::OK, &replies[0]),
            _ => json_response(StatusCode::OK, &Value::Array(replies)),
        }
    }

    /// One JSON-RPC message in, at most one reply out.
    async fn dispatch(&self, caller: &Caller, message: Value) -> Option<Value> {
        let method = message.get("method").and_then(Value::as_str)?.to_string();
        // A notification has no id and gets no reply.
        let id = message.get("id").cloned()?;
        let params = message.get("params").cloned().unwrap_or(Value::Null);

        let result = match method.as_str() {
            "initialize" => {
                let asked = params
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let version = PROTOCOL_VERSIONS
                    .iter()
                    .find(|v| **v == asked)
                    .unwrap_or(&PROTOCOL_VERSIONS[0]);
                tracing::info!(node = %caller.node, login = %caller.login, "mcp: initialize");
                json!({
                    "protocolVersion": version,
                    "capabilities": { "tools": { "listChanged": false } },
                    "serverInfo": { "name": "omatether", "version": env!("CARGO_PKG_VERSION") },
                    "instructions": INSTRUCTIONS,
                })
            }
            "ping" => json!({}),
            "tools/list" => json!({ "tools": tools() }),
            "tools/call" => {
                let name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let args = params.get("arguments").cloned().unwrap_or(json!({}));
                match self.call(caller, name, &args).await {
                    Ok(result) => result,
                    Err(e) => tool_error(&format!("{e:#}")),
                }
            }
            _ => {
                return Some(rpc_error(
                    id,
                    -32601,
                    &format!("method not found: {method}"),
                ))
            }
        };
        Some(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
    }

    async fn call(&self, caller: &Caller, tool: &str, args: &Value) -> Result<Value> {
        let session = session_name(args)?;
        let wait = args
            .get("wait_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_WAIT)
            .min(MAX_WAIT);
        let key = thread_key(caller, &session);

        match tool {
            "run_task" => {
                let files = self.receive(&key, args)?;
                let prompt = args
                    .get("prompt")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if prompt.trim().is_empty() && files.is_empty() {
                    bail!("'prompt' is required, unless files are sent");
                }
                self.submit_with(caller, &key, prompt, files, true).await?;
                Ok(self.wait(&key, &session, wait).await)
            }
            "session_command" => {
                let command = string_arg(args, "command")?;
                if !command.starts_with('/') {
                    bail!("a command starts with '/'; send a prompt with run_task");
                }
                // `/stop`, `/allow` and `/deny` are about the turn in flight, so
                // they must get through while it runs; everything else waits
                // its turn like a prompt would.
                let now = matches!(
                    command.split_whitespace().next(),
                    Some("/stop" | "/allow" | "/yes" | "/deny" | "/no")
                );
                self.submit(caller, &key, command, !now).await?;
                Ok(self.wait(&key, &session, wait).await)
            }
            "stop_task" => {
                self.submit(caller, &key, "/stop", false).await?;
                Ok(self.wait(&key, &session, wait).await)
            }
            "get_result" => Ok(self.wait(&key, &session, wait).await),
            "list_sessions" => Ok(self.list(caller)),
            other => bail!("unknown tool: {other}"),
        }
    }

    /// Hand a caller's text to the core as if typed in a chat.
    ///
    /// A request to an idle session clears what the last one collected, so a
    /// result is always "what came back for this". One about a turn still in
    /// flight (`exclusive` false: `/stop`) keeps it, since its reply belongs to
    /// that turn; an exclusive one is refused while a turn runs.
    async fn submit(
        &self,
        caller: &Caller,
        key: &ThreadKey,
        text: &str,
        exclusive: bool,
    ) -> Result<()> {
        self.submit_with(caller, key, text, Vec::new(), exclusive)
            .await
    }

    /// Save the files a `run_task` carries into the inbox, as the chat
    /// channels do with a photo or a document.
    ///
    /// A file that cannot be decoded or is too big is still an [`Attachment`],
    /// with the reason as its error, so the agent is told about it rather than
    /// the file silently going missing. Only a malformed `files` argument is
    /// refused outright, since that is the caller's bug to fix.
    fn receive(&self, key: &ThreadKey, args: &Value) -> Result<Vec<Attachment>> {
        let Some(list) = args.get("files") else {
            return Ok(Vec::new());
        };
        let list = list.as_array().context("'files' must be an array")?;
        let mut files = Vec::new();
        for (i, file) in list.iter().enumerate() {
            let name = file
                .get("name")
                .and_then(Value::as_str)
                .filter(|n| !n.trim().is_empty())
                .with_context(|| format!("files[{i}] needs a 'name'"))?
                .to_string();
            let bytes = match (
                file.get("data").and_then(Value::as_str),
                file.get("text").and_then(Value::as_str),
            ) {
                (Some(data), None) => base64::engine::general_purpose::STANDARD
                    .decode(data.trim())
                    .map_err(|e| format!("its data is not valid base64 ({e})")),
                (None, Some(text)) => Ok(text.as_bytes().to_vec()),
                _ => bail!("files[{i}] needs exactly one of 'data' (base64) or 'text'"),
            };
            let mime = file
                .get("mime_type")
                .and_then(Value::as_str)
                .unwrap_or_else(|| mime_for(&name))
                .to_string();
            let size = bytes.as_ref().map_or(0, |b| b.len() as u64);
            let saved = bytes.and_then(|bytes| {
                if bytes.len() as u64 > inbox::MAX_BYTES {
                    return Err(format!(
                        "it is {}, over the {} limit",
                        inbox::human(bytes.len() as u64),
                        inbox::human(inbox::MAX_BYTES)
                    ));
                }
                inbox::save(&self.inbox_dir, key, &name, &bytes)
                    .map_err(|e| format!("it could not be saved: {e}"))
            });
            files.push(Attachment {
                name,
                mime,
                size,
                saved,
            });
        }
        if !files.is_empty() {
            inbox::prune(&self.inbox_dir);
        }
        Ok(files)
    }

    async fn submit_with(
        &self,
        caller: &Caller,
        key: &ThreadKey,
        text: &str,
        files: Vec<Attachment>,
        exclusive: bool,
    ) -> Result<()> {
        {
            let mut sessions = self.sessions.lock().unwrap();
            let session = sessions.entry(key.to_string()).or_default();
            if exclusive && session.busy {
                bail!(
                    "session is still working on the last request ({}s so far). \
                     Call get_result to wait for it, or stop_task to interrupt it.",
                    session.since.elapsed().as_secs()
                );
            }
            if !session.busy {
                session.items.clear();
                session.progress.clear();
                session.since = Instant::now();
            }
            session.busy = true;
        }

        let message = Inbound {
            thread: key.clone(),
            user_id: format!("key {} ({} on {})", caller.key, caller.login, caller.node),
            kind: InboundKind::Text {
                text: text.to_string(),
                files,
            },
        };
        if self.inbound.send(message).await.is_err() {
            if let Some(session) = self.sessions.lock().unwrap().get_mut(&key.to_string()) {
                session.busy = false;
            }
            bail!("omatether is shutting down");
        }
        Ok(())
    }

    /// Wait for the session to go idle, up to `seconds`, and say what came back.
    async fn wait(&self, key: &ThreadKey, session: &str, seconds: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
        loop {
            let notify = {
                let mut sessions = self.sessions.lock().unwrap();
                sessions.entry(key.to_string()).or_default().notify.clone()
            };
            // Registered before the check, so an idle landing in between is
            // not missed.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let busy = self
                .sessions
                .lock()
                .unwrap()
                .get(&key.to_string())
                .is_some_and(|s| s.busy);
            if !busy {
                break;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
        }
        self.result(key, session)
    }

    fn result(&self, key: &ThreadKey, name: &str) -> Value {
        let sessions = self.sessions.lock().unwrap();
        let Some(session) = sessions.get(&key.to_string()) else {
            return json!({ "content": [{ "type": "text", "text": "No such session yet." }] });
        };

        let mut content = Vec::new();
        let texts: Vec<&str> = session
            .items
            .iter()
            .filter_map(|(_, item)| match item {
                Item::Text(text) => Some(text.as_str()),
                Item::File { .. } => None,
            })
            .collect();
        let mut text = texts.join("\n\n");

        if session.busy {
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            let elapsed = session.since.elapsed().as_secs();
            if session.progress.is_empty() {
                text.push_str(&format!(
                    "[still running after {elapsed}s; nothing to show yet]"
                ));
            } else {
                text.push_str(&format!(
                    "[still running after {elapsed}s. So far (▸ is a tool call):]\n{}",
                    tail(&session.progress, PROGRESS_SHOWN)
                ));
            }
            text.push_str(&format!(
                "\n\n[call get_result with session \"{name}\" to keep waiting, or stop_task \
                 to interrupt]"
            ));
        } else if text.is_empty() && session.items.is_empty() {
            text.push_str("(no output)");
        }
        content.push(json!({ "type": "text", "text": text }));

        for (_, item) in &session.items {
            if let Item::File {
                name,
                mime,
                data,
                caption,
            } = item
            {
                if !caption.is_empty() {
                    content.push(json!({ "type": "text", "text": format!("{name}: {caption}") }));
                }
                if mime.starts_with("image/") {
                    content.push(json!({ "type": "image", "data": data, "mimeType": mime }));
                } else {
                    content.push(json!({
                        "type": "resource",
                        "resource": { "uri": format!("file:///{name}"), "mimeType": mime, "blob": data },
                    }));
                }
            }
        }

        json!({
            "content": content,
            "structuredContent": {
                "session": name,
                "status": if session.busy { "running" } else { "done" },
            },
        })
    }

    fn list(&self, caller: &Caller) -> Value {
        let prefix = format!("{NAME}:{}:", caller.key);
        let sessions = self.sessions.lock().unwrap();
        let mut lines: Vec<String> = sessions
            .iter()
            .filter_map(|(key, s)| {
                let name = key.strip_prefix(&prefix)?;
                Some(if s.busy {
                    format!("{name}: running ({}s)", s.since.elapsed().as_secs())
                } else {
                    format!("{name}: idle")
                })
            })
            .collect();
        lines.sort();
        let text = if lines.is_empty() {
            "No sessions since omatether started. Any session name works; run_task creates it."
                .to_string()
        } else {
            lines.join("\n")
        };
        json!({ "content": [{ "type": "text", "text": text }] })
    }

    /// The name of the API key this request carries, or why it is refused.
    ///
    /// `Authorization: Bearer <key>`, or `X-API-Key: <key>` for clients that
    /// only let you set a plain header. Fails closed: with no keys made, every
    /// request is refused and the message says how to make one.
    fn authenticate(&self, headers: &hyper::HeaderMap) -> Result<String> {
        let presented = headers
            .get(hyper::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                v.strip_prefix("Bearer ")
                    .or_else(|| v.strip_prefix("bearer "))
            })
            .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()))
            .map(str::trim)
            .filter(|k| !k.is_empty());
        let Some(presented) = presented else {
            bail!("an API key is required: send `Authorization: Bearer <key>`");
        };
        let keys = crate::mcp_keys::load(&self.keys_file)?;
        if keys.is_empty() {
            bail!("no API keys are set up on this machine; its owner can make one with `omatether mcp-key add <name>`");
        }
        crate::mcp_keys::verify(&keys, presented).context("that API key is not valid")
    }

    /// Who is at `ip`, according to the tailnet — or why they may not call.
    async fn identify(&self, ip: IpAddr) -> Result<Caller> {
        let cached = self
            .whois
            .lock()
            .unwrap()
            .get(&ip)
            .filter(|(_, at)| at.elapsed() < WHOIS_TTL)
            .map(|(caller, _)| caller.clone());
        let caller = match cached {
            Some(caller) => caller,
            None => {
                let caller = whois(ip).await?;
                self.whois
                    .lock()
                    .unwrap()
                    .insert(ip, (caller.clone(), Instant::now()));
                caller
            }
        };

        if let Some(allowed) = &self.allowed {
            let ok = allowed.iter().any(|a| {
                a.eq_ignore_ascii_case(&caller.node) || a.eq_ignore_ascii_case(&caller.login)
            });
            if !ok {
                bail!(
                    "{} ({}) is not in OMATETHER_MCP_ALLOWED",
                    caller.node,
                    caller.login
                );
            }
        }
        Ok(caller)
    }

    fn push(&self, thread: &ThreadKey, item: Item) -> MessageId {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .to_string();
        self.sessions
            .lock()
            .unwrap()
            .entry(thread.to_string())
            .or_default()
            .items
            .push((id.clone(), item));
        id
    }
}

#[async_trait]
impl Channel for Mcp {
    fn name(&self) -> &'static str {
        NAME
    }

    /// Nothing is read until the turn is over, so there is nothing to edit.
    fn can_edit(&self) -> bool {
        false
    }

    fn can_send_files(&self) -> bool {
        true
    }

    fn audience(&self) -> Audience {
        Audience::Agent
    }

    async fn send(&self, thread: &ThreadKey, text: &str) -> Result<MessageId> {
        Ok(self.push(thread, Item::Text(text.to_string())))
    }

    async fn edit(&self, thread: &ThreadKey, id: &MessageId, text: &str) -> Result<()> {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&thread.to_string()) {
            if let Some((_, item)) = session.items.iter_mut().find(|(i, _)| i == id) {
                *item = Item::Text(text.to_string());
            }
        }
        Ok(())
    }

    async fn send_file(&self, thread: &ThreadKey, path: &Path, caption: &str) -> Result<MessageId> {
        let size = tokio::fs::metadata(path)
            .await
            .with_context(|| format!("{} is gone", path.display()))?
            .len();
        if size > MAX_FILE {
            bail!(
                "{} MB is over the {} MB a file can be returned inline",
                size / 1024 / 1024,
                MAX_FILE / 1024 / 1024
            );
        }
        let bytes = tokio::fs::read(path).await?;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_string());
        Ok(self.push(
            thread,
            Item::File {
                mime: mime_for(&name).to_string(),
                name,
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
                caption: caption.to_string(),
            },
        ))
    }

    async fn ask_permission(
        &self,
        thread: &ThreadKey,
        text: &str,
        _question: &str,
    ) -> Result<MessageId> {
        let question = format!(
            "{text}\n\nThe agent is waiting for approval. Answer with session_command \
             \"/allow\" or \"/deny <why>\"."
        );
        Ok(self.push(thread, Item::Text(question)))
    }

    fn wants_progress(&self) -> bool {
        true
    }

    async fn progress(&self, thread: &ThreadKey, log: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.entry(thread.to_string()).or_default();
        // Only while a request is open on it: progress landing after the idle
        // would be a finished turn's, shown under the next request.
        if session.busy {
            session.progress = log.to_string();
        }
    }

    async fn idle(&self, thread: &ThreadKey) {
        let notify = {
            let mut sessions = self.sessions.lock().unwrap();
            let session = sessions.entry(thread.to_string()).or_default();
            session.busy = false;
            session.notify.clone()
        };
        notify.notify_waiters();
    }
}

// ---- tailnet ----------------------------------------------------------------

/// Whether an address is one Tailscale hands out: 100.64.0.0/10, or its
/// IPv6 range fd7a:115c:a1e0::/48.
fn in_tailnet(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, ..] = v4.octets();
            a == 100 && (64..128).contains(&b)
        }
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => in_tailnet(IpAddr::V4(v4)),
            None => {
                let s = v6.segments();
                s[0] == 0xfd7a && s[1] == 0x115c && s[2] == 0xa1e0
            }
        },
    }
}

/// Listen on every Tailscale address this machine has.
async fn bind(port: u16) -> Result<Vec<tokio::net::TcpListener>> {
    let mut addresses = Vec::new();
    for family in ["-4", "-6"] {
        let output = tokio::process::Command::new("tailscale")
            .args(["ip", family])
            .output()
            .await
            .context("running `tailscale ip`")?;
        if output.status.success() {
            addresses.extend(
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .filter_map(|line| line.trim().parse::<IpAddr>().ok())
                    .filter(|ip| in_tailnet(*ip)),
            );
        }
    }
    if addresses.is_empty() {
        bail!("Tailscale has no address yet — is it up?");
    }

    let mut listeners = Vec::new();
    let mut last_error = None;
    for ip in addresses {
        match tokio::net::TcpListener::bind(SocketAddr::new(ip, port)).await {
            Ok(listener) => listeners.push(listener),
            Err(e) => last_error = Some(anyhow::anyhow!("binding {ip}:{port}: {e}")),
        }
    }
    match (listeners.is_empty(), last_error) {
        (true, Some(e)) => Err(e),
        _ => Ok(listeners),
    }
}

/// Ask tailscaled who is at `ip`. Fails for anything not on the tailnet.
async fn whois(ip: IpAddr) -> Result<Caller> {
    let output = tokio::process::Command::new("tailscale")
        .args(["whois", "--json", &ip.to_string()])
        .output()
        .await
        .context("running `tailscale whois`")?;
    if !output.status.success() {
        bail!("{ip} is not a node on this tailnet");
    }
    let info: Value =
        serde_json::from_slice(&output.stdout).context("parsing `tailscale whois`")?;
    parse_whois(&info).with_context(|| format!("`tailscale whois {ip}` named no node"))
}

fn parse_whois(info: &Value) -> Option<Caller> {
    let fqdn = info.pointer("/Node/Name").and_then(Value::as_str)?;
    let node = fqdn
        .split('.')
        .next()
        .filter(|n| !n.is_empty())?
        .to_string();
    let login = info
        .pointer("/UserProfile/LoginName")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    // The key is not known yet: it comes from the request, not the network.
    Some(Caller {
        key: String::new(),
        node,
        login,
    })
}

// ---- protocol ---------------------------------------------------------------

const INSTRUCTIONS: &str = "omatether runs coding agents (Claude Code by default; \
also codex, pi and others) on the user's workstation. Delegate a task with run_task: \
the agent works in its session's directory with full tool access and returns its final \
answer. A session keeps its conversation across calls; use one session name per line of \
work and several to run tasks in parallel. Long tasks return a 'running' status — keep \
calling get_result. session_command takes omatether's chat commands: /status, /new, \
/cd <path>, /agent <name>, /model [name], /auto [on|off], /log, /send <path>, /help.";

fn tools() -> Value {
    let session = json!({
        "type": "string",
        "description": "Session name (letters, digits, - and _). Each is its own conversation \
                        and working directory. Default \"default\".",
    });
    let wait = json!({
        "type": "integer",
        "minimum": 0,
        "maximum": MAX_WAIT,
        "description": format!("Seconds to wait for the agent before returning a 'running' \
                                status. Default {DEFAULT_WAIT}; keep it under your client's \
                                request timeout."),
    });
    json!([
        {
            "name": "run_task",
            "title": "Run a task with a local agent",
            "description": "Send a prompt to the coding agent in a session on the user's \
                workstation and wait for its answer. Continues that session's conversation. \
                Fails if the session is still busy with an earlier task. Files and images \
                can go with it: they are saved on the workstation and the agent is told \
                where, the same as a photo sent from a phone.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "prompt": { "type": "string", "description": "The task, as you would type it to the agent. May be empty when files are sent." },
                    "files": {
                        "type": "array",
                        "description": format!("Files for the agent, each up to {}. Give \
                            'data' as base64 for any file, or 'text' for a text file.",
                            inbox::human(inbox::MAX_BYTES)),
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string", "description": "File name, e.g. screenshot.png." },
                                "data": { "type": "string", "description": "Contents, base64." },
                                "text": { "type": "string", "description": "Contents, as text (instead of data)." },
                                "mime_type": { "type": "string", "description": "Optional; guessed from the name." },
                            },
                            "required": ["name"],
                        },
                    },
                    "session": session,
                    "wait_seconds": wait,
                },
            },
        },
        {
            "name": "get_result",
            "title": "Wait for a session's result",
            "description": "Wait for the running task in a session and return everything it \
                has produced so far. Returns the last result again if nothing is running.",
            "inputSchema": {
                "type": "object",
                "properties": { "session": session, "wait_seconds": wait },
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "stop_task",
            "title": "Interrupt a running task",
            "description": "Interrupt the task running in a session. The conversation is kept.",
            "inputSchema": {
                "type": "object",
                "properties": { "session": session },
            },
        },
        {
            "name": "session_command",
            "title": "Run an omatether command",
            "description": "Run one of omatether's commands in a session: /status, /new (fresh \
                conversation), /cd <path> (change working directory; starts fresh), /agent \
                <claude|codex|pi|...>, /model [name], /auto [on|off], /allow, /deny <why>, \
                /log (last turn with every tool call), /send <path>, /help.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "The command, starting with '/'." },
                    "session": session,
                    "wait_seconds": wait,
                },
                "required": ["command"],
            },
        },
        {
            "name": "list_sessions",
            "title": "List your sessions",
            "description": "Your sessions since omatether last started, and whether each is running.",
            "inputSchema": { "type": "object", "properties": {} },
            "annotations": { "readOnlyHint": true },
        },
    ])
}

fn session_name(args: &Value) -> Result<String> {
    let name = args
        .get("session")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("default");
    let ok = name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !ok {
        bail!("session names are up to 64 letters, digits, '-' and '_'");
    }
    Ok(name.to_string())
}

fn string_arg<'a>(args: &'a Value, name: &str) -> Result<&'a str> {
    args.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .with_context(|| format!("'{name}' is required"))
}

fn thread_key(caller: &Caller, session: &str) -> ThreadKey {
    ThreadKey {
        channel: NAME,
        chat_id: caller.key.clone(),
        topic_id: Some(session.to_string()),
    }
}

/// The last `max` characters of `text`, starting on a line boundary, with a
/// marker where the rest was left out.
fn tail(text: &str, max: usize) -> String {
    let count = text.chars().count();
    if count <= max {
        return text.to_string();
    }
    let start = text.char_indices().nth(count - max).map_or(0, |(i, _)| i);
    let cut = &text[start..];
    let cut = cut.find('\n').map_or(cut, |n| &cut[n + 1..]);
    format!("… (earlier steps left out)\n{cut}")
}

fn tool_error(text: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": true })
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn json_response(status: StatusCode, value: &Value) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(value.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    response
}

fn plain(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(text.to_string())));
    *response.status_mut() = status;
    response
}

fn mime_for(name: &str) -> &'static str {
    let ext = name
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "txt" | "log" | "md" | "csv" | "rs" | "py" | "sh" | "toml" | "yaml" | "yml" => "text/plain",
        "html" => "text/html",
        "zip" => "application/zip",
        "mp4" => "video/mp4",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            port: 0,
            allowed: None,
            inbox_dir: std::env::temp_dir().join(format!("omt-mcp-inbox-{}", std::process::id())),
            keys_file: std::env::temp_dir().join(format!("omt-mcp-keys-{}", std::process::id())),
        }
    }

    /// A file sent with a task lands in the inbox and reaches the core as an
    /// attachment; one that cannot be decoded still arrives, as its reason.
    #[tokio::test]
    async fn files_sent_with_a_task_arrive_as_attachments() {
        let (mcp, mut rx) = Mcp::new(&test_config());
        mcp.call(
            &caller(),
            "run_task",
            &json!({
                "prompt": "",
                "files": [
                    { "name": "../../shot.png", "data": base64::engine::general_purpose::STANDARD.encode(b"PNGDATA") },
                    { "name": "notes.md", "text": "hello" },
                    { "name": "broken.bin", "data": "!!!not base64" },
                ],
                "wait_seconds": 0,
            }),
        )
        .await
        .unwrap();
        let inbound = rx.recv().await.unwrap();
        let InboundKind::Text { files, .. } = inbound.kind else {
            panic!()
        };
        assert_eq!(files.len(), 3);

        let shot = files[0].saved.as_ref().unwrap();
        assert_eq!(std::fs::read(shot).unwrap(), b"PNGDATA");
        assert!(shot.starts_with(&test_config().inbox_dir), "{shot:?}");
        assert!(shot.to_string_lossy().ends_with("-shot.png"), "{shot:?}");
        assert_eq!(files[0].mime, "image/png");
        assert_eq!(
            std::fs::read_to_string(files[1].saved.as_ref().unwrap()).unwrap(),
            "hello"
        );
        assert!(files[2].saved.as_ref().unwrap_err().contains("base64"));

        assert!(mcp
            .call(
                &caller(),
                "run_task",
                &json!({ "session": "x", "files": [{ "name": "a" }] })
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
        assert!(mcp
            .call(&caller(), "run_task", &json!({ "session": "y" }))
            .await
            .is_err());
        std::fs::remove_dir_all(test_config().inbox_dir).ok();
    }

    #[test]
    fn requests_need_a_valid_key_and_none_set_up_refuses_all() {
        let config = test_config();
        let (mcp, _rx) = Mcp::new(&config);
        let headers = |pairs: &[(&'static str, &str)]| {
            let mut map = hyper::HeaderMap::new();
            for (k, v) in pairs {
                map.insert(*k, v.parse().unwrap());
            }
            map
        };
        let _ = std::fs::remove_file(&config.keys_file);
        assert!(mcp
            .authenticate(&headers(&[]))
            .unwrap_err()
            .to_string()
            .contains("required"));
        assert!(mcp
            .authenticate(&headers(&[("authorization", "Bearer omt_x")]))
            .unwrap_err()
            .to_string()
            .contains("mcp-key add"));

        let key = crate::mcp_keys::add(&config.keys_file, "grokbot").unwrap();
        let bearer = format!("Bearer {key}");
        assert_eq!(
            mcp.authenticate(&headers(&[("authorization", &bearer)]))
                .unwrap(),
            "grokbot"
        );
        assert_eq!(
            mcp.authenticate(&headers(&[("x-api-key", &key)])).unwrap(),
            "grokbot"
        );
        assert!(mcp
            .authenticate(&headers(&[("authorization", "Bearer omt_wrong")]))
            .is_err());
        // Revoked takes effect on the next request, no restart.
        crate::mcp_keys::revoke(&config.keys_file, "grokbot").unwrap();
        assert!(mcp
            .authenticate(&headers(&[("authorization", &bearer)]))
            .is_err());
        std::fs::remove_file(&config.keys_file).ok();
    }

    #[tokio::test]
    async fn a_running_task_reports_how_far_it_has_got() {
        let (mcp, mut rx) = Mcp::new(&test_config());
        let first = mcp
            .call(
                &caller(),
                "run_task",
                &json!({ "prompt": "go", "wait_seconds": 0 }),
            )
            .await
            .unwrap();
        assert!(first["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("nothing to show yet"));
        let inbound = rx.recv().await.unwrap();

        mcp.progress(
            &inbound.thread,
            "▸ Bash systemd-analyze\nKernel stage is slow.",
        )
        .await;
        let running = mcp
            .call(&caller(), "get_result", &json!({ "wait_seconds": 0 }))
            .await
            .unwrap();
        let text = running["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("▸ Bash systemd-analyze") && text.contains("get_result"),
            "{text}"
        );

        mcp.send(&inbound.thread, "Answer.").await.unwrap();
        mcp.idle(&inbound.thread).await;
        // Late progress from the finished turn must not leak into the result.
        mcp.progress(&inbound.thread, "▸ stale").await;
        let done = mcp
            .call(&caller(), "get_result", &json!({ "wait_seconds": 0 }))
            .await
            .unwrap();
        assert_eq!(done["content"][0]["text"], "Answer.");
    }

    #[test]
    fn progress_keeps_the_newest_end() {
        let log: String = (0..500).map(|i| format!("▸ step {i}\n")).collect();
        let cut = tail(&log, 300);
        assert!(
            cut.starts_with("… (earlier steps left out)\n▸ step"),
            "{cut}"
        );
        assert!(cut.ends_with("▸ step 499\n"));
        assert!(cut.chars().count() < 340);
        assert_eq!(tail("short", 300), "short");
    }

    fn caller() -> Caller {
        Caller {
            key: "macbook-agent".into(),
            node: "macbook-air".into(),
            login: "someone@".into(),
        }
    }

    #[test]
    fn only_tailscale_addresses_are_in_the_tailnet() {
        for ok in [
            "100.64.0.1",
            "100.89.169.35",
            "100.127.255.254",
            "fd7a:115c:a1e0::1",
            "::ffff:100.90.1.1",
        ] {
            assert!(in_tailnet(ok.parse().unwrap()), "{ok}");
        }
        for no in [
            "100.63.255.255",
            "100.128.0.1",
            "192.168.1.10",
            "127.0.0.1",
            "10.0.0.1",
            "::1",
            "fd7a:115c:a1e1::1",
        ] {
            assert!(!in_tailnet(no.parse().unwrap()), "{no}");
        }
    }

    #[test]
    fn whois_names_the_node_by_its_short_name() {
        let info = json!({
            "Node": { "Name": "meta-story-2.tail476844.ts.net." },
            "UserProfile": { "LoginName": "someone@example.com" },
        });
        let caller = parse_whois(&info).unwrap();
        assert_eq!(caller.node, "meta-story-2");
        assert_eq!(caller.login, "someone@example.com");
        assert!(parse_whois(&json!({})).is_none());
    }

    #[test]
    fn session_names_are_safe_to_put_in_a_thread_key() {
        assert_eq!(session_name(&json!({})).unwrap(), "default");
        assert_eq!(
            session_name(&json!({ "session": "build_2-x" })).unwrap(),
            "build_2-x"
        );
        assert!(session_name(&json!({ "session": "a:b" })).is_err());
        assert!(session_name(&json!({ "session": "../x" })).is_err());
        assert_eq!(thread_key(&caller(), "x").to_string(), "mcp:macbook-agent:x");
    }

    #[tokio::test]
    async fn initialize_and_list_tools() {
        let (mcp, _rx) = Mcp::new(&test_config());
        let init = mcp
            .dispatch(
                &caller(),
                json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": "2025-03-26" } }),
            )
            .await
            .unwrap();
        assert_eq!(init["result"]["protocolVersion"], "2025-03-26");
        assert!(mcp
            .dispatch(
                &caller(),
                json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })
            )
            .await
            .is_none());
        let list = mcp
            .dispatch(
                &caller(),
                json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
            )
            .await
            .unwrap();
        let names: Vec<&str> = list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "run_task",
                "get_result",
                "stop_task",
                "session_command",
                "list_sessions"
            ]
        );
        let unknown = mcp
            .dispatch(
                &caller(),
                json!({ "jsonrpc": "2.0", "id": 3, "method": "nope" }),
            )
            .await
            .unwrap();
        assert_eq!(unknown["error"]["code"], -32601);
    }

    /// The whole round trip with a stand-in for the core: the prompt arrives as
    /// an inbound, the reply is collected, and idle is what releases the caller.
    #[tokio::test]
    async fn a_task_returns_what_the_core_sent_once_it_is_idle() {
        let (mcp, mut rx) = Mcp::new(&test_config());
        let core = {
            let mcp = mcp.clone();
            tokio::spawn(async move {
                let inbound = rx.recv().await.unwrap();
                let InboundKind::Text { text, .. } = &inbound.kind else {
                    panic!()
                };
                assert_eq!(text, "build it");
                assert_eq!(inbound.thread.to_string(), "mcp:macbook-agent:default");
                tokio::time::sleep(Duration::from_millis(50)).await;
                mcp.send(&inbound.thread, "Built.").await.unwrap();
                mcp.idle(&inbound.thread).await;
            })
        };
        let result = mcp
            .call(
                &caller(),
                "run_task",
                &json!({ "prompt": "build it", "wait_seconds": 5 }),
            )
            .await
            .unwrap();
        core.await.unwrap();
        assert_eq!(result["content"][0]["text"], "Built.");
        assert_eq!(result["structuredContent"]["status"], "done");
    }

    #[tokio::test]
    async fn a_busy_session_refuses_a_second_task_but_not_a_stop() {
        let (mcp, mut rx) = Mcp::new(&test_config());
        let first = mcp
            .call(
                &caller(),
                "run_task",
                &json!({ "prompt": "long", "wait_seconds": 0 }),
            )
            .await
            .unwrap();
        assert_eq!(first["structuredContent"]["status"], "running");
        rx.recv().await.unwrap();

        let second = mcp
            .call(
                &caller(),
                "run_task",
                &json!({ "prompt": "again", "wait_seconds": 0 }),
            )
            .await;
        assert!(second.unwrap_err().to_string().contains("still working"));

        mcp.call(&caller(), "stop_task", &json!({ "wait_seconds": 0 }))
            .await
            .unwrap();
        let stop = rx.recv().await.unwrap();
        assert!(matches!(stop.kind, InboundKind::Text { ref text, .. } if text == "/stop"));
    }
}
