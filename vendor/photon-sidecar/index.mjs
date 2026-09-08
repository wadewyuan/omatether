// Photon (iMessage) sidecar.
//
// Photon's Spectrum SDK is TypeScript only, so the send path cannot live in the
// Rust core. This process owns the SDK and exposes the two things omatether
// needs over loopback HTTP:
//
//   GET  /inbound  -> NDJSON stream, one normalized inbound message per line
//                     (`text` may be empty — an attachment with no words)
//   POST /send     -> { spaceId, text, format? } -> { ok, messageId }
//   POST /typing   -> { spaceId, state? }          -> { ok }
//
// Both require `X-Omatether-Token`. It binds to 127.0.0.1 only; nothing here
// is safe to expose.
//
// The SDK usage below (Spectrum construction, the `app.messages` async
// iterator, space resolution by phone vs opaque id) was learned from the Hermes
// Photon plugin, which solved the same problem first. The code is ours; the
// protocol knowledge is theirs.
//
// Lifetime: with PHOTON_SIDECAR_WATCH_STDIN=1 the process exits when stdin
// closes, so spawning it with a piped stdin binds it to the parent's life.

import http from "node:http";

const projectId = requireEnv("PHOTON_PROJECT_ID");
const projectSecret = requireEnv("PHOTON_PROJECT_SECRET");
const token = requireEnv("PHOTON_SIDECAR_TOKEN");
const port = Number(process.env.PHOTON_SIDECAR_PORT || 8789);

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`photon-sidecar: ${name} is required`);
    process.exit(2);
  }
  return value;
}

const { Spectrum, text: spectrumText, markdown: spectrumMarkdown } = await import("spectrum-ts");
const { imessage } = await import("spectrum-ts/providers/imessage");

// Spectrum validates credentials against Photon's cloud during construction,
// so a bad project id fails here rather than at first use. Report it as a
// sentence: an unhandled rejection would give the supervisor a stack trace and
// a 20-second readiness timeout instead of the actual reason.
let app;
try {
  app = await Spectrum({
    projectId,
    projectSecret,
    providers: [imessage.config()],
    // Treat a group and its members as one space, matching how a chat thread
    // reads to a person.
    options: { flattenGroups: true },
  });
} catch (e) {
  console.error("photon-sidecar: could not start Spectrum — " + String(e?.message ?? e));
  process.exit(4);
}

// ---------------------------------------------------------------------------
// Inbound

/** Open /inbound responses. Each gets every message from the moment it connects. */
const subscribers = new Set();

function broadcast(event) {
  const line = JSON.stringify(event) + "\n";
  for (const res of subscribers) {
    try {
      res.write(line);
    } catch {
      subscribers.delete(res);
    }
  }
}

function normalize(space, message) {
  const content = message?.content;
  // Content is a list of parts on some platforms and a bare string on others.
  const text =
    typeof content === "string"
      ? content
      : Array.isArray(content)
        ? content
            .map((part) => (typeof part === "string" ? part : (part?.text ?? "")))
            .join("")
        : (content?.text ?? "");

  return {
    messageId: message?.id ?? null,
    spaceId: space?.id ?? message?.space?.id ?? null,
    spaceType: space?.type ?? "dm",
    senderId: message?.sender?.id ?? space?.phone ?? null,
    text: (text || "").trim(),
  };
}

// The SDK's stream can end or error without being fatal. Re-subscribe with
// capped backoff rather than dying; omatether dedupes on messageId, so a
// catch-up replay is harmless.
(async () => {
  let backoff = 1000;
  for (;;) {
    try {
      for await (const [space, message] of app.messages) {
        backoff = 1000;
        // Our own outbound messages come back through the same stream.
        if (message?.direction && message.direction !== "inbound") continue;

        const event = normalize(space, message);
        // Forwarded even with no text: a voice note, a sticker or a bare image
        // is still someone talking to the bridge, and omatether answers it
        // rather than leaving them looking at a chat where nothing happened.
        // Deciding that here would put the rule in two places; the space id is
        // the only thing that is genuinely unusable.
        if (event.spaceId) broadcast(event);
      }
      console.error("photon-sidecar: inbound stream ended — re-subscribing");
    } catch (e) {
      console.error("photon-sidecar: inbound stream error — " + String(e));
    }
    await new Promise((r) => setTimeout(r, backoff + Math.random() * backoff * 0.2));
    backoff = Math.min(backoff * 2, 30000);
  }
})();

// ---------------------------------------------------------------------------
// Outbound

const spaces = new Map();
const E164 = /^\+[1-9]\d{6,14}$/;
// Photon writes DM ids as `any;-;+15551234567`.
const DM_ID = /;-;(\+[1-9]\d{6,14})$/;

async function resolveSpace(spaceId) {
  const cached = spaces.get(spaceId);
  if (cached) return cached;

  const im = imessage(app);
  const phone = E164.test(spaceId) ? spaceId : (spaceId.match(DM_ID)?.[1] ?? null);

  let space = null;
  // A phone number addresses a DM directly, which also lets a caller send to a
  // number that has never written to us.
  if (phone) {
    try {
      space = await im.space.create(phone);
    } catch (e) {
      console.error("photon-sidecar: space.create failed — " + String(e));
    }
  }
  // Anything else is an opaque group id, rehydrated from the persisted id so
  // groups stay reachable across a restart.
  if (!space) {
    space = await im.space.get(spaceId);
  }
  if (!space) throw new Error(`unable to resolve space ${spaceId}`);

  spaces.set(spaceId, space);
  return space;
}

// ---------------------------------------------------------------------------
// HTTP

/// Send `text`, asking iMessage to render it as markdown when the caller says
/// it is markdown.
///
/// `markdown` is not a formatting hint the platform may ignore: the iMessage
/// adapter parses it and sends plain text plus native bold/italic ranges, so
/// `**bold**` arrives as bold rather than as four asterisks. It also means the
/// text is *rewritten* — which is why the caller chooses, and why anything that
/// must arrive verbatim (a permission question quoting a tool's own arguments)
/// asks for "text" instead. See `Photon::ask_permission`.
async function sendAs(space, text, format) {
  if (format !== "markdown") return space.send(spectrumText(text));

  try {
    return await space.send(spectrumMarkdown(text));
  } catch (e) {
    // The renderer refuses text that renders to nothing at all (`**`, an HTML
    // comment). That is a bad reason to lose a message, and it is raised
    // before anything is sent, so falling back cannot double-send. Any other
    // failure is a real one and stays thrown.
    if (e?.kind === "content" && e?.contentType === "markdown") {
      console.error("photon-sidecar: markdown render refused, sending as text — " + String(e?.message ?? e));
      return space.send(spectrumText(text));
    }
    throw e;
  }
}

function reply(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

async function readJson(req) {
  let raw = "";
  for await (const chunk of req) {
    raw += chunk;
    if (raw.length > 1_000_000) throw new Error("body too large");
  }
  return raw ? JSON.parse(raw) : {};
}

const server = http.createServer(async (req, res) => {
  if (req.headers["x-omatether-token"] !== token) {
    return reply(res, 401, { ok: false, error: "unauthorized" });
  }

  try {
    if (req.method === "GET" && req.url === "/inbound") {
      res.writeHead(200, {
        "content-type": "application/x-ndjson",
        "cache-control": "no-cache",
      });
      subscribers.add(res);
      req.on("close", () => subscribers.delete(res));
      return;
    }

    if (req.method === "POST" && req.url === "/send") {
      const { spaceId, text, format } = await readJson(req);
      if (!spaceId || typeof text !== "string") {
        return reply(res, 400, { ok: false, error: "spaceId and text required" });
      }
      if (format !== undefined && format !== "text" && format !== "markdown") {
        return reply(res, 400, { ok: false, error: `unknown format ${format}` });
      }
      const space = await resolveSpace(spaceId);
      // Defaulting to "text" keeps the unsafe direction the explicit one: a
      // caller that forgets the field gets its string through untouched.
      const result = await sendAs(space, text, format ?? "text");
      return reply(res, 200, { ok: true, messageId: result?.id ?? null });
    }

    // `state` defaults to "start" so an older caller keeps working, and
    // "stop" exists because iMessage's indicator does not expire by itself.
    if (req.method === "POST" && req.url === "/typing") {
      const { spaceId, state } = await readJson(req);
      if (!spaceId) return reply(res, 400, { ok: false, error: "spaceId required" });
      if (state !== undefined && state !== "start" && state !== "stop") {
        return reply(res, 400, { ok: false, error: `unknown state ${state}` });
      }
      const space = await resolveSpace(spaceId);
      // These are the names the SDK actually has. This was `space.typing?.()`,
      // which is not a method on a Space — the optional call swallowed it, so
      // the indicator never once appeared and nothing anywhere said so.
      if (state === "stop") await space.stopTyping();
      else await space.startTyping();
      return reply(res, 200, { ok: true });
    }

    if (req.method === "GET" && req.url === "/health") {
      return reply(res, 200, { ok: true, subscribers: subscribers.size });
    }

    return reply(res, 404, { ok: false, error: "not found" });
  } catch (e) {
    console.error("photon-sidecar: " + String(e));
    return reply(res, 500, { ok: false, error: String(e) });
  }
});

// Without this, a failed listen is an uncaught exception: Node exits 1 with a
// stack trace and the supervisor has nothing useful to report.
server.on("error", (e) => {
  if (e?.code === "EADDRINUSE") {
    console.error(
      `photon-sidecar: 127.0.0.1:${port} is already in use — another sidecar ` +
        "(Hermes runs one too) is on that port. Set OMATETHER_PHOTON_PORT, " +
        "or leave it unset to be given a free one."
    );
  } else {
    console.error("photon-sidecar: server error — " + String(e?.message ?? e));
  }
  process.exit(5);
});

process.on("uncaughtException", (e) => {
  console.error("photon-sidecar: " + String(e?.stack ?? e));
  process.exit(6);
});
process.on("unhandledRejection", (e) => {
  console.error("photon-sidecar: unhandled rejection — " + String(e?.stack ?? e));
  process.exit(6);
});

server.listen(port, "127.0.0.1", () => {
  console.error(`photon-sidecar: listening on 127.0.0.1:${port}`);
});

// ---------------------------------------------------------------------------
// Lifetime

let shuttingDown = false;
function shutdown(reason) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.error(`photon-sidecar: shutting down (${reason})`);
  server.close(() => process.exit(0));
  // Do not let a wedged connection hold the process open.
  setTimeout(() => process.exit(0), 2000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

if (process.env.PHOTON_SIDECAR_WATCH_STDIN === "1") {
  process.stdin.resume();
  process.stdin.on("end", () => shutdown("stdin closed — parent exited"));
  process.stdin.on("error", () => shutdown("stdin error — parent exited"));
}
