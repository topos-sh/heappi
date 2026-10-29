// The demo Worker: heappi on Cloudflare with Pi Durable as the harness, a
// real model through pi-ai, MCP servers (one hosted here over R2, two
// public ones), external signals with Durable Object alarms as timers,
// server-sent events, and the chat page. One Durable Object per agent
// (DemoCell), one for the list of agents (AgentIndex).
//
//   /                          the chat page (the login card without the token)
//   POST /api/login {token}    sets the cookie the page uses (DEMO_TOKEN secret)
//   GET  /api/agents           agents, model, tool catalog
//   POST /api/agents {id?}     create an agent
//   /api/agents/:id/events     SSE: deltas, tool starts and ends, replies, activations
//   /api/agents/:id/messages   POST {text}
//   /api/agents/:id/signals    POST {name, payload, afterMs}
//   /api/agents/:id/transcript | info | evict | reset
//   /mcp/notes                 the notes MCP server (streamable HTTP), same token
import { DurableObject } from "cloudflare:workers";
import PI_HARNESS from "./pi-harness.txt";
import UI from "./ui.html";
import LOGIN from "./login.html";
import INSTRUCTIONS from "./instructions.txt";
import { CellBase, fnv } from "./cell.mjs";
import { getGateway } from "./gateway.mjs";
import { connectMcp, notesServer } from "./mcp.mjs";
import { fetchSource } from "../../host/scenario.mjs";

const json = (o, status = 200, headers = {}) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json", ...headers } });
const enc = new TextEncoder();

const BUILTIN_TOOLS = [
  { name: "fetch", description: "Fetch a URL over HTTP and return its status and text (first 20 KB).", inputSchema: { type: "object", properties: { url: { type: "string" } }, required: ["url"] } },
  { name: "now", description: "Current date and time, ISO 8601.", inputSchema: { type: "object", properties: {} } },
  { name: "list_agents", description: "The other agents running in this deployment, by name.", inputSchema: { type: "object", properties: {} } },
  { name: "send_to_agent", description: "Send a message to another agent by name. It arrives in that agent's mailbox as [signal:from:<your name>] and it may answer you the same way.", inputSchema: { type: "object", properties: { agent: { type: "string", description: "the recipient's name" }, text: { type: "string" } }, required: ["agent", "text"] } },
];
const mcpFor = (env) => connectMcp({
  notes: { handler: notesServer(env.COLD) },
  cloudflare_docs: { url: "https://docs.mcp.cloudflare.com/mcp" },
  deepwiki: { url: "https://mcp.deepwiki.com/mcp", tools: ["read_wiki_structure", "read_wiki_contents", "ask_question"] },
});
const catalogFor = async (env) => [...BUILTIN_TOOLS, ...(await mcpFor(env)).catalog];

function authorized(request, env) {
  if (!env.DEMO_TOKEN) return true; // no token configured: open (local dev)
  const url = new URL(request.url);
  const cookie = /(?:^|;\s*)heappi=([^;]+)/.exec(request.headers.get("cookie") ?? "")?.[1];
  const bearer = /^Bearer\s+(.+)$/.exec(request.headers.get("authorization") ?? "")?.[1];
  return [cookie, bearer, url.searchParams.get("token")].some((t) => t && t === env.DEMO_TOKEN);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.protocol === "http:" && url.hostname !== "localhost" && url.hostname !== "127.0.0.1") { url.protocol = "https:"; return Response.redirect(url.href, 308); } // never serve the page or take the token over plain HTTP
    const page = (html) => new Response(html, { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "x-robots-tag": "noindex", "strict-transport-security": "max-age=31536000", "referrer-policy": "no-referrer", "x-content-type-options": "nosniff" } });
    if (url.pathname === "/" || url.pathname === "/index.html") return page(authorized(request, env) ? UI : LOGIN); // nothing of the interface before the token
    if (url.pathname === "/api/login" && request.method === "POST") {
      const { token } = await request.json().catch(() => ({}));
      if (env.DEMO_TOKEN && token !== env.DEMO_TOKEN) return json({ error: "wrong token" }, 401);
      return json({ ok: true }, 200, { "set-cookie": `heappi=${token ?? ""}; Path=/; HttpOnly; SameSite=Lax${url.protocol === "https:" ? "; Secure" : ""}; Max-Age=2592000` });
    }
    if (!authorized(request, env)) return json({ error: "unauthorized" }, 401);
    if (url.pathname === "/mcp/notes") return notesServer(env.COLD)(request);
    const m = /^\/api\/agents(?:\/([\w-]+))?(?:\/(\w+))?$/.exec(url.pathname);
    if (!m) return json({ error: "not found" }, 404);
    const [, id, op] = m;
    const index = env.INDEX.get(env.INDEX.idFromName("index"));
    try {
      if (!id && request.method === "GET") { const mcp = await mcpFor(env); return json({ agents: await index.list(), model: getGateway(env).modelId, demo: getGateway(env).modelId === "scripted", tools: (await catalogFor(env)).map((t) => t.name), mcp: mcp.servers }); }
      if (!id && request.method === "POST") {
        const { id: want, name } = await request.json().catch(() => ({}));
        const newId = (want && /^[\w-]{1,40}$/.test(want) ? want : "agent-" + Date.now().toString(36));
        const stub = env.CELL.get(env.CELL.idFromName(newId));
        const r = await stub.fetch(new Request(`http://cell/api/agents/${newId}/create`, { method: "POST", body: JSON.stringify({ tools: await catalogFor(env) }) }));
        if (!r.ok) return r;
        const added = await index.add(newId, name);
        return json({ id: newId, name: added.name, ...(await r.json()) });
      }
      if (!id) return json({ error: "method" }, 405);
      const stub = env.CELL.get(env.CELL.idFromName(id));
      if (op === "rename" && request.method === "POST") { const { name } = await request.json().catch(() => ({})); const r = await index.rename(id, name); return r.error ? json(r, 400) : json(r); }
      if ((op === "delete" && request.method === "POST") || (!op && request.method === "DELETE")) { await index.remove(id); return stub.fetch(new Request(`http://cell/api/agents/${id}/reset`, { method: "POST" })); }
      if (op === "reset" && request.method === "POST") await index.remove(id);
      return stub.fetch(request);
    } catch (e) { return json({ error: String(e?.stack ?? e) }, 500); }
  },
};

/** the list of agents with their display names; names are unique and are how agents address each other */
export class AgentIndex extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env); this.sql = ctx.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, name TEXT, created INTEGER)");
    if (!this.sql.exec("PRAGMA table_info(agents)").toArray().some((c) => c.name === "name")) this.sql.exec("ALTER TABLE agents ADD COLUMN name TEXT"); // an index created before names
    this.sql.exec("CREATE UNIQUE INDEX IF NOT EXISTS agents_name ON agents (name)");
    this.sql.exec("UPDATE agents SET name = id WHERE name IS NULL");
  }
  list() { return this.sql.exec("SELECT id, name FROM agents ORDER BY created").toArray().map((r) => ({ id: r.id, name: r.name })); }
  validName(name) { return typeof name === "string" && /^[\w][\w .-]{0,39}$/.test(name) ? name.trim() : null; }
  add(id, name) {
    let n = this.validName(name) ?? id;
    if (this.sql.exec("SELECT 1 FROM agents WHERE name = ? AND id != ?", n, id).toArray().length) n = `${n}-${id.slice(-4)}`;
    this.sql.exec("INSERT OR IGNORE INTO agents (id, name, created) VALUES (?, ?, ?)", id, n, Date.now());
    return { id, name: this.nameOf(id) };
  }
  rename(id, name) {
    const n = this.validName(name);
    if (!n) return { error: "a name is 1 to 40 letters, digits, space, dot, dash or underscore" };
    if (this.sql.exec("SELECT 1 FROM agents WHERE name = ? AND id != ?", n, id).toArray().length) return { error: `another agent is called ${n}` };
    this.sql.exec("UPDATE agents SET name = ? WHERE id = ?", n, id);
    return { id, name: n };
  }
  remove(id) { this.sql.exec("DELETE FROM agents WHERE id = ?", id); }
  nameOf(id) { return this.sql.exec("SELECT name FROM agents WHERE id = ?", id).toArray()[0]?.name ?? id; }
  /** an agent by name or id */
  resolve(ref) { return this.sql.exec("SELECT id, name FROM agents WHERE name = ? OR id = ? LIMIT 1", ref, ref).toArray()[0] ?? null; }
}

export class DemoCell extends CellBase {
  constructor(ctx, env) {
    super(ctx, env, { harnessSrc: PI_HARNESS, prefix: "demo/agents" });
    this.subs = new Set(); // SSE writers
    this.running = null;
    this.HOT_MAX_LINEAR = 48 * 1024 * 1024;
    this.FOLD_MIN = 256 * 1024; // Pi's checkpoints are larger than the small harness's; fold less often
  }
  ensureSchema() {
    super.ensureSchema();
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS mailbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT, event TEXT, processed INTEGER DEFAULT 0);
      CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, name TEXT, args TEXT, sent_at INTEGER, done INTEGER DEFAULT 0);
      CREATE TABLE IF NOT EXISTS alarms (id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER, name TEXT, payload TEXT);
      CREATE TABLE IF NOT EXISTS completions (hash TEXT PRIMARY KEY, body TEXT);
    `);
  }

  // ---- mailbox, outbox, events ----
  enqueue(event, sender) { this.sql.exec("INSERT INTO mailbox (sender, event) VALUES (?, ?)", sender, JSON.stringify(event)); }
  pending() { return this.sql.exec("SELECT seq, sender, event FROM mailbox WHERE processed = 0 ORDER BY seq").toArray().map((r) => ({ seq: r.seq, sender: r.sender, event: JSON.parse(r.event) })); }
  publish(ev) {
    const bytes = enc.encode(`data: ${JSON.stringify(ev)}\n\n`);
    for (const w of this.subs) w.write(bytes).catch(() => { this.subs.delete(w); });
  }
  transcript() {
    return this.sql.exec("SELECT data FROM journal WHERE kind = 'pi.entry' ORDER BY seq").toArray()
      .map((r) => JSON.parse(r.data)).filter((e) => e.entry !== "pi.system").map((e) => ({ id: e.id, kind: e.entry, model: e.model?.[0] ?? null }));
  }

  async fetch(request) {
    const url = new URL(request.url);
    const op = url.pathname.split("/")[4] ?? "info";
    try {
      if (op === "create") {
        const { tools } = await request.json();
        const r = await this.create();
        this.enqueue({ type: "config", config: { tools, instructions: INSTRUCTIONS } }, "server");
        await this.run();
        return json({ ...r, info: this.info() });
      }
      if (op === "reset") return json(await this.reset());
      if (!this.exists()) return json({ error: "no such agent" }, 404);
      switch (op) {
        case "events": return this.events();
        case "messages": { const { text } = await request.json(); if (typeof text !== "string" || !text.trim()) return json({ error: "text" }, 400); this.enqueue({ type: "user", text }, "ui"); this.ctx.waitUntil(this.run()); return json({ ok: true }); }
        case "signals": {
          const { name = "ping", payload = null, afterMs = 0 } = await request.json();
          if (afterMs > 0) { this.sql.exec("INSERT INTO alarms (at, name, payload) VALUES (?, ?, ?)", Date.now() + afterMs, String(name), JSON.stringify(payload)); await this.scheduleAlarm(); return json({ ok: true, scheduled: true, at: Date.now() + afterMs }); }
          this.enqueue({ type: "signal", name: String(name), payload }, "signals"); this.ctx.waitUntil(this.run()); return json({ ok: true, scheduled: false });
        }
        case "transcript": return json({ transcript: this.transcript() });
        case "evict": this.engine?.drop(); this.engine = null; return json({ ok: true });
        case "info": {
          const s = this.shape();
          return json({ id: this.agentId, stats: await this.stats(), sizes: { chainLive: s.liveBytes, layers: s.layersBytes, journal: this.sql.exec("SELECT COALESCE(SUM(LENGTH(data)),0) AS n FROM journal").one().n }, hot: !!this.engine, journalRows: this.sql.exec("SELECT COUNT(*) AS n FROM journal").one().n, pending: this.pending().length, alarms: this.sql.exec("SELECT COUNT(*) AS n FROM alarms").one().n, cell: this.info(), colo: request.cf?.colo ?? null });
        }
        default: return json({ error: "unknown op " + op }, 404);
      }
    } catch (e) { return json({ error: String(e?.stack ?? e) }, 500); }
  }

  /** server-sent events: the transcript so far, then everything that happens */
  async events() {
    const { readable, writable } = new TransformStream();
    const w = writable.getWriter();
    this.subs.add(w);
    const hello = { type: "hello", transcript: this.transcript(), stats: await this.stats() };
    w.write(enc.encode(`data: ${JSON.stringify(hello)}\n\n`)).catch(() => {});
    const ping = setInterval(() => { w.write(enc.encode(": ping\n\n")).catch(() => { clearInterval(ping); this.subs.delete(w); }); }, 15000);
    return new Response(readable, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" } });
  }

  // ---- timers: Durable Object alarms ----
  async scheduleAlarm() {
    const next = this.sql.exec("SELECT MIN(at) AS at FROM alarms").one().at;
    if (next) await this.ctx.storage.setAlarm(next);
  }
  async alarm() {
    const due = this.sql.exec("SELECT id, name, payload FROM alarms WHERE at <= ? ORDER BY at", Date.now() + 50).toArray();
    for (const a of due) { this.enqueue({ type: "signal", name: a.name, payload: JSON.parse(a.payload) }, "alarm"); this.sql.exec("DELETE FROM alarms WHERE id = ?", a.id); }
    await this.scheduleAlarm();
    this.publish({ type: "sys", text: `timer fired: ${due.map((a) => a.name).join(", ")}` });
    await this.run();
  }

  // ---- the run loop: activations until the mailbox is empty ----
  run() {
    if (this.running) { this.rerun = true; return this.running; }
    this.running = (async () => {
      try { do { this.rerun = false; await this.loop(); } while (this.rerun || this.pending().length); }
      catch (e) { this.publish({ type: "error", error: String(e?.message ?? e) }); console.log("run loop failed", e?.stack ?? e); }
      finally { this.running = null; }
    })();
    return this.running;
  }
  async loop() {
    this.redispatchStale();
    for (;;) {
      const events = this.pending();
      if (!events.length) return;
      const { rec, calls, replies, out } = await this.activate(events.map((e) => e.event), {
        inCommit: ({ calls }) => {
          for (const e of events) this.sql.exec("UPDATE mailbox SET processed = 1 WHERE seq = ?", e.seq);
          for (const c of calls) this.sql.exec("INSERT OR REPLACE INTO outbox (id, name, args, sent_at, done) VALUES (?, ?, ?, ?, 0)", c.id, c.name, JSON.stringify(c.args), Date.now());
        },
      });
      for (const e of events) if (e.event.type === "signal" || (e.event.type === "user" && e.sender !== "ui")) this.publish({ type: "input", sender: e.sender, event: e.event });
      for (const o of out) if (o.kind === "progress" || o.kind === "error" || o.kind === "pi.report" || o.kind === "late_result") this.publish({ type: o.kind, ...o });
      this.publish({ type: "activation", tier: rec.tier, how: rec.how, kind: rec.kind, checkpointBytes: rec.checkpointBytes, restoreMs: rec.restoreMs, runMs: rec.runMs, calls: rec.calls, heapUsed: rec.heapUsed, chainLength: rec.chainLength, evicted: rec.evicted });
      for (const r of replies) this.publish({ type: "reply", text: r.text, turn: r.turn });
      for (const c of calls) this.dispatch(c);
    }
  }
  /** a call whose result never came back (the object was evicted mid-call) goes out again; the gateway cache makes a repeat cheap */
  redispatchStale() {
    for (const r of this.sql.exec("SELECT id, name, args FROM outbox WHERE done = 0 AND sent_at < ?", Date.now() - 90000).toArray()) {
      this.sql.exec("UPDATE outbox SET sent_at = ? WHERE id = ?", Date.now(), r.id);
      this.dispatch({ id: r.id, name: r.name, args: JSON.parse(r.args) });
    }
  }
  dispatch(call) {
    const p = this.broker(call).then(
      (value) => this.enqueue({ type: "result", id: call.id, value }, "broker"),
      (err) => this.enqueue({ type: "result", id: call.id, error: String(err?.message ?? err) }, "broker"),
    ).then(() => { this.sql.exec("UPDATE outbox SET done = 1 WHERE id = ?", call.id); return this.run(); });
    this.ctx.waitUntil(p);
  }

  /** the broker: the model through the gateway (stored under the request hash first), the tools */
  async broker(call) {
    if (call.name === "model") {
      const key = fnv(JSON.stringify(call.args));
      const row = this.sql.exec("SELECT body FROM completions WHERE hash = ?", key).toArray()[0];
      if (row) { this.publish({ type: "model_cached" }); return JSON.parse(row.body); }
      const message = await getGateway(this.env).call(call.args, (ev) => this.publish(ev));
      this.sql.exec("INSERT OR IGNORE INTO completions (hash, body) VALUES (?, ?)", key, JSON.stringify(message));
      return message;
    }
    if (call.name === "tool") {
      const { name, args } = call.args;
      if (name === "fetch") {
        if (args?.source !== undefined && getGateway(this.env).modelId === "scripted") return fetchSource(Number(args.source)); // the scripted scenario's sources
        if (typeof args?.url !== "string" || !/^https?:\/\//.test(args.url)) throw new Error("fetch needs { url }");
        const r = await fetch(args.url, { signal: AbortSignal.timeout(20000), headers: { "user-agent": "heappi-demo" } });
        return { status: r.status, text: (await r.text()).slice(0, 20000) };
      }
      if (name === "now") return new Date().toISOString();
      if (name === "list_agents") { const index = this.env.INDEX.get(this.env.INDEX.idFromName("index")); return (await index.list()).filter((a) => a.id !== this.agentId).map((a) => a.name); }
      if (name === "send_to_agent") {
        const index = this.env.INDEX.get(this.env.INDEX.idFromName("index"));
        const to = await index.resolve(String(args?.agent ?? ""));
        if (!to) throw new Error(`no agent called ${args?.agent}; list_agents gives the names`);
        if (to.id === this.agentId) throw new Error("that is you");
        const me = await index.nameOf(this.agentId);
        const r = await this.env.CELL.get(this.env.CELL.idFromName(to.id)).fetch(new Request(`http://cell/api/agents/${to.id}/signals`, { method: "POST", body: JSON.stringify({ name: `from:${me}`, payload: String(args?.text ?? "") }) }));
        if (!r.ok) throw new Error(`delivery to ${to.name} failed: ${(await r.text()).slice(0, 200)}`);
        return `delivered to ${to.name}`;
      }
      if (name.startsWith("mcp__")) return (await mcpFor(this.env)).call(name, args);
      throw new Error("unknown tool " + name);
    }
    throw new Error("unknown call " + call.name);
  }
}
