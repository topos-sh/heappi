// The test environment: a Node server over the cell with Pi Durable as the
// harness, a real model through pi-ai, MCP tools, external signals and
// timers, and server-sent events for a chat page.
//
//   ANTHROPIC_API_KEY=... node --no-warnings app/server.mjs        # real model
//   node --no-warnings app/server.mjs --demo                        # scripted model, no key
import http from "node:http";
import { readFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { Store } from "../host/store.mjs";
import { Cell } from "../host/cell.mjs";
import { createGateway } from "./gateway.mjs";
import { connectMcp } from "./mcp.mjs";
import { fetchSource } from "../host/scenario.mjs";

const here = fileURLToPath(new URL(".", import.meta.url));
// secrets and settings from heappi/.env (never committed); the shell environment wins
try { const before = { ...process.env }; process.loadEnvFile(join(here, "..", ".env")); for (const k of Object.keys(before)) process.env[k] = before[k]; } catch {}
const DEMO = process.argv.includes("--demo");
const PORT = Number(process.env.PORT ?? 8788);
if (!DEMO && !process.env.ANTHROPIC_API_KEY && !process.env.ANTHROPIC_OAUTH_TOKEN) {
  console.error("No model key. Put ANTHROPIC_API_KEY=sk-ant-... in heappi/.env (see .env.example), or run with --demo for the scripted model.");
  process.exit(1);
}

const dataDir = join(here, "data"); mkdirSync(dataDir, { recursive: true });
const store = new Store(join(dataDir, "cells.sqlite"), join(dataDir, "cold"));
const gateway = createGateway({ demo: DEMO });
const mcp = await connectMcp(join(here, "mcp.json"));
const builtinTools = [
  { name: "fetch", description: "Fetch a URL over HTTP and return its text (first 20 KB).", inputSchema: { type: "object", properties: { url: { type: "string" }, source: { type: "number", description: "demo only: a numbered sample source" } } } },
  { name: "now", description: "Current date and time, ISO 8601.", inputSchema: { type: "object", properties: {} } },
];
const catalog = [...builtinTools, ...mcp.catalog];
const INSTRUCTIONS = readFileSync(join(here, "instructions.md"), "utf8");

// subscribers per agent for SSE
const sse = new Map();
const publish = (agentId, event) => { for (const res of sse.get(agentId) ?? []) res.write(`data: ${JSON.stringify(event)}\n\n`); };

const broker = {
  inflight: new Set(),
  call(agentId, _id, name, args) {
    const p = (async () => {
      if (name === "model") return gateway.call(agentId, args);
      if (name === "tool") {
        const { name: tool, args: a } = args;
        if (tool === "fetch") {
          if (a?.source !== undefined) return fetchSource(Number(a.source)); // the demo scenario's sources
          if (typeof a?.url !== "string") throw new Error("fetch needs { url }");
          const r = await fetch(a.url, { signal: AbortSignal.timeout(20000) });
          return { status: r.status, text: (await r.text()).slice(0, 20000) };
        }
        if (tool === "now") return new Date().toISOString();
        if (tool.startsWith("mcp__")) return mcp.call(tool, a);
        throw new Error("unknown tool " + tool);
      }
      throw new Error("unknown call " + name);
    })();
    this.inflight.add(p); p.catch(() => {}).finally(() => this.inflight.delete(p));
    return p;
  },
  async settle() { while (this.inflight.size) await Promise.allSettled([...this.inflight]); },
};

const cell = new Cell(store, broker, { harnessSrc: readFileSync(join(here, "..", "embed", "pi-harness.js"), "utf8"), checkRebuild: false });
cell.onOutbox = (agentId, o) => { if (o.kind === "progress" || o.kind === "error" || o.kind === "pi.report") publish(agentId, { type: o.kind, ...o }); };
gateway.subscribe = ((orig) => (id, fn) => orig.call(gateway, id, fn))(gateway.subscribe);
const unsub = new Map();

// one run loop per agent: activations until the mailbox is empty
const running = new Map();
function run(agentId) {
  const prev = running.get(agentId) ?? Promise.resolve();
  const next = prev.then(async () => {
    if (!unsub.has(agentId)) unsub.set(agentId, gateway.subscribe(agentId, (ev) => publish(agentId, ev)));
    for (;;) {
      await broker.settle();
      let rec;
      try { rec = await cell.activate(agentId); } catch (e) { publish(agentId, { type: "error", error: String(e?.message ?? e) }); break; }
      if (!rec) break;
      publish(agentId, { type: "activation", tier: rec.tier, kind: rec.kind, checkpointBytes: rec.checkpointBytes, restoreMs: rec.restoreMs, runMs: rec.runMs, calls: rec.calls, heapUsed: rec.heapUsed });
      for (const r of rec.replies) publish(agentId, { type: "reply", text: r.text, turn: r.turn });
    }
  }).catch((e) => publish(agentId, { type: "error", error: String(e?.message ?? e) }));
  running.set(agentId, next);
  return next;
}

async function createAgent(agentId) {
  await cell.create(agentId);
  store.enqueue(agentId, { type: "config", config: { tools: catalog, instructions: INSTRUCTIONS } }, { sender: "server" });
  await run(agentId);
}
function transcript(agentId) {
  return store.journal(agentId).filter((r) => r.kind === "pi.entry").map((r) => r.data).filter((e) => e.entry !== "pi.system").map((e) => ({ id: e.id, kind: e.entry, model: e.model?.[0] ?? null }));
}
const agents = () => store.db.prepare("SELECT id, head_seq FROM agents ORDER BY rowid").all();

const json = (res, o, status = 200) => { res.writeHead(status, { "content-type": "application/json" }); res.end(JSON.stringify(o)); };
const body = (req) => new Promise((resolve) => { let s = ""; req.on("data", (c) => (s += c)); req.on("end", () => resolve(s ? JSON.parse(s) : {})); });

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  const m = /^\/api\/agents(?:\/([\w-]+))?(?:\/(\w+))?$/.exec(url.pathname);
  try {
    if (url.pathname === "/" || url.pathname === "/index.html") { res.writeHead(200, { "content-type": "text/html" }); return res.end(readFileSync(join(here, "ui", "index.html"))); }
    if (!m) return json(res, { error: "not found" }, 404);
    const [, id, op] = m;
    if (!id && req.method === "GET") return json(res, { agents: agents().map((a) => a.id), model: gateway.modelId, demo: DEMO, tools: catalog.map((t) => t.name) });
    if (!id && req.method === "POST") { const { id: want } = await body(req); const newId = want || "agent-" + Date.now().toString(36); await createAgent(newId); return json(res, { id: newId }); }
    if (!store.agent(id)) return json(res, { error: "no such agent" }, 404);
    if (op === "events") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
      res.write(`data: ${JSON.stringify({ type: "hello", transcript: transcript(id), stats: await cell.stats(id) })}\n\n`);
      (sse.get(id) ?? sse.set(id, new Set()).get(id)).add(res);
      req.on("close", () => sse.get(id)?.delete(res));
      return;
    }
    if (op === "messages" && req.method === "POST") { const { text } = await body(req); store.enqueue(id, { type: "user", text }, { sender: "ui" }); run(id); return json(res, { ok: true }); }
    if (op === "signals" && req.method === "POST") { const { name, payload, afterMs } = await body(req); const fire = () => { store.enqueue(id, { type: "signal", name, payload }, { sender: "signals" }); run(id); }; if (afterMs) setTimeout(fire, afterMs); else fire(); return json(res, { ok: true, scheduled: !!afterMs }); }
    if (op === "transcript") return json(res, { transcript: transcript(id) });
    if (op === "evict" && req.method === "POST") { cell.evictHot(id); return json(res, { ok: true }); }
    if (op === "info" || !op) return json(res, { id, stats: await cell.stats(id), sizes: store.sizes(id), hot: cell.hot.has(id), journalRows: store.journal(id).length });
    return json(res, { error: "unknown op" }, 404);
  } catch (e) { return json(res, { error: String(e?.stack ?? e) }, 500); }
});
server.listen(PORT, () => console.log(`heappi app on http://localhost:${PORT} (${DEMO ? "demo model" : gateway.modelId}; ${catalog.length} tools: ${catalog.map((t) => t.name).join(", ")})`));
process.on("unhandledRejection", (e) => console.error("unhandled:", e?.stack ?? e));
process.on("SIGINT", async () => { await mcp.close(); store.close(); process.exit(0); });
