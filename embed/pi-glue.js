// Pi Durable as the harness inside the heap. Evaluated after prelude.js
// and pi-bundle.js (globalThis.PI). Implements the same host contract as
// heap/harness.js: __deliver, __drain, __pump, __stats, __stateHash,
// __migrate, __peek.
//
// How the joins work:
// - model: pi-ai's faux provider with an async response factory; the
//   factory awaits hostCall("model", ...) and converts both ways.
// - tools: Pi tools whose execute() awaits hostCall("tool", ...); large
//   results become handles in working memory, as in harness.js.
// - code mode: a `run` tool that compiles the script in a script context
//   through the same string-only bridge as harness.js.
// - storage: Pi's MemoryStorage lives in the heap and is what the
//   checkpoint carries; every committed entry is mirrored into the outbox
//   as a `pi.entry` record, which the cell journals (the readable shadow).
"use strict";
(() => {
  const HARNESS_VERSION = 100; // Pi-based harness line
  const POOL = 2;
  const HANDLE_THRESHOLD = 1024;
  const { BACKGROUND_CONTEXT, createModels, fauxProvider, fauxAssistantMessage, fauxToolCall, Type, createRegistry, defineExtension, defineTool, Harness, MemoryStorage } = PI;

  const S = globalThis.__agent ??= {
    version: HARNESS_VERSION, pending: new Map(), outbox: [], callSeq: 0,
    memory: new Map(), handleSeq: 0, turn: 0, modelCalls: 0, toolCalls: 0, scripts: 0,
    mirrored: 0, lastEntryId: null, poolReady: false, harness: null, root: null,
  };

  // ---- host calls ----
  function hostCall(name, args) {
    return new Promise((resolve, reject) => {
      const id = ++S.callSeq;
      S.pending.set(id, { resolve, reject });
      S.outbox.push({ kind: "call", id, name, args });
    });
  }

  // ---- working memory (handles) ----
  function digest(v) {
    if (Array.isArray(v)) return { type: "array", length: v.length, head: v.slice(0, 2).map(digest) };
    if (v && typeof v === "object") { const keys = Object.keys(v); const d = { type: "object", keys: keys.slice(0, 8) }; for (const k of keys.slice(0, 8)) if (Array.isArray(v[k])) d[k] = `array[${v[k].length}]`; return d; }
    if (typeof v === "string") return { type: "string", length: v.length, head: v.slice(0, 40) };
    return { type: typeof v, value: v };
  }
  function keep(value, origin) { const handle = "$r" + (++S.handleSeq); S.memory.set(handle, { value, origin }); return { handle, digest: digest(value), origin }; }

  // ---- the model: pi-ai messages → gateway shape → pi-ai assistant message ----
  function toGateway(messages) {
    const out = [];
    for (const m of messages) {
      if (m.role === "user") out.push({ role: "user", content: typeof m.content === "string" ? m.content : m.content.map((c) => c.text ?? "").join("") });
      else if (m.role === "assistant") out.push({ role: "assistant", content: m.content.filter((c) => c.type === "text").map((c) => c.text).join(""), tool_calls: m.content.filter((c) => c.type === "toolCall").map((c) => ({ id: c.id, name: c.name, args: c.arguments })) });
      else if (m.role === "toolResult") out.push({ role: "tool", tool_call_id: m.toolCallId, content: (m.content.find((c) => c.type === "text")?.text ?? "") });
    }
    return out;
  }
  const faux = fauxProvider();
  const factory = async (ctx) => {
    faux.appendResponses([factory]); // keep the queue fed
    S.modelCalls++;
    const r = await hostCall("model", { messages: toGateway(ctx.messages), tools: ["fetch", "run"] });
    if (r.tool_calls && r.tool_calls.length) return fauxAssistantMessage(r.tool_calls.map((tc) => fauxToolCall(tc.name, tc.args, { id: tc.id })), { stopReason: "toolUse" });
    return fauxAssistantMessage(r.content ?? "");
  };
  faux.setResponses([factory, factory]);
  const models = createModels(); models.setProvider(faux.provider);

  // ---- script realms (same bridge as harness.js) ----
  const PRELUDE = `(() => {
    const pending = new Map(); let seq = 0;
    const call = (op, payload) => __hostCall(op, JSON.stringify(payload));
    const async = (op, payload) => new Promise((resolve, reject) => { const id = call(op, { ...payload, id: ++seq }); pending.set(String(id), { resolve, reject }); });
    const tools = new Proxy({}, { get: (_, name) => (args) => async("tool", { name, args }) });
    const mem = new Proxy({}, { get: (_, k) => JSON.parse(call("mem", { handle: k })), has: (_, k) => JSON.parse(call("has", { handle: k })) });
    const remember = (value, origin) => call("remember", { value, origin });
    const log = (...a) => call("log", a);
    globalThis.__settle = (id, ok, json) => { const p = pending.get(String(id)); if (!p) return; pending.delete(String(id)); ok ? p.resolve(JSON.parse(json)) : p.reject(new Error(json)); };
    globalThis.__run = (src) => new Function("tools", "mem", "remember", "log", "return (async () => {\\n" + src + "\\n})();")(tools, mem, remember, log);
    return true;
  })()`;
  const settlers = [];
  function ensurePool() {
    if (S.poolReady && settlers.filter(Boolean).length === POOL) return;
    for (let i = 1; i <= POOL; i++) { __evalIn(i, PRELUDE); settlers[i] = __evalIn(i, "__settle"); }
    S.poolReady = true;
  }
  const HOST_TOOLS = { fetch: (args) => hostCall("tool", { name: "fetch", args }) };
  globalThis.__scriptCall = (ctx, op, json) => {
    const p = JSON.parse(json);
    switch (op) {
      case "tool": {
        if (p.name === "run") throw new Error("scripts cannot start scripts");
        if (!HOST_TOOLS[p.name]) throw new Error("no tool " + p.name);
        S.toolCalls++;
        HOST_TOOLS[p.name](p.args).then((v) => settlers[ctx](p.id, true, JSON.stringify(v ?? null)), (e) => settlers[ctx](p.id, false, String(e && e.message || e)));
        return String(p.id);
      }
      case "mem": return JSON.stringify(S.memory.get(p.handle)?.value ?? null);
      case "has": return JSON.stringify(S.memory.has(p.handle));
      case "remember": return keep(p.value, p.origin).handle;
      case "log": __log("[script]", ...p); return "";
      default: throw new Error("unknown op " + op);
    }
  };
  async function runScript(code) {
    ensurePool();
    const ctx = 1 + (S.scripts++ % POOL);
    const value = await __evalIn(ctx, "__run")(code);
    return JSON.parse(JSON.stringify(value ?? null));
  }

  // ---- Pi tools: execute() is a host call or a script ----
  function asResult(out, origin) {
    const json = JSON.stringify(out);
    const text = json !== undefined && json.length > HANDLE_THRESHOLD ? JSON.stringify(keep(out, origin)) : (json ?? "undefined");
    return { output: [{ type: "text", text }] };
  }
  const fetchTool = defineTool({ name: "fetch", description: "Fetch a source", parameters: Type.Object({ source: Type.Number() }), replay: "safe",
    execute: async (args) => { S.toolCalls++; return asResult(await HOST_TOOLS.fetch(args), "fetch"); } });
  const runTool = defineTool({ name: "run", description: "Run JavaScript against the tools and working memory", parameters: Type.Object({ code: Type.String() }), replay: "safe",
    execute: async (args) => asResult(await runScript(args.code), "run") });
  const registry = createRegistry();
  registry.install(defineExtension({ name: "image", tools: [fetchTool, runTool] }));

  // ---- Pi harness on in-heap storage; entries mirrored to the journal ----
  const settings = { progress: { partialIntervalMs: 0, outputIntervalMs: 0 } };
  async function open() {
    if (S.root) return S.root;
    S.harness = await Harness.open(new MemoryStorage(), { models, registry, settings, onReport: (r) => S.outbox.push({ kind: "pi.report", text: String(r?.stack ?? r).slice(0, 500) }) }, BACKGROUND_CONTEXT);
    S.root = await S.harness.root(BACKGROUND_CONTEXT, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
    return S.root;
  }
  async function mirror(root) {
    // append committed entries newer than the last mirrored one to the outbox
    const page = await root.entries({ order: "ascending", ...(S.lastEntryId ? { minEntryId: S.lastEntryId } : {}) }, 200, undefined, BACKGROUND_CONTEXT);
    for (const e of page.items) {
      if (e.id === S.lastEntryId) continue;
      S.outbox.push({ kind: "pi.entry", id: e.id, entry: e.kind, model: e.model ?? null, data: e.data ?? null });
      S.lastEntryId = e.id; S.mirrored++;
    }
  }
  async function handleUser(text) {
    S.turn++;
    const root = await open();
    const sub = await root.submit({ type: "input", content: text }, BACKGROUND_CONTEXT);
    const settled = await sub.wait(BACKGROUND_CONTEXT);
    await mirror(root);
    const page = await root.entries({ order: "descending" }, 2, undefined, BACKGROUND_CONTEXT);
    const a = page.items.find((x) => x.kind === "pi.assistant");
    const reply = a?.model?.[0]?.content?.filter((c) => c.type === "text").map((c) => c.text).join("") ?? "";
    S.outbox.push({ kind: "reply", turn: S.turn, text: settled.status === "done" ? reply : `unanswered: ${JSON.stringify(settled).slice(0, 200)}` });
  }

  // ---- host contract ----
  globalThis.__deliver = (json) => {
    const ev = JSON.parse(json);
    switch (ev.type) {
      case "result": { const p = S.pending.get(ev.id); if (!p) { S.outbox.push({ kind: "late_result", id: ev.id }); return; } S.pending.delete(ev.id); ev.error ? p.reject(new Error(ev.error)) : p.resolve(ev.value); return; }
      case "user": handleUser(ev.text).catch((e) => S.outbox.push({ kind: "error", error: String(e && e.stack || e) })); return;
      default: S.outbox.push({ kind: "error", error: "unknown event " + ev.type });
    }
  };
  globalThis.__pump = () => __runTimers(); // Pi schedules with setTimeout; the host pumps until quiet
  globalThis.__drain = () => { const s = JSON.stringify(S.outbox); S.outbox.length = 0; return s; };
  globalThis.__stats = () => JSON.stringify({ version: S.version, turn: S.turn, handles: S.memory.size, pending: S.pending.size, modelCalls: S.modelCalls, toolCalls: S.toolCalls, scripts: S.scripts, mirrored: S.mirrored, harness: "pi-durable" });
  globalThis.__peek = (handle) => JSON.stringify(S.memory.get(handle)?.value ?? null);
  function fnv(str) { let h = 0x811c9dc5; for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, "0"); }
  // content-only hash: Pi's ids and timestamps differ between a live heap and a rebuilt one
  globalThis.__stateHash = () => {
    const handles = [...S.memory.entries()].map(([k, v]) => [k, v.origin, fnv(JSON.stringify(v.value))]);
    return fnv(JSON.stringify({ turn: S.turn, handles, entries: S.mirrored, callSeq: S.callSeq, lastKinds: S.__lastKinds ?? null }));
  };
  globalThis.__migrate = () => { const from = S.version; S.version = HARNESS_VERSION; return JSON.stringify({ from, to: HARNESS_VERSION }); };
})();
