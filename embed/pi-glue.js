// Pi Durable as the harness inside the heap. Evaluated after prelude.js
// and pi-bundle.js (globalThis.PI). Implements the host contract of
// heap/harness.js: __deliver, __drain, __pump, __stats, __stateHash,
// __migrate, __peek.
//
// The joins:
// - model: pi-ai's faux provider with an async response factory; the
//   factory sends the transcript as a pi-ai Context through hostCall and
//   returns the assistant message the gateway produced.
// - tools: a catalog delivered by a `config` event (MCP tools from the
//   host, the built-in fetch) becomes Pi tools whose execute() awaits
//   hostCall("tool", ...); large results become handles. `run` compiles a
//   script in a script context through the string-only bridge.
// - storage: Pi's MemoryStorage lives in the heap and is what the
//   checkpoint carries; every committed entry is mirrored into the outbox
//   as a `pi.entry` record (the readable shadow). Tool starts and ends go
//   out as `progress` records for a UI.
// - signals: a `signal` event is submitted to Pi as user input tagged
//   with its name, so the agent reacts to external events between turns.
// - crash safety: each catalog tool says whether it may run twice (`replay`);
//   the flag rides on the host call, and the host reruns or reports a call a
//   crash cut off. Tools that may not run twice get a round of their own.
// - steering: `steer` and `cancel` events go to Pi's own steer and abort;
//   `requestId` on an input is Pi's own idempotency key.
// - large catalogs: MCP tools are deferred, found through Pi's tool_search.
"use strict";
(() => {
  const HARNESS_VERSION = 102;
  const POOL = 2;
  const HANDLE_THRESHOLD = 1024;
  const DEFER_OVER = 8; // catalogs larger than this load their MCP tools on demand
  const { BACKGROUND_CONTEXT, createModels, fauxProvider, Type, createRegistry, defineExtension, defineTool, Harness, MemoryStorage, createToolSearchExtension } = PI;

  const S = globalThis.__agent ??= {
    version: HARNESS_VERSION, pending: new Map(), outbox: [], callSeq: 0,
    memory: new Map(), handleSeq: 0, turn: 0, modelCalls: 0, toolCalls: 0, scripts: 0,
    mirrored: 0, lastEntryId: null, poolReady: false, harness: null, root: null, active: 0, seenInputs: new Set(),
    config: { tools: [], instructions: "", model: { provider: "faux", modelId: "faux-1" } },
  };

  // ---- host calls ----
  function hostCall(name, args) {
    return new Promise((resolve, reject) => {
      const id = ++S.callSeq;
      S.pending.set(id, { resolve, reject });
      S.outbox.push({ kind: "call", id, name, args });
    });
  }
  const progress = (event, data) => S.outbox.push({ kind: "progress", event, ...data });

  // ---- working memory (handles) ----
  function digest(v) {
    if (Array.isArray(v)) return { type: "array", length: v.length, head: v.slice(0, 2).map(digest) };
    if (v && typeof v === "object") { const keys = Object.keys(v); const d = { type: "object", keys: keys.slice(0, 8) }; for (const k of keys.slice(0, 8)) if (Array.isArray(v[k])) d[k] = `array[${v[k].length}]`; return d; }
    if (typeof v === "string") return { type: "string", length: v.length, head: v.slice(0, 40) };
    return { type: typeof v, value: v };
  }
  function keep(value, origin) { const handle = "$r" + (++S.handleSeq); S.memory.set(handle, { value, origin }); return { handle, digest: digest(value), origin }; }

  // ---- the model: the transcript goes out as a pi-ai Context ----
  function transcriptToContext(messages) {
    let systemPrompt = "";
    const tools = new Map();
    const rest = [];
    for (const m of messages) {
      if (m.role !== "system") { rest.push(m); continue; }
      const text = typeof m.content === "string" ? m.content : (m.content ?? []).map((c) => c.text ?? "").join("");
      const sections = m.sections ? Object.values(m.sections).filter(Boolean).join("\n") : "";
      if (text || sections) systemPrompt = [systemPrompt, text, sections].filter(Boolean).join("\n\n");
      for (const t of m.toolsRemoved ?? []) tools.delete(typeof t === "string" ? t : t.name);
      for (const t of m.toolsAdded ?? []) tools.set(t.name, { name: t.name, description: t.description, parameters: t.parameters });
    }
    return { systemPrompt, messages: rest, tools: [...tools.values()] };
  }
  const faux = fauxProvider();
  const factory = async (ctx, options) => {
    faux.appendResponses([factory]);
    S.modelCalls++;
    progress("model_call", { n: S.modelCalls });
    const r = await hostCall("model", { context: transcriptToContext(ctx.messages), options: { maxTokens: options?.maxTokens, toolChoice: options?.toolChoice } });
    if (!r || r.role !== "assistant") throw new Error("gateway returned no assistant message");
    return r;
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
  const replayOf = (name) => S.config.tools.find((t) => t.name === name)?.replay === "safe" ? "safe" : "unsafe";
  const hostTool = (name, args) => hostCall("tool", { name, args, replay: replayOf(name) });
  globalThis.__scriptCall = (ctx, op, json) => {
    const p = JSON.parse(json);
    switch (op) {
      case "tool": {
        if (p.name === "run") throw new Error("scripts cannot start scripts");
        if (!S.config.tools.some((t) => t.name === p.name) && p.name !== "fetch") throw new Error("no tool " + p.name);
        S.toolCalls++;
        hostTool(p.name, p.args).then((v) => settlers[ctx](p.id, true, JSON.stringify(v ?? null)), (e) => settlers[ctx](p.id, false, String(e && e.message || e)));
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

  // ---- Pi tools from the catalog ----
  function asResult(out, origin) {
    const json = JSON.stringify(out);
    const text = json !== undefined && json.length > HANDLE_THRESHOLD ? JSON.stringify(keep(out, origin)) : (json ?? "undefined");
    return { output: [{ type: "text", text }] };
  }
  function buildRegistry() {
    const tools = [];
    const defer = S.config.tools.length > DEFER_OVER;
    for (const t of S.config.tools) {
      const safe = t.replay === "safe";
      tools.push(defineTool({ name: t.name, description: t.description ?? t.name, parameters: Type.Unsafe(t.inputSchema ?? { type: "object", properties: {} }), replay: safe ? "safe" : "unsafe", ...(safe ? {} : { executionMode: "sequential" }), ...(defer && t.server ? { deferred: true } : {}),
        execute: async (args) => { S.toolCalls++; progress("tool_start", { name: t.name, args }); try { const v = await hostTool(t.name, args); progress("tool_end", { name: t.name, ok: true }); return asResult(v, t.name); } catch (e) { progress("tool_end", { name: t.name, ok: false, error: String(e?.message ?? e) }); throw e; } } }));
    }
    tools.push(defineTool({ name: "run", description: "Run JavaScript. `tools.<name>(args)` calls any tool, `mem[handle]` reads working memory, `remember(value, label)` keeps a value and returns its handle, `log(...)` prints. Return a small value; keep large data with remember.", parameters: Type.Object({ code: Type.String() }), replay: "safe",
      execute: async (args) => { progress("tool_start", { name: "run", args: { code: args.code.slice(0, 200) } }); try { const v = await runScript(args.code); progress("tool_end", { name: "run", ok: true }); return asResult(v, "run"); } catch (e) { progress("tool_end", { name: "run", ok: false, error: String(e?.message ?? e) }); throw e; } } }));
    const registry = createRegistry();
    registry.install(defineExtension({ name: "heappi", tools }));
    if (defer) registry.install(createToolSearchExtension());
    return registry;
  }

  // ---- Pi harness on in-heap storage; entries mirrored to the journal ----
  const settings = { progress: { partialIntervalMs: 0, outputIntervalMs: 0 } };
  async function open() {
    if (S.root) return S.root;
    S.harness = await Harness.open(new MemoryStorage(), { models, registry: buildRegistry(), settings, onReport: (r) => S.outbox.push({ kind: "pi.report", text: String(r?.stack ?? r).slice(0, 500) }) }, BACKGROUND_CONTEXT);
    S.root = await S.harness.root(BACKGROUND_CONTEXT, { agent: { model: { provider: "faux", modelId: "faux-1" }, instructions: S.config.instructions || undefined } });
    return S.root;
  }
  async function mirror(root) {
    const page = await root.entries({ order: "ascending", ...(S.lastEntryId ? { minEntryId: S.lastEntryId } : {}) }, 500, undefined, BACKGROUND_CONTEXT);
    for (const e of page.items) {
      if (e.id === S.lastEntryId) continue;
      S.outbox.push({ kind: "pi.entry", id: e.id, entry: e.kind, model: e.model ?? null, data: e.data ?? null });
      S.lastEntryId = e.id; S.mirrored++;
    }
  }
  const setActive = (n) => { const was = S.active > 0; S.active += n; if (was !== S.active > 0) progress("busy", { busy: S.active > 0 }); };
  async function submit(text, requestId) {
    setActive(1);
    try {
      const root = await open();
      const sub = await root.submit({ type: "input", content: text, whenBusy: "followUp", ...(requestId ? { requestId } : {}) }, BACKGROUND_CONTEXT);
      if (requestId) { if (S.seenInputs.has(sub.id)) return; S.seenInputs.add(sub.id); } // Pi handed back the first submission: a retry, already being answered
      S.turn++;
      const settled = await sub.wait(BACKGROUND_CONTEXT);
      await mirror(root);
      const page = await root.entries({ order: "descending" }, 2, undefined, BACKGROUND_CONTEXT);
      const a = page.items.find((x) => x.kind === "pi.assistant");
      const reply = a?.model?.[0]?.content?.filter((c) => c.type === "text").map((c) => c.text).join("") ?? "";
      S.outbox.push({ kind: "reply", turn: S.turn, text: settled.status === "done" ? reply : S.cancelled ? "(stopped)" : `unanswered: ${JSON.stringify(settled).slice(0, 300)}`, ...(S.cancelled ? { cancelled: true } : {}) });
    } finally { setActive(-1); if (!S.active) S.cancelled = false; }
  }
  /** joins the running work at the next safe point; with nothing running it is an ordinary input */
  async function steer(text, requestId) {
    if (!S.active) return submit(text, requestId);
    await (await open()).submit({ type: "input", content: text, whenBusy: "steer", ...(requestId ? { requestId } : {}) }, BACKGROUND_CONTEXT);
    progress("steered", { text });
  }
  /** stops the run; a host call still out is answered here at once, and its late result is dropped */
  async function cancel() {
    if (!S.root || !S.active) return;
    S.cancelled = true;
    const stopping = S.root.abort(BACKGROUND_CONTEXT);
    for (const [id, p] of S.pending) { S.pending.delete(id); p.reject(new Error("cancelled")); }
    await stopping;
    // the stopped request stays in the transcript; without this the model would answer it with the next message
    await S.root.submit({ type: "write", entry: { kind: "heappi.stopped", data: null, model: [{ role: "user", content: "[The user stopped the request above. Do not continue or answer it unless asked again.]", timestamp: Date.now() }] } }, BACKGROUND_CONTEXT);
    await mirror(S.root);
  }
  const report = (e) => S.outbox.push({ kind: "error", error: String(e && e.stack || e) });

  // ---- host contract ----
  globalThis.__deliver = (json) => {
    const ev = JSON.parse(json);
    switch (ev.type) {
      case "result": { const p = S.pending.get(ev.id); if (!p) { S.outbox.push({ kind: "late_result", id: ev.id }); return; } S.pending.delete(ev.id); ev.error ? p.reject(new Error(ev.error)) : p.resolve(ev.value); return; }
      case "config": S.config = { ...S.config, ...ev.config }; if (S.root) S.outbox.push({ kind: "error", error: "config after open is not applied until the next harness upgrade" }); return;
      case "user": submit(ev.text, ev.requestId).catch(report); return;
      case "steer": steer(ev.text, ev.requestId).catch(report); return;
      case "cancel": cancel().catch(report); return;
      case "signal": submit(`[signal:${ev.name}] ${typeof ev.payload === "string" ? ev.payload : JSON.stringify(ev.payload ?? null)}`).catch(report); return;
      default: S.outbox.push({ kind: "error", error: "unknown event " + ev.type });
    }
  };
  globalThis.__pump = () => __runTimers();
  globalThis.__drain = () => { const s = JSON.stringify(S.outbox); S.outbox.length = 0; return s; };
  globalThis.__stats = () => JSON.stringify({ version: S.version, turn: S.turn, handles: S.memory.size, pending: S.pending.size, modelCalls: S.modelCalls, toolCalls: S.toolCalls, scripts: S.scripts, mirrored: S.mirrored, tools: S.config.tools.length, busy: S.active > 0, harness: "pi-durable" });
  globalThis.__peek = (handle) => JSON.stringify(S.memory.get(handle)?.value ?? null);
  function fnv(str) { let h = 0x811c9dc5; for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, "0"); }
  globalThis.__stateHash = () => {
    const handles = [...S.memory.entries()].map(([k, v]) => [k, v.origin, fnv(JSON.stringify(v.value))]);
    return fnv(JSON.stringify({ turn: S.turn, handles, entries: S.mirrored, callSeq: S.callSeq }));
  };
  globalThis.__migrate = () => { const from = S.version; S.version = HARNESS_VERSION; return JSON.stringify({ from, to: HARNESS_VERSION }); };
})();
