// The agent loop. This file runs INSIDE the heap, never on the host.
//
// State lives on globalThis.__agent so this file can be evaluated again
// over a live heap (a harness upgrade): functions are replaced, state is
// kept, and __migrate(fromVersion) adjusts its shape when needed.
//
// Scripts run in script contexts (own realm). Only strings cross the
// boundary: the script realm has a native __hostCall(op, json) that
// reaches __scriptCall here, and the harness settles script promises by
// calling a function the script realm defined. No harness object is ever
// handed to a script, so there is no prototype chain to escape through.
//
// The host knows: __deliver(eventJson), __drain(), __stats(), __stateHash(),
// __migrate(from), __peek(handle).
"use strict";
(() => {
  const HARNESS_VERSION = 2;
  const WINDOW = 40;           // messages kept in the heap; the journal keeps the rest
  const HANDLE_THRESHOLD = 1024; // bytes of JSON above which a result becomes a handle
  const POOL = 2;              // script contexts the shim created

  // ---- state: one global object, reusable across harness versions ----
  const S = globalThis.__agent ??= {
    version: HARNESS_VERSION,
    pending: new Map(),        // call id → { resolve, reject }
    outbox: [],
    callSeq: 0,
    memory: new Map(),         // handle → { value, origin }
    handleSeq: 0,
    messages: [], dropped: 0, turn: 0, modelCalls: 0, toolCalls: 0, scripts: 0,
    scriptPending: new Map(),  // script-realm call id → { ctx, name }
    scriptSeq: 0,
    poolReady: false,
  };

  // ---- host calls: every external interaction is a journaled call ----
  function hostCall(name, args) {
    return new Promise((resolve, reject) => {
      const id = ++S.callSeq;
      S.pending.set(id, { resolve, reject });
      S.outbox.push({ kind: "call", id, name, args });
    });
  }
  const caps = { model: (req) => hostCall("model", req), tool: (name, args) => hostCall("tool", { name, args }) };

  // ---- working memory ----
  function digest(v) {
    if (Array.isArray(v)) return { type: "array", length: v.length, head: v.slice(0, 2).map(digest) };
    if (v && typeof v === "object") {
      const keys = Object.keys(v); const d = { type: "object", keys: keys.slice(0, 8) };
      for (const k of keys.slice(0, 8)) if (Array.isArray(v[k])) d[k] = `array[${v[k].length}]`;
      return d;
    }
    if (typeof v === "string") return { type: "string", length: v.length, head: v.slice(0, 40) };
    return { type: typeof v, value: v };
  }
  function keep(value, origin) {
    const handle = "$r" + (++S.handleSeq);
    S.memory.set(handle, { value, origin });
    return { handle, digest: digest(value), origin };
  }

  // ---- conversation window ----
  function remember(msg) {
    S.messages.push(msg);
    if (S.messages.length > WINDOW) { S.messages.splice(0, S.messages.length - WINDOW); S.dropped++; }
  }

  // ---- tools as functions ----
  const TOOLS = { fetch: (args) => caps.tool("fetch", args), run: (args) => runScript(args.code) };

  // ---- script realms ----
  // Evaluated once per script context. Everything a script can touch is
  // defined here, in its own realm; __hostCall is the shim's native.
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
    for (let i = 1; i <= POOL; i++) {
      __evalIn(i, PRELUDE);
      settlers[i] = __evalIn(i, "__settle");
    }
    S.poolReady = true;
  }
  // the harness side of the bridge: (contextIndex, op, json) → string
  globalThis.__scriptCall = (ctx, op, json) => {
    const p = JSON.parse(json);
    switch (op) {
      case "tool": {
        if (p.name === "run") throw new Error("scripts cannot start scripts");
        if (!TOOLS[p.name]) throw new Error("no tool " + p.name);
        S.toolCalls++;
        TOOLS[p.name](p.args).then(
          (v) => settlers[ctx](p.id, true, JSON.stringify(v ?? null)),
          (e) => settlers[ctx](p.id, false, String(e && e.message || e)));
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
    const run = __evalIn(ctx, "__run");
    const value = await run(code); // a script-realm promise; the harness is trusted to await it
    return JSON.parse(JSON.stringify(value ?? null)); // copy into this realm
  }

  // ---- one user turn ----
  async function handleUser(text) {
    S.turn++;
    remember({ role: "user", content: text });
    for (;;) {
      S.modelCalls++;
      const r = await caps.model({ messages: S.messages, tools: Object.keys(TOOLS) });
      remember({ role: "assistant", content: r.content ?? "", tool_calls: r.tool_calls ?? [] });
      if (!r.tool_calls || r.tool_calls.length === 0) { S.outbox.push({ kind: "reply", turn: S.turn, text: r.content }); return; }
      const results = await Promise.all(r.tool_calls.map(async (tc) => {
        S.toolCalls++;
        let out;
        try { out = await (TOOLS[tc.name] ?? (() => { throw new Error("no tool " + tc.name); }))(tc.args); }
        catch (e) { return { id: tc.id, content: "error: " + (e && e.message || e) }; }
        const json = JSON.stringify(out);
        if (json !== undefined && json.length > HANDLE_THRESHOLD) return { id: tc.id, content: JSON.stringify(keep(out, tc.name)) };
        return { id: tc.id, content: json ?? "undefined" };
      }));
      for (const res of results) remember({ role: "tool", tool_call_id: res.id, content: res.content });
    }
  }

  // ---- the globals the host knows ----
  globalThis.__deliver = (json) => {
    const ev = JSON.parse(json);
    switch (ev.type) {
      case "result": {
        const p = S.pending.get(ev.id);
        if (!p) { S.outbox.push({ kind: "late_result", id: ev.id }); return; }
        S.pending.delete(ev.id);
        ev.error ? p.reject(new Error(ev.error)) : p.resolve(ev.value);
        return;
      }
      case "user":
        handleUser(ev.text).catch((e) => S.outbox.push({ kind: "error", error: String(e && e.stack || e) }));
        return;
      default:
        S.outbox.push({ kind: "error", error: "unknown event " + ev.type });
    }
  };
  globalThis.__drain = () => { const s = JSON.stringify(S.outbox); S.outbox.length = 0; return s; };
  globalThis.__stats = () => JSON.stringify({
    version: S.version, turn: S.turn, messages: S.messages.length, dropped: S.dropped, handles: S.memory.size,
    pending: S.pending.size, modelCalls: S.modelCalls, toolCalls: S.toolCalls, scripts: S.scripts,
  });
  globalThis.__peek = (handle) => JSON.stringify(S.memory.get(handle)?.value ?? null);

  // canonical hash of the state a rebuild from the journal must reproduce
  function fnv(str) { let h = 0x811c9dc5; for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, "0"); }
  globalThis.__stateHash = () => {
    const handles = [...S.memory.entries()].map(([k, v]) => [k, v.origin, fnv(JSON.stringify(v.value))]);
    return fnv(JSON.stringify({ messages: S.messages, dropped: S.dropped, turn: S.turn, handles, callSeq: S.callSeq, handleSeq: S.handleSeq }));
  };

  // harness upgrade: this file was evaluated again over a live heap
  globalThis.__migrate = () => {
    const from = S.version;
    if (from < 2) { S.scriptPending ??= new Map(); S.scriptSeq ??= 0; S.poolReady = false; }
    S.version = HARNESS_VERSION;
    return JSON.stringify({ from, to: HARNESS_VERSION });
  };
})();
