// The agent loop. This file runs INSIDE the heap, never on the host.
//
// It owns: the host-call outbox and the pending-promise map, the
// conversation window, working memory with handles, tools as functions,
// and code-mode scripts as nested calls with a restricted capability set.
// The host only knows two globals: __deliver(eventJson) and __drain().
"use strict";
(() => {
  // ---- host calls: every external interaction is a journaled call -------
  const pending = new Map();
  const outbox = [];
  let callSeq = 0;
  function hostCall(name, args) {
    return new Promise((resolve, reject) => {
      const id = ++callSeq;
      pending.set(id, { resolve, reject });
      outbox.push({ kind: "call", id, name, args });
    });
  }

  // ---- capabilities: objects, not globals -------------------------------
  const caps = {
    model: (req) => hostCall("model", req),
    tool: (name, args) => hostCall("tool", { name, args }),
  };

  // ---- working memory: large values stay here, the model sees a digest --
  const memory = new Map();
  let handleSeq = 0;
  const HANDLE_THRESHOLD = 1024; // bytes of JSON
  function digest(v) {
    if (Array.isArray(v)) return { type: "array", length: v.length, head: v.slice(0, 2).map(digest) };
    if (v && typeof v === "object") {
      const keys = Object.keys(v);
      const d = { type: "object", keys: keys.slice(0, 8) };
      for (const k of keys.slice(0, 8)) if (Array.isArray(v[k])) d[k] = `array[${v[k].length}]`;
      return d;
    }
    if (typeof v === "string") return { type: "string", length: v.length, head: v.slice(0, 40) };
    return { type: typeof v, value: v };
  }
  function keep(value, origin) {
    const handle = "$r" + (++handleSeq);
    memory.set(handle, { value, origin });
    return { handle, digest: digest(value), origin };
  }
  const memView = new Proxy({}, {
    get: (_, k) => memory.get(k)?.value,
    has: (_, k) => memory.has(k),
    ownKeys: () => [...memory.keys()],
    getOwnPropertyDescriptor: (_, k) => memory.has(k) ? { enumerable: true, configurable: true, value: memory.get(k).value } : undefined,
  });

  // ---- conversation ------------------------------------------------------
  const WINDOW = 40; // messages kept in the heap; the journal keeps the rest
  const agent = { messages: [], dropped: 0, turn: 0, modelCalls: 0, toolCalls: 0, scripts: 0 };
  function remember(msg) {
    agent.messages.push(msg);
    if (agent.messages.length > WINDOW) {
      agent.messages.splice(0, agent.messages.length - WINDOW);
      agent.dropped++;
    }
  }

  // ---- tools as functions ------------------------------------------------
  const TOOLS = {
    fetch: (args) => caps.tool("fetch", args),
    run: (args) => runScript(args.code),
  };

  // ---- code mode: a nested call with a narrower capability set ----------
  async function runScript(code) {
    agent.scripts++;
    const tools = new Proxy({}, { get: (_, name) => (args) => (name === "run" ? Promise.reject(new Error("scripts cannot start scripts")) : TOOLS[name] ? TOOLS[name](args) : Promise.reject(new Error("no tool " + String(name)))) });
    const api = { tools, mem: memView, remember: (v, origin) => keep(v, origin).handle };
    const fn = new Function("tools", "mem", "remember", `return (async () => {\n${code}\n})();`);
    return await fn(api.tools, api.mem, api.remember);
  }

  // ---- one user turn -----------------------------------------------------
  async function handleUser(text) {
    agent.turn++;
    remember({ role: "user", content: text });
    for (;;) {
      agent.modelCalls++;
      const r = await caps.model({ messages: agent.messages, tools: Object.keys(TOOLS) });
      remember({ role: "assistant", content: r.content ?? "", tool_calls: r.tool_calls ?? [] });
      if (!r.tool_calls || r.tool_calls.length === 0) {
        outbox.push({ kind: "reply", turn: agent.turn, text: r.content });
        return;
      }
      const results = await Promise.all(r.tool_calls.map(async (tc) => {
        agent.toolCalls++;
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

  // ---- the two globals the host knows ------------------------------------
  globalThis.__deliver = (json) => {
    const ev = JSON.parse(json);
    switch (ev.type) {
      case "result": {
        const p = pending.get(ev.id);
        if (!p) { outbox.push({ kind: "late_result", id: ev.id }); return; }
        pending.delete(ev.id);
        ev.error ? p.reject(new Error(ev.error)) : p.resolve(ev.value);
        return;
      }
      case "user":
        handleUser(ev.text).catch((e) => outbox.push({ kind: "error", error: String(e && e.stack || e) }));
        return;
      default:
        outbox.push({ kind: "error", error: "unknown event " + ev.type });
    }
  };
  globalThis.__drain = () => { const s = JSON.stringify(outbox); outbox.length = 0; return s; };
  globalThis.__stats = () => JSON.stringify({
    turn: agent.turn, messages: agent.messages.length, dropped: agent.dropped, handles: memory.size,
    pending: pending.size, modelCalls: agent.modelCalls, toolCalls: agent.toolCalls, scripts: agent.scripts,
  });
  globalThis.__peek = (handle) => JSON.stringify(memory.get(handle)?.value ?? null);
})();
