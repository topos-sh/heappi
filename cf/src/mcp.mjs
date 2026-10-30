// MCP on the Worker, in both directions, with nothing but fetch.
//
// Client: a minimal streamable-HTTP client (initialize, tools/list,
// tools/call) over a `send(body) → Response` function, which is either a
// fetch to a remote server or a direct call into a server in this Worker.
// The catalog is built once per isolate and exposed into the heap as
// `mcp__<server>__<tool>`.
//
// Server: a notes server backed by R2, mounted at /mcp/notes for external
// clients and called directly by the agents here.
const PROTOCOL = "2025-06-18";

async function rpcResponse(res, id) {
  if (res.status === 202 || res.status === 204) return null;
  const ct = res.headers.get("content-type") ?? "";
  if (!res.ok) throw new Error(`MCP ${res.status}: ${(await res.text()).slice(0, 300)}`);
  if (ct.includes("text/event-stream")) {
    const text = await res.text();
    for (const block of text.split("\n\n")) {
      const data = block.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).join("\n");
      if (!data) continue;
      try { const msg = JSON.parse(data); if (msg.id === id) return msg; } catch {}
    }
    throw new Error("MCP: no response in event stream");
  }
  return res.json();
}

export function mcpClient(name, send) {
  let seq = 0, session = null;
  const rpc = async (method, params, notify = false) => {
    const id = notify ? undefined : ++seq;
    const headers = { "content-type": "application/json", accept: "application/json, text/event-stream", "mcp-protocol-version": PROTOCOL };
    if (session) headers["mcp-session-id"] = session;
    const res = await send(JSON.stringify({ jsonrpc: "2.0", id, method, params }), headers);
    if (res.headers.get("mcp-session-id")) session = res.headers.get("mcp-session-id");
    const msg = await rpcResponse(res, id);
    if (notify) return null;
    if (msg?.error) throw new Error(`${name}: ${msg.error.message ?? JSON.stringify(msg.error)}`);
    return msg?.result;
  };
  const connect = async () => {
    session = null;
    await rpc("initialize", { protocolVersion: PROTOCOL, capabilities: {}, clientInfo: { name: "heappi", version: "0.1.0" } });
    await rpc("notifications/initialized", {}, true);
    return (await rpc("tools/list", {})).tools ?? [];
  };
  return {
    name, connect,
    async call(tool, args, { key } = {}) {
      const once = () => rpc("tools/call", { name: tool, arguments: args ?? {}, ...(key ? { _meta: { "heappi/idempotency-key": key } } : {}) });
      let r;
      try { r = await once(); } catch (e) { if (!/MCP 4\d\d/.test(e.message)) throw e; await connect(); r = await once(); } // a lost session: reconnect once
      const text = (r.content ?? []).map((c) => c.type === "text" ? c.text : `[${c.type}]`).join("\n");
      if (r.isError) throw new Error(text || "tool error");
      if (r.structuredContent) return r.structuredContent;
      try { return JSON.parse(text); } catch { return text; }
    },
  };
}

/** servers: { name: { url } | { handler: (request) → Response } } */
let catalogPromise = null;
export function connectMcp(servers) {
  catalogPromise ??= (async () => {
    const catalog = [], clients = new Map();
    await Promise.all(Object.entries(servers).map(async ([name, spec]) => {
      const send = spec.url
        ? (body, headers) => fetch(spec.url, { method: "POST", headers, body, signal: AbortSignal.timeout(20000) })
        : (body, headers) => spec.handler(new Request("http://local/mcp", { method: "POST", headers, body }));
      const client = mcpClient(name, send);
      try {
        const tools = (await client.connect()).filter((t) => !spec.tools || spec.tools.includes(t.name));
        for (const t of tools) catalog.push({ name: `mcp__${name}__${t.name}`, description: `[${name}] ${t.description ?? t.name}`.slice(0, 1000), inputSchema: t.inputSchema ?? { type: "object", properties: {} }, server: name, tool: t.name, replay: t.annotations?.readOnlyHint || t.annotations?.idempotentHint ? "safe" : "unsafe" });
        clients.set(name, client);
        console.log(`mcp: ${name} connected, ${tools.length} tools`);
      } catch (e) { console.log(`mcp: ${name} failed: ${e.message}`); }
    }));
    catalog.sort((a, b) => a.name.localeCompare(b.name)); // servers connect in any order; a stable list keeps the cached part of a prompt valid
    return {
      catalog,
      servers: [...clients.keys()],
      async call(toolName, args, options) {
        const entry = catalog.find((t) => t.name === toolName);
        if (!entry) throw new Error(`no MCP tool ${toolName}`);
        return clients.get(entry.server).call(entry.tool, args, options);
      },
    };
  })();
  return catalogPromise;
}

// ---- the notes server: an MCP server over R2 ----
const NOTE_TOOLS = [
  { name: "list_notes", description: "List the notes in the shared notebook.", inputSchema: { type: "object", properties: {} } },
  { name: "read_note", description: "Read a note by name.", inputSchema: { type: "object", properties: { name: { type: "string" } }, required: ["name"] } },
  { name: "write_note", description: "Create or replace a note.", inputSchema: { type: "object", properties: { name: { type: "string" }, text: { type: "string" } }, required: ["name", "text"] } },
  { name: "append_note", description: "Append a line to a note, creating it if needed.", inputSchema: { type: "object", properties: { name: { type: "string" }, text: { type: "string" } }, required: ["name", "text"] } },
  { name: "delete_note", description: "Delete a note.", inputSchema: { type: "object", properties: { name: { type: "string" } }, required: ["name"] } },
];
export function notesServer(bucket, prefix = "notes/") {
  const key = (name) => { if (!/^[\w .-]{1,80}$/.test(name ?? "")) throw new Error("note names are letters, digits, space, dot, dash, underscore"); return prefix + name; };
  const ok = (id, result) => Response.json({ jsonrpc: "2.0", id, result });
  const text = (id, s) => ok(id, { content: [{ type: "text", text: typeof s === "string" ? s : JSON.stringify(s) }] });
  async function call(name, a = {}) {
    switch (name) {
      case "list_notes": { const l = await bucket.list({ prefix }); return l.objects.map((o) => ({ name: o.key.slice(prefix.length), bytes: o.size, modified: o.uploaded })); }
      case "read_note": { const o = await bucket.get(key(a.name)); if (!o) throw new Error(`no note ${a.name}`); return await o.text(); }
      case "write_note": await bucket.put(key(a.name), String(a.text ?? "")); return `wrote ${a.name}`;
      case "append_note": { const o = await bucket.get(key(a.name)); const cur = o ? await o.text() : ""; await bucket.put(key(a.name), (cur && !cur.endsWith("\n") ? cur + "\n" : cur) + String(a.text ?? "") + "\n"); return `appended to ${a.name}`; }
      case "delete_note": await bucket.delete(key(a.name)); return `deleted ${a.name}`;
      default: throw new Error("unknown tool " + name);
    }
  }
  return async (request) => {
    if (request.method !== "POST") return Response.json({ jsonrpc: "2.0", error: { code: -32600, message: "POST only" } }, { status: 405 });
    let msg; try { msg = await request.json(); } catch { return Response.json({ jsonrpc: "2.0", error: { code: -32700, message: "parse error" } }, { status: 400 }); }
    const { id, method, params } = msg;
    try {
      switch (method) {
        case "initialize": return ok(id, { protocolVersion: PROTOCOL, capabilities: { tools: {} }, serverInfo: { name: "heappi-notes", version: "0.1.0" } });
        case "notifications/initialized": return new Response(null, { status: 202 });
        case "ping": return ok(id, {});
        case "tools/list": return ok(id, { tools: NOTE_TOOLS });
        case "tools/call": return text(id, await call(params?.name, params?.arguments));
        default: return Response.json({ jsonrpc: "2.0", id, error: { code: -32601, message: "method not found: " + method } });
      }
    } catch (e) { return ok(id, { content: [{ type: "text", text: String(e?.message ?? e) }], isError: true }); }
  };
}
