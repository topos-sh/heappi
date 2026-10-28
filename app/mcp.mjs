// MCP servers on the host. Each server's tools are exposed into the heap as
// `mcp__<server>__<tool>` with their JSON schemas; calls come back as host
// calls and are forwarded here. Config: app/mcp.json.
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

export async function connectMcp(configPath) {
  const config = JSON.parse(readFileSync(configPath, "utf8"));
  const base = dirname(configPath);
  const abs = (v) => typeof v === "string" && v.startsWith("./") ? resolve(base, "..", v) : v; // paths in the config are relative to the repo root
  const clients = new Map();
  const catalog = [];
  for (const [name, spec] of Object.entries(config.servers ?? {})) {
    const client = new Client({ name: "heappi", version: "0.1.0" });
    try {
      const transport = spec.url
        ? new StreamableHTTPClientTransport(new URL(spec.url))
        : new StdioClientTransport({ command: spec.command, args: (spec.args ?? []).map(abs), env: { ...process.env, ...Object.fromEntries(Object.entries(spec.env ?? {}).map(([k, v]) => [k, abs(v)])) }, stderr: "pipe" });
      await client.connect(transport);
      const { tools } = await client.listTools();
      for (const t of tools) catalog.push({ name: `mcp__${name}__${t.name}`, description: `[${name}] ${t.description ?? t.name}`, inputSchema: t.inputSchema ?? { type: "object", properties: {} }, server: name, tool: t.name });
      clients.set(name, client);
      console.log(`mcp: ${name} connected, ${tools.length} tools`);
    } catch (e) {
      console.log(`mcp: ${name} failed: ${e.message}`);
    }
  }
  return {
    catalog,
    async call(toolName, args) {
      const entry = catalog.find((t) => t.name === toolName);
      if (!entry) throw new Error(`no MCP tool ${toolName}`);
      const r = await clients.get(entry.server).callTool({ name: entry.tool, arguments: args ?? {} });
      const text = (r.content ?? []).map((c) => c.type === "text" ? c.text : `[${c.type}]`).join("\n");
      if (r.isError) throw new Error(text || "tool error");
      if (r.structuredContent) return r.structuredContent;
      try { return JSON.parse(text); } catch { return text; }
    },
    async close() { for (const c of clients.values()) await c.close().catch(() => {}); },
  };
}
