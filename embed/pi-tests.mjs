// Checks for Pi in the heap: a call a crash cut off, a repeated input,
// steering, stop, tools loaded on demand, a prompt whose start stays put.
// Run: node --no-warnings embed/pi-tests.mjs
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../host/store.mjs";
import { Cell } from "../host/cell.mjs";
import { simpleToAssistant } from "../host/pi-shape.mjs";

const harnessSrc = readFileSync(new URL("./pi-harness.js", import.meta.url), "utf8");
const dir = mkdtempSync(join(tmpdir(), "heappi-pi-tests-"));
let n = 0;
const ok = (name) => console.log(`ok ${++n} ${name}`);
const tool = (name, replay, server) => ({ name, description: `the ${name} tool`, inputSchema: { type: "object", properties: {} }, replay, ...(server ? { server } : {}) });
const textOf = (m) => typeof m.content === "string" ? m.content : (m.content ?? []).map((c) => c.text ?? "").join("");

/** a broker with a scripted model: `model(context, nth)` returns { content, tool_calls }, `tools[name]` runs a tool */
class TestBroker {
  constructor({ model, tools = {} }) { this.model = model; this.tools = tools; this.requests = []; this.toolCalls = []; this.inflight = new Set(); }
  call(_agent, _id, name, args, { key, signal } = {}) {
    const p = (async () => {
      await Promise.resolve();
      if (name === "model") { this.requests.push(args.context); return simpleToAssistant(this.model(args.context, this.requests.length)); }
      this.toolCalls.push({ name: args.name, key });
      const run = this.tools[args.name];
      if (!run) throw new Error("no tool " + args.name);
      return run(args.args, { signal });
    })();
    if (!this.hold?.has(args?.name)) { this.inflight.add(p); p.catch(() => {}).finally(() => this.inflight.delete(p)); }
    return p;
  }
  async settle() { while (this.inflight.size) await Promise.allSettled([...this.inflight]); }
}
async function agent(name, broker, tools = []) {
  const store = new Store(join(dir, name + ".sqlite"), null);
  const cell = new Cell(store, broker, { harnessSrc, checkRebuild: false });
  await cell.create("a");
  store.enqueue("a", { type: "config", config: { tools, instructions: "be brief" } });
  await cell.drain("a");
  return { store, cell };
}
const replies = (recs) => recs.flatMap((r) => r.replies);
const callThen = (toolName, answer) => (ctx) => ctx.messages.at(-1).role === "user" ? { content: "", tool_calls: [{ id: "c1", name: toolName, args: {} }] } : { content: answer(ctx), tool_calls: [] };

// ---- 1. a crash with a tool running: the unsafe one is reported, never run again ----
{
  const model = callThen("send_email", (ctx) => "saw: " + textOf(ctx.messages.at(-1)));
  const never = new Promise(() => {});
  const first = new TestBroker({ model, tools: { send_email: () => never } });
  first.hold = new Set(["send_email"]);
  const { store, cell } = await agent("crash-unsafe", first, [tool("send_email", "unsafe")]);
  store.enqueue("a", { type: "user", text: "send it" });
  await cell.drain("a");
  assert.equal(first.toolCalls.length, 1);
  store.close();

  // a new process: the same file, a fresh cell, nothing in memory
  const second = new TestBroker({ model, tools: { send_email: () => { throw new Error("must not run twice"); } } });
  const store2 = new Store(join(dir, "crash-unsafe.sqlite"), null);
  const cell2 = new Cell(store2, second, { harnessSrc, checkRebuild: false });
  const r = cell2.recover("a");
  assert.deepEqual([r.rerun.length, r.interrupted.length], [0, 1]);
  const reply = replies(await cell2.drain("a"))[0].text;
  assert.match(reply, /interrupted.*send_email.*may have partly happened/);
  assert.equal(second.toolCalls.length, 0);
  ok("a crash during an unsafe tool: not run again, the model is told, the turn finishes");
  store2.close();
}

// ---- 2. a crash with a safe tool running: it runs again under the same key ----
{
  const model = callThen("lookup", (ctx) => "saw: " + textOf(ctx.messages.at(-1)));
  const first = new TestBroker({ model, tools: { lookup: () => new Promise(() => {}) } });
  first.hold = new Set(["lookup"]);
  const { store, cell } = await agent("crash-safe", first, [tool("lookup", "safe")]);
  store.enqueue("a", { type: "user", text: "look it up" });
  await cell.drain("a");
  const firstKey = first.toolCalls[0].key;
  store.close();

  const second = new TestBroker({ model, tools: { lookup: () => "found" } });
  const store2 = new Store(join(dir, "crash-safe.sqlite"), null);
  const cell2 = new Cell(store2, second, { harnessSrc, checkRebuild: false });
  assert.equal(cell2.recover("a").rerun.length, 1);
  assert.match(replies(await cell2.drain("a"))[0].text, /found/);
  assert.equal(second.toolCalls[0].key, firstKey);
  ok(`a crash during a safe tool: runs again under the same key (${firstKey}), the turn finishes`);
  store2.close();
}

// ---- 3. a repeated input with the same requestId is answered once (Pi's own key) ----
{
  const broker = new TestBroker({ model: () => ({ content: "hello", tool_calls: [] }) });
  const { store, cell } = await agent("dup", broker);
  store.enqueue("a", { type: "user", text: "hi", requestId: "r-1" });
  store.enqueue("a", { type: "user", text: "hi", requestId: "r-1" });
  const first = replies(await cell.drain("a"));
  store.enqueue("a", { type: "user", text: "hi", requestId: "r-1" });
  const later = replies(await cell.drain("a"));
  assert.equal(first.length, 1);
  assert.equal(later.length, 0);
  assert.equal(broker.requests.length, 1);
  ok("the same requestId, sent three times: one model call, one reply");
  store.close();
}

// ---- 4. steering: a message sent mid-tool joins the same run ----
{
  let release;
  const model = (ctx) => {
    const last = ctx.messages.at(-1);
    if (ctx.messages.filter((m) => m.role === "assistant").length === 0) return { content: "", tool_calls: [{ id: "c1", name: "slow", args: {} }] };
    return { content: "done, last message was: " + textOf(last), tool_calls: [] };
  };
  const broker = new TestBroker({ model, tools: { slow: () => new Promise((r) => { release = r; }) } });
  broker.hold = new Set(["slow"]);
  const { store, cell } = await agent("steer", broker, [tool("slow", "safe")]);
  store.enqueue("a", { type: "user", text: "start" });
  const before = await cell.drain("a");
  assert.equal(replies(before).length, 0);
  assert.equal((await cell.stats("a")).busy, true);
  store.enqueue("a", { type: "steer", text: "use pnpm, not npm" });
  await cell.drain("a");
  release("tool output");
  await new Promise((r) => setTimeout(r));
  const reply = replies(await cell.drain("a"));
  assert.equal(reply.length, 1);
  assert.match(reply[0].text, /use pnpm/);
  assert.equal((await cell.stats("a")).busy, false);
  ok("a steer sent while a tool runs reaches the next model request, one reply for the run");
  store.close();
}

// ---- 5. stop: a run stuck on a tool ends at once, and the agent still works ----
{
  const model = (ctx) => textOf(ctx.messages.at(-1)).includes("again") ? { content: "back", tool_calls: [] } : { content: "", tool_calls: [{ id: "c1", name: "stuck", args: {} }] };
  const broker = new TestBroker({ model, tools: { stuck: () => new Promise(() => {}) } });
  broker.hold = new Set(["stuck"]);
  const { store, cell } = await agent("cancel", broker, [tool("stuck", "safe")]);
  store.enqueue("a", { type: "user", text: "go" });
  await cell.drain("a");
  store.enqueue("a", { type: "cancel" });
  cell.abortCalls("a");
  const stopped = replies(await cell.drain("a"));
  assert.equal(stopped[0]?.text, "(stopped)");
  assert.equal((await cell.stats("a")).busy, false);
  store.enqueue("a", { type: "user", text: "again" });
  assert.equal(replies(await cell.drain("a"))[0].text, "back");
  assert.ok(broker.requests.at(-1).messages.some((m) => textOf(m).includes("stopped the request above")), "the model is told about the stop");
  ok("stop ends a run stuck on a tool, the model is told, and the next message is answered");
  store.close();
}

// ---- 6. large catalogs: tools load on demand, and the start of each prompt stays the same ----
{
  const catalog = [tool("fetch", "safe"), tool("now", "safe")];
  for (let i = 0; i < 25; i++) catalog.push({ ...tool(`mcp__fs__op${i}`, "unsafe", "fs"), description: `file operation number ${i}: `.padEnd(400, "x"), inputSchema: { type: "object", properties: { path: { type: "string", description: "x".repeat(200) } } } });
  const model = (_ctx, nth) => nth === 1 ? { content: "", tool_calls: [{ id: "c1", name: "tool_search", args: { query: "operation number 7" } }] }
    : nth === 2 ? { content: "", tool_calls: [{ id: "c2", name: "mcp__fs__op7", args: {} }] }
    : { content: "ok", tool_calls: [] };
  const broker = new TestBroker({ model, tools: { mcp__fs__op7: () => "done" } });
  const { store, cell } = await agent("defer", broker, catalog);
  store.enqueue("a", { type: "user", text: "do op 7" });
  assert.equal(replies(await cell.drain("a"))[0].text, "ok");
  const [first, second, third] = broker.requests;
  const names = (r) => r.tools.map((t) => t.name);
  assert.ok(names(first).includes("tool_search") && !names(first).some((t) => t.startsWith("mcp__")), "no MCP schemas on the first request");
  assert.ok(names(second).includes("mcp__fs__op7"), "the match is offered next");
  assert.equal(broker.toolCalls.at(-1).name, "mcp__fs__op7");
  const size = (r) => JSON.stringify(r.tools).length;
  const all = JSON.stringify(catalog.map((t) => ({ name: t.name, description: t.description, parameters: t.inputSchema }))).length;
  assert.equal(first.systemPrompt, third.systemPrompt);
  assert.deepEqual(third.messages.slice(0, second.messages.length), second.messages);
  ok(`tool schemas on the first request: ${size(first)} B instead of ${all} B; a search loads what is needed; system prompt and earlier messages unchanged between requests`);
  store.close();
}

console.log(`\n${n} checks passed`);
