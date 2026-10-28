// The benchmark. Same scenario, four systems:
//   image      the agent image (this repo): one heap per agent, chained checkpoints, SQLite + cold files
//   transcript a conversation-level store (Infinity / Pi shape), payload kept or dropped
//   pi         the real Pi Durable harness with its code mode, Node SQLite storage
//   v8         the same harness.js resident in a node:vm context (memory only, no persistence)
// Measures disk per agent, bytes per checkpoint, restore time per tier, time per turn,
// resident memory per agent, and bytes sent to the model per request.
//
//   node --no-warnings bench/run.mjs --turns 50 --agents 3 --items 200 --mem-agents 40
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { Store } from "../host/store.mjs";
import { Cell } from "../host/cell.mjs";
import { Broker } from "../host/broker.mjs";
import { NodeEngine as Engine } from "../host/engine-node.mjs";
import { V8Cell } from "../host/v8cell.mjs";
import { userMessage, expectedCount, DEFAULTS } from "../host/scenario.mjs";
import { TranscriptStore, runTranscriptAgent } from "../baselines/transcript.mjs";
import { openPi, runPiAgent, closePi } from "../baselines/pi.mjs";
import { inspectSqlite } from "./inspect-sqlite.mjs";

const { values: args } = parseArgs({ options: {
  turns: { type: "string", default: String(DEFAULTS.turns) },
  agents: { type: "string", default: "3" },
  items: { type: "string", default: String(DEFAULTS.items) },
  "mem-agents": { type: "string", default: "40" },
  "skip-pi": { type: "boolean", default: false },
} });
const TURNS = Number(args.turns), AGENTS = Number(args.agents), ITEMS = Number(args.items), MEM_AGENTS = Number(args["mem-agents"]);
const work = mkdtempSync(join(tmpdir(), "agent-image-bench-"));
const results = { params: { turns: TURNS, agents: AGENTS, items: ITEMS, memAgents: MEM_AGENTS, node: process.version, platform: process.platform, arch: process.arch, date: new Date().toISOString() } };
const kb = (b) => (b / 1024).toFixed(1);
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s.length ? s[Math.floor(s.length / 2)] : NaN; };
const rss = () => { global.gc?.(); global.gc?.(); return process.memoryUsage().rss; };
const heap = () => { global.gc?.(); global.gc?.(); const m = process.memoryUsage(); return m.heapUsed + m.external + m.arrayBuffers; };
const log = (...a) => console.log(...a);

// ---------------------------------------------------------------- image
async function benchImage() {
  log(`\n== image: ${AGENTS} agents × ${TURNS} turns, ${ITEMS} items per fetch`);
  const store = new Store(join(work, "image.sqlite"), join(work, "cold"));
  const broker = new Broker({ items: ITEMS });
  const cell = new Cell(store, broker);
  const perAgent = [];
  let baseBytes = 0;
  for (let a = 0; a < AGENTS; a++) {
    const id = `agent-${a}`;
    const created = await cell.create(id);
    baseBytes = created.engine + created.harness;
    const t0 = performance.now();
    let correct = 0;
    for (let t = 1; t <= TURNS; t++) {
      store.enqueue(id, { type: "user", text: userMessage(t) });
      const recs = await cell.drain(id);
      const reply = recs.flatMap((r) => r.replies)[0];
      if (reply?.text.includes(`has ${expectedCount(t, ITEMS)} records`)) correct++;
    }
    const ms = performance.now() - t0;
    const sizes = store.sizes(id);
    const stats = await cell.stats(id);
    const last = cell.metrics.filter((m) => m.agentId === id);
    const folds = last.filter((m) => m.kind === "base");
    perAgent.push({ id, correct, ms, msPerTurn: ms / TURNS, ...sizes, writtenPerTurn: last.reduce((s, m) => s + m.checkpointBytes, 0) / TURNS, journalPerTurn: sizes.journal / TURNS, cold: store.coldBytes(id), lastBase: folds.at(-1)?.checkpointBytes ?? baseBytes, heapUsed: last.at(-1).heapUsed, objects: last.at(-1).objects, linearBytes: last.at(-1).linearBytes, stats, activations: last.length });
  }
  const acts = cell.metrics.filter((m) => m.tier === "hot");
  const byKind = {};
  for (const m of acts) { const k = `${m.kind}:${m.calls ? "park" : "reply"}`; (byKind[k] ??= []).push(m.checkpointBytes); }
  const msgs = store.bodyStats();

  // restore tiers on the last agent, after TURNS turns; median of 7
  const id = perAgent.at(-1).id;
  const tiers = {};
  for (const tier of ["hot", "warm", "cold"]) {
    const samples = [];
    for (let i = 0; i < 7; i++) {
      store.enqueue(id, { type: "user", text: userMessage(TURNS + 1 + i) });
      const recs = await cell.drain(id, tier === "hot" ? {} : { forceTier: tier });
      samples.push(recs[0]);
    }
    tiers[tier] = { restoreMs: median(samples.map((s) => s.restoreMs)), readMs: median(samples.map((s) => s.readMs ?? 0)), instantiateMs: median(samples.map((s) => s.instantiateMs ?? 0)), applyMs: median(samples.map((s) => s.applyMs ?? 0)), chainLength: samples[0].chainLength, chainBytes: samples[0].chainBytes };
  }
  // model-context bytes per request: what the heap sent (window + handles)
  const modelCalls = store.db.prepare("SELECT data FROM journal WHERE agent = ? AND kind = 'call'").all(id).map((r) => JSON.parse(r.data)).filter((d) => d.name === "model");
  const bodyOf = store.db.prepare("SELECT LENGTH(body) AS n FROM bodies WHERE hash = ?");
  const ctxBytes = modelCalls.map((c) => c.messages.reduce((s, h) => s + bodyOf.get(h).n, 0));
  const file = inspectSqlite(join(work, "image.sqlite"));

  results.image = {
    baseBytes, perAgent, activationsPerTurn: perAgent[0].activations / TURNS,
    checkpointBytes: Object.fromEntries(Object.entries(byKind).map(([k, v]) => [k, { median: median(v), max: Math.max(...v), n: v.length }])),
    snapshotMs: median(acts.map((m) => m.snapshotMs)), commitMs: median(acts.map((m) => m.commitMs)), runMs: median(acts.map((m) => m.runMs)),
    tiers, modelContextBytes: { first: ctxBytes[0], last: ctxBytes.at(-1), median: median(ctxBytes), max: Math.max(...ctxBytes) },
    bodiesTable: msgs, fileBytes: file.file, tables: file.tables, rebuildChecks: cell.rebuilds.length, rebuildOk: cell.rebuilds.every((r) => r.ok), rebuildMs: median(cell.rebuilds.map((r) => r.ms)),
    broker: { modelCalls: broker.modelCalls, cacheHits: broker.cacheHits, toolCalls: broker.toolCalls },
  };
  const p = perAgent[0];
  log(`  correct ${p.correct}/${TURNS}; ${p.activations / TURNS} activations per turn; ${p.msPerTurn.toFixed(1)} ms per turn (all tiers hot)`);
  log(`  written per turn: ${kb(p.writtenPerTurn)} KB of checkpoints (folds included) + ${kb(p.journalPerTurn)} KB of journal`);
  log(`  per agent after ${TURNS} turns: live chain ${kb(p.chainLive)} KB (base ${kb(p.lastBase)} KB + ${p.headSeq - p.baseSeq} deltas), journal ${kb(p.journal)} KB, cold dir ${kb(p.cold)} KB, heap used ${kb(p.heapUsed)} KB, wasm linear ${kb(p.linearBytes)} KB, handles ${p.stats.handles}`);
  log(`  checkpoint bytes:`, Object.entries(results.image.checkpointBytes).map(([k, v]) => `${k} med ${v.median} max ${v.max}`).join("; "));
  log(`  restore: hot ${tiers.hot.restoreMs.toFixed(2)} ms, warm ${tiers.warm.restoreMs.toFixed(2)} ms (instantiate ${tiers.warm.instantiateMs.toFixed(2)} + apply ${tiers.warm.applyMs.toFixed(2)}), cold ${tiers.cold.restoreMs.toFixed(2)} ms; chain ${tiers.warm.chainLength} deltas, ${kb(tiers.warm.chainBytes)} KB`);
  log(`  model context bytes per request: first ${ctxBytes[0]}, median ${median(ctxBytes)}, last ${ctxBytes.at(-1)}`);
  log(`  rebuild checks at folds: ${cell.rebuilds.length}, all equal: ${cell.rebuilds.every((r) => r.ok)}, median ${median(cell.rebuilds.map((r) => r.ms)).toFixed(1)} ms over ${cell.rebuilds.at(-1)?.events} events`);
  store.close();
}

// ----------------------------------------------------------- transcript
function benchTranscript() {
  log(`\n== transcript store (conversation-level), ${AGENTS} agents × ${TURNS} turns`);
  results.transcript = {};
  for (const keepPayload of [true, false]) {
    const path = join(work, `transcript-${keepPayload ? "keep" : "drop"}.sqlite`);
    const store = new TranscriptStore(path);
    const t0 = performance.now();
    for (let a = 0; a < AGENTS; a++) runTranscriptAgent(store, `agent-${a}`, TURNS, { keepPayload, items: ITEMS });
    const ms = (performance.now() - t0) / AGENTS;
    const perAgentBytes = store.bytes("agent-0");
    // context bytes per request = whole history (no window): sum over the last request
    const hist = store.history("agent-0");
    const ctxLast = JSON.stringify(hist).length;
    const file = store.fileBytes();
    store.close();
    results.transcript[keepPayload ? "keep" : "drop"] = { perAgentMessageBytes: perAgentBytes, fileBytesPerAgent: file / AGENTS, msPerTurn: ms / TURNS, modelContextBytesLast: ctxLast };
    log(`  payload ${keepPayload ? "kept" : "dropped"}: ${kb(perAgentBytes)} KB of messages per agent, file ${kb(file / AGENTS)} KB per agent, last request context ${kb(ctxLast)} KB, ${(ms / TURNS).toFixed(2)} ms per turn`);
  }
}

// ------------------------------------------------------------------- pi
async function benchPi() {
  log(`\n== Pi Durable 1.4.0 (real harness, code mode, Node SQLite), ${AGENTS} conversations × ${TURNS} turns`);
  // an empty harness file, to subtract schema overhead
  const empty = await openPi(join(work, "pi-empty.sqlite"), { items: ITEMS });
  await empty.harness.root(BACKGROUND, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
  await closePi(empty);
  const emptyBytes = inspectSqlite(join(work, "pi-empty.sqlite")).file;
  const files = [];
  const before = rss();
  const open = [];
  let msTotal = 0;
  for (let a = 0; a < AGENTS; a++) {
    const path = join(work, `pi-${a}.sqlite`);
    const pi = await openPi(path, { items: ITEMS });
    const t0 = performance.now();
    await runPiAgent(pi, "root", TURNS);
    msTotal += performance.now() - t0;
    open.push(pi); files.push(path);
  }
  const resident = (rss() - before) / AGENTS;
  for (const pi of open) await closePi(pi);
  const insp = files.map((f) => inspectSqlite(f));
  const perAgentFile = median(insp.map((i) => i.file)) - emptyBytes;
  const tables = insp[0].tables.slice(0, 6);
  results.pi = { emptyFileBytes: emptyBytes, fileBytesPerAgent: perAgentFile, msPerTurn: msTotal / AGENTS / TURNS, residentPerConversationWhileOpen: resident, tables };
  log(`  file per conversation (schema subtracted): ${kb(perAgentFile)} KB; ${(msTotal / AGENTS / TURNS).toFixed(1)} ms per turn; top tables: ${tables.map((t) => `${t.table} ${kb(t.bytes)} KB`).join(", ")}`);
}
import { BACKGROUND_CONTEXT as BACKGROUND } from "@earendil-works/chord/context";

// --------------------------------------------------------------- memory
async function benchMemory() {
  log(`\n== resident memory per agent, ${MEM_AGENTS} agents each after 10 turns`);
  const turns = 10;
  // image: restore MEM_AGENTS heaps from one agent's chain, keep them resident
  const store = new Store(join(work, "mem.sqlite"), null);
  const broker = new Broker({ items: ITEMS });
  const cell = new Cell(store, broker);
  await cell.create("m");
  for (let t = 1; t <= turns; t++) { store.enqueue("m", { type: "user", text: userMessage(t) }); await cell.drain("m"); }
  const chain = store.chain("m");
  cell.evictHot("m");
  let base = rss(); let baseHeap = heap();
  const engines = [];
  const tInst = performance.now();
  for (let i = 0; i < MEM_AGENTS; i++) { const e = await Engine.create(); e.restore(chain.base, chain.deltas); engines.push(e); }
  const instMs = (performance.now() - tInst) / MEM_AGENTS;
  const imageRss = (rss() - base) / MEM_AGENTS;
  const imageHeap = (heap() - baseHeap) / MEM_AGENTS;
  const linear = engines[0].linearBytes;
  const heapUsed = engines[0].mem().memory_used;
  engines.forEach((e) => e.drop());
  engines.length = 0;
  store.close();

  // v8: the same harness in MEM_AGENTS vm contexts, same 10 turns each
  base = rss(); baseHeap = heap();
  const v8 = new V8Cell(new Broker({ items: ITEMS }));
  for (let i = 0; i < MEM_AGENTS; i++) {
    v8.create(`v${i}`);
    for (let t = 1; t <= turns; t++) await v8.turn(`v${i}`, { type: "user", text: userMessage(t) });
  }
  const v8Rss = (rss() - base) / MEM_AGENTS;
  const v8Heap = (heap() - baseHeap) / MEM_AGENTS;
  const v8Stats = v8.stats("v0");

  results.memory = { turns, image: { rssPerAgent: imageRss, jsHeapPerAgent: imageHeap, linearBytes: linear, heapUsed, instantiateAndRestoreMs: instMs, parked: 0 }, v8vm: { rssPerAgent: v8Rss, jsHeapPerAgent: v8Heap, stats: v8Stats } };
  log(`  image resident heap: ${kb(imageRss)} KB RSS per agent, ${kb(imageHeap)} KB V8-accounted (wasm linear ${kb(linear)} KB, engine heap used ${kb(heapUsed)} KB), ${instMs.toFixed(2)} ms to instantiate + restore each; parked: 0 bytes of RAM`);
  log(`  same harness in a node:vm context: ${kb(v8Rss)} KB RSS, ${kb(v8Heap)} KB V8 heap per agent (lower bound for a V8 isolate)`);
}

await benchImage();
benchTranscript();
if (!args["skip-pi"]) await benchPi();
await benchMemory();

mkdirSync(join(import.meta.dirname, "out"), { recursive: true });
writeFileSync(join(import.meta.dirname, "out", "results.json"), JSON.stringify(results, null, 2));
log(`\nresults → bench/out/results.json (work dir ${work})`);
