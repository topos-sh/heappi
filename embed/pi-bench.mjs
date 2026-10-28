// Side by side: the small harness (heap/harness.js) and Pi Durable as the
// harness (embed/pi-harness.js), same cell, same scenario, same store.
//   node --no-warnings embed/pi-bench.mjs --turns 50
import { mkdtempSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { Store } from "../host/store.mjs";
import { Cell } from "../host/cell.mjs";
import { Broker } from "../host/broker.mjs";
import { userMessage, expectedCount } from "../host/scenario.mjs";

const { values: args } = parseArgs({ options: { turns: { type: "string", default: "50" } } });
const TURNS = Number(args.turns);
const kb = (b) => (b / 1024).toFixed(1);
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s.length ? s[Math.floor(s.length / 2)] : NaN; };
const work = mkdtempSync(join(tmpdir(), "pi-bench-"));

async function run(name, harnessSrc) {
  const store = new Store(join(work, `${name}.sqlite`), join(work, `${name}-cold`));
  const broker = new Broker();
  const cell = new Cell(store, broker, { harnessSrc, checkRebuild: true });
  const id = "a";
  const created = await cell.create(id);
  const t0 = performance.now();
  let correct = 0;
  for (let t = 1; t <= TURNS; t++) {
    store.enqueue(id, { type: "user", text: userMessage(t) });
    const recs = await cell.drain(id);
    if (recs.flatMap((r) => r.replies)[0]?.text.includes(`has ${expectedCount(t)} records`)) correct++;
  }
  const ms = performance.now() - t0;
  const acts = cell.metrics.filter((m) => m.agentId === id);
  const parks = acts.filter((m) => m.kind === "chain" && m.calls > 0), replies = acts.filter((m) => m.kind === "chain" && !m.calls);
  const sizes = store.sizes(id);
  // restore samples
  const tiers = {};
  for (const tier of ["warm", "cold"]) {
    const s = [];
    for (let i = 0; i < 5; i++) { store.enqueue(id, { type: "user", text: userMessage(TURNS + 1 + i) }); s.push((await cell.drain(id, { forceTier: tier }))[0]); }
    tiers[tier] = { restoreMs: median(s.map((x) => x.restoreMs)), applyMs: median(s.map((x) => x.applyMs)), chainBytes: s[0].chainBytes, chainLength: s[0].chainLength };
  }
  const modelCalls = store.db.prepare("SELECT data FROM journal WHERE agent = ? AND kind = 'call'").all(id).map((r) => JSON.parse(r.data)).filter((d) => d.name === "model");
  const bodyOf = store.db.prepare("SELECT LENGTH(body) AS n FROM bodies WHERE hash = ?");
  const ctx = modelCalls.map((c) => c.messages.reduce((s, h) => s + bodyOf.get(h).n, 0));
  const mem = acts.at(-1);
  const r = { name, correct, msPerTurn: ms / TURNS, layers: created.engine + created.harness, harnessLayer: created.harness, createMs: created.restoreMs,
    parkMedian: median(parks.map((m) => m.checkpointBytes)), parkMax: Math.max(...parks.map((m) => m.checkpointBytes)), replyMedian: median(replies.map((m) => m.checkpointBytes)),
    writtenPerTurn: acts.reduce((s, m) => s + m.checkpointBytes, 0) / TURNS, chainLive: sizes.chainLive, journal: sizes.journal, folds: acts.filter((m) => m.kind === "fold").length, rebuilds: cell.rebuilds.length, rebuildOk: cell.rebuilds.every((x) => x.ok), rebuildMs: median(cell.rebuilds.map((x) => x.ms)),
    snapshotMs: median(acts.map((m) => m.snapshotMs)), runMs: median(acts.map((m) => m.runMs)), heapUsed: mem.heapUsed, objects: mem.objects, linearBytes: mem.linearBytes, ctxLast: ctx.at(-1), tiers, journalKinds: store.journal(id).reduce((m, x) => (m[x.kind] = (m[x.kind] ?? 0) + 1, m), {}) };
  store.close();
  return r;
}

const small = await run("small", readFileSync(new URL("../heap/harness.js", import.meta.url), "utf8"));
const pi = await run("pi", readFileSync(new URL("./pi-harness.js", import.meta.url), "utf8"));
const rows = [
  ["Correct turns", (r) => `${r.correct}/${TURNS}`],
  ["Harness layer (stored once)", (r) => kb(r.harnessLayer) + " KB"],
  ["New agent from layers", (r) => r.createMs.toFixed(1) + " ms"],
  ["Checkpoint, park (median / max)", (r) => `${kb(r.parkMedian)} / ${kb(r.parkMax)} KB`],
  ["Checkpoint, reply (median)", (r) => kb(r.replyMedian) + " KB"],
  ["Written per turn, folds included", (r) => kb(r.writtenPerTurn) + " KB"],
  [`Live chain after ${TURNS} turns`, (r) => kb(r.chainLive) + " KB"],
  ["Journal", (r) => kb(r.journal) + " KB"],
  ["Folds / rebuild checks ok", (r) => `${r.folds} / ${r.rebuildOk ? "yes" : "NO"} (${(r.rebuildMs || 0).toFixed(0)} ms)`],
  ["Time per turn", (r) => r.msPerTurn.toFixed(1) + " ms"],
  ["Run inside the heap per activation", (r) => r.runMs.toFixed(2) + " ms"],
  ["Snapshot write per activation", (r) => r.snapshotMs.toFixed(2) + " ms"],
  ["Warm restore", (r) => `${r.tiers.warm.restoreMs.toFixed(1)} ms (${r.tiers.warm.chainLength} deltas, ${kb(r.tiers.warm.chainBytes)} KB)`],
  ["Cold restore", (r) => r.tiers.cold.restoreMs.toFixed(1) + " ms"],
  ["Engine heap used / objects", (r) => `${kb(r.heapUsed)} KB / ${r.objects}`],
  ["Wasm linear memory", (r) => kb(r.linearBytes) + " KB"],
  ["Model context, last request", (r) => kb(r.ctxLast) + " KB"],
  ["Journal rows by kind", (r) => Object.entries(r.journalKinds).map(([k, n]) => `${k} ${n}`).join(", ")],
];
const lines = [`| | Small harness | Pi Durable in the heap |`, `|---|---|---|`, ...rows.map(([label, f]) => `| ${label} | ${f(small)} | ${f(pi)} |`)];
console.log(lines.join("\n"));
mkdirSync(join(import.meta.dirname, "out"), { recursive: true });
writeFileSync(join(import.meta.dirname, "out", "pi-vs-small.md"), lines.join("\n") + "\n");
writeFileSync(join(import.meta.dirname, "out", "pi-vs-small.json"), JSON.stringify({ small, pi }, null, 2));
