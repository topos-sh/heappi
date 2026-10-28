// Drive the Worker (local wrangler dev or deployed) through the same
// scenario and collect the object's own timings plus client round trips.
//   node cf/bench.mjs http://localhost:8787 --turns 30 --label local
import { parseArgs } from "node:util";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const { values: args, positionals } = parseArgs({ allowPositionals: true, options: { turns: { type: "string", default: "30" }, label: { type: "string", default: "local" }, agent: { type: "string", default: "bench-1" }, rep: { type: "string", default: "async" } } });
const base = positionals[0] ?? "http://localhost:8787";
const TURNS = Number(args.turns);
const kb = (b) => (b / 1024).toFixed(1);
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s.length ? s[Math.floor(s.length / 2)] : NaN; };

async function call(path) {
  const t0 = performance.now();
  const r = await fetch(`${base}/agents/${args.agent}${path}`);
  const body = await r.json();
  if (!r.ok || body.error) throw new Error(`${path}: ${JSON.stringify(body).slice(0, 800)}`);
  return { ...body, clientMs: performance.now() - t0 };
}

console.log(`== ${args.label}: ${base}, agent ${args.agent}, ${TURNS} turns`);
const noops = []; let colo; for (let i = 0; i < 9; i++) { const n = await call("/noop"); noops.push(n.clientMs); colo = n.colo; }
const rtt = median(noops);
console.log(`  round trip (noop): ${rtt.toFixed(1)} ms, colo ${colo}, replication ${args.rep}`);
const created = await call("/create");
console.log(`  created: base ${kb(created.baseBytes)} KB, R2 put ${created.r2Ms} ms`);

const turns = [];
let correct = 0;
for (let t = 1; t <= TURNS; t++) {
  const tier = t % 10 === 0 ? "cold" : t % 5 === 0 ? "warm" : "hot";
  const r = await call(`/turn?t=${t}&tier=${tier}&rep=${args.rep}`);
  if (r.correct) correct++;
  turns.push({ t, tier, ...r });
  if (t <= 2 || t % 10 === 0) console.log(`  turn ${t} (${tier}): ${r.correct ? "ok" : "WRONG"} ${r.activations.length} acts, object ${r.turnMs} ms, client ${r.clientMs.toFixed(0)} ms, first load ${r.activations[0].restoreMs} ms, R2 per act ${r.activations.map((a) => a.r2Ms).join("/")} ms, live chain ${kb(r.info.liveChainBytes)} KB (${r.info.deltas} deltas), db ${kb(r.info.dbBytes)} KB, wasm linear ${kb(r.activations.at(-1).linearBytes)} KB`);
}
const acts = turns.flatMap((x) => x.activations);
const parks = acts.filter((a) => a.kind === "chain");
const summary = {
  label: args.label, base, turns: TURNS, correct, rttMs: rtt, colo, replication: args.rep,
  turnObjectMs: median(turns.map((x) => x.turnMs)), turnClientMs: median(turns.map((x) => x.clientMs)),
  activationsPerTurn: acts.length / TURNS,
  checkpointBytes: { parkMedian: median(parks.map((a) => a.checkpointBytes)), parkMax: Math.max(...parks.map((a) => a.checkpointBytes)), folds: acts.filter((a) => a.kind === "base").map((a) => a.checkpointBytes) },
  snapshotMs: median(acts.map((a) => a.snapshotMs)), commitMs: median(acts.map((a) => a.commitMs)), r2Ms: median(acts.map((a) => a.r2Ms)), runMs: median(acts.map((a) => a.runMs)),
  restore: {},
  final: turns.at(-1).info, linearBytes: acts.at(-1).linearBytes, heapUsed: acts.at(-1).heapUsed,
};
// restore-only timings after the run: 5 samples per tier, object time and client time
for (const tier of ["warm", "cold"]) {
  const s = [];
  for (let i = 0; i < 5; i++) s.push(await call(`/restore?tier=${tier}`));
  summary.restore[tier] = { objectMs: median(s.map((x) => x.restoreMs)), readMs: median(s.map((x) => x.readMs)), instantiateMs: median(s.map((x) => x.instantiateMs)), applyMs: median(s.map((x) => x.applyMs)), clientMs: median(s.map((x) => x.clientMs)), clientMinusRtt: median(s.map((x) => x.clientMs)) - rtt, chainLength: s[0].chainLength, chainBytes: s[0].chainBytes };
}
console.log(`\n  ${correct}/${TURNS} correct; ${summary.activationsPerTurn} activations per turn; turn: object ${summary.turnObjectMs} ms, client ${summary.turnClientMs.toFixed(0)} ms (rtt ${rtt.toFixed(0)} ms)`);
console.log(`  per activation: run ${summary.runMs} ms, snapshot ${summary.snapshotMs} ms, SQLite commit ${summary.commitMs} ms, R2 replicate ${summary.r2Ms} ms`);
console.log(`  checkpoints: park median ${kb(summary.checkpointBytes.parkMedian)} KB, max ${kb(summary.checkpointBytes.parkMax)} KB; folds ${summary.checkpointBytes.folds.map(kb).join(", ")} KB`);
for (const [tier, r] of Object.entries(summary.restore)) console.log(`  restore ${tier}: object ${r.objectMs} ms (read ${r.readMs}, instantiate ${r.instantiateMs}, apply ${r.applyMs}); client ${r.clientMs.toFixed(0)} ms, minus rtt ${r.clientMinusRtt.toFixed(0)} ms; chain ${r.chainLength} deltas, ${kb(r.chainBytes)} KB`);
console.log(`  final: live chain ${kb(summary.final.liveChainBytes)} KB, DO SQLite ${kb(summary.final.dbBytes)} KB, wasm linear ${kb(summary.linearBytes)} KB, engine heap ${kb(summary.heapUsed)} KB; R2 replication ${summary.final.repItems} checkpoints in ${summary.final.repBatches} batches`);
mkdirSync(join(import.meta.dirname, "out"), { recursive: true });
writeFileSync(join(import.meta.dirname, "out", `results-${args.label}.json`), JSON.stringify({ summary, turns }, null, 2));
