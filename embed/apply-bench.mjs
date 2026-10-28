// Restore an agent three ways: full chain restore; live apply of the agent's
// checkpoints onto a runtime that holds the layers; the same on a clone of a
// template instance that restored the layers once. Pi is the harness.
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NodeEngine, compileEngine } from "../host/engine-node.mjs";
import { Engine } from "../host/engine.mjs";
import { Store } from "../host/store.mjs";
import { Cell } from "../host/cell.mjs";
import { Broker } from "../host/broker.mjs";
import { userMessage, expectedCount } from "../host/scenario.mjs";

const turns = Number(process.argv[2] ?? 8);
const dir = mkdtempSync(join(tmpdir(), "apply-bench-"));
const store = new Store(join(dir, "s.sqlite"), null);
const cell = new Cell(store, new Broker(), { harnessSrc: readFileSync(new URL("./pi-harness.js", import.meta.url), "utf8"), checkRebuild: false, foldEvery: 1000 });
await cell.create("a");
for (let t = 1; t <= turns; t++) { store.enqueue("a", { type: "user", text: userMessage(t) }); await cell.drain("a"); }
const chain = store.chain("a");
const stats = JSON.stringify(await cell.stats("a"));
const hash = await cell.stateHash("a");
const kb = (b) => (b / 1024).toFixed(0);
const med = (xs) => xs.sort((a, b) => a - b)[Math.floor(xs.length / 2)];
console.log(`agent after ${turns} turns: harness layer ${kb(chain.deltas[0].length)} KB + ${chain.deltas.length - 1} checkpoints ${kb(chain.deltas.slice(1).reduce((s, d) => s + d.length, 0))} KB`);

let xs = [];
for (let i = 0; i < 7; i++) { const e = await NodeEngine.create(); const t0 = performance.now(); e.restore(chain.base, chain.deltas, { anchorAfter: 0 }); xs.push(performance.now() - t0); if (i === 0) console.log("  full restore state:", e.call("__stateHash").result === hash ? "matches" : "DIFFERENT"); e.drop(); }
console.log(`full chain restore:              ${med(xs).toFixed(2)} ms`);

xs = [];
for (let i = 0; i < 7; i++) { const e = await NodeEngine.create(); e.restore(chain.base, chain.deltas.slice(0, 1), { anchorAfter: 0 }); const t0 = performance.now(); e.apply(chain.deltas.slice(1)); xs.push(performance.now() - t0); if (i === 0) console.log("  live apply state:", e.call("__stateHash").result === hash ? "matches" : "DIFFERENT", JSON.parse(e.call("__stats").result).turn === JSON.parse(stats).turn ? "" : "turn differs"); e.drop(); }
console.log(`live apply of the checkpoints:   ${med(xs).toFixed(2)} ms (the layers already in memory)`);

const mod = await compileEngine();
const template = Engine.fromModule(mod); template.init(); template.drop(); template.restore(chain.base, chain.deltas.slice(0, 1), { anchorAfter: 0 });
xs = []; let clones = [];
for (let i = 0; i < 7; i++) { const t0 = performance.now(); const e = template.clone(); const t1 = performance.now(); e.apply(chain.deltas.slice(1)); xs.push(performance.now() - t0); clones.push(t1 - t0); if (i === 0) console.log("  clone + apply state:", e.call("__stateHash").result === hash ? "matches" : "DIFFERENT"); e.drop(); }
console.log(`template clone + live apply:     ${med(xs).toFixed(2)} ms (clone ${med(clones).toFixed(2)} ms, template memory ${kb(template.linearBytes)} KB)`);
const e = template.clone(); e.apply(chain.deltas.slice(1));
cell.hot.set("a", { engine: e, epoch: store.agent("a").epoch, lastApplyMs: 0 });
store.enqueue("a", { type: "user", text: userMessage(turns + 1) });
const recs = await cell.drain("a");
const reply = recs.flatMap((r) => r.replies)[0]?.text ?? "";
console.log(`a turn on the clone: ${reply.includes(`has ${expectedCount(turns + 1)} records`) ? "correct" : "WRONG " + reply.slice(0, 80)}, ${recs.length} activations, checkpoints ${recs.map((r) => r.checkpointBytes).join("/")} B`);
store.close();
