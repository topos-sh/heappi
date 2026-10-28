// Smoke test: pending promise across a chained checkpoint, restore in a
// fresh instance, deliver the result, and a CPU-budget suspension.
import { Engine, SNAPSHOT_BASE, SNAPSHOT_CHAIN } from "./engine.mjs";

const e = await Engine.create();
e.init();
e.eval(`
  globalThis.pending = new Map(); let seq = 0; const outbox = [];
  globalThis.hostCall = (name, args) => new Promise((res, rej) => { const id = ++seq; pending.set(id, {res, rej}); outbox.push({id, name, args}); });
  globalThis.__drain = () => { const s = JSON.stringify(outbox); outbox.length = 0; return s; };
  globalThis.__deliver = (json) => { const ev = JSON.parse(json); const p = pending.get(ev.id); pending.delete(ev.id); p.res(ev.value); };
  globalThis.state = { log: [] };
`);
const base = e.snapshot(SNAPSHOT_BASE);
console.log("base bytes", base.length, "base id", e.baseId());

e.eval(`(async () => { state.log.push("start"); const r = await hostCall("fetch", { url: "x" }); state.log.push("got " + r.n); })();`);
const out = JSON.parse(e.call("__drain").result);
console.log("outbox", out);
const c1 = e.snapshot(SNAPSHOT_CHAIN);
console.log("checkpoint bytes", c1.length, "new base id", e.baseId());
e.drop();

const f = await Engine.create();
f.restore(base, [c1]);
console.log("restored, base id", f.baseId(), "log", f.eval("JSON.stringify(state.log)") && f.result());
f.call("__deliver", JSON.stringify({ id: out[0].id, value: { n: 42 } }));
f.eval("JSON.stringify(state.log)");
console.log("after deliver:", f.result());
console.log("mem", f.mem(), "linear MB", (f.linearBytes / 1048576).toFixed(1));

// CPU budget: a synchronous loop called via a global function is suspended
f.eval(`globalThis.compute = () => { let s = 0; for (let i = 0; i < 2e7; i++) s += i % 7; return s; };`);
f.setBudgetMs(5);
const t0 = performance.now();
let r = f.call("compute");
let slices = 1;
const chainParts = [];
while (r.status === "suspended") {
  chainParts.push(f.snapshot(SNAPSHOT_CHAIN)); // checkpoint while suspended
  r = f.resume(); slices++;
}
console.log("compute result", r.result, "slices", slices, "ms", (performance.now() - t0).toFixed(1),
  "checkpoint sizes while suspended", chainParts.map((c) => c.length).slice(0, 5));
f.setBudgetMs(0);
f.drop();
