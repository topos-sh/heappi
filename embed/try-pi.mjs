// Can Pi Durable run inside the agent-image heap? Evaluate the bundle in
// the engine, open a Harness on MemoryStorage with the faux model and one
// tool, run turns, snapshot, restore in a fresh instance, continue.
import { readFileSync } from "node:fs";
import { NodeEngine as Engine, SNAPSHOT_BASE, SNAPSHOT_CHAIN } from "../host/engine-node.mjs";

const e = await Engine.create();
e.init();
e.setMemoryLimit(256 << 20);
const t0 = performance.now();
e.eval(readFileSync("embed/prelude.js", "utf8"), "prelude.js");
try { e.eval(readFileSync("embed/pi-bundle.js", "utf8"), "pi-bundle.js"); }
catch (err) { console.log("BUNDLE EVAL FAILED:", err.message.slice(0, 600)); process.exit(1); }
console.log("bundle evaluated in", (performance.now() - t0).toFixed(0), "ms; heap", e.mem());

e.eval(`
globalThis.__log_ = (...a) => __log(a.map(String).join(" "));
globalThis.__state = { done: false, answers: [], error: null, reports: [], entries: [] };
const { BACKGROUND_CONTEXT, createModels, fauxProvider, fauxAssistantMessage, fauxToolCall, Type, createRegistry, defineExtension, defineTool, Harness, MemoryStorage } = PI;
const faux = fauxProvider();
faux.setResponses(Array.from({ length: 1000 }, () => (ctx) => {
  const last = ctx.messages.at(-1);
  if (last.role === "user") { const t = Number(/source (\\d+)/.exec(typeof last.content === "string" ? last.content : last.content.map((c) => c.text ?? "").join(""))[1]); return fauxAssistantMessage([fauxToolCall("fetch", { source: t }, { id: "call_" + t })], { stopReason: "toolUse" }); }
  if (last.role === "toolResult") { const text = last.content.map((c) => c.text ?? "").join(""); __state.lastTool = text.slice(0, 160); const m = /above50[^0-9]*(\\d+)/.exec(text); return fauxAssistantMessage("Source " + last.toolCallId.slice(5) + " has " + (m ? m[1] : "?") + " records scoring above 50."); }
  return fauxAssistantMessage("ok");
}));
const models = createModels(); models.setProvider(faux.provider);
const fetchTool = defineTool({ name: "fetch", description: "Fetch a source", parameters: Type.Object({ source: Type.Number() }), replay: "safe",
  execute: async (args) => {
    // the tool is a host call: it parks until the host delivers the payload (possibly in another process)
    const payload = await new Promise((resolve) => { globalThis.__pendingFetch = { source: args.source, resolve }; });
    const above50 = payload.items.filter((i) => i.score > 50).length;
    return { output: [{ type: "text", text: JSON.stringify({ above50 }) }] }; } });
const registry = createRegistry(); registry.install(defineExtension({ name: "research", tools: [fetchTool] }));
globalThis.__turn = async (t) => {
  try {
    globalThis.__harness ??= await Harness.open(new MemoryStorage(), { models, registry, onReport: (r) => __state.reports.push(String(r?.stack ?? r?.error?.stack ?? r).slice(0, 900)) }, BACKGROUND_CONTEXT);
    const root = await __harness.root(BACKGROUND_CONTEXT, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
    const sub = await root.submit({ type: "input", content: "Look at source " + t + " and count." }, BACKGROUND_CONTEXT);
    const settled = await sub.wait(BACKGROUND_CONTEXT);
    const page = await root.entries({ order: "descending" }, 6, undefined, BACKGROUND_CONTEXT);
    __state.entries = page.items.map((x) => x.kind + ": " + JSON.stringify(x.model?.[0]?.content ?? x.data ?? "").slice(0, 140));
    const a = page.items.find((x) => x.kind === "pi.assistant");
    __state.answers.push((settled.status) + ": " + (a?.model?.[0]?.content?.map((c) => c.text ?? "").join("") ?? "?"));
  } catch (err) { __state.error = String(err && err.stack || err); }
  __state.done = true;
};
`, "driver.js");

function answerFetch(engine) {
  const p = engine.eval("JSON.stringify(globalThis.__pendingFetch ? { source: __pendingFetch.source } : null)") && JSON.parse(engine.result());
  if (!p) return false;
  const items = []; for (let i = 0; i < 200; i++) items.push({ id: i, score: (i * 7919 + p.source * 31) % 100 });
  engine.eval(`{ const r = __pendingFetch.resolve; __pendingFetch = null; r(${JSON.stringify({ source: p.source, items })}); }`);
  return true;
}
function pump(engine, maxIters = 20000, { leaveParked = false } = {}) {
  for (let i = 0; i < maxIters; i++) {
    engine.run();
    if (!leaveParked) answerFetch(engine);
    else if (engine.eval("String(!!globalThis.__pendingFetch)") && engine.result() === "true") return "parked";
    const r = engine.eval("JSON.stringify({ done: __state.done, ran: __runTimers(), err: __state.error })") && JSON.parse(engine.result());
    if (r.err) throw new Error(r.err.slice(0, 1200));
    if (r.done) return i;
  }
  throw new Error("pump: not done");
}
for (let t = 1; t <= 3; t++) {
  const t1 = performance.now();
  e.eval(`__state.done = false; __turn(${t});`);
  const iters = pump(e);
  console.log(`turn ${t}: ${(performance.now() - t1).toFixed(1)} ms, ${iters} pump iterations,`, e.eval("__state.answers.at(-1)") && e.result());
  if (t === 1) { console.log("  entries:", e.eval("JSON.stringify(__state.entries, null, 1)") && e.result()); console.log("  reports:", e.eval("JSON.stringify(__state.reports)") && e.result()); }
}
console.log("heap after 3 turns", e.mem(), "linear MB", (e.linearBytes / 1048576).toFixed(1));

// park and resume: base after the bundle would be the harness layer; here one full base
const t2 = performance.now();
const base = e.snapshot(SNAPSHOT_BASE);
console.log("snapshot with Pi inside:", base.length, "bytes in", (performance.now() - t2).toFixed(1), "ms");
e.eval(`__state.done = false; __turn(4);`);
const st = pump(e, 20000, { leaveParked: true });
console.log("turn 4 parked inside Pi's tool task:", st, "| done flag", e.eval("String(__state.done)") && e.result());
const delta = e.snapshot(SNAPSHOT_CHAIN);
console.log("chain checkpoint while parked mid-turn:", delta.length, "bytes");
e.drop();

const f = await Engine.create();
f.setMemoryLimit(256 << 20);
const t3 = performance.now();
f.restore(base, [delta]);
console.log("restored in", (performance.now() - t3).toFixed(1), "ms; still parked:", f.eval("String(!!globalThis.__pendingFetch)") && f.result());
pump(f); // answers the parked fetch in the new process; Pi's tool task completes, generation continues
console.log("turn 4 finished after restore:", f.eval("__state.answers.at(-1)") && f.result());
f.eval(`__state.done = false; __turn(5);`); pump(f);
console.log("turn 5 in restored heap:", f.eval("__state.answers.at(-1)") && f.result(), "answers total", f.eval("__state.answers.length") && f.result());
