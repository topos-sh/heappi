// Checks for the follow-ups: script realm isolation, CPU budget inside a
// script, rebuild from the journal, harness upgrade, tampered checkpoint,
// mailbox dedup, hot-tier eviction. Run: node --no-warnings host/tests.mjs
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store, Tampered } from "./store.mjs";
import { Cell, HARNESS_PATH } from "./cell.mjs";
import { Broker } from "./broker.mjs";
import { userMessage, expectedCount } from "./scenario.mjs";

const dir = mkdtempSync(join(tmpdir(), "heappi-tests-"));
let n = 0;
const ok = (name) => console.log(`ok ${++n} ${name}`);

/** a broker whose model can be told to emit arbitrary scripts */
class ScriptBroker extends Broker {
  constructor() { super(); this.scripts = new Map(); }
  call(agentId, id, name, args) {
    if (name === "model") {
      const last = args.messages.at(-1);
      if (last.role === "user" && this.scripts.has(last.content)) {
        const code = this.scripts.get(last.content);
        const p = Promise.resolve({ content: "", tool_calls: [{ id: "call_x", name: "run", args: { code } }] });
        this.inflight.add(p); p.finally(() => this.inflight.delete(p)); return p;
      }
      if (last.role === "tool" && last.tool_call_id === "call_x") {
        const p = Promise.resolve({ content: "result: " + last.content, tool_calls: [] });
        this.inflight.add(p); p.finally(() => this.inflight.delete(p)); return p;
      }
    }
    return super.call(agentId, id, name, args);
  }
}
async function ask(cell, store, id, text) {
  store.enqueue(id, { type: "user", text });
  const recs = await cell.drain(id);
  return recs.flatMap((r) => r.replies)[0]?.text;
}

// ---- 1. script realm isolation ----
{
  const store = new Store(join(dir, "iso.sqlite"), null);
  const broker = new ScriptBroker();
  const cell = new Cell(store, broker, { checkRebuild: false });
  await cell.create("a");
  broker.scripts.set("globals", `return [typeof __agent, typeof __deliver, typeof __evalIn, typeof __hostCall, typeof tools, typeof mem];`);
  assert.equal(await ask(cell, store, "a", "globals"), 'result: ["undefined","undefined","undefined","function","object","object"]');
  broker.scripts.set("escape", `const tries = [];
    try { tries.push(tools.constructor.constructor("return typeof __agent")()); } catch (e) { tries.push("blocked"); }
    try { const r = await tools.fetch({ source: 1 }); tries.push(r.constructor.constructor("return typeof __agent")()); } catch (e) { tries.push("blocked"); }
    try { tries.push(mem.constructor.constructor("return typeof __deliver")()); } catch (e) { tries.push("blocked"); }
    return tries;`);
  // Function reached through any passed value is the script realm's own: it sees no harness globals
  assert.equal(await ask(cell, store, "a", "escape"), 'result: ["undefined","undefined","blocked"]');
  ok("scripts run in their own realm: harness globals unreachable through any passed value");
  broker.scripts.set("memory", `const r = await tools.fetch({ source: 2 }); const h = remember(r, "s2"); return [h, mem[h].items.length, h in mem];`);
  assert.equal(await ask(cell, store, "a", "memory"), 'result: ["$r1",200,true]');
  ok("working memory crosses the realm as JSON: remember, mem[handle], has");
  broker.scripts.set("nested", `return await tools.run({ code: "return 1" });`);
  assert.match(await ask(cell, store, "a", "nested"), /scripts cannot start scripts/);
  ok("a script cannot start a script");
  store.close();
}

// ---- 2. CPU budget inside a script (a promise job: terminated, not suspended) ----
{
  const store = new Store(join(dir, "budget.sqlite"), null);
  const broker = new ScriptBroker();
  const cell = new Cell(store, broker, { checkRebuild: false });
  await cell.create("b");
  const engine = cell.hot.get("b").engine;
  engine.setBudgetTicks(2000);
  broker.scripts.set("loop", `await tools.fetch({ source: 1 }); let s = 0; for (;;) s++;`);
  const reply = await ask(cell, store, "b", "loop");
  assert.match(reply, /error: .*interrupted/);
  engine.setBudgetTicks(0);
  assert.equal(await ask(cell, store, "b", userMessage(3)), `Source 3 has ${expectedCount(3)} records scoring above 50.`);
  ok("an endless script is terminated at the budget, the model sees an error, the agent continues");
  store.close();
}

// ---- 3. rebuild from the journal at every fold, hash equal; survives a harness upgrade ----
{
  const store = new Store(join(dir, "rebuild.sqlite"), null);
  const broker = new Broker();
  const cell = new Cell(store, broker, { foldMinBytes: 16 * 1024 });
  await cell.create("c");
  for (let t = 1; t <= 6; t++) assert.equal(await ask(cell, store, "c", userMessage(t)), `Source ${t} has ${expectedCount(t)} records scoring above 50.`);
  assert.ok(cell.rebuilds.length >= 2, "at least two folds");
  assert.ok(cell.rebuilds.every((r) => r.ok), "every rebuild matches the live heap");
  ok(`rebuild from the journal matches the live state at ${cell.rebuilds.length} folds (${cell.rebuilds.at(-1).events} events, ${cell.rebuilds.at(-1).ms.toFixed(0)} ms)`);

  // upgrade: a new harness version over the live heap; state and handles survive; later rebuilds replay the upgrade
  const v3 = readFileSync(HARNESS_PATH, "utf8").replace("const HARNESS_VERSION = 2;", "const HARNESS_VERSION = 3;");
  const before = await cell.stats("c");
  const up = await cell.upgradeHarness("c", v3);
  assert.deepEqual([up.from, up.to], [2, 3]);
  const after = await cell.stats("c");
  assert.equal(after.version, 3); assert.equal(after.handles, before.handles); assert.equal(after.turn, before.turn);
  assert.equal(await ask(cell, store, "c", userMessage(7)), `Source 7 has ${expectedCount(7)} records scoring above 50.`);
  for (let t = 8; t <= 12; t++) await ask(cell, store, "c", userMessage(t));
  assert.ok(cell.rebuilds.every((r) => r.ok), "rebuilds after the upgrade still match");
  ok(`harness upgraded in place v2→v3 (${up.checkpointBytes} B checkpoint), handles kept, rebuild still matches`);

  // the hash really detects divergence: a rebuild against a journal missing one event differs
  const live = cell.hot.get("c").engine;
  const rows = store.journal("c");
  const firstResult = rows.find((r) => r.kind === "event" && r.data.type === "result");
  store.db.prepare("DELETE FROM journal WHERE agent = ? AND seq = ?").run("c", firstResult.seq);
  const r = await cell.rebuildCheck("c", live);
  assert.equal(r.ok, false);
  ok("a journal missing an event rebuilds to a different hash (the check has teeth)");
  store.close();
}

// ---- 4. tampered checkpoint is refused ----
{
  const store = new Store(join(dir, "tamper.sqlite"), null);
  const cell = new Cell(store, new Broker(), { checkRebuild: false });
  await cell.create("d");
  await ask(cell, store, "d", userMessage(1));
  store.tamper("d", 2);
  cell.evictHot("d");
  store.enqueue("d", { type: "user", text: userMessage(2) });
  await assert.rejects(cell.activate("d"), Tampered);
  ok("a checkpoint with a bad HMAC is refused before the engine sees it");
  store.close();
}

// ---- 5. mailbox dedup by (sender, seq) ----
{
  const store = new Store(join(dir, "dedup.sqlite"), null);
  const cell = new Cell(store, new Broker(), { checkRebuild: false });
  await cell.create("e");
  assert.equal(store.enqueue("e", { type: "user", text: userMessage(1) }, { sender: "ui", seq: 1 }), true);
  assert.equal(store.enqueue("e", { type: "user", text: userMessage(1) }, { sender: "ui", seq: 1 }), false);
  assert.equal(store.enqueue("e", { type: "user", text: "stale" }, { sender: "ui", seq: 0 }), false);
  assert.equal(store.pendingEvents("e").length, 1);
  await cell.drain("e");
  assert.equal((await cell.stats("e")).turn, 1);
  ok("a redelivered or stale event from the same sender is dropped");
  store.close();
}

// ---- 6. hot-tier eviction when wasm memory bloats; memory limit ----
{
  const store = new Store(join(dir, "evict.sqlite"), null);
  const cell = new Cell(store, new Broker(), { checkRebuild: false, hotMaxLinear: 1 });
  await cell.create("f");
  store.enqueue("f", { type: "user", text: userMessage(1) });
  const recs = await cell.drain("f");
  assert.ok(recs.every((r) => r.evicted), "every activation evicted the hot copy");
  assert.ok(recs.slice(1).every((r) => r.tier === "warm"), "later activations restored warm");
  assert.equal(recs.flatMap((r) => r.replies)[0].text, `Source 1 has ${expectedCount(1)} records scoring above 50.`);
  ok("past the linear-memory threshold the hot copy is dropped and the next activation restores warm");
  const tiny = new Cell(store, new Broker(), { checkRebuild: false, memoryLimit: 1024 * 1024 });
  await tiny.create("g");
  const engine = tiny.hot.get("g").engine;
  assert.throws(() => engine.eval("globalThis.big = new Array(1e6).fill('x'.repeat(100))"), /out of memory/i);
  assert.equal(engine.eval("1+1") && engine.result(), "2");
  ok("the per-agent memory limit turns runaway allocation into a catchable error, heap intact");
  store.close();
}

console.log(`\n${n} checks passed`);
