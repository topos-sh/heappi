// One agent, a few turns, every activation printed. Evicts the hot copy
// between turns to show warm and cold restores.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "./store.mjs";
import { Cell } from "./cell.mjs";
import { Broker } from "./broker.mjs";
import { userMessage, expectedCount } from "./scenario.mjs";

const dir = mkdtempSync(join(tmpdir(), "agent-image-"));
const store = new Store(join(dir, "cells.sqlite"), join(dir, "cold"));
const broker = new Broker();
const cell = new Cell(store, broker, { foldMinBytes: 16 * 1024 });
const id = "agent-1";
console.log("created", await cell.create(id));

const turns = Number(process.argv[2] ?? 3);
for (let t = 1; t <= turns; t++) {
  if (t === 2) cell.evictHot(id);
  store.enqueue(id, { type: "user", text: userMessage(t) });
  const recs = await cell.drain(id, t === 3 ? { forceTier: "cold" } : {});
  for (const r of recs) console.log(`  act tier=${r.tier.padEnd(4)} restore=${r.restoreMs.toFixed(2)}ms run=${r.runMs.toFixed(2)}ms ${r.kind} ${r.checkpointBytes}B snap=${r.snapshotMs.toFixed(2)}ms commit=${r.commitMs.toFixed(2)}ms calls=${r.calls} heap=${(r.heapUsed / 1024).toFixed(0)}KB` + (r.replies.length ? `  reply: ${r.replies[0].text}` : ""));
  const reply = recs.flatMap((r) => r.replies)[0];
  const ok = reply && reply.text.includes(`has ${expectedCount(t)} records`);
  console.log(`turn ${t}: ${ok ? "correct" : "WRONG " + JSON.stringify(reply)}`, await cell.stats(id));
}
console.log("sizes", store.sizes(id), "cold bytes", store.coldBytes(id), "bodies", store.bodyStats(), "rebuild checks", cell.rebuilds);
console.log("handle $r1 head:", JSON.stringify(await cell.peek(id, "$r1")).slice(0, 120));
console.log("broker", { modelCalls: broker.modelCalls, cacheHits: broker.cacheHits, toolCalls: broker.toolCalls });
store.close();
