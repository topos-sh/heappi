// The real Cell with Pi Durable as the harness: the research scenario,
// forced warm and cold restores, a fold with its rebuild check.
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../host/store.mjs";
import { Cell } from "../host/cell.mjs";
import { Broker } from "../host/broker.mjs";
import { userMessage, expectedCount } from "../host/scenario.mjs";

const turns = Number(process.argv[2] ?? 6);
const dir = mkdtempSync(join(tmpdir(), "pi-cell-"));
const store = new Store(join(dir, "cells.sqlite"), join(dir, "cold"));
const cell = new Cell(store, new Broker(), { harnessSrc: readFileSync(new URL("./pi-harness.js", import.meta.url), "utf8"), foldMinBytes: 64 * 1024, foldEvery: Number(process.argv[3] ?? 64), gcBeforeSnapshot: process.argv.includes("--gc") });
const id = "pi-1";
const t0 = performance.now();
const created = await cell.create(id);
console.log(`created from layers in ${created.restoreMs.toFixed(1)} ms: engine ${created.engine} B, harness ${created.harness} B`);
let correct = 0;
for (let t = 1; t <= turns; t++) {
  const tier = t === 3 ? "warm" : t === 5 ? "cold" : undefined;
  if (tier) cell.evictHot(id);
  store.enqueue(id, { type: "user", text: userMessage(t) });
  const recs = await cell.drain(id, tier ? { forceTier: tier } : {});
  const reply = recs.flatMap((r) => r.replies)[0]?.text ?? "";
  const ok = reply.includes(`has ${expectedCount(t)} records`); if (ok) correct++;
  const errs = recs.flatMap((r) => r.replies.length ? [] : []);
  console.log(`turn ${t} (${recs[0].tier}): ${ok ? "ok " : "WRONG " + reply.slice(0, 120)} ${recs.length} acts, first load ${recs[0].restoreMs.toFixed(1)} ms, checkpoints ${recs.map((r) => r.kind[0] + r.checkpointBytes).join("/")} B, run ${recs.reduce((s, r) => s + r.runMs, 0).toFixed(1)} ms${recs.some((r) => r.rebuild) ? ", rebuild " + (recs.find((r) => r.rebuild).rebuild.ok ? "ok" : "MISMATCH") : ""}`);
}
const j = store.journal(id);
console.log(`${correct}/${turns} correct in ${(performance.now() - t0).toFixed(0)} ms; journal rows ${j.length}: ${Object.entries(j.reduce((m, r) => (m[r.kind] = (m[r.kind] ?? 0) + 1, m), {})).map(([k, n]) => k + " " + n).join(", ")}`);
console.log("stats", await cell.stats(id), "sizes", store.sizes(id));
const last = j.filter((r) => r.kind === "pi.entry").slice(-2).map((r) => r.data.entry + " " + JSON.stringify(r.data.model?.[0]?.content ?? "").slice(0, 100));
console.log("last mirrored Pi entries:", last);
store.close();
