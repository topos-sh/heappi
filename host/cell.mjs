// The cell: a generic heap host. deliver → run → commit → emit, with the
// hot / warm / cold tiers in front of the store. It never parses agent
// state; it only moves events in, outbox entries out, and bytes around.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { NodeEngine as Engine, SNAPSHOT_BASE, SNAPSHOT_CHAIN } from "./engine-node.mjs";

const HARNESS_SRC = readFileSync(fileURLToPath(new URL("../heap/harness.js", import.meta.url)), "utf8");

export class Cell {
  /**
   * @param store Store
   * @param broker { call(agentId, name, args) → Promise<value> }: the model gateway and tool broker
   * @param opts { foldEvery, hotTtlMs }
   */
  constructor(store, broker, opts = {}) {
    this.store = store;
    this.broker = broker;
    // the writer folds when the deltas since the base outweigh it (or after foldEvery deltas)
    this.foldEvery = opts.foldEvery ?? 64;
    this.foldMinBytes = opts.foldMinBytes ?? 64 * 1024;
    this.hot = new Map(); // agentId → { engine, epoch }
    this.metrics = [];
  }

  /** boot a brand-new agent: engine + harness, first checkpoint is a base */
  async create(agentId) {
    const epoch = this.store.lease(agentId);
    const engine = await Engine.create();
    engine.init();
    engine.eval(HARNESS_SRC, "harness.js");
    const bytes = engine.snapshot(SNAPSHOT_BASE);
    this.store.commit(agentId, epoch, { checkpoint: { kind: "base", bytes }, journal: [{ kind: "created", data: { harness: "v1" } }] });
    this.hot.set(agentId, { engine, epoch });
    return { baseBytes: bytes.length };
  }

  evictHot(agentId) {
    const h = this.hot.get(agentId);
    if (h) { h.engine.drop(); this.hot.delete(agentId); }
  }

  /** load the heap for an activation; returns { engine, tier, restoreMs, chainLength } */
  async #load(agentId, epoch, forceTier) {
    const t0 = performance.now();
    const h = this.hot.get(agentId);
    if (h && forceTier !== "warm" && forceTier !== "cold") {
      h.epoch = epoch;
      return { engine: h.engine, tier: "hot", restoreMs: performance.now() - t0, chainLength: 0 };
    }
    if (h) this.evictHot(agentId);
    const chain = forceTier === "cold" ? this.store.coldChain(agentId) : this.store.chain(agentId);
    const tRead = performance.now();
    const engine = await Engine.create();
    const tInst = performance.now();
    engine.restore(chain.base, chain.deltas);
    const now = performance.now();
    this.hot.set(agentId, { engine, epoch });
    return { engine, tier: forceTier === "cold" ? "cold" : "warm", restoreMs: now - t0, readMs: tRead - t0, instantiateMs: tInst - tRead, applyMs: now - tInst, chainLength: chain.length, chainBytes: chain.base.length + chain.deltas.reduce((s, d) => s + d.length, 0) };
  }

  /**
   * One activation: drain the mailbox into the heap, run to quiescence,
   * commit one checkpoint, emit the outbox. Returns the activation record.
   */
  async activate(agentId, { forceTier } = {}) {
    const events = this.store.pendingEvents(agentId);
    if (events.length === 0) return null;
    const epoch = this.store.lease(agentId);
    const loaded = await this.#load(agentId, epoch, forceTier);
    const { engine } = loaded;

    const tRun = performance.now();
    const journal = [];
    for (const { event } of events) {
      journal.push({ kind: "event", data: event.type === "result" ? { ...event, value: undefined, bytes: JSON.stringify(event.value ?? "").length } : event });
      engine.call("__deliver", JSON.stringify(event));
    }
    const out = JSON.parse(engine.call("__drain").result ?? "[]");
    const runMs = performance.now() - tRun;

    // calls go to the journal (messages interned once) and to the outbox
    const outbox = [];
    const replies = [];
    for (const o of out) {
      if (o.kind === "call") {
        const data = o.name === "model" ? { id: o.id, name: o.name, messages: this.store.internMessages(o.args.messages), tools: o.args.tools } : { id: o.id, name: o.name, args: o.args };
        journal.push({ kind: "call", data });
        outbox.push(o);
      } else if (o.kind === "reply") replies.push(o);
      else journal.push({ kind: o.kind, data: o });
    }

    // the writer folds: a full base once the deltas outweigh it
    const shape = this.store.chainShape(agentId);
    const kind = shape.deltas + 1 >= this.foldEvery || shape.deltaBytes >= Math.max(shape.baseBytes, this.foldMinBytes) ? "base" : "chain";
    const tSnap = performance.now();
    const bytes = engine.snapshot(kind === "base" ? SNAPSHOT_BASE : SNAPSHOT_CHAIN);
    const snapshotMs = performance.now() - tSnap;

    const tCommit = performance.now();
    this.store.commit(agentId, epoch, { checkpoint: { kind, bytes }, journal, outbox, processed: events.map((e) => e.seq) });
    const commitMs = performance.now() - tCommit;

    // emit after commit: the broker answers by enqueueing a result event
    for (const o of this.store.unsent(agentId)) {
      this.broker.call(agentId, o.id, o.name, o.args)
        .then((value) => this.store.enqueue(agentId, { type: "result", id: o.id, value }),
              (err) => this.store.enqueue(agentId, { type: "result", id: o.id, error: String(err?.message ?? err) }));
      this.store.markSent(agentId, o.id);
    }

    const mem = engine.mem();
    const rec = { agentId, events: events.length, tier: loaded.tier, restoreMs: loaded.restoreMs, readMs: loaded.readMs, instantiateMs: loaded.instantiateMs, applyMs: loaded.applyMs, chainLength: loaded.chainLength, chainBytes: loaded.chainBytes, runMs, kind, checkpointBytes: bytes.length, snapshotMs, commitMs, calls: outbox.length, replies, heapUsed: mem.memory_used, objects: mem.obj_count, linearBytes: engine.linearBytes };
    this.metrics.push(rec);
    return rec;
  }

  /** run activations until the mailbox is empty and nothing is in flight */
  async drain(agentId, opts = {}) {
    const recs = [];
    for (;;) {
      await this.broker.settle?.();
      const r = await this.activate(agentId, opts);
      if (!r) break;
      recs.push(r);
    }
    return recs;
  }

  stats(agentId) { return JSON.parse(this.hot.get(agentId).engine.call("__stats").result); }
  peek(agentId, handle) { return JSON.parse(this.hot.get(agentId).engine.call("__peek", handle).result); }
}
