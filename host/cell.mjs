// The cell: a generic heap host. deliver → run → commit → emit, with the
// hot / warm / cold tiers in front of the store. It never parses agent
// state; it only moves events in, outbox entries out, and bytes around.
//
// Also here: the layers (engine, harness) shared by all agents, the fold
// (by bytes, or when the last restore was slow), the rebuild from the
// journal with the hash check at every fold, the harness upgrade in place,
// and the hot-tier eviction when the wasm memory bloats.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { NodeEngine as Engine, SNAPSHOT_BASE, SNAPSHOT_CHAIN } from "./engine-node.mjs";

export const HARNESS_PATH = fileURLToPath(new URL("../heap/harness.js", import.meta.url));

export class Cell {
  /**
   * @param store Store
   * @param broker { call(agentId, id, name, args) → Promise<value>, settle?() }
   * @param opts { foldEvery, foldMinBytes, foldMaxApplyMs, hotMaxLinear, memoryLimit, harnessSrc, checkRebuild }
   */
  constructor(store, broker, opts = {}) {
    this.store = store;
    this.broker = broker;
    this.foldEvery = opts.foldEvery ?? 64;
    this.foldMinBytes = opts.foldMinBytes ?? 64 * 1024;
    this.foldMaxApplyMs = opts.foldMaxApplyMs ?? 25;   // a slow restore triggers the next fold
    this.hotMaxLinear = opts.hotMaxLinear ?? 32 * 1024 * 1024; // evict the hot copy past this
    this.memoryLimit = opts.memoryLimit ?? 64 * 1024 * 1024;
    this.harnessSrc = opts.harnessSrc ?? readFileSync(HARNESS_PATH, "utf8");
    this.checkRebuild = opts.checkRebuild ?? true;
    this.hot = new Map();      // agentId → { engine, epoch, lastApplyMs }
    this.layers = null;        // [engineHash, harnessHash] for the current harness
    this.metrics = [];
    this.rebuilds = [];
  }

  // ---- layers: engine (natives only) and harness, built once, stored once ----
  async #ensureLayers() {
    if (this.layers) return this.layers;
    const engine = await Engine.create();
    engine.init();
    const engineBytes = engine.snapshot(SNAPSHOT_BASE);
    engine.eval(this.harnessSrc, "harness.js");
    const harnessBytes = engine.snapshot(SNAPSHOT_CHAIN);
    engine.drop();
    this.layers = [this.store.putLayer("engine", engineBytes), this.store.putLayer("harness", harnessBytes)];
    this.layerBytes = { engine: engineBytes.length, harness: harnessBytes.length };
    return this.layers;
  }

  /** a new agent starts from the shared layers: no eval, just a restore (golden image) */
  async create(agentId) {
    const layers = await this.#ensureLayers();
    this.store.create(agentId, layers);
    const epoch = this.store.lease(agentId);
    const engine = await this.#fresh();
    const t0 = performance.now();
    engine.restore(this.store.getLayer(layers[0]), [this.store.getLayer(layers[1])]);
    this.hot.set(agentId, { engine, epoch, lastApplyMs: 0 });
    return { layers, ...this.layerBytes, restoreMs: performance.now() - t0 };
  }

  async #fresh() { const e = await Engine.create(); e.setMemoryLimit(this.memoryLimit); return e; }

  evictHot(agentId) {
    const h = this.hot.get(agentId);
    if (h) { h.engine.drop(); this.hot.delete(agentId); }
  }

  async #load(agentId, epoch, forceTier) {
    const t0 = performance.now();
    const h = this.hot.get(agentId);
    if (h && forceTier !== "warm" && forceTier !== "cold") { h.epoch = epoch; return { engine: h.engine, tier: "hot", restoreMs: performance.now() - t0, chainLength: 0 }; }
    if (h) this.evictHot(agentId);
    const chain = forceTier === "cold" ? this.store.coldChain(agentId) : this.store.chain(agentId);
    const tRead = performance.now();
    const engine = await this.#fresh();
    const tInst = performance.now();
    engine.restore(chain.base, chain.deltas);
    const now = performance.now();
    this.hot.set(agentId, { engine, epoch, lastApplyMs: now - tInst });
    return { engine, tier: forceTier === "cold" ? "cold" : "warm", restoreMs: now - t0, readMs: tRead - t0, instantiateMs: tInst - tRead, applyMs: now - tInst, chainLength: chain.length, chainBytes: chain.base.length + chain.deltas.reduce((s, d) => s + d.length, 0) };
  }

  /** run the heap over the events and collect the outbox */
  #runEvents(engine, events) {
    const journal = [];
    for (const ev of events) {
      if (ev.type === "result") journal.push({ kind: "event", data: { type: "result", id: ev.id, error: ev.error, hash: ev.error ? undefined : this.store.intern(ev.value ?? null) } });
      else journal.push({ kind: "event", data: ev });
      engine.call("__deliver", JSON.stringify(ev));
    }
    const out = JSON.parse(engine.call("__drain").result ?? "[]");
    return { journal, out };
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
    const { journal, out } = this.#runEvents(engine, events.map((e) => e.event));
    const runMs = performance.now() - tRun;

    const outbox = [], replies = [];
    for (const o of out) {
      if (o.kind === "call") {
        const data = o.name === "model" ? { id: o.id, name: o.name, messages: o.args.messages.map((m) => this.store.intern(m)), tools: o.args.tools } : { id: o.id, name: o.name, args: o.args };
        journal.push({ kind: "call", data }); outbox.push(o);
      } else if (o.kind === "reply") replies.push(o);
      else journal.push({ kind: o.kind, data: o });
    }

    // the writer folds: when the deltas outweigh the base, after foldEvery deltas, or when the last restore was slow
    const shape = this.store.chainShape(agentId);
    const slow = (this.hot.get(agentId)?.lastApplyMs ?? 0) > this.foldMaxApplyMs;
    const kind = shape.deltas + 1 >= this.foldEvery || shape.deltaBytes >= Math.max(shape.baseBytes, this.foldMinBytes) || slow ? "base" : "chain";
    const tSnap = performance.now();
    const bytes = engine.snapshot(kind === "base" ? SNAPSHOT_BASE : SNAPSHOT_CHAIN);
    const snapshotMs = performance.now() - tSnap;
    const mem = engine.mem();

    // at a fold, check that the journal rebuilds this state (the harness is a fold over the journal)
    let rebuild;
    if (kind === "base" && this.checkRebuild) rebuild = await this.rebuildCheck(agentId, engine, journal);

    const metrics = { runMs, snapshotMs, objects: mem.obj_count, heapUsed: mem.memory_used, linearBytes: engine.linearBytes, events: events.length, calls: outbox.length, chainLength: loaded.chainLength ?? shape.deltas, applyMs: loaded.applyMs, wait: outbox.length ? outbox[0].name : "reply", foldReason: kind === "base" ? (slow ? "slow-restore" : "bytes") : undefined };
    if (rebuild) journal.push({ kind: "rebuild_check", data: rebuild });
    const tCommit = performance.now();
    this.store.commit(agentId, epoch, { checkpoint: { kind, bytes, metrics }, journal, outbox, processed: events.map((e) => e.seq) });
    const commitMs = performance.now() - tCommit;

    for (const o of this.store.unsent(agentId)) {
      this.broker.call(agentId, o.id, o.name, o.args)
        .then((value) => this.store.enqueue(agentId, { type: "result", id: o.id, value }, { sender: "broker" }),
              (err) => this.store.enqueue(agentId, { type: "result", id: o.id, error: String(err?.message ?? err) }, { sender: "broker" }));
      this.store.markSent(agentId, o.id);
    }
    if (engine.linearBytes > this.hotMaxLinear) this.evictHot(agentId); // bloat: the next activation restores warm

    const rec = { agentId, events: events.length, tier: loaded.tier, restoreMs: loaded.restoreMs, readMs: loaded.readMs, instantiateMs: loaded.instantiateMs, applyMs: loaded.applyMs, chainLength: loaded.chainLength, chainBytes: loaded.chainBytes, runMs, kind, checkpointBytes: bytes.length, snapshotMs, commitMs, calls: outbox.length, replies, heapUsed: mem.memory_used, objects: mem.obj_count, linearBytes: engine.linearBytes, rebuild, evicted: !this.hot.has(agentId) };
    this.metrics.push(rec);
    return rec;
  }

  /**
   * Rebuild the agent from its journal in a scratch heap: layers, then every
   * event in order (results come from the interned bodies). Compare the
   * canonical state hash with the live heap. A mismatch is recorded, never
   * thrown: it is a bug report about the harness.
   */
  async rebuildCheck(agentId, liveEngine, pendingJournal = []) {
    const t0 = performance.now();
    const [engineHash, harnessHash] = this.store.layersOf(agentId);
    const scratch = await this.#fresh();
    scratch.restore(this.store.getLayer(engineHash), [this.store.getLayer(harnessHash)]);
    const rows = [...this.store.journal(agentId), ...pendingJournal.map((j) => ({ kind: j.kind, data: j.data }))];
    let events = 0;
    for (const j of rows) {
      if (j.kind === "upgrade") { scratch.eval(j.data.src, "harness.js"); scratch.call("__migrate"); continue; }
      if (j.kind !== "event") continue;
      const ev = j.data.type === "result" ? { type: "result", id: j.data.id, error: j.data.error, value: j.data.error ? undefined : this.store.body(j.data.hash) } : j.data;
      scratch.call("__deliver", JSON.stringify(ev)); scratch.call("__drain"); events++;
    }
    const rebuilt = scratch.call("__stateHash").result, live = liveEngine.call("__stateHash").result;
    scratch.drop();
    const r = { ok: rebuilt === live, live, rebuilt, events, ms: performance.now() - t0 };
    this.rebuilds.push(r);
    return r;
  }

  /** harness upgrade in place: eval the new source over the live heap, migrate, checkpoint */
  async upgradeHarness(agentId, src) {
    const epoch = this.store.lease(agentId);
    const { engine } = await this.#load(agentId, epoch);
    engine.eval(src, "harness.js");
    const migrated = JSON.parse(engine.call("__migrate").result ?? "{}");
    this.harnessSrc = src;
    const bytes = engine.snapshot(SNAPSHOT_CHAIN);
    this.store.commit(agentId, epoch, { checkpoint: { kind: "chain", bytes, metrics: { wait: "upgrade" } }, journal: [{ kind: "upgrade", data: { ...migrated, src } }] });
    return { ...migrated, checkpointBytes: bytes.length };
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

  async #hotEngine(agentId) { const h = this.hot.get(agentId); if (h) return h.engine; return (await this.#load(agentId, this.store.agent(agentId).epoch)).engine; }
  async stats(agentId) { return JSON.parse((await this.#hotEngine(agentId)).call("__stats").result); }
  async peek(agentId, handle) { return JSON.parse((await this.#hotEngine(agentId)).call("__peek", handle).result); }
  async stateHash(agentId) { return (await this.#hotEngine(agentId)).call("__stateHash").result; }
}
