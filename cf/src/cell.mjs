// The cell as a Durable Object, harness agnostic: one object per agent.
//   hot   the engine instance kept in the object's memory between requests
//   warm  the object's SQLite (layers, checkpoints, journal, head)
//   cold  R2: the layers once by hash, the agent's fold and checkpoints, and
//         a head object written with a conditional put (lease and fence)
// Layers (engine, harness) are built once per isolate from the harness
// source, and an idle template that restored them is cloned per restore:
// the clone applies only the agent's own checkpoints (the live apply of the
// engine), with a full restore as the fallback. Folds are relative to the
// harness layer, so the layer stays shared after a fold.
import { DurableObject } from "cloudflare:workers";
import engineModule from "./heappi.wasm";
import { Engine, SNAPSHOT_BASE, SNAPSHOT_CHAIN, SNAPSHOT_FOLD } from "../../host/engine.mjs";

const log = (s) => console.log("[heap]", s);
export const newEngine = () => Engine.fromModule(engineModule, log);
async function sha(bytes) { const h = await crypto.subtle.digest("SHA-256", bytes); return [...new Uint8Array(h)].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 32); }
export const fnv = (s) => {
  let h = 0xcbf29ce4, g = 0x84222325;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; g ^= s.charCodeAt(i) >> 3; g = Math.imul(g, 0x01000193) >>> 0; }
  return h.toString(16).padStart(8, "0") + g.toString(16).padStart(8, "0");
};

// ---- layers and templates ----
// The layers are canonical per harness source: keyed by its hash, stored
// once in R2, and every isolate loads the same bytes, so a template built
// here matches the checkpoints any isolate wrote. Built and put only when
// missing (or when the stored layers do not load in this engine build).
const enc = new TextEncoder();
const layerCache = new Map(); // key → Promise<layers>
export async function layersFor(harnessSrc, bucket) {
  const key = await sha(enc.encode(harnessSrc));
  let p = layerCache.get(key);
  if (!p) { p = loadOrBuildLayers(key, harnessSrc, bucket).catch((e) => { layerCache.delete(key); throw e; }); layerCache.set(key, p); }
  return p;
}
export const layerKeys = (key) => ({ engine: `layers/${key}-engine.bin`, harness: `layers/${key}-harness.bin` });
async function readLayers(key, bucket) {
  const k = layerKeys(key);
  const [eo, ho] = await Promise.all([bucket.get(k.engine), bucket.get(k.harness)]);
  if (!eo || !ho) return null;
  return { key, engineBytes: new Uint8Array(await eo.arrayBuffer()), harnessBytes: new Uint8Array(await ho.arrayBuffer()) };
}
async function loadOrBuildLayers(key, harnessSrc, bucket) {
  let layers = await readLayers(key, bucket);
  if (layers) {
    try { templateFor(layers); return { ...layers, source: "r2" }; }
    catch (e) { console.log("stored layers do not load in this engine build, rebuilding:", e.message); templates.delete(key); }
  }
  const e = newEngine(); e.init();
  const engineBytes = e.snapshot(SNAPSHOT_BASE);
  e.eval(harnessSrc, "harness.js");
  const harnessBytes = e.snapshot(SNAPSHOT_CHAIN);
  e.drop();
  const k = layerKeys(key);
  const mustNotExist = { onlyIf: { uploadedBefore: new Date(0) } };
  await Promise.all([bucket.put(k.engine, engineBytes, mustNotExist), bucket.put(k.harness, harnessBytes, mustNotExist)]);
  layers = await readLayers(key, bucket); // adopt whatever landed, ours or a concurrent isolate's
  templateFor(layers);
  return { ...layers, source: "built" };
}
const templates = new Map(); // layer key → an idle instance holding the layers
export function templateFor(layers) {
  let t = templates.get(layers.key);
  if (!t) { t = newEngine(); t.restore(layers.engineBytes, [layers.harnessBytes], { anchorAfter: 0 }); templates.set(layers.key, t); }
  return t;
}

/** a harness that schedules with timers defines __pump; run it until nothing fires */
export function quiesce(engine) {
  if (engine.hasPump === undefined) engine.hasPump = (engine.eval("typeof __pump") && engine.result()) === "function";
  if (!engine.hasPump) return;
  for (let i = 0; i < 10000; i++) { const n = Number(engine.call("__pump").result ?? 0); engine.run(); if (!n) return; }
}

const ENGINE_SEQ = -2, HARNESS_SEQ = -1; // the layers sit below the agent's checkpoints

export class CellBase extends DurableObject {
  constructor(ctx, env, { harnessSrc, prefix = "agents" }) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.harnessSrc = harnessSrc;
    this.prefix = prefix;
    this.ensureSchema();
    this.engine = null; // the hot tier
    this.lastApplyMs = 0;
    this.CHUNK = 1024 * 1024; // under the 2 MB blob limit of DO SQLite
    this.FOLD_MIN = 64 * 1024;
    this.FOLD_EVERY = 64;
    this.FOLD_SLOW_MS = 25;
    this.HOT_MAX_LINEAR = 32 * 1024 * 1024;
    this.CADENCE_MS = 500; // replication batches at most twice a second
    this.syncReplication = false;
  }
  ensureSchema() {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS head (k INTEGER PRIMARY KEY CHECK (k = 1), epoch INTEGER, head_seq INTEGER, base_seq INTEGER, call_seq INTEGER, layers TEXT);
      CREATE TABLE IF NOT EXISTS checkpoints (seq INTEGER PRIMARY KEY, kind TEXT, size INTEGER, chunks INTEGER);
      CREATE TABLE IF NOT EXISTS blobs (seq INTEGER, part INTEGER, bytes BLOB, PRIMARY KEY (seq, part));
      CREATE TABLE IF NOT EXISTS journal (seq INTEGER PRIMARY KEY, kind TEXT, data TEXT);
      CREATE TABLE IF NOT EXISTS messages (hash TEXT PRIMARY KEY, body TEXT);
      INSERT OR IGNORE INTO head (k, epoch, head_seq, base_seq, call_seq, layers) VALUES (1, 0, 0, 0, 0, NULL);
    `);
  }

  get agentId() { return this.ctx.id.name ?? String(this.ctx.id); }
  head() { return this.sql.exec("SELECT * FROM head WHERE k = 1").one(); }
  layersOf() { const l = this.head().layers; return l ? JSON.parse(l) : null; }
  exists() { return !!this.head().layers; }

  /** bytes of the latest fold (0 if none) and of the checkpoints since it */
  shape() {
    const h = this.head();
    const rows = this.sql.exec("SELECT seq, size FROM checkpoints WHERE seq >= ? ORDER BY seq", Math.max(h.base_seq, 1)).toArray();
    const fold = h.base_seq ? rows[0]?.size ?? 0 : 0;
    const live = rows.reduce((s, r) => s + r.size, 0);
    const layers = this.sql.exec("SELECT COALESCE(SUM(size),0) AS n FROM checkpoints WHERE seq < 1").one().n;
    return { foldBytes: fold, deltaBytes: live - fold, deltas: rows.length - (h.base_seq ? 1 : 0), liveBytes: live, layersBytes: layers, headSeq: h.head_seq, baseSeq: h.base_seq };
  }
  info() {
    const h = this.head();
    const s = this.shape();
    return { agent: this.agentId, epoch: h.epoch, headSeq: h.head_seq, baseSeq: h.base_seq, layers: this.layersOf(), hot: !!this.engine, repBatches: this.repBatches ?? 0, repItems: this.repItems ?? 0, liveChainBytes: s.liveBytes, deltas: s.deltas, baseBytes: s.foldBytes, layersBytes: s.layersBytes, journalRows: this.sql.exec("SELECT COUNT(*) AS n FROM journal").one().n, dbBytes: this.ctx.storage.sql.databaseSize, linearBytes: this.engine?.linearBytes ?? 0 };
  }

  async reset() {
    this.engine?.drop(); this.engine = null;
    await this.ctx.storage.deleteAlarm?.();
    await this.ctx.storage.deleteAll();
    this.ensureSchema();
    const list = await this.env.COLD.list({ prefix: `${this.prefix}/${this.agentId}/` });
    await Promise.all(list.objects.map((o) => this.env.COLD.delete(o.key)));
    return { reset: true };
  }

  // ---- storage ----
  putCheckpoint(seq, kind, bytes) {
    const chunks = Math.max(1, Math.ceil(bytes.length / this.CHUNK));
    this.sql.exec("INSERT INTO checkpoints (seq, kind, size, chunks) VALUES (?, ?, ?, ?)", seq, kind, bytes.length, chunks);
    for (let i = 0; i < chunks; i++) this.sql.exec("INSERT INTO blobs (seq, part, bytes) VALUES (?, ?, ?)", seq, i, bytes.subarray(i * this.CHUNK, (i + 1) * this.CHUNK));
  }
  getCheckpoint(seq) {
    const parts = this.sql.exec("SELECT bytes FROM blobs WHERE seq = ? ORDER BY part", seq).toArray().map((r) => new Uint8Array(r.bytes));
    if (parts.length === 1) return parts[0];
    const out = new Uint8Array(parts.reduce((s, p) => s + p.length, 0));
    let o = 0; for (const p of parts) { out.set(p, o); o += p.length; }
    return out;
  }
  /** engine layer as the base, then the harness layer (the anchor), the latest fold, the checkpoints after it */
  warmChain() {
    const h = this.head();
    if (!h.layers) return null;
    const rows = this.sql.exec("SELECT seq FROM checkpoints WHERE seq >= ? ORDER BY seq", Math.max(h.base_seq, 1)).toArray();
    const deltas = [this.getCheckpoint(HARNESS_SEQ), ...rows.map((r) => this.getCheckpoint(r.seq))];
    return { base: this.getCheckpoint(ENGINE_SEQ), deltas, anchorAfter: 0, length: rows.length };
  }
  async coldChain() {
    await this.pendingReplication; // a cold load after our own writes waits for them to land
    const headObj = await this.env.COLD.get(`${this.prefix}/${this.agentId}/head`);
    if (!headObj) return null;
    const head = await headObj.json();
    const objs = await Promise.all(head.keys.map((k) => this.env.COLD.get(k).then((o) => { if (!o) throw new Error("missing " + k); return o.arrayBuffer(); })));
    return { base: new Uint8Array(objs[0]), deltas: objs.slice(1).map((b) => new Uint8Array(b)), anchorAfter: 0, length: objs.length - 2, headEtag: headObj.etag };
  }
  /**
   * Replicate commits to R2: every pending checkpoint in one batch (blobs in
   * parallel, one conditional put of the head: the fence). Inline for
   * `rep=sync`, else queued and drained after the response on a cadence.
   */
  async replicate(seq, kind, bytes, epoch) {
    this.repQueue ??= [];
    this.repQueue.push({ seq, kind, bytes, epoch });
    const t0 = Date.now();
    if (this.syncReplication) { await this.drainReplication(); return Date.now() - t0; }
    this.repTimer ??= new Promise((res) => setTimeout(res, this.CADENCE_MS)).then(() => { this.repTimer = null; return this.drainReplication(); });
    return this.repTimer;
  }
  async drainReplication() {
    if (this.repDraining) return this.repDraining;
    this.repDraining = (async () => {
      try { while (this.repQueue.length) await this.replicateBatch(this.repQueue.splice(0)); }
      finally { this.repDraining = null; }
    })();
    return this.repDraining;
  }
  async replicateBatch(batch) {
    const prefix = `${this.prefix}/${this.agentId}/`;
    const epoch = batch.at(-1).epoch;
    const keyOf = (c) => `${prefix}${String(c.seq).padStart(6, "0")}-${c.kind}.bin`;
    await Promise.all(batch.map((c) => this.env.COLD.put(keyOf(c), c.bytes)));
    const cur = await this.env.COLD.get(`${prefix}head`);
    if (!cur) throw new Error("no head object");
    const head = await cur.json();
    if (head.epoch > epoch) throw new Error(`fenced: head epoch ${head.epoch} > ${epoch}`);
    let keys = [...head.keys];
    const swept = [];
    for (const c of batch) { if (c.kind === "fold") { swept.push(...keys.slice(2)); keys = keys.slice(0, 2); } keys.push(keyOf(c)); } // a fold replaces everything after the layers
    const put = await this.env.COLD.put(`${prefix}head`, JSON.stringify({ epoch, headSeq: batch.at(-1).seq, keys }), { onlyIf: { etagMatches: cur.etag } });
    if (!put) throw new Error("fenced: head changed under us");
    if (swept.length) await Promise.all(swept.map((k) => this.env.COLD.delete(k))); // the sweeper
    this.repBatches = (this.repBatches ?? 0) + 1; this.repItems = (this.repItems ?? 0) + batch.length;
  }

  // ---- the cell ----
  /** a new agent: the layers into SQLite and R2 (once by hash), a clone of the template as the hot copy */
  async create() {
    await this.reset();
    const layers = await layersFor(this.harnessSrc, this.env.COLD);
    const epoch = 1;
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("UPDATE head SET epoch = ?, head_seq = 0, base_seq = 0, layers = ? WHERE k = 1", epoch, JSON.stringify({ key: layers.key, source: layers.source }));
      this.putCheckpoint(ENGINE_SEQ, "layer", layers.engineBytes);
      this.putCheckpoint(HARNESS_SEQ, "layer", layers.harnessBytes);
    });
    const t0 = Date.now();
    const k = layerKeys(layers.key);
    await this.env.COLD.put(`${this.prefix}/${this.agentId}/head`, JSON.stringify({ epoch, headSeq: 0, keys: [k.engine, k.harness] }));
    const r2Ms = Date.now() - t0;
    this.engine = templateFor(layers).clone();
    this.lastApplyMs = 0;
    return { layers: this.layersOf(), engineBytes: layers.engineBytes.length, harnessBytes: layers.harnessBytes.length, baseBytes: layers.harnessBytes.length, r2Ms };
  }

  async load(tier) {
    const t0 = Date.now();
    if (this.engine && tier === "hot") return { tier: "hot", how: "hot", restoreMs: 0, chainLength: 0 };
    this.engine?.drop(); this.engine = null;
    const chain = tier === "cold" ? await this.coldChain() : this.warmChain();
    if (!chain) throw new Error(`no agent ${this.agentId}`);
    const tRead = Date.now();
    let engine = null, how = "full", tInst;
    const mine = this.layersOf();
    const layers = await layersFor(this.harnessSrc, this.env.COLD);
    if (mine && mine.key === layers.key) {
      engine = templateFor(layers).clone();
      tInst = Date.now();
      try { engine.apply(chain.deltas.slice(1)); how = "template"; }
      catch (e) { console.log("live apply failed, restoring fully:", e.message); engine.drop(); engine = null; }
    }
    if (!engine) { engine = newEngine(); tInst = Date.now(); engine.restore(chain.base, chain.deltas, { anchorAfter: 0 }); }
    const tApply = Date.now();
    this.engine = engine;
    this.lastApplyMs = tApply - tInst;
    return { tier: tier === "cold" ? "cold" : "warm", how, restoreMs: tApply - t0, readMs: tRead - t0, instantiateMs: tInst - tRead, applyMs: tApply - tInst, chainLength: chain.length, chainBytes: chain.base.length + chain.deltas.reduce((s, d) => s + d.length, 0) };
  }

  intern(value) {
    const body = JSON.stringify(value);
    const hash = fnv(body);
    this.sql.exec("INSERT OR IGNORE INTO messages (hash, body) VALUES (?, ?)", hash, body);
    return hash;
  }

  /**
   * One activation: deliver events, run to quiescence, commit one checkpoint
   * (a fold relative to the harness layer when due), replicate. `inCommit`
   * runs inside the transaction for the subclass's own rows.
   */
  async activate(events, { tier = "hot", inCommit } = {}) {
    const loaded = await this.load(tier);
    const engine = this.engine;
    const h = this.head();
    const epoch = h.epoch;
    const journal = [];
    const tRun = Date.now();
    for (const ev of events) {
      journal.push({ kind: "event", data: ev.type === "result" ? { type: "result", id: ev.id, error: ev.error, hash: ev.error ? undefined : this.intern(ev.value ?? null) } : ev });
      engine.call("__deliver", JSON.stringify(ev));
      quiesce(engine);
    }
    const out = JSON.parse(engine.call("__drain").result ?? "[]");
    const runMs = Date.now() - tRun;
    const calls = [], replies = [];
    for (const o of out) {
      if (o.kind === "call") {
        const msgs = o.args.messages ?? o.args.context?.messages ?? [];
        const data = o.name === "model"
          ? { id: o.id, name: o.name, messages: msgs.map((m) => this.intern(m)), tools: o.args.context?.tools?.map((t) => t.name), systemPrompt: o.args.context?.systemPrompt ? this.intern(o.args.context.systemPrompt) : undefined }
          : { id: o.id, name: o.name, args: o.args };
        journal.push({ kind: "call", data }); calls.push(o);
      } else if (o.kind === "reply") replies.push(o);
      else journal.push({ kind: o.kind, data: o });
    }
    // the writer folds relative to the harness layer
    const s = this.shape();
    const slow = this.lastApplyMs > this.FOLD_SLOW_MS;
    const kind = s.deltas + 1 >= this.FOLD_EVERY || s.deltaBytes >= Math.max(s.foldBytes, this.FOLD_MIN) || slow ? "fold" : "chain";
    const tSnap = Date.now();
    const bytes = engine.snapshot(kind === "fold" ? SNAPSHOT_FOLD : SNAPSHOT_CHAIN);
    const tCommit = Date.now();
    const seq = h.head_seq + 1;
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("UPDATE head SET head_seq = ?, base_seq = ?, call_seq = call_seq + ? WHERE k = 1 AND epoch = ?", seq, kind === "fold" ? seq : h.base_seq, calls.length, epoch);
      this.putCheckpoint(seq, kind, bytes);
      let jseq = this.sql.exec("SELECT COALESCE(MAX(seq),0) AS n FROM journal").one().n;
      for (const j of journal) this.sql.exec("INSERT INTO journal (seq, kind, data) VALUES (?, ?, ?)", ++jseq, j.kind, JSON.stringify(j.data));
      if (kind === "fold") { this.sql.exec("DELETE FROM blobs WHERE seq >= 1 AND seq < ?", seq); this.sql.exec("DELETE FROM checkpoints WHERE seq >= 1 AND seq < ?", seq); }
      inCommit?.({ seq, calls, kind });
    });
    const tRep = Date.now();
    let r2Ms = -1;
    if (this.syncReplication) r2Ms = await this.replicate(seq, kind, bytes, epoch);
    else this.ctx.waitUntil(this.pendingReplication = this.replicate(seq, kind, bytes, epoch).catch((e) => console.log("replication failed", e)));
    const mem = engine.mem();
    const linearBytes = engine.linearBytes;
    if (linearBytes > this.HOT_MAX_LINEAR) { engine.drop(); this.engine = null; } // bloat: the next activation restores warm
    const tEnd = Date.now();
    const rec = { ...loaded, events: events.length, runMs, kind, checkpointBytes: bytes.length, snapshotMs: tCommit - tSnap, commitMs: tRep - tCommit, r2Ms, totalMs: tEnd - tRun, linearBytes, heapUsed: mem.memory_used, calls: calls.length, evicted: !this.engine, foldReason: kind === "fold" ? (slow ? "slow-restore" : "bytes") : undefined };
    return { rec, calls, replies, out };
  }

  async hotEngine() { if (!this.engine) await this.load("warm"); return this.engine; }
  async stats() { return JSON.parse((await this.hotEngine()).call("__stats").result); }
}
