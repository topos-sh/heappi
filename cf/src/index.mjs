// The agent image on Cloudflare: one Durable Object per agent.
//   hot   the engine instance kept in the object's memory between requests
//   warm  the object's SQLite (checkpoints, journal, head)
//   cold  R2: base + deltas per agent, and a head object written with a
//         conditional put (the lease and the fence of the design)
// The broker (scripted model, fetch tool) runs inside the object so a turn
// is deterministic; every host call still parks, commits and resumes.
import { DurableObject } from "cloudflare:workers";
import engineModule from "./agent-image.wasm";
import HARNESS_SRC from "./harness.txt";
import { Engine, SNAPSHOT_BASE, SNAPSHOT_CHAIN } from "../../host/engine.mjs";
import { fetchSource, scriptedModel, userMessage, expectedCount } from "../../host/scenario.mjs";

const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json" } });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const m = /^\/agents\/([\w-]+)(\/\w+)?$/.exec(url.pathname);
    if (!m) return json({ routes: ["/agents/:id/create", "/agents/:id/turn?t=1&tier=hot|warm|cold", "/agents/:id/restore?tier=warm|cold", "/agents/:id/info", "/agents/:id/noop"] });
    const stub = env.CELL.get(env.CELL.idFromName(m[1]));
    return stub.fetch(request);
  },
};

export class AgentCell extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.ensureSchema();
    this.engine = null; // the hot tier
    this.completions = new Map(); // gateway cache, content keyed
    this.CHUNK = 1024 * 1024; // under the 2 MB blob limit of DO SQLite
    this.FOLD_MIN = 64 * 1024;
  }
  ensureSchema() {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS head (k INTEGER PRIMARY KEY CHECK (k = 1), epoch INTEGER, head_seq INTEGER, base_seq INTEGER, call_seq INTEGER);
      CREATE TABLE IF NOT EXISTS checkpoints (seq INTEGER PRIMARY KEY, kind TEXT, size INTEGER, chunks INTEGER);
      CREATE TABLE IF NOT EXISTS blobs (seq INTEGER, part INTEGER, bytes BLOB, PRIMARY KEY (seq, part));
      CREATE TABLE IF NOT EXISTS journal (seq INTEGER PRIMARY KEY, kind TEXT, data TEXT);
      CREATE TABLE IF NOT EXISTS messages (hash TEXT PRIMARY KEY, body TEXT);
      INSERT OR IGNORE INTO head (k, epoch, head_seq, base_seq, call_seq) VALUES (1, 0, 0, 0, 0);
    `);
  }

  get agentId() { return this.ctx.id.name ?? String(this.ctx.id); }
  head() { return this.sql.exec("SELECT * FROM head WHERE k = 1").one(); }

  async fetch(request) {
    const url = new URL(request.url);
    const op = url.pathname.split("/")[3] ?? "info";
    const tier = url.searchParams.get("tier") ?? "hot";
    this.syncReplication = url.searchParams.get("rep") === "sync"; // default: replicate after the response
    try {
      switch (op) {
        case "noop": return json({ ok: true, colo: request.cf?.colo ?? null });
        case "r2ping": {
          const key = `ping/${this.agentId}`; const out = {};
          let t = Date.now(); await this.env.COLD.put(key, "x"); out.putMs = Date.now() - t;
          t = Date.now(); const o = await this.env.COLD.get(key); await o.text(); out.getMs = Date.now() - t;
          t = Date.now(); await this.env.COLD.head(key); out.headMs = Date.now() - t;
          t = Date.now(); await this.env.COLD.put(key, "y", { onlyIf: { etagMatches: o.etag } }); out.condPutMs = Date.now() - t;
          t = Date.now(); this.sql.exec("SELECT 1").toArray(); out.sqlMs = Date.now() - t;
          return json({ ...out, colo: request.cf?.colo ?? null });
        }
        case "create": return json(await this.create());
        case "turn": return json(await this.turn(Number(url.searchParams.get("t") ?? 1), tier));
        case "restore": return json(await this.restoreOnly(tier));
        case "reset": return json(await this.reset());
        case "info": return json(this.info());
        default: return json({ error: "unknown op " + op }, 404);
      }
    } catch (e) {
      return json({ error: String(e?.stack ?? e) }, 500);
    }
  }

  info() {
    const h = this.head();
    const cp = this.sql.exec("SELECT kind, size FROM checkpoints WHERE seq >= ? ORDER BY seq", h.base_seq).toArray();
    return { agent: this.agentId, ...h, hot: !!this.engine, repBatches: this.repBatches ?? 0, repItems: this.repItems ?? 0, liveChainBytes: cp.reduce((s, r) => s + r.size, 0), deltas: cp.length - 1, baseBytes: cp[0]?.size ?? 0, journalRows: this.sql.exec("SELECT COUNT(*) AS n FROM journal").one().n, dbBytes: this.ctx.storage.sql.databaseSize, linearBytes: this.engine?.linearBytes ?? 0 };
  }

  async reset() {
    this.engine?.drop(); this.engine = null;
    await this.ctx.storage.deleteAll();
    this.ensureSchema();
    const list = await this.env.COLD.list({ prefix: `agents/${this.agentId}/` });
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
  warmChain() {
    const h = this.head();
    if (h.head_seq === 0) return null;
    const rows = this.sql.exec("SELECT seq FROM checkpoints WHERE seq >= ? ORDER BY seq", h.base_seq).toArray();
    return { base: this.getCheckpoint(rows[0].seq), deltas: rows.slice(1).map((r) => this.getCheckpoint(r.seq)), length: rows.length - 1 };
  }
  async coldChain() {
    await this.pendingReplication; // a cold load after our own writes waits for them to land
    const headObj = await this.env.COLD.get(`agents/${this.agentId}/head`);
    if (!headObj) return null;
    const head = await headObj.json();
    const objs = await Promise.all(head.keys.map((k) => this.env.COLD.get(k).then((o) => o.arrayBuffer())));
    return { base: new Uint8Array(objs[0]), deltas: objs.slice(1).map((b) => new Uint8Array(b)), length: objs.length - 1, headEtag: headObj.etag };
  }
  /**
   * Replicate commits to R2: every pending checkpoint in one batch (blobs in
   * parallel, one conditional put of the head: the fence). Called inline for
   * `rep=sync`, else queued and drained by one loop after the response, so a
   * burst of parks costs one R2 round trip set, not one per park.
   */
  async replicate(seq, kind, bytes, epoch) {
    this.repQueue ??= [];
    this.repQueue.push({ seq, kind, bytes, epoch });
    if (this.repDraining) return this.repDraining;
    const t0 = Date.now();
    this.repDraining = (async () => {
      try {
        while (this.repQueue.length) {
          const batch = this.repQueue.splice(0);
          await this.replicateBatch(batch);
        }
      } finally { this.repDraining = null; }
    })();
    await this.repDraining;
    return Date.now() - t0;
  }
  async replicateBatch(batch) {
    const prefix = `agents/${this.agentId}/`;
    const epoch = batch.at(-1).epoch;
    const keyOf = (c) => `${prefix}${String(c.seq).padStart(6, "0")}-${c.kind}.bin`;
    await Promise.all(batch.map((c) => this.env.COLD.put(keyOf(c), c.bytes)));
    const cur = await this.env.COLD.get(`${prefix}head`);
    const head = cur ? await cur.json() : { epoch: 0, keys: [] };
    if (head.epoch > epoch) throw new Error(`fenced: head epoch ${head.epoch} > ${epoch}`);
    let keys = [...head.keys];
    const swept = [];
    for (const c of batch) { if (c.kind === "base") { swept.push(...keys); keys = []; } keys.push(keyOf(c)); }
    const put = await this.env.COLD.put(`${prefix}head`, JSON.stringify({ epoch, headSeq: batch.at(-1).seq, keys }), cur ? { onlyIf: { etagMatches: cur.etag } } : { onlyIf: { uploadedBefore: new Date(0) } });
    if (!put && cur) throw new Error("fenced: head changed under us");
    if (swept.length) await Promise.all(swept.map((k) => this.env.COLD.delete(k))); // the sweeper
    this.repBatches = (this.repBatches ?? 0) + 1; this.repItems = (this.repItems ?? 0) + batch.length;
  }

  // ---- the cell ----
  async create() {
    await this.reset();
    const engine = Engine.fromModule(engineModule, (s) => console.log("[heap]", s));
    engine.init();
    engine.eval(HARNESS_SRC, "harness.js");
    const bytes = engine.snapshot(SNAPSHOT_BASE);
    const epoch = 1;
    this.sql.exec("UPDATE head SET epoch = ?, head_seq = 1, base_seq = 1 WHERE k = 1", epoch);
    this.putCheckpoint(1, "base", bytes);
    const r2Ms = await this.replicate(1, "base", bytes, epoch);
    this.engine = engine;
    return { baseBytes: bytes.length, r2Ms, colo: this.colo ?? null };
  }

  async load(tier) {
    const t0 = Date.now();
    if (this.engine && tier === "hot") return { tier: "hot", restoreMs: 0 };
    this.engine?.drop(); this.engine = null;
    const chain = tier === "cold" ? await this.coldChain() : this.warmChain();
    const tRead = Date.now();
    const engine = Engine.fromModule(engineModule, (s) => console.log("[heap]", s));
    const tInst = Date.now();
    engine.restore(chain.base, chain.deltas);
    const tApply = Date.now();
    this.engine = engine;
    return { tier, restoreMs: tApply - t0, readMs: tRead - t0, instantiateMs: tInst - tRead, applyMs: tApply - tInst, chainLength: chain.length, chainBytes: chain.base.length + chain.deltas.reduce((s, d) => s + d.length, 0) };
  }

  async restoreOnly(tier) {
    const r = await this.load(tier === "hot" ? "warm" : tier);
    return { ...r, linearBytes: this.engine.linearBytes, heap: this.engine.mem() };
  }

  /** one activation: deliver events, run, commit (SQLite transaction), replicate */
  async activate(events, tier) {
    const loaded = await this.load(tier);
    const engine = this.engine;
    const h = this.head();
    const epoch = h.epoch;
    const journal = [];
    const tRun = Date.now();
    for (const ev of events) { journal.push({ kind: "event", data: ev.type === "result" ? { ...ev, value: undefined } : ev }); engine.call("__deliver", JSON.stringify(ev)); }
    const out = JSON.parse(engine.call("__drain").result ?? "[]");
    const runMs = Date.now() - tRun;
    const calls = [], replies = [];
    for (const o of out) {
      if (o.kind === "call") {
        const data = o.name === "model" ? { id: o.id, name: o.name, messages: this.intern(o.args.messages) } : { id: o.id, name: o.name, args: o.args };
        journal.push({ kind: "call", data }); calls.push(o);
      } else if (o.kind === "reply") replies.push(o); else journal.push({ kind: o.kind, data: o });
    }
    // fold when the deltas outweigh the base
    const cps = this.sql.exec("SELECT seq, size FROM checkpoints WHERE seq >= ? ORDER BY seq", h.base_seq).toArray();
    const baseBytes = cps[0].size, deltaBytes = cps.slice(1).reduce((s, r) => s + r.size, 0);
    const kind = deltaBytes >= Math.max(baseBytes, this.FOLD_MIN) || cps.length >= 64 ? "base" : "chain";
    const tSnap = Date.now();
    const bytes = engine.snapshot(kind === "base" ? SNAPSHOT_BASE : SNAPSHOT_CHAIN);
    const tCommit = Date.now();
    const seq = h.head_seq + 1;
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("UPDATE head SET head_seq = ?, base_seq = ?, call_seq = call_seq + ? WHERE k = 1 AND epoch = ?", seq, kind === "base" ? seq : h.base_seq, calls.length, epoch);
      this.putCheckpoint(seq, kind, bytes);
      let jseq = this.sql.exec("SELECT COALESCE(MAX(seq),0) AS n FROM journal").one().n;
      for (const j of journal) this.sql.exec("INSERT INTO journal (seq, kind, data) VALUES (?, ?, ?)", ++jseq, j.kind, JSON.stringify(j.data));
      if (kind === "base") { this.sql.exec("DELETE FROM blobs WHERE seq < ?", seq); this.sql.exec("DELETE FROM checkpoints WHERE seq < ?", seq); }
    });
    const tRep = Date.now();
    let r2Ms = -1;
    if (this.syncReplication) r2Ms = await this.replicate(seq, kind, bytes, epoch);
    else this.ctx.waitUntil(this.pendingReplication = this.replicate(seq, kind, bytes, epoch).catch((e) => console.log("replication failed", e)));
    const tEnd = Date.now();
    return { rec: { ...loaded, events: events.length, runMs, kind, checkpointBytes: bytes.length, snapshotMs: tCommit - tSnap, commitMs: tRep - tCommit, r2Ms, totalMs: tEnd - tRun, linearBytes: engine.linearBytes, heapUsed: engine.mem().memory_used }, calls, replies };
  }

  intern(messages) {
    return messages.map((m) => {
      const body = JSON.stringify(m);
      const hash = fnv(body);
      this.sql.exec("INSERT OR IGNORE INTO messages (hash, body) VALUES (?, ?)", hash, body);
      return hash;
    });
  }

  /** the broker: store-then-post gateway for the model, the fetch tool */
  async broker(call) {
    if (call.name === "model") {
      const key = fnv(JSON.stringify(call.args));
      if (!this.completions.has(key)) this.completions.set(key, scriptedModel(call.args));
      return this.completions.get(key);
    }
    if (call.name === "tool" && call.args.name === "fetch") return fetchSource(call.args.args.source);
    throw new Error("unknown call " + call.name);
  }

  /** one user turn = activations until a reply; tier applies to the first load */
  async turn(t, tier) {
    const t0 = Date.now();
    let events = [{ type: "user", text: userMessage(t) }];
    const acts = [];
    let reply;
    let first = true;
    while (events.length) {
      const { rec, calls, replies } = await this.activate(events, first ? tier : "hot");
      first = false;
      acts.push(rec);
      if (replies.length) reply = replies[0];
      events = [];
      for (const c of calls) {
        try { events.push({ type: "result", id: c.id, value: await this.broker(c) }); }
        catch (e) { events.push({ type: "result", id: c.id, error: String(e?.message ?? e) }); }
      }
    }
    const correct = !!reply && reply.text.includes(`has ${expectedCount(t)} records`);
    return { t, correct, reply: reply?.text, activations: acts, turnMs: Date.now() - t0, stats: JSON.parse(this.engine.call("__stats").result), info: this.info() };
  }
}

function fnv(s) {
  let h = 0xcbf29ce4, g = 0x84222325;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; g ^= s.charCodeAt(i) >> 3; g = Math.imul(g, 0x01000193) >>> 0; }
  return h.toString(16).padStart(8, "0") + g.toString(16).padStart(8, "0");
}
