// Durable store for cells: SQLite (node:sqlite) as the warm tier and a
// directory of files standing in for object storage (the cold home).
//
// Layers: the engine layer (natives only, one per engine build) and the
// harness layer (one per harness version) are stored once, content
// addressed; a young agent's chain starts on top of them. After its first
// fold an agent has a base of its own.
// One commit = one transaction: checkpoint, journal, outbox, processed
// events, head bump fenced by the epoch inside the write. Every checkpoint
// carries an HMAC that is verified before it is restored, and a metrics
// record. Mailbox events are deduplicated by (sender, seq) high-water mark.
import { DatabaseSync } from "node:sqlite";
import { mkdirSync, writeFileSync, readFileSync, appendFileSync, existsSync, readdirSync, statSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createHash, createHmac, randomBytes } from "node:crypto";

export class Fenced extends Error {}
export class Tampered extends Error {}

const sha = (b) => createHash("sha256").update(b).digest("base64url").slice(0, 22);

export class Store {
  constructor(sqlitePath, coldDir) {
    this.db = new DatabaseSync(sqlitePath);
    this.path = sqlitePath;
    this.db.exec(`
      PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
      CREATE TABLE IF NOT EXISTS layers (hash TEXT PRIMARY KEY, kind TEXT, bytes BLOB, size INTEGER, mac TEXT);
      CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, head_seq INTEGER NOT NULL, base_seq INTEGER NOT NULL, journal_seq INTEGER NOT NULL, layers TEXT);
      CREATE TABLE IF NOT EXISTS checkpoints (agent TEXT, seq INTEGER, kind TEXT, bytes BLOB, size INTEGER, mac TEXT, metrics TEXT, PRIMARY KEY (agent, seq));
      CREATE TABLE IF NOT EXISTS journal (agent TEXT, seq INTEGER, kind TEXT, data TEXT, PRIMARY KEY (agent, seq));
      CREATE TABLE IF NOT EXISTS bodies (hash TEXT PRIMARY KEY, body TEXT);
      CREATE TABLE IF NOT EXISTS mailbox (agent TEXT, seq INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT, sseq INTEGER, event TEXT, processed INTEGER DEFAULT 0);
      CREATE TABLE IF NOT EXISTS senders (agent TEXT, sender TEXT, high INTEGER, PRIMARY KEY (agent, sender));
      CREATE TABLE IF NOT EXISTS outbox (agent TEXT, call_id INTEGER, name TEXT, args TEXT, sent INTEGER DEFAULT 0, PRIMARY KEY (agent, call_id));
    `);
    let key = this.db.prepare("SELECT v FROM meta WHERE k = 'mac_key'").get()?.v;
    if (!key) { key = randomBytes(32).toString("hex"); this.db.prepare("INSERT INTO meta (k, v) VALUES ('mac_key', ?)").run(key); }
    this.key = Buffer.from(key, "hex");
    this.coldDir = coldDir;
    if (coldDir) mkdirSync(coldDir, { recursive: true });
    const p = (sql) => this.db.prepare(sql);
    this.q = {
      getAgent: p("SELECT * FROM agents WHERE id = ?"),
      newAgent: p("INSERT INTO agents (id, epoch, head_seq, base_seq, journal_seq, layers) VALUES (?, 0, 0, 0, 0, ?)"),
      lease: p("UPDATE agents SET epoch = epoch + 1 WHERE id = ? RETURNING epoch"),
      bump: p("UPDATE agents SET head_seq = ?, base_seq = ?, journal_seq = ? WHERE id = ? AND epoch = ?"),
      putLayer: p("INSERT OR IGNORE INTO layers (hash, kind, bytes, size, mac) VALUES (?, ?, ?, ?, ?)"),
      getLayer: p("SELECT bytes, mac, size FROM layers WHERE hash = ?"),
      putCheckpoint: p("INSERT INTO checkpoints (agent, seq, kind, bytes, size, mac, metrics) VALUES (?, ?, ?, ?, ?, ?, ?)"),
      chain: p("SELECT seq, kind, bytes, mac FROM checkpoints WHERE agent = ? AND seq >= ? ORDER BY seq"),
      sweep: p("DELETE FROM checkpoints WHERE agent = ? AND seq < ?"),
      putJournal: p("INSERT INTO journal (agent, seq, kind, data) VALUES (?, ?, ?, ?)"),
      journal: p("SELECT seq, kind, data FROM journal WHERE agent = ? ORDER BY seq"),
      putBody: p("INSERT OR IGNORE INTO bodies (hash, body) VALUES (?, ?)"),
      getBody: p("SELECT body FROM bodies WHERE hash = ?"),
      high: p("SELECT high FROM senders WHERE agent = ? AND sender = ?"),
      setHigh: p("INSERT INTO senders (agent, sender, high) VALUES (?, ?, ?) ON CONFLICT(agent, sender) DO UPDATE SET high = excluded.high"),
      enqueue: p("INSERT INTO mailbox (agent, sender, sseq, event) VALUES (?, ?, ?, ?)"),
      pendingEvents: p("SELECT seq, event FROM mailbox WHERE agent = ? AND processed = 0 ORDER BY seq"),
      markProcessed: p("UPDATE mailbox SET processed = 1 WHERE seq = ?"),
      putOutbox: p("INSERT INTO outbox (agent, call_id, name, args) VALUES (?, ?, ?, ?)"),
      unsent: p("SELECT call_id, name, args FROM outbox WHERE agent = ? AND sent = 0 ORDER BY call_id"),
      markSent: p("UPDATE outbox SET sent = 1 WHERE agent = ? AND call_id = ?"),
      sizes: p("SELECT (SELECT COALESCE(SUM(size),0) FROM checkpoints WHERE agent = ?) AS chain, (SELECT COALESCE(SUM(LENGTH(data)),0) FROM journal WHERE agent = ?) AS journal"),
      liveSizes: p("SELECT COALESCE(SUM(size),0) AS live FROM checkpoints WHERE agent = ? AND seq >= ?"),
      baseSize: p("SELECT size FROM checkpoints WHERE agent = ? AND seq = ?"),
      bodyBytes: p("SELECT COALESCE(SUM(LENGTH(body)),0) AS b, COUNT(*) AS n FROM bodies"),
      metrics: p("SELECT seq, kind, size, metrics FROM checkpoints WHERE agent = ? ORDER BY seq"),
    };
  }

  mac(bytes) { return createHmac("sha256", this.key).update(bytes).digest("hex"); }
  #verify(bytes, mac, what) { if (this.mac(bytes) !== mac) throw new Tampered(`${what}: HMAC mismatch`); return new Uint8Array(bytes); }

  // ---- layers ----
  putLayer(kind, bytes) {
    const hash = sha(bytes);
    this.q.putLayer.run(hash, kind, bytes, bytes.length, this.mac(bytes));
    if (this.coldDir) { const dir = join(this.coldDir, "layers"); mkdirSync(dir, { recursive: true }); writeFileSync(join(dir, `${hash}-${kind}.bin`), bytes); }
    return hash;
  }
  getLayer(hash) { const r = this.q.getLayer.get(hash); if (!r) throw new Error(`no layer ${hash}`); return this.#verify(r.bytes, r.mac, `layer ${hash}`); }

  // ---- agents ----
  agent(id) { return this.q.getAgent.get(id); }
  create(id, layers) { this.q.newAgent.run(id, JSON.stringify(layers)); return this.agent(id); }
  lease(id) { return this.q.lease.get(id).epoch; }
  layersOf(id) { return JSON.parse(this.agent(id).layers); }

  // ---- mailbox with per-sender dedup ----
  /** returns true if enqueued, false if a duplicate or out of order */
  enqueue(id, event, { sender = "host", seq } = {}) {
    const high = this.q.high.get(id, sender)?.high ?? 0;
    const sseq = seq ?? high + 1;
    if (sseq <= high) return false;
    this.db.exec("BEGIN IMMEDIATE");
    try { this.q.enqueue.run(id, sender, sseq, JSON.stringify(event)); this.q.setHigh.run(id, sender, sseq); this.db.exec("COMMIT"); }
    catch (e) { this.db.exec("ROLLBACK"); throw e; }
    return true;
  }
  pendingEvents(id) { return this.q.pendingEvents.all(id).map((r) => ({ seq: r.seq, event: JSON.parse(r.event) })); }

  // ---- bodies: anything the journal references, stored once by content ----
  intern(value) { const body = JSON.stringify(value); const hash = sha(body); this.q.putBody.run(hash, body); return hash; }
  body(hash) { const r = this.q.getBody.get(hash); if (!r) throw new Error(`no body ${hash}`); return JSON.parse(r.body); }

  /**
   * One atomic commit. `checkpoint` = { kind: 'base'|'chain', bytes, metrics }.
   * Throws Fenced when `epoch` is no longer the agent's epoch.
   */
  commit(id, epoch, { checkpoint, journal = [], outbox = [], processed = [] }) {
    const a = this.agent(id);
    const headSeq = a.head_seq + 1;
    const baseSeq = checkpoint.kind === "fold" ? headSeq : a.base_seq;
    let jseq = a.journal_seq;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const r = this.q.bump.run(headSeq, baseSeq, jseq + journal.length, id, epoch);
      if (r.changes !== 1) throw new Fenced(`agent ${id}: epoch ${epoch} is stale`);
      this.q.putCheckpoint.run(id, headSeq, checkpoint.kind, checkpoint.bytes, checkpoint.bytes.length, this.mac(checkpoint.bytes), JSON.stringify(checkpoint.metrics ?? {}));
      for (const j of journal) this.q.putJournal.run(id, ++jseq, j.kind, JSON.stringify(j.data));
      for (const o of outbox) this.q.putOutbox.run(id, o.id, o.name, JSON.stringify(o.args));
      for (const seq of processed) this.q.markProcessed.run(seq);
      if (checkpoint.kind === "fold") this.q.sweep.run(id, headSeq); // the sweeper, inline: a fold replaces everything before it
      this.db.exec("COMMIT");
    } catch (e) { this.db.exec("ROLLBACK"); throw e; }
    if (this.coldDir) this.replicate(id, headSeq, checkpoint, journal);
    return { headSeq, baseSeq };
  }

  /**
   * The chain to restore from. Always the engine layer as the base, then the
   * harness layer (the anchor of the folds), then the latest fold if any,
   * then the checkpoints after it. `anchorAfter` is the index of the harness
   * layer among the deltas, for the engine to rebuild the anchor.
   */
  chain(id) {
    const a = this.agent(id);
    if (!a) return null;
    const [engine, harness] = this.layersOf(id);
    const rows = this.q.chain.all(id, Math.max(a.base_seq, 1));
    const deltas = [this.getLayer(harness), ...rows.map((r) => this.#verify(r.bytes, r.mac, `checkpoint ${r.seq}`))];
    return { base: this.getLayer(engine), deltas, anchorAfter: 0, baseSeq: a.base_seq, headSeq: a.head_seq, length: deltas.length, fromLayers: true };
  }
  /** bytes of the latest fold (or 0) and of the checkpoints since it */
  chainShape(id) {
    const a = this.agent(id);
    const fold = a.base_seq ? (this.q.baseSize.get(id, a.base_seq)?.size ?? 0) : 0;
    const live = this.q.liveSizes.get(id, Math.max(a.base_seq, 1)).live;
    return { baseBytes: fold, deltaBytes: live - fold, deltas: a.head_seq - Math.max(a.base_seq, 0) - (a.base_seq ? 0 : 0), layersBytes: this.layersOf(id).reduce((s, h) => s + this.q.getLayer.get(h).size, 0) };
  }

  journal(id) { return this.q.journal.all(id).map((r) => ({ seq: r.seq, kind: r.kind, data: JSON.parse(r.data) })); }
  unsent(id) { return this.q.unsent.all(id).map((r) => ({ id: r.call_id, name: r.name, args: JSON.parse(r.args) })); }
  markSent(id, callId) { this.q.markSent.run(id, callId); }
  metrics(id) { return this.q.metrics.all(id).map((r) => ({ seq: r.seq, kind: r.kind, size: r.size, ...JSON.parse(r.metrics) })); }

  sizes(id) {
    const a = this.agent(id);
    const s = this.q.sizes.get(id, id);
    const live = this.q.liveSizes.get(id, Math.max(a.base_seq, 1)).live;
    const layers = this.layersOf(id).reduce((s, h) => s + this.q.getLayer.get(h).size, 0);
    return { chainAll: s.chain, chainLive: live, layers, journal: s.journal, headSeq: a.head_seq, baseSeq: a.base_seq };
  }
  bodyStats() { return this.q.bodyBytes.get(); }

  /** corrupt a stored checkpoint (tests only) */
  tamper(id, seq) { this.db.prepare("UPDATE checkpoints SET bytes = X'00' || substr(bytes, 2) WHERE agent = ? AND seq = ?").run(id, seq); }

  // ---- cold home: files, one directory per agent ----
  replicate(id, seq, checkpoint, journal) {
    const dir = join(this.coldDir, id);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, `${String(seq).padStart(6, "0")}-${checkpoint.kind}.bin`), checkpoint.bytes);
    if (journal.length) appendFileSync(join(dir, "journal.jsonl"), journal.map((j) => JSON.stringify(j)).join("\n") + "\n");
    if (checkpoint.kind === "fold") for (const f of readdirSync(dir)) { const m = /^(\d{6})-(fold|chain)\.bin$/.exec(f); if (m && Number(m[1]) < seq) rmSync(join(dir, f)); }
  }
  coldChain(id) {
    const dir = join(this.coldDir, id);
    if (!existsSync(dir)) return null;
    const files = readdirSync(dir).filter((f) => /\.bin$/.test(f)).sort();
    const fi = files.findLastIndex((f) => f.endsWith("-fold.bin"));
    const tail = files.slice(Math.max(fi, 0));
    const [engine, harness] = this.layersOf(id);
    const ld = join(this.coldDir, "layers");
    const layer = (h, k) => new Uint8Array(readFileSync(join(ld, `${h}-${k}.bin`)));
    const deltas = [layer(harness, "harness"), ...tail.map((f) => new Uint8Array(readFileSync(join(dir, f))))];
    return { base: layer(engine, "engine"), deltas, anchorAfter: 0, length: deltas.length };
  }
  coldBytes(id) { const dir = join(this.coldDir, id); if (!existsSync(dir)) return 0; return readdirSync(dir).reduce((s, f) => s + statSync(join(dir, f)).size, 0); }

  checkpointWal() { this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)"); }
  fileBytes() { this.checkpointWal(); return statSync(this.path).size; }
  close() { this.db.close(); }
}
