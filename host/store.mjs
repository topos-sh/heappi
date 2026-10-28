// Durable store for cells: SQLite (node:sqlite) as the warm tier and a
// directory of files standing in for object storage (the cold home).
// One commit = one transaction: checkpoint + journal + outbox + head bump,
// fenced by the epoch inside the write.
import { DatabaseSync } from "node:sqlite";
import { mkdirSync, writeFileSync, readFileSync, appendFileSync, existsSync, readdirSync, statSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";

export class Fenced extends Error {}

export class Store {
  constructor(sqlitePath, coldDir) {
    this.db = new DatabaseSync(sqlitePath);
    this.path = sqlitePath;
    this.db.exec(`
      PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, head_seq INTEGER NOT NULL, base_seq INTEGER NOT NULL, journal_seq INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS checkpoints (agent TEXT, seq INTEGER, kind TEXT, bytes BLOB, size INTEGER, PRIMARY KEY (agent, seq));
      CREATE TABLE IF NOT EXISTS journal (agent TEXT, seq INTEGER, kind TEXT, data TEXT, PRIMARY KEY (agent, seq));
      CREATE TABLE IF NOT EXISTS messages (hash TEXT PRIMARY KEY, body TEXT);
      CREATE TABLE IF NOT EXISTS mailbox (agent TEXT, seq INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT, processed INTEGER DEFAULT 0);
      CREATE TABLE IF NOT EXISTS outbox (agent TEXT, call_id INTEGER, name TEXT, args TEXT, sent INTEGER DEFAULT 0, PRIMARY KEY (agent, call_id));
    `);
    this.coldDir = coldDir;
    if (coldDir) mkdirSync(coldDir, { recursive: true });
    this.q = {
      getAgent: this.db.prepare("SELECT * FROM agents WHERE id = ?"),
      newAgent: this.db.prepare("INSERT INTO agents (id, epoch, head_seq, base_seq, journal_seq) VALUES (?, 0, 0, 0, 0)"),
      lease: this.db.prepare("UPDATE agents SET epoch = epoch + 1 WHERE id = ? RETURNING epoch"),
      bump: this.db.prepare("UPDATE agents SET head_seq = ?, base_seq = ?, journal_seq = ? WHERE id = ? AND epoch = ?"),
      putCheckpoint: this.db.prepare("INSERT INTO checkpoints (agent, seq, kind, bytes, size) VALUES (?, ?, ?, ?, ?)"),
      chain: this.db.prepare("SELECT seq, kind, bytes FROM checkpoints WHERE agent = ? AND seq >= ? ORDER BY seq"),
      sweep: this.db.prepare("DELETE FROM checkpoints WHERE agent = ? AND seq < ?"),
      putJournal: this.db.prepare("INSERT INTO journal (agent, seq, kind, data) VALUES (?, ?, ?, ?)"),
      putMessage: this.db.prepare("INSERT OR IGNORE INTO messages (hash, body) VALUES (?, ?)"),
      enqueue: this.db.prepare("INSERT INTO mailbox (agent, event) VALUES (?, ?)"),
      pendingEvents: this.db.prepare("SELECT seq, event FROM mailbox WHERE agent = ? AND processed = 0 ORDER BY seq"),
      markProcessed: this.db.prepare("UPDATE mailbox SET processed = 1 WHERE seq = ?"),
      putOutbox: this.db.prepare("INSERT INTO outbox (agent, call_id, name, args) VALUES (?, ?, ?, ?)"),
      unsent: this.db.prepare("SELECT call_id, name, args FROM outbox WHERE agent = ? AND sent = 0 ORDER BY call_id"),
      markSent: this.db.prepare("UPDATE outbox SET sent = 1 WHERE agent = ? AND call_id = ?"),
      sizes: this.db.prepare("SELECT (SELECT COALESCE(SUM(size),0) FROM checkpoints WHERE agent = ?) AS chain, (SELECT COALESCE(SUM(LENGTH(data)),0) FROM journal WHERE agent = ?) AS journal"),
      liveSizes: this.db.prepare("SELECT COALESCE(SUM(size),0) AS live FROM checkpoints WHERE agent = ? AND seq >= ?"),
      msgBytes: this.db.prepare("SELECT COALESCE(SUM(LENGTH(body)),0) AS b, COUNT(*) AS n FROM messages"),
      baseSize: this.db.prepare("SELECT size FROM checkpoints WHERE agent = ? AND seq = ?"),
    };
  }

  agent(id) {
    let a = this.q.getAgent.get(id);
    if (!a) { this.q.newAgent.run(id); a = this.q.getAgent.get(id); }
    return a;
  }
  /** take the lease: a new epoch; older activations are fenced at commit */
  lease(id) { this.agent(id); return this.q.lease.get(id).epoch; }

  enqueue(id, event) { this.q.enqueue.run(id, JSON.stringify(event)); }
  pendingEvents(id) { return this.q.pendingEvents.all(id).map((r) => ({ seq: r.seq, event: JSON.parse(r.event) })); }

  /** message bodies are stored once by content hash; the journal lists hashes */
  internMessages(messages) {
    return messages.map((m) => {
      const body = JSON.stringify(m);
      const hash = createHash("sha256").update(body).digest("base64url").slice(0, 22);
      this.q.putMessage.run(hash, body);
      return hash;
    });
  }

  /**
   * One atomic commit. `checkpoint` = { kind: 'base'|'chain', bytes }.
   * Throws Fenced when `epoch` is no longer the agent's epoch.
   */
  commit(id, epoch, { checkpoint, journal = [], outbox = [], processed = [] }) {
    const a = this.q.getAgent.get(id);
    const headSeq = a.head_seq + 1;
    const baseSeq = checkpoint.kind === "base" ? headSeq : a.base_seq;
    let jseq = a.journal_seq;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const r = this.q.bump.run(headSeq, baseSeq, jseq + journal.length, id, epoch);
      if (r.changes !== 1) throw new Fenced(`agent ${id}: epoch ${epoch} is stale`);
      this.q.putCheckpoint.run(id, headSeq, checkpoint.kind, checkpoint.bytes, checkpoint.bytes.length);
      for (const j of journal) this.q.putJournal.run(id, ++jseq, j.kind, JSON.stringify(j.data));
      for (const o of outbox) this.q.putOutbox.run(id, o.id, o.name, JSON.stringify(o.args));
      for (const seq of processed) this.q.markProcessed.run(seq);
      if (checkpoint.kind === "base") this.q.sweep.run(id, headSeq); // the sweeper, inline
      this.db.exec("COMMIT");
    } catch (e) { this.db.exec("ROLLBACK"); throw e; }
    if (this.coldDir) this.replicate(id, headSeq, checkpoint, journal);
    return { headSeq, baseSeq };
  }

  /** the chain to restore from: base + checkpoints after it */
  chain(id) {
    const a = this.q.getAgent.get(id);
    if (!a || a.head_seq === 0) return null;
    const rows = this.q.chain.all(id, a.base_seq);
    return { base: new Uint8Array(rows[0].bytes), deltas: rows.slice(1).map((r) => new Uint8Array(r.bytes)), baseSeq: a.base_seq, headSeq: a.head_seq, length: rows.length - 1 };
  }

  unsent(id) { return this.q.unsent.all(id).map((r) => ({ id: r.call_id, name: r.name, args: JSON.parse(r.args) })); }
  markSent(id, callId) { this.q.markSent.run(id, callId); }

  sizes(id) {
    const a = this.q.getAgent.get(id);
    const s = this.q.sizes.get(id, id);
    const live = this.q.liveSizes.get(id, a.base_seq).live;
    return { chainAll: s.chain, chainLive: live, journal: s.journal, headSeq: a.head_seq, baseSeq: a.base_seq };
  }
  messageStats() { return this.q.msgBytes.get(); }
  /** bytes of the current base and of the deltas written since it */
  chainShape(id) {
    const a = this.q.getAgent.get(id);
    const base = this.q.baseSize.get(id, a.base_seq)?.size ?? 0;
    const live = this.q.liveSizes.get(id, a.base_seq).live;
    return { baseBytes: base, deltaBytes: live - base, deltas: a.head_seq - a.base_seq };
  }

  // ---- cold home: files, one directory per agent ----
  replicate(id, seq, checkpoint, journal) {
    const dir = join(this.coldDir, id);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, `${String(seq).padStart(6, "0")}-${checkpoint.kind}.bin`), checkpoint.bytes);
    if (journal.length) appendFileSync(join(dir, "journal.jsonl"), journal.map((j) => JSON.stringify(j)).join("\n") + "\n");
    if (checkpoint.kind === "base") {
      for (const f of readdirSync(dir)) {
        const m = /^(\d{6})-(base|chain)\.bin$/.exec(f);
        if (m && Number(m[1]) < seq) rmSync(join(dir, f));
      }
    }
  }
  coldChain(id) {
    const dir = join(this.coldDir, id);
    if (!existsSync(dir)) return null;
    const files = readdirSync(dir).filter((f) => /\.bin$/.test(f)).sort();
    const bi = files.findLastIndex((f) => f.endsWith("-base.bin"));
    const base = readFileSync(join(dir, files[bi]));
    const deltas = files.slice(bi + 1).map((f) => readFileSync(join(dir, f)));
    return { base: new Uint8Array(base), deltas: deltas.map((d) => new Uint8Array(d)), length: deltas.length };
  }
  coldBytes(id) {
    const dir = join(this.coldDir, id);
    if (!existsSync(dir)) return 0;
    return readdirSync(dir).reduce((s, f) => s + statSync(join(dir, f)).size, 0);
  }

  checkpointWal() { this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)"); }
  fileBytes() { this.checkpointWal(); return statSync(this.path).size; }
  close() { this.db.close(); }
}
