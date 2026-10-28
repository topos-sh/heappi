// Baseline A: a conversation-level store, the shape Infinity and Pi Durable
// persist: the transcript rows plus processed-message ids, in SQLite.
// The same scripted model drives it, but there is no heap to keep the
// payload in, so to retain the same information the tool result is stored
// in the transcript (the direct-tool-call shape). A second variant drops the
// payload after the script returns (code mode without working memory), to
// show the two ends: everything kept vs nothing kept.
import { DatabaseSync } from "node:sqlite";
import { statSync } from "node:fs";
import { fetchSource, scriptedModel, userMessage, scriptFor } from "../host/scenario.mjs";

export class TranscriptStore {
  constructor(path) {
    this.path = path;
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS messages (agent TEXT, seq INTEGER, role TEXT, body TEXT, PRIMARY KEY (agent, seq));
      CREATE TABLE IF NOT EXISTS processed (agent TEXT, message_id TEXT, PRIMARY KEY (agent, message_id));
      CREATE TABLE IF NOT EXISTS state (agent TEXT PRIMARY KEY, seq INTEGER, status TEXT);`);
    this.q = {
      put: this.db.prepare("INSERT INTO messages (agent, seq, role, body) VALUES (?, ?, ?, ?)"),
      proc: this.db.prepare("INSERT OR IGNORE INTO processed (agent, message_id) VALUES (?, ?)"),
      state: this.db.prepare("INSERT INTO state (agent, seq, status) VALUES (?, ?, ?) ON CONFLICT(agent) DO UPDATE SET seq = excluded.seq, status = excluded.status"),
      all: this.db.prepare("SELECT role, body FROM messages WHERE agent = ? ORDER BY seq"),
      bytes: this.db.prepare("SELECT COALESCE(SUM(LENGTH(body)),0) AS b FROM messages WHERE agent = ?"),
    };
    this.seq = new Map();
  }
  append(agent, role, msg) {
    const seq = (this.seq.get(agent) ?? 0) + 1;
    this.seq.set(agent, seq);
    this.db.exec("BEGIN IMMEDIATE");
    this.q.put.run(agent, seq, role, JSON.stringify(msg));
    this.q.proc.run(agent, `${agent}:${seq}`);
    this.q.state.run(agent, seq, "idle");
    this.db.exec("COMMIT");
  }
  history(agent) { return this.q.all.all(agent).map((r) => JSON.parse(r.body)); }
  bytes(agent) { return this.q.bytes.get(agent).b; }
  fileBytes() { this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)"); return statSync(this.path).size; }
  close() { this.db.close(); }
}

/**
 * Run `turns` turns for one agent. Each turn: load the history (a slice
 * does), ask the model, run the tool, append every message.
 * keepPayload: true stores the fetched payload in the transcript (what a
 * conversation-level system must do to keep the information); false
 * stores only the script's small result.
 */
export function runTranscriptAgent(store, agent, turns, { keepPayload = true, items } = {}) {
  let modelCalls = 0;
  for (let t = 1; t <= turns; t++) {
    store.append(agent, "user", { role: "user", content: userMessage(t) });
    for (;;) {
      const messages = store.history(agent);
      modelCalls++;
      const r = scriptedModel({ messages, tools: ["fetch", "run"] });
      store.append(agent, "assistant", { role: "assistant", content: r.content, tool_calls: r.tool_calls });
      if (r.tool_calls.length === 0) break;
      for (const tc of r.tool_calls) {
        // the script fetches and counts; a transcript store can keep the payload or not
        const payload = fetchSource(t, items);
        const result = { above50: payload.items.filter((i) => i.score > 50).length };
        if (keepPayload) result.payload = payload;
        store.append(agent, "tool", { role: "tool", tool_call_id: tc.id, content: JSON.stringify(result), script: scriptFor(t) });
      }
    }
  }
  return { modelCalls };
}
