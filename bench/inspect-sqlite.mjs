// Bytes per table of a SQLite file (after a WAL checkpoint), via dbstat.
import { DatabaseSync } from "node:sqlite";
import { statSync } from "node:fs";

export function inspectSqlite(path) {
  const db = new DatabaseSync(path);
  db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").all().map((r) => r.name);
  const rows = [];
  for (const name of tables) {
    const n = db.prepare(`SELECT COUNT(*) AS c FROM "${name}"`).get().c;
    const b = db.prepare("SELECT COALESCE(SUM(pgsize),0) AS s FROM dbstat WHERE name = ?").get(name).s;
    rows.push({ table: name, rows: n, bytes: b });
  }
  const out = { file: statSync(path).size, tables: rows.sort((a, b) => b.bytes - a.bytes) };
  db.close();
  return out;
}

if (process.argv[1] && import.meta.url.endsWith(process.argv[1].split("/").pop())) {
  const r = inspectSqlite(process.argv[2]);
  console.log("file bytes", r.file);
  for (const t of r.tables) console.log(t.table.padEnd(30), String(t.rows).padStart(7), String(t.bytes).padStart(10));
}
