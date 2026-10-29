// Pi Durable on its own SQLite storage: 50 turns, then the resume a fresh
// process would do (open the storage, open the harness, continue), and the
// context Pi sends to the model on the last request.
//   node --no-warnings baselines/pi-resume.mjs [turns]
import { mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { openPi, runPiAgent, closePi, piFileBytes, piMarks } from "./pi.mjs";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { userMessage, expectedCount, DEFAULTS } from "../host/scenario.mjs";

const turns = Number(process.argv[2] ?? DEFAULTS.turns);
const dir = new URL("../bench/out/pi-resume", import.meta.url).pathname; mkdirSync(dir, { recursive: true });
const path = join(dir, "pi.sqlite"); for (const f of [path, path + "-wal", path + "-shm"]) rmSync(f, { force: true });
let pi = await openPi(path, { items: DEFAULTS.items });
const t0 = performance.now();
const { replies } = await runPiAgent(pi, "root", turns);
const msPerTurn = (performance.now() - t0) / turns;
// correctness from Pi's own entries: every assistant answer against the expected count
const all = (await (await pi.harness.root(BACKGROUND_CONTEXT, { agent: { model: { provider: "faux", modelId: "faux-1" } } })).entries({ order: "ascending" }, 1000, undefined, BACKGROUND_CONTEXT)).items;
const answers = all.filter((e) => e.kind === "pi.assistant").map((e) => e.model?.[0]?.content?.filter((c) => c.type === "text").map((c) => c.text).join("") ?? "").filter((t) => /^Source \d+ has/.test(t));
const correctTurns = answers.filter((t) => { const m = /^Source (\d+) has (-?\d+) records/.exec(t); return m && Number(m[2]) === expectedCount(Number(m[1])); }).length;
const firstWrong = answers.findIndex((t) => { const m = /^Source (\d+) has (-?\d+) records/.exec(t); return !(m && Number(m[2]) === expectedCount(Number(m[1]))); }) + 1;
const bytesOpen = piFileBytes(pi);
await closePi(pi);
const bytesBefore = piFileBytes(pi); // after close: WAL checkpointed into the main file
// resume: open storage and harness again, find the conversation, run one more turn
const t1 = performance.now();
pi = await openPi(path, { items: DEFAULTS.items });
const conv = await pi.harness.root(BACKGROUND_CONTEXT, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
const resumeMs = performance.now() - t1;
const t2 = performance.now();
const settled = await (await conv.submit({ type: "input", content: userMessage(turns + 1) }, BACKGROUND_CONTEXT)).wait(BACKGROUND_CONTEXT);
const nextTurnMs = performance.now() - t2;
const page = await conv.entries({ order: "descending" }, 2, undefined, BACKGROUND_CONTEXT);
const reply = page.items.find((x) => x.kind === "pi.assistant")?.model?.[0]?.content?.filter((c) => c.type === "text").map((c) => c.text).join("") ?? "";
const tail = (await conv.entries({ order: "descending" }, 6, undefined, BACKGROUND_CONTEXT)).items.map((e) => ({ kind: e.kind, text: JSON.stringify(e.model?.[0]?.content ?? e.data ?? null).slice(0, 220) }));
const bytesAfter = piFileBytes(pi);
await closePi(pi);
console.log(JSON.stringify({ system: "Pi Durable 1.4.0 on its SQLite storage", turns, correct: correctTurns, firstWrongTurn: firstWrong || null, answered: replies.length, msPerTurn, fileBytes: bytesBefore, fileBytesWithWalOpen: bytesOpen, fileBytesAfterOneMore: bytesAfter, settled: settled.status, replyAfterResume: reply.slice(0, 80), tailAfterResume: tail, resumeMs, nextTurnMs, okAfterResume: settled.status === "done" && reply.includes(`has ${expectedCount(turns + 1)} records`), lastContextBytes: piMarks.lastContextBytes, modelCalls: piMarks.modelCalls }, null, 2));
