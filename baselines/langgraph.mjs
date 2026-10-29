// Baseline C: LangGraph with its SQLite checkpointer, the step-checkpoint
// family. The same research scenario as a StateGraph: a model node (the
// scripted model), a tool node that fetches the source and keeps the
// payload in graph state (the equivalent of working memory), and the
// transcript in the messages channel. LangGraph writes a checkpoint of the
// channel values at every superstep; that is what is measured.
//   node baselines/langgraph.mjs [turns]
import { statSync, rmSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { StateGraph, Annotation, START, END } from "@langchain/langgraph";
import { SqliteSaver } from "@langchain/langgraph-checkpoint-sqlite";
import { fetchSource, scriptedModel, userMessage, expectedCount, DEFAULTS } from "../host/scenario.mjs";

const State = Annotation.Root({
  messages: Annotation({ reducer: (a, b) => a.concat(b), default: () => [] }),
  data: Annotation({ reducer: (a, b) => ({ ...a, ...b }), default: () => ({}) }), // fetched sources, kept by handle
});

let lastContextBytes = 0, modelCalls = 0;
function modelNode(state) {
  modelCalls++;
  lastContextBytes = JSON.stringify(state.messages).length; // what a real model would receive
  const r = scriptedModel({ messages: state.messages, tools: ["fetch", "run"] });
  return { messages: [{ role: "assistant", content: r.content, tool_calls: r.tool_calls }] };
}
function toolNode(state) {
  const last = state.messages.at(-1);
  const out = [], data = {};
  for (const tc of last.tool_calls) {
    const t = Number(tc.id.slice(5));
    const payload = fetchSource(t, DEFAULTS.items);
    data[`source-${t}`] = payload;
    out.push({ role: "tool", tool_call_id: tc.id, content: JSON.stringify({ handle: `source-${t}`, above50: payload.items.filter((i) => i.score > 50).length }) });
  }
  return { messages: out, data };
}
const route = (state) => (state.messages.at(-1).tool_calls?.length ? "tool" : END);

export function buildGraph(dbPath) {
  const saver = SqliteSaver.fromConnString(dbPath);
  const graph = new StateGraph(State)
    .addNode("model", modelNode).addNode("tool", toolNode)
    .addEdge(START, "model").addConditionalEdges("model", route).addEdge("tool", "model")
    .compile({ checkpointer: saver });
  return { graph, saver };
}

export async function runLangGraph(dir, turns = DEFAULTS.turns, agent = "lg-1") {
  mkdirSync(dir, { recursive: true });
  const dbPath = join(dir, "langgraph.sqlite");
  rmSync(dbPath, { force: true });
  let { graph } = buildGraph(dbPath);
  const config = { configurable: { thread_id: agent } };
  let correct = 0;
  const t0 = performance.now();
  for (let t = 1; t <= turns; t++) {
    const r = await graph.invoke({ messages: [{ role: "user", content: userMessage(t) }] }, config);
    if (r.messages.at(-1).content.includes(`has ${expectedCount(t)} records`)) correct++;
  }
  const msPerTurn = (performance.now() - t0) / turns;

  // storage: what the checkpointer wrote
  const db = new DatabaseSync(dbPath);
  const cps = db.prepare("SELECT LENGTH(checkpoint) AS b, LENGTH(metadata) AS m FROM checkpoints WHERE thread_id = ? ORDER BY rowid").all(agent);
  const writes = db.prepare("SELECT COALESCE(SUM(LENGTH(value)),0) AS b, COUNT(*) AS n FROM writes WHERE thread_id = ?").get(agent);
  db.close();
  const fileBytes = statSync(dbPath).size;

  // resume: a fresh process would open the checkpointer and load the thread's latest checkpoint
  const t1 = performance.now();
  ({ graph } = buildGraph(dbPath));
  const snap = await graph.getState(config);
  const resumeMs = performance.now() - t1;
  const t2 = performance.now();
  await graph.invoke({ messages: [{ role: "user", content: userMessage(turns + 1) }] }, config);
  const nextTurnMs = performance.now() - t2;

  return {
    system: "LangGraph 1.5 + SqliteSaver", turns, correct, msPerTurn, modelCalls,
    checkpoints: cps.length, checkpointBytes: { median: median(cps.map((c) => c.b)), max: Math.max(...cps.map((c) => c.b)), last: cps.at(-1).b, total: cps.reduce((s, c) => s + c.b + c.m, 0) },
    writesBytes: writes.b, writesRows: writes.n, fileBytes, lastContextBytes, resumeMs, nextTurnMs, stateMessages: snap.values.messages.length, stateDataKeys: Object.keys(snap.values.data).length,
  };
}
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };

if (import.meta.url === `file://${process.argv[1]}`) {
  const r = await runLangGraph(new URL("../bench/out/langgraph", import.meta.url).pathname, Number(process.argv[2] ?? DEFAULTS.turns));
  console.log(JSON.stringify(r, null, 2));
}
