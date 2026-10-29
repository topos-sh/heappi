// Baseline D: DBOS, the journal-and-replay family. The agent is one durable
// workflow per conversation: it waits for user messages with DBOS.recv,
// runs the model and the tool as steps whose outputs are journaled in the
// system database, keeps the transcript and the fetched sources in workflow
// memory, and publishes each reply as an event. After the run the process
// restarts and DBOS recovers the workflow by replaying every step from the
// journal, which is what resumption costs in this family.
//   node baselines/dbos.mjs [turns]   (needs DBOS_SYSTEM_DATABASE_URL, a Postgres)
import { DBOS } from "@dbos-inc/dbos-sdk";
import pg from "pg";
import { fetchSource, scriptedModel, userMessage, expectedCount, DEFAULTS } from "../host/scenario.mjs";

const URL_ = process.env.DBOS_SYSTEM_DATABASE_URL ?? "postgresql://postgres@127.0.0.1:54329/dbos_bench";
const APP = "heappi-bench";
export const marks = { replayedTurns: 0, recoveredAt: 0, enteredAt: 0, modelCalls: 0, lastContextBytes: 0, live: true };

async function modelStep(messages) {
  if (marks.live) { marks.modelCalls++; marks.lastContextBytes = JSON.stringify(messages).length; }
  return scriptedModel({ messages, tools: ["fetch", "run"] });
}
async function toolStep(t) {
  const payload = fetchSource(t, DEFAULTS.items);
  return { payload, above50: payload.items.filter((i) => i.score > 50).length };
}

/** the agent: a workflow that lives for the whole conversation */
const agentWorkflow = DBOS.registerWorkflow(async (turns) => {
  if (!marks.live) marks.enteredAt = performance.now(); // recovery re-enters the function here and replays
  const messages = [];
  const data = {}; // fetched sources, kept in workflow memory (rebuilt from step outputs on replay)
  for (let turn = 1; turn <= turns; turn++) {
    const text = await DBOS.recv("user", 600); // durable wait for the next user message
    if (text === null) throw new Error("no user message within the timeout");
    messages.push({ role: "user", content: text });
    for (;;) {
      const r = await DBOS.runStep(() => modelStep(messages), { name: "model" });
      messages.push({ role: "assistant", content: r.content, tool_calls: r.tool_calls });
      if (!r.tool_calls.length) break;
      for (const tc of r.tool_calls) {
        const t = Number(tc.id.slice(5));
        const res = await DBOS.runStep(() => toolStep(t), { name: "tool" });
        data[`source-${t}`] = res.payload;
        messages.push({ role: "tool", tool_call_id: tc.id, content: JSON.stringify({ handle: `source-${t}`, above50: res.above50 }) });
      }
    }
    await DBOS.setEvent(`reply-${turn}`, messages.at(-1).content);
    marks.replayedTurns = turn;
    if (!marks.live && turn === turns) marks.recoveredAt = performance.now();
  }
  return { turns, messages: messages.length, sources: Object.keys(data).length };
}, { name: "agentWorkflow" });

async function tableBytes(client, wfid) {
  const q = async (sql, ...p) => (await client.query(sql, p)).rows[0];
  const ops = await q("SELECT COUNT(*)::int AS n, COALESCE(SUM(LENGTH(output)),0)::bigint AS b FROM dbos.operation_outputs WHERE workflow_uuid = $1", wfid);
  const notif = await q("SELECT COUNT(*)::int AS n, COALESCE(SUM(LENGTH(message)),0)::bigint AS b FROM dbos.notifications WHERE destination_uuid = $1", wfid);
  const ev = await q("SELECT COUNT(*)::int AS n, COALESCE(SUM(LENGTH(value)),0)::bigint AS b FROM dbos.workflow_events WHERE workflow_uuid = $1", wfid);
  const status = await q("SELECT COALESCE(LENGTH(inputs),0) + COALESCE(LENGTH(output),0) AS b FROM dbos.workflow_status WHERE workflow_uuid = $1", wfid);
  const total = await q("SELECT pg_total_relation_size('dbos.operation_outputs') + pg_total_relation_size('dbos.notifications') + pg_total_relation_size('dbos.workflow_events') + pg_total_relation_size('dbos.workflow_status') AS b");
  return { stepRows: ops.n, stepBytes: Number(ops.b), messageRows: notif.n, messageBytes: Number(notif.b), eventRows: ev.n, eventBytes: Number(ev.b), statusBytes: Number(status.b), relationBytes: Number(total.b) };
}

export async function runDbos(turns = DEFAULTS.turns) {
  const wfid = `agent-${Date.now().toString(36)}`;
  DBOS.setConfig({ name: APP, systemDatabaseUrl: URL_, logLevel: "error" });
  await DBOS.launch();
  const handle = await DBOS.startWorkflow(agentWorkflow, { workflowID: wfid })(turns);
  let correct = 0;
  const t0 = performance.now();
  for (let t = 1; t <= turns; t++) {
    await DBOS.send(wfid, userMessage(t), "user");
    const reply = await DBOS.getEvent(wfid, `reply-${t}`, 60);
    if (reply?.includes(`has ${expectedCount(t)} records`)) correct++;
  }
  const msPerTurn = (performance.now() - t0) / turns;
  const result = await handle.getResult();
  const client = new pg.Client({ connectionString: URL_ }); await client.connect();
  const storage = await tableBytes(client, wfid);
  await DBOS.shutdown();

  // recovery: a second workflow is driven to turns-1, then the process "restarts" and DBOS replays it
  const wf2 = `${wfid}-r`;
  DBOS.setConfig({ name: APP, systemDatabaseUrl: URL_, logLevel: "error" });
  await DBOS.launch();
  await DBOS.startWorkflow(agentWorkflow, { workflowID: wf2 })(turns);
  for (let t = 1; t < turns; t++) { await DBOS.send(wf2, userMessage(t), "user"); await DBOS.getEvent(wf2, `reply-${t}`, 60); }
  await DBOS.shutdown({ runEndpointsCleanup: true }).catch(() => DBOS.shutdown());
  marks.live = false; marks.replayedTurns = 0;
  DBOS.setConfig({ name: APP, systemDatabaseUrl: URL_, logLevel: "error" });
  const t1 = performance.now();
  await DBOS.launch(); // recovery of pending workflows starts here: every step replays from the journal
  await DBOS.send(wf2, userMessage(turns), "user");
  const reply = await DBOS.getEvent(wf2, `reply-${turns}`, 120);
  const recoveredMs = marks.recoveredAt ? marks.recoveredAt - t1 : NaN;
  const replayOnlyMs = marks.recoveredAt && marks.enteredAt ? marks.recoveredAt - marks.enteredAt : NaN;
  const launchToEntryMs = marks.enteredAt ? marks.enteredAt - t1 : NaN;
  const replayedTurns = marks.replayedTurns;
  const okAfterRecovery = !!reply?.includes(`has ${expectedCount(turns)} records`);
  await DBOS.retrieveWorkflow(wf2).getResult();
  await DBOS.shutdown();
  await client.end();
  return { system: "DBOS 5.4 (Postgres)", turns, correct, msPerTurn, modelCalls: marks.modelCalls, lastContextBytes: marks.lastContextBytes, storage, result, recovery: { replayedTurns, launchToReplyMs: recoveredMs, launchToEntryMs, replayOnlyMs, okAfterRecovery } };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const r = await runDbos(Number(process.argv[2] ?? DEFAULTS.turns));
  console.log(JSON.stringify(r, null, 2));
  process.exit(0);
}
