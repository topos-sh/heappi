// Baseline B: the real Pi Durable harness (@earendil-works/pi-durable 1.4.0)
// on its Node SQLite storage, driven by pi-ai's faux provider with the same
// scripted model, the same `fetch` tool and the same script, run through
// Pi's own code mode (QuickJS via quickjs-wasi, replay journal).
// The script keeps the payload with Pi's `store(key, value)`, the closest
// thing Pi has to working memory.
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension, defineTool, Harness } from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { createCodemodeExtension } from "@earendil-works/pi-durable/codemode";
import { CodemodeSandbox } from "@earendil-works/pi-codemode";
import { statSync } from "node:fs";
import { fetchSource, userMessage, answerFor } from "../host/scenario.mjs";

const context = BACKGROUND_CONTEXT;

/** the script Pi's model writes: same work as scriptFor(t), Pi's store() instead of remember() */
export function piScriptFor(t) {
  return `const r = await tools.fetch({ source: ${t} });
await store("source-${t}", r);
return { above50: r.items.filter(i => i.score > 50).length };`;
}

/** the scripted model as a faux response factory: looks only at the last message */
export const piMarks = { lastContextBytes: 0, modelCalls: 0 };
function respond(context) {
  const msgs = context.messages;
  piMarks.modelCalls++; piMarks.lastContextBytes = JSON.stringify({ systemPrompt: context.systemPrompt, messages: msgs, tools: context.tools?.map((t) => t.name) }).length;
  const last = msgs[msgs.length - 1];
  if (last.role === "user") {
    const text = typeof last.content === "string" ? last.content : last.content.map((c) => c.text ?? "").join("");
    const t = Number(/source (\d+)/.exec(text)[1]);
    return fauxAssistantMessage([fauxToolCall("codemode", { code: piScriptFor(t) }, { id: `call_${t}` })], { stopReason: "toolUse" });
  }
  if (last.role === "toolResult") {
    const t = Number(last.toolCallId.slice(5));
    const text = last.content.map((c) => c.text ?? "").join("");
    const m = /"above50":\s*(\d+)/.exec(text);
    return fauxAssistantMessage(answerFor(t, m ? Number(m[1]) : -1));
  }
  return fauxAssistantMessage("ok");
}

export async function openPi(sqlitePath, { items, codemode = true } = {}) {
  const faux = fauxProvider();
  faux.setResponses(Array.from({ length: 100000 }, () => respond));
  const models = createModels();
  models.setProvider(faux.provider);
  const fetchTool = defineTool({
    name: "fetch",
    description: "Fetch a source",
    parameters: Type.Object({ source: Type.Number() }),
    structuredOutputSchema: Type.Object({ source: Type.Number(), fetchedAt: Type.String(), items: Type.Array(Type.Any()) }),
    replay: "safe",
    execute: async (args) => {
      const payload = fetchSource(args.source, items);
      return { output: [{ type: "text", text: JSON.stringify(payload) }], structuredOutput: payload };
    },
  });
  const registry = createRegistry();
  registry.install(defineExtension({ name: "research", tools: [fetchTool] }));
  if (codemode) registry.install(createCodemodeExtension({ sandbox: new CodemodeSandbox({ memoryLimitBytes: 64 * 1024 * 1024 }), models: false }));
  const storage = await openNodeSqliteStorage(sqlitePath);
  const harness = await Harness.open(storage, { models, registry }, context);
  return { harness, storage, faux, sqlitePath };
}

export async function runPiAgent(pi, _conversationId, turns) {
  const conv = await pi.harness.root(context, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
  const replies = [];
  for (let t = 1; t <= turns; t++) {
    const submission = await conv.submit({ type: "input", content: userMessage(t) }, context);
    const settled = await submission.wait(context);
    if (settled.status !== "done") throw new Error(`turn ${t}: ${JSON.stringify(settled)}`.slice(0, 500));
    replies.push(settled);
  }
  return { conversation: conv, replies };
}

export function piFileBytes(pi) {
  return statSync(pi.sqlitePath).size + (statSync(pi.sqlitePath + "-wal", { throwIfNoEntry: false })?.size ?? 0);
}

export async function closePi(pi) { await pi.harness.close(context); }
