// The model gateway, on pi-ai. It holds the connection to the provider,
// streams deltas to whoever subscribed to the agent, stores the finished
// message under the hash of the request, and only then hands it back. A
// retry with the same request returns the stored message.
import { createHash } from "node:crypto";
import { createModels } from "@earendil-works/pi-ai/models";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { scriptedModel } from "../host/scenario.mjs";
import { transcriptToSimple, simpleToAssistant } from "../host/pi-shape.mjs";

export function createGateway({ provider = "anthropic", modelId = process.env.HEAPPI_MODEL ?? "claude-sonnet-5-5", demo = false } = {}) {
  const models = createModels();
  models.setProvider(anthropicProvider());
  const model = models.getModel(provider, modelId);
  if (!demo && !model) throw new Error(`unknown model ${provider}/${modelId}`);
  const completions = new Map();
  const subscribers = new Map(); // agentId → Set<fn>
  const emit = (agentId, event) => { for (const fn of subscribers.get(agentId) ?? []) fn(event); };

  return {
    modelId, demo,
    subscribe(agentId, fn) { (subscribers.get(agentId) ?? subscribers.set(agentId, new Set()).get(agentId)).add(fn); return () => subscribers.get(agentId)?.delete(fn); },
    emit,
    /** request = { context: { systemPrompt, messages, tools }, options } from the heap */
    async call(agentId, request, { signal } = {}) {
      const key = createHash("sha256").update(JSON.stringify(request)).digest("base64url");
      if (completions.has(key)) { emit(agentId, { type: "model_cached" }); return completions.get(key); }
      let message;
      if (demo) {
        message = simpleToAssistant(scriptedModel({ messages: transcriptToSimple(request.context.messages), tools: [] }));
        for (const c of message.content) if (c.type === "text") emit(agentId, { type: "text_delta", delta: c.text });
      } else {
        const stream = models.stream(model, request.context, { maxTokens: request.options?.maxTokens ?? 4096, signal, sessionId: agentId });
        emit(agentId, { type: "model_start", model: modelId });
        for await (const ev of stream) {
          if (ev.type === "text_delta") emit(agentId, { type: "text_delta", delta: ev.delta });
          else if (ev.type === "done") message = ev.message;
          else if (ev.type === "error") message = ev.error;
        }
        message ??= await stream.result?.();
      }
      if (message?.stopReason !== "error" && message?.stopReason !== "aborted") completions.set(key, message); // a retry of a failed request asks again
      emit(agentId, { type: "model_done", stopReason: message?.stopReason, usage: message?.usage });
      return message;
    },
  };
}
