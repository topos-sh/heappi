// The model gateway inside the Worker, on pi-ai. Streams deltas to the
// emitter (the object's SSE subscribers), and returns the finished message;
// the object stores it under the hash of the request before it is handed to
// the heap, so a retry returns the stored message. MODEL=scripted runs the
// research scenario's scripted model instead, for a keyless local check.
import { createModels } from "@earendil-works/pi-ai/models";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { scriptedModel } from "../../host/scenario.mjs";
import { transcriptToSimple, simpleToAssistant } from "../../host/pi-shape.mjs";

let gateway = null;
export function getGateway(env) {
  if (gateway) return gateway;
  const modelId = env.MODEL || "claude-sonnet-5-5";
  const scripted = modelId === "scripted";
  let models = null, model = null;
  if (!scripted) {
    models = createModels({ authContext: { env: async (k) => (typeof env[k] === "string" && env[k] ? env[k] : undefined), fileExists: async () => false } });
    models.setProvider(anthropicProvider());
    model = models.getModel("anthropic", modelId);
    if (!model) throw new Error(`unknown model anthropic/${modelId}`);
  }
  gateway = {
    modelId,
    /** request = { context: { systemPrompt, messages, tools }, options } from the heap */
    async call(request, emit) {
      if (scripted) {
        const message = simpleToAssistant(scriptedModel({ messages: transcriptToSimple(request.context.messages), tools: [] }));
        for (const c of message.content) if (c.type === "text") emit({ type: "text_delta", delta: c.text });
        return message;
      }
      const stream = models.stream(model, request.context, { maxTokens: request.options?.maxTokens ?? 4096 });
      emit({ type: "model_start", model: modelId });
      let message;
      for await (const ev of stream) {
        if (ev.type === "text_delta") emit({ type: "text_delta", delta: ev.delta });
        else if (ev.type === "done") message = ev.message;
        else if (ev.type === "error") message = ev.error;
      }
      message ??= await stream.result?.();
      emit({ type: "model_done", stopReason: message?.stopReason, usage: message?.usage });
      return message;
    },
  };
  return gateway;
}
