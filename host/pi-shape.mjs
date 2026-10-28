// Conversions between pi-ai's transcript shape (what the heap sends) and
// the simple shape the scripted model understands, so the research
// scenario keeps running when the harness is Pi.
export function transcriptToSimple(messages) {
  const out = [];
  for (const m of messages) {
    if (m.role === "user") out.push({ role: "user", content: typeof m.content === "string" ? m.content : m.content.map((c) => c.text ?? "").join("") });
    else if (m.role === "assistant") out.push({ role: "assistant", content: m.content.filter((c) => c.type === "text").map((c) => c.text).join(""), tool_calls: m.content.filter((c) => c.type === "toolCall").map((c) => ({ id: c.id, name: c.name, args: c.arguments })) });
    else if (m.role === "toolResult") out.push({ role: "tool", tool_call_id: m.toolCallId, content: m.content.find((c) => c.type === "text")?.text ?? "" });
  }
  return out;
}

/** a simple { content, tool_calls } completion as a pi-ai assistant message */
export function simpleToAssistant(r) {
  const content = [];
  if (r.content) content.push({ type: "text", text: r.content });
  for (const tc of r.tool_calls ?? []) content.push({ type: "toolCall", id: tc.id, name: tc.name, arguments: tc.args });
  return { role: "assistant", content, api: "faux", provider: "faux", model: "faux-1", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: r.tool_calls?.length ? "toolUse" : "stop", timestamp: Date.now() };
}
