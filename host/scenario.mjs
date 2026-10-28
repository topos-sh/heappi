// The workload every system under test runs: a research agent.
// Each turn: user asks for source t; the model writes a script that fetches
// the source (a payload of `items` records), keeps it in working memory and
// returns a small count; the model then answers in one sentence.
// Deterministic, so the image, the transcript baseline and Pi Durable see
// the same bytes.

export const DEFAULTS = { turns: 50, items: 200 }; // 200 items ≈ 20 KB payload

function mulberry32(seed) {
  return () => {
    seed |= 0; seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** the tool: payload for source t */
export function fetchSource(t, items = DEFAULTS.items) {
  const rnd = mulberry32(t * 7919 + 17);
  const out = [];
  for (let i = 0; i < items; i++) {
    out.push({
      id: t * 100000 + i,
      name: `record-${t}-${i}`,
      score: Math.floor(rnd() * 100),
      tags: ["alpha", "beta", "gamma"].filter(() => rnd() > 0.5),
      note: "lorem ipsum dolor sit amet ".repeat(1 + Math.floor(rnd() * 3)).trim(),
    });
  }
  return { source: t, fetchedAt: "2026-10-27T00:00:00Z", items: out };
}

export function userMessage(t) { return `Look at source ${t} and tell me how many records score above 50.`; }

export function scriptFor(t) {
  return `const r = await tools.fetch({ source: ${t} });
const h = remember(r, "source-${t}");
return { handle: h, above50: r.items.filter(i => i.score > 50).length };`;
}

export function answerFor(t, count) { return `Source ${t} has ${count} records scoring above 50.`; }

/** expected count, for checking results */
export function expectedCount(t, items = DEFAULTS.items) {
  return fetchSource(t, items).items.filter((i) => i.score > 50).length;
}

/**
 * The scripted model, in the shape the image harness uses:
 * request = { messages, tools } → { content, tool_calls }.
 * It looks only at the last message, like a model would.
 */
export function scriptedModel(req) {
  const msgs = req.messages;
  const last = msgs[msgs.length - 1];
  if (last.role === "user") {
    const m = /source (\d+)/.exec(last.content);
    if (!m) return { content: `(demo model) noted: ${String(last.content).slice(0, 80)}`, tool_calls: [] };
    const t = Number(m[1]);
    return { content: "", tool_calls: [{ id: `call_${t}`, name: "run", args: { code: scriptFor(t) } }] };
  }
  if (last.role === "tool") {
    const t = Number(last.tool_call_id.slice(5));
    let count;
    try { count = JSON.parse(last.content).above50; } catch { count = -1; }
    return { content: answerFor(t, count), tool_calls: [] };
  }
  return { content: "ok", tool_calls: [] };
}
