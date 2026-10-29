// The benchmark Worker: the cell with the small harness and the scripted
// research scenario, so the remote run is deterministic. The broker runs
// inside the object; every host call still parks, commits and resumes.
// The demo Worker (demo.mjs) is the same cell with Pi Durable inside.
import HARNESS_SRC from "./harness.txt";
import { CellBase, fnv } from "./cell.mjs";
import { fetchSource, scriptedModel, userMessage, expectedCount } from "../../host/scenario.mjs";

const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json" } });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const m = /^\/agents\/([\w-]+)(\/\w+)?$/.exec(url.pathname);
    if (!m) return json({ routes: ["/agents/:id/create", "/agents/:id/turn?t=1&tier=hot|warm|cold", "/agents/:id/restore?tier=warm|cold", "/agents/:id/info", "/agents/:id/noop"] });
    const stub = env.CELL.get(env.CELL.idFromName(m[1]));
    return stub.fetch(request);
  },
};

export class AgentCell extends CellBase {
  constructor(ctx, env) {
    super(ctx, env, { harnessSrc: HARNESS_SRC });
    this.completions = new Map(); // gateway cache, content keyed
  }

  async fetch(request) {
    const url = new URL(request.url);
    const op = url.pathname.split("/")[3] ?? "info";
    const tier = url.searchParams.get("tier") ?? "hot";
    this.syncReplication = url.searchParams.get("rep") === "sync"; // default: replicate after the response
    try {
      switch (op) {
        case "noop": return json({ ok: true, colo: request.cf?.colo ?? null });
        case "r2ping": {
          const key = `ping/${this.agentId}`; const out = {};
          let t = Date.now(); await this.env.COLD.put(key, "x"); out.putMs = Date.now() - t;
          t = Date.now(); const o = await this.env.COLD.get(key); await o.text(); out.getMs = Date.now() - t;
          t = Date.now(); await this.env.COLD.head(key); out.headMs = Date.now() - t;
          t = Date.now(); await this.env.COLD.put(key, "y", { onlyIf: { etagMatches: o.etag } }); out.condPutMs = Date.now() - t;
          t = Date.now(); this.sql.exec("SELECT 1").toArray(); out.sqlMs = Date.now() - t;
          return json({ ...out, colo: request.cf?.colo ?? null });
        }
        case "create": { this.syncReplication = true; return json(await this.create()); }
        case "turn": return json(await this.turn(Number(url.searchParams.get("t") ?? 1), tier));
        case "restore": { const r = await this.load(tier === "hot" ? "warm" : tier); return json({ ...r, linearBytes: this.engine.linearBytes, heap: this.engine.mem() }); }
        case "reset": return json(await this.reset());
        case "info": return json(this.info());
        default: return json({ error: "unknown op " + op }, 404);
      }
    } catch (e) {
      return json({ error: String(e?.stack ?? e) }, 500);
    }
  }

  /** the broker: store-then-post gateway for the model, the fetch tool */
  async broker(call) {
    if (call.name === "model") {
      const key = fnv(JSON.stringify(call.args));
      if (!this.completions.has(key)) this.completions.set(key, scriptedModel(call.args));
      return this.completions.get(key);
    }
    if (call.name === "tool" && call.args.name === "fetch") return fetchSource(call.args.args.source);
    throw new Error("unknown call " + call.name);
  }

  /** one user turn = activations until a reply; tier applies to the first load */
  async turn(t, tier) {
    const t0 = Date.now();
    let events = [{ type: "user", text: userMessage(t) }];
    const acts = [];
    let reply;
    let first = true;
    while (events.length) {
      const { rec, calls, replies } = await this.activate(events, { tier: first ? tier : "hot" });
      first = false;
      acts.push(rec);
      if (replies.length) reply = replies[0];
      events = [];
      for (const c of calls) {
        try { events.push({ type: "result", id: c.id, value: await this.broker(c) }); }
        catch (e) { events.push({ type: "result", id: c.id, error: String(e?.message ?? e) }); }
      }
    }
    const correct = !!reply && reply.text.includes(`has ${expectedCount(t)} records`);
    return { t, correct, reply: reply?.text, activations: acts, turnMs: Date.now() - t0, stats: await this.stats(), info: this.info() };
  }
}
