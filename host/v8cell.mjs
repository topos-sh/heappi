// The same harness.js in a V8 context (node:vm) per agent, driven by the
// same broker, with no persistence: a stand-in for "the agent loop resident
// in a V8 isolate" (Dynamic Workers, workerd). node:vm contexts share one
// V8 heap, so the per-agent memory measured here is a lower bound for an
// isolate, which adds its own heap and code space.
import vm from "node:vm";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const HARNESS_SRC = readFileSync(fileURLToPath(new URL("../heap/harness.js", import.meta.url)), "utf8");
const script = new vm.Script(HARNESS_SRC, { filename: "harness.js" });

export class V8Cell {
  constructor(broker) { this.broker = broker; this.agents = new Map(); }
  create(agentId) {
    // the harness context plus two script contexts, bridged the way the shim does it: strings only
    const ctx = vm.createContext({ __log: () => {} });
    const scripts = [null, 1, 2].map((i) => i && vm.createContext({ __hostCall: (op, json) => ctx.__scriptCall(i, op, json) }));
    ctx.__evalIn = (i, src) => vm.runInContext(src, scripts[i], { filename: "<script>" });
    script.runInContext(ctx);
    this.agents.set(agentId, { ctx, scripts, deliver: ctx.__deliver, drain: ctx.__drain, stats: ctx.__stats });
  }
  async turn(agentId, event) {
    const a = this.agents.get(agentId);
    let events = [event];
    let replies = [];
    while (events.length) {
      for (const ev of events) a.deliver(JSON.stringify(ev));
      const out = JSON.parse(a.drain());
      events = [];
      for (const o of out) {
        if (o.kind === "call") {
          const value = await this.broker.call(agentId, o.id, o.name, o.args);
          events.push({ type: "result", id: o.id, value });
        } else if (o.kind === "reply") replies.push(o);
      }
    }
    return replies;
  }
  stats(agentId) { return JSON.parse(this.agents.get(agentId).stats()); }
}
