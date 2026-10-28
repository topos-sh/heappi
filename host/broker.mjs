// The model gateway and tool broker, in-process. Completions are stored by
// content key before they are handed back (store-then-post). Calls settle
// asynchronously; `settle()` waits for everything in flight, so a driver
// can run activation after activation deterministically.
import { createHash } from "node:crypto";
import { fetchSource, scriptedModel } from "./scenario.mjs";

export class Broker {
  constructor({ items } = {}) {
    this.items = items;
    this.completions = new Map(); // content key → completion
    this.inflight = new Set();
    this.modelCalls = 0; this.cacheHits = 0; this.toolCalls = 0;
  }
  call(_agentId, _id, name, args) {
    const p = (async () => {
      await Promise.resolve(); // results arrive in a later activation, never the same one
      if (name === "model") {
        const key = createHash("sha256").update(JSON.stringify(args)).digest("base64url");
        if (this.completions.has(key)) { this.cacheHits++; return this.completions.get(key); }
        this.modelCalls++;
        const c = scriptedModel(args);
        this.completions.set(key, c);
        return c;
      }
      if (name === "tool") {
        this.toolCalls++;
        if (args.name === "fetch") return fetchSource(args.args.source, this.items);
        throw new Error("unknown tool " + args.name);
      }
      throw new Error("unknown call " + name);
    })();
    this.inflight.add(p);
    p.finally(() => this.inflight.delete(p));
    return p;
  }
  async settle() { while (this.inflight.size) await Promise.allSettled([...this.inflight]); }
}
