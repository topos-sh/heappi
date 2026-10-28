# Using heappi

How to run the prototype, what its interfaces are, and how the pieces fit. The [README](README.md) has the design summary and the benchmark results; this file is the operator's and integrator's view.

## 1. Prerequisites and quick start

- Node 24 (uses `node:sqlite`; `node:wasi` is not needed).
- Nothing else to run what is committed: the engine binary `engine/heappi.wasm` is in the repo.
- To rebuild the engine: a wasi-sdk (25 was used) and the engine fork at `../dquickts-chain`.
- To run the Cloudflare cell: `wrangler` 4 (fetched by `npx`), a Cloudflare account with an R2 bucket named in `cf/wrangler.jsonc`.

```bash
npm install                 # Pi Durable and pi-ai, used only by the baselines
npm run smoke               # engine: checkpoint, restore, deliver, CPU-budget suspension
npm run demo                # one agent, three turns, every activation printed
npm test                    # eleven checks: isolation, budget, rebuild, upgrade, tamper, dedup, eviction
node --no-warnings --expose-gc bench/run.mjs --turns 50 --agents 3 --mem-agents 40
node bench/report.mjs       # bench/out/results.md from bench/out/results.json
```

Rebuild the engine after changing `engine/shim.c` or the fork:

```bash
WASI_SDK=/path/to/wasi-sdk ENGINE=../dquickts-chain ./engine/build.sh
```

## 2. The model in five sentences

An **agent** is one engine heap. The **harness** (`heap/harness.js`) runs inside that heap and is the agent loop: it talks to the model, runs tools and scripts, and keeps working memory. The **host** never looks inside the heap; it delivers events, runs the heap until nothing is runnable, writes one **checkpoint** (a chained delta of the heap) plus a **journal** record in one transaction, and sends the heap's outbound calls after the commit. Everything external, the model included, is a **call** that comes back later as an **event**. Between events the agent is parked: on disk, in the warm SQLite, in the cold home, or still hot in memory.

## 3. Interfaces

### 3.1 The heap contract

The host calls six globals the harness defines. Strings in, strings out.

| Global | Called by the host to | Returns |
|---|---|---|
| `__deliver(eventJson)` | hand in one event | nothing; the heap runs its jobs |
| `__drain()` | collect what the heap wants sent | JSON array of outbox entries |
| `__stats()` | read counters | JSON: version, turn, messages, dropped, handles, pending, modelCalls, toolCalls, scripts |
| `__stateHash()` | get the canonical state hash for the rebuild check | 8-hex string |
| `__migrate()` | finish a harness upgrade after the new source was evaluated | JSON `{ from, to }` |
| `__peek(handle)` | read a working-memory value (debugging) | JSON |

Events the harness understands:

```json
{ "type": "user",   "text": "Look at source 3 ..." }
{ "type": "result", "id": 12, "value": { "...": "..." } }
{ "type": "result", "id": 12, "error": "message" }
```

Outbox entries the harness emits:

```json
{ "kind": "call",  "id": 12, "name": "model", "args": { "messages": [...], "tools": ["fetch", "run"] } }
{ "kind": "call",  "id": 13, "name": "tool",  "args": { "name": "fetch", "args": { "source": 3 } } }
{ "kind": "reply", "turn": 3, "text": "Source 3 has 92 records scoring above 50." }
{ "kind": "late_result", "id": 9 }
{ "kind": "error", "error": "stack" }
```

A `call` must be answered by a `result` event with the same `id`, in a later activation. The harness keeps the pending promise; the host keeps nothing.

### 3.2 The model protocol

The harness sends `{ messages, tools }` and expects `{ content, tool_calls }`, where each tool call is `{ id, name, args }`. `messages` is the conversation window (the last 40 messages: `user`, `assistant` with `tool_calls`, and `tool` with `tool_call_id` and `content`). A tool result larger than 1 KB of JSON reaches the model as a handle, `{ handle: "$r12", digest: {...}, origin: "fetch" }`, and the value stays in the heap. Any gateway that speaks this shape can drive the harness; `host/scenario.mjs` has the scripted one.

### 3.3 Tools

Tools are functions in the harness, in the `TOOLS` map:

```js
const TOOLS = {
  fetch: (args) => caps.tool("fetch", args),   // a host call: the broker answers by name
  run:   (args) => runScript(args.code),       // code mode: a nested call in a script context
};
```

To add a tool: add a line here, and make the broker answer `{ name: "tool", args: { name, args } }` for it. The host never knows the tool's schema.

### 3.4 What a script sees

Model-written code runs in a script context with its own global object. It gets four names and nothing else:

| Name | Meaning |
|---|---|
| `tools.<name>(args)` | call a tool; returns a promise; `run` is refused |
| `mem[handle]`, `handle in mem` | read working memory by handle; a JSON copy |
| `remember(value, origin)` | keep a value in working memory; returns its handle |
| `log(...args)` | print through the host |

Only strings cross between the script and the harness, so no object a script receives has a prototype chain into the harness. A script that computes without yielding is terminated at the CPU budget with an ordinary rejection the harness turns into a tool error. A script's return value is copied out as JSON.

### 3.5 The engine wrapper (`host/engine.mjs`, `host/engine-node.mjs`)

```js
import { NodeEngine, SNAPSHOT_BASE, SNAPSHOT_CHAIN } from "./host/engine-node.mjs";
const e = await NodeEngine.create();       // Engine.fromModule(module) anywhere else
e.init();                                  // harness context + two script contexts
e.eval(src, "harness.js");                 // "done" | "suspended" | "terminated"
e.call("__deliver", json);                 // { status, result }
const bytes = e.snapshot(SNAPSHOT_CHAIN);  // Uint8Array; SNAPSHOT_BASE for a full base
e.restore(base, [delta1, delta2]);         // fresh runtime in this instance
e.resume(); e.run(); e.isSuspended();
e.setBudgetMs(5); e.setBudgetTicks(2000); e.setMemoryLimit(64 << 20);
e.mem(); e.baseId(); e.contextCount(); e.linearBytes; e.drop();
```

`restore` needs the exact same engine build the snapshot was written with. The wrapper's WASI imports (`wasiImports`) are five functions: clock, write, close, seek, random.

### 3.6 The C exports (`engine/shim.c`)

For a host in another language: `ai_init`, `ai_drop`, `ai_eval(src, len, name)`, `ai_call(fn, arg, len)`, `ai_resume`, `ai_run`, `ai_is_suspended`, `ai_snapshot(flags)` with `ai_snap_ptr/len/free`, `ai_restore(base, len, deltas[], lens[], n)`, `ai_base_id`, `ai_mem`, `ai_gc`, `ai_set_budget_ms`, `ai_set_budget_ticks`, `ai_set_memory_limit`, `ai_context_count`, `ai_malloc/free`, `ai_error`, `ai_result_ptr/len`. Statuses: 0 done, 1 suspended, 2 terminated, −1 error. Memory crosses as pointers into the module's linear memory; strings are UTF-8.

### 3.7 The store (`host/store.mjs`)

```js
const store = new Store("cells.sqlite", "cold/");   // coldDir optional
store.create(id, [engineLayerHash, harnessLayerHash]);
store.lease(id)                                      // → new epoch
store.enqueue(id, event, { sender: "ui", seq: 7 })   // false if duplicate or stale
store.pendingEvents(id)                              // [{ seq, event }]
store.commit(id, epoch, { checkpoint: { kind, bytes, metrics }, journal, outbox, processed })
store.chain(id) / store.coldChain(id)                // { base, deltas, length, fromLayers }
store.journal(id); store.body(hash); store.intern(value)
store.sizes(id); store.chainShape(id); store.metrics(id)
store.putLayer(kind, bytes); store.getLayer(hash)
```

`commit` throws `Fenced` when the epoch is stale and nothing is written. `chain` throws `Tampered` when an HMAC does not match. Journal rows are `{ kind: "event" | "call" | "rebuild_check" | "upgrade" | ..., data }`; result values are not in the journal but in `bodies`, referenced by hash.

### 3.8 The cell (`host/cell.mjs`)

```js
const cell = new Cell(store, broker, {
  foldEvery: 64, foldMinBytes: 65536, foldMaxApplyMs: 25,
  hotMaxLinear: 32 << 20, memoryLimit: 64 << 20, checkRebuild: true,
});
await cell.create(id);                     // from the shared layers: no eval
store.enqueue(id, { type: "user", text }); // any event source
const recs = await cell.drain(id);         // activations until the mailbox is empty
await cell.activate(id, { forceTier: "warm" | "cold" });   // one activation, tier forced
cell.evictHot(id);
await cell.rebuildCheck(id, engine);       // { ok, live, rebuilt, events, ms }
await cell.upgradeHarness(id, newSource);  // { from, to, checkpointBytes }
await cell.stats(id); await cell.peek(id, "$r1"); await cell.stateHash(id);
cell.metrics; cell.rebuilds;               // every activation and every rebuild check
```

The broker is anything with `call(agentId, callId, name, args) → Promise<value>` and an optional `settle()` the driver can await; it must answer by enqueueing a `result` event, which `Cell` does for it. `host/broker.mjs` is the in-process one with the scripted model and the content-keyed completion cache.

### 3.9 The Cloudflare cell (`cf/`)

```bash
./cf/sync.sh                                         # copy engine and harness into cf/src
npx wrangler@4 dev --config cf/wrangler.jsonc --local
npx wrangler@4 deploy --config cf/wrangler.jsonc
node cf/bench.mjs https://<worker-url> --turns 30 --label remote --agent a1 [--rep sync]
```

One Durable Object per agent, keyed by name. Endpoints under `/agents/:id/`:

| Path | Does |
|---|---|
| `create` | reset, restore nothing, boot the harness, write the base, replicate |
| `turn?t=N&tier=hot|warm|cold&rep=async|sync` | run one user turn: activations until a reply; `tier` applies to the first load; `rep=sync` replicates on the commit path |
| `restore?tier=warm|cold` | drop the hot copy and restore, with timings |
| `info` | head, chain bytes, DB size, hot or not, replication counts |
| `r2ping` | raw R2 latencies from the object |
| `reset` | delete storage and R2 objects for the agent |

The object's SQLite holds `head`, `checkpoints`, `blobs` (chunked under 2 MB), `journal`, `messages`. R2 holds `agents/<id>/<seq>-<kind>.bin` and `agents/<id>/head`, written by conditional put. In-object timings read zero for pure CPU because Workers freeze the clock; time from the client.

## 4. How it works

**An activation.** `pendingEvents` → `lease` (epoch + 1) → load the heap (hot, else warm from SQLite, else cold) → `__deliver` each event → `__drain` → decide the checkpoint kind → `snapshot` → one `commit` → send the outbox through the broker → maybe evict. Everything the heap sent is in the journal before anything leaves.

**Checkpoints.** Each is a delta relative to the previous one (`SNAPSHOT_CHAIN`), a few KB, or 30 KB when it carries a fetched payload. The engine's attached base moves forward with every chained checkpoint, so a restore is the base plus every delta after it, applied in order.

**Layers.** The engine layer (natives only) and the harness layer (the harness evaluated, as a chained delta) are written once and shared. A new agent is `restore(engineLayer, [harnessLayer])`. After an agent's first fold it has its own base.

**The fold.** When the deltas since the base outweigh it, after 64 deltas, or when the last restore applied slower than 25 ms, the next checkpoint is a full base and older rows are swept in the same transaction. The restore cost is bounded by this.

**The rebuild check.** At every fold the cell restores the layers into a scratch heap, replays the journal's events in order (results from `bodies`), and compares `__stateHash` with the live heap. The verdict is journaled. This is also the fallback when a checkpoint cannot load: the same replay produces a live agent, minus any script that was mid-flight. It costs a journal replay per fold; a production cell would sample.

**The harness upgrade.** `upgradeHarness` evaluates the new source over the live heap. The harness keeps all state on `globalThis.__agent`, so functions are replaced and state stays; `__migrate` adjusts the shape and bumps the version; a checkpoint follows. Later rebuilds replay the upgrade from the journal.

**Tiers.** Hot is the engine instance kept by the cell; it is dropped when wasm linear memory passes `hotMaxLinear`, because that memory never shrinks. Warm is the chain in SQLite, a few milliseconds. Cold is the home: files on Node, R2 on Cloudflare, two reads.

**The budget.** The interrupt handler asks the engine to suspend when the budget is hit; synchronous code called from the host suspends at a safepoint and can be checkpointed mid-loop (85 bytes per slice). Inside a promise job the stack is not suspendable, so after a grace period the job is terminated with a catchable error, which the harness reports as a tool error.

**Safety.** Scripts run in their own realm and only strings cross. Every layer and checkpoint carries an HMAC verified before the engine sees the bytes. Mailbox events are deduplicated per sender by sequence number. A per-agent memory limit makes runaway allocation a catchable error.

## 5. Changing things

- **The harness** is the product surface. Edit `heap/harness.js`; bump `HARNESS_VERSION`; put shape changes in `__migrate`. Keep state on `__agent`, keep every external effect a `hostCall`, and keep the state a function of the events, or the rebuild check will tell you.
- **The engine contract** is `engine/shim.c`; a change there needs a rebuild and makes old snapshots unloadable, so bump the layer by rebuilding and let agents rebuild from their journals.
- **A new host** needs: the five WASI imports, the C exports, a store with fenced commits, and a broker that answers calls with events. `cf/src/index.mjs` is the second host and is 300 lines.

## 6. Known limits

Strings and atoms in a base stay pinned until a fold. Script contexts are a pool of two, reused across scripts of the same agent. The rebuild check at every fold is for the prototype. On Node one process plays cell, gateway and broker. The Cloudflare cell replicates from inside the object, which holds it active while R2 answers; moving the upload to the Worker is the next step there.
