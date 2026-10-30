# heappi

**[Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable) running unchanged inside a snapshottable QuickJS heap. The heap is the agent; chained heap checkpoints are the durability.**

- **Engine:** [dquickts](https://github.com/stencil-hq/dquickts), a QuickJS fork with heap snapshots and safepoint suspension, plus seven patches from this project (`engine/patches/`): chained checkpoints (deltas against a base), tombstones, folds relative to an anchor layer, and applying checkpoints onto a live runtime. Compiled to a wasm32-wasi reactor, 1.7 MB.
- **Harness:** `@earendil-works/pi-durable` 1.4.0 bundled with esbuild (minified, 413 KB of source), evaluated inside the heap on a 20-line web-platform prelude and 150 lines of glue. Its model calls and tool calls go out as host calls; its `MemoryStorage` lives in the heap and is what the checkpoint carries. Pi's own replay journal and task graph ride along unused.
- **Host:** an engine, a lease and a journal. It delivers events, runs the heap until everything is parked on a host call, commits one checkpoint plus journal rows in one transaction, and emits the calls. It never parses agent state. Two hosts: Node with SQLite, and a Cloudflare Durable Object per agent with R2 as the cold store.
- **What that buys:** a park at any `await`, including inside a running script, in 2 to 25 KB; a restore in a few milliseconds on any machine with the same engine build; working memory as handles the model refers to without seeing; no replay and no determinism rule; an idle agent that is storage only.

A prototype, not a product. Benchmarked against Pi Durable on its own storage and against a transcript store; deployed as a demo on Cloudflare with a real model, MCP servers and agents that message each other.

## Run it

Locally, with a real model (Node 24):

```bash
npm install
cp .env.example .env      # put ANTHROPIC_API_KEY in it
npm run app               # chat page at http://localhost:8788
```

`npm run app:demo` runs the same page with a scripted model and no key. The page has the conversation with streamed replies and tool calls, a panel with the agent's state (in memory or parked, checkpoint chain size, last restores), an "evict" button to force the next message through a restore, and a form for external signals with an optional delay.

On Cloudflare:

```bash
./cf/sync.sh
npx wrangler@4 r2 bucket create <bucket>     # then name it in cf/wrangler.jsonc
npx wrangler@4 deploy --config cf/wrangler.jsonc
npx wrangler@4 secret put ANTHROPIC_API_KEY --config cf/wrangler.jsonc
npx wrangler@4 secret put DEMO_TOKEN --config cf/wrangler.jsonc     # the page asks for it once
```

One Durable Object per agent, R2 as the cold store, alarms for delayed signals. Tools in the demo: `fetch`, `now`, `run`, `list_agents`, `send_to_agent`, a notes MCP server hosted in the Worker over R2, and the public Cloudflare docs and DeepWiki MCP servers.

`npm test` runs the checks; the benchmark scripts are in `bench/`, `baselines/` and `embed/`. Rebuilding the engine needs wasi-sdk 25 and dquickts with the seven patches in `engine/patches/` (instructions there); the binary is committed.

## How it differs

The field makes agents durable in three ways: a journal of steps to replay (Temporal, Restate, DBOS, Cloudflare Workflows, Pi's code mode), a checkpoint of declared state at step boundaries (LangGraph, Infinity, Pi Durable turns, fx, Cloudflare Agents SDK), or a snapshot of a whole machine (E2B, Modal, Vercel Sandbox, CRIU). heappi stores an image of a language heap: it parks where a machine snapshot can, at the size a turn checkpoint has, with no replay. The price is that the checkpoint is an engine-specific binary rather than rows, so every engine or harness change needs a layer that stays deployable or a rebuild from the journal.

## Benchmarks

Same scenario on each system: 50 turns, each a model call, a script that fetches a 19 KB source and keeps it as working memory, a tool call, a reply. The model is scripted. Scripts in `baselines/`; each writes its raw numbers to `bench/out/`.

| | heappi, Pi inside the heap | Pi Durable on its own SQLite storage | LangGraph with its SQLite checkpointer |
|---|---|---|---|
| Durability unit | heap checkpoint at any `await`, inside a script too | turn checkpoint; scripts replayed from a journal | state checkpoint after every graph node |
| Disk after 50 turns | 2.6 MB, plus a 1.6 MB layer shared by all agents | 2.5 MB | 170 MB |
| One durable write | 24 KB median, 53 KB max | entries and document revisions per step | the whole state, 0.7 to 1.4 MB |
| Resume | 4 to 25 ms, depending on checkpoints since the last fold | 5 ms to open, 175 ms for the next turn | 15 ms to load, 93 ms for the next turn |
| Time per turn | 108 ms, half of it a journal rebuild check at every fold | 115 ms | 47 ms |
| Model context at turn 50 | 75 KB | 78 KB | 27 KB |

Notes:

- Pi on its own storage fails from turn 40: its code-mode `store()` caps at 1 MB of JSON, and the fetched sources fill it. Inside the heap the same data lives in handles with no cap.
- LangGraph's SQLite checkpointer serializes the whole state at every superstep, 250 times here. Its Postgres checkpointer stores channels by version and would not. Its context is small because the payload sits in a state channel the model does not see.
- heappi pays for four checkpoint-and-commit cycles per turn and for the rebuild check, which proves the journal is sufficient and would be sampled in production. Write amplification is the real cost: about 200 KB per turn with Pi inside, since every Pi step commits task, live and usage documents.
- Wasm linear memory grows to about 30 MB per hot agent after 50 turns and never shrinks; the host evicts past a threshold and pays a warm restore. Parked agents cost nothing.
- On Cloudflare: a hot turn is tens of milliseconds of work in the object, warm restore about 10 ms, cold restore from R2 300 to 500 ms, so R2 replication is batched after the response.

## Where it comes from

| Source | What was taken |
|---|---|
| Smalltalk images | The idea: the program and its state are one file that resumes where it stopped. |
| dquickts (Stencil Labs) | `JS_WriteSnapshot` / `JS_ReadSnapshot`, base plus delta format, `JS_RequestSuspend` at safepoints, the host-call pattern of a promise whose resolver lives in the heap. Forked; nothing pushed upstream yet. |
| Pi Durable (Earendil Works) | Three uses: the vocabulary and discipline of the small benchmark harness (commit before anything is shown, tools as durable units, code mode as a nested call); the baseline, run as published; and the harness itself, embedded. |
| [Infinity](https://infinity.hydro.run/) (Hydro, UC Berkeley) | The slice discipline: an execution never blocks on anything external. One FIFO per agent, stable event ids, dedup by sender sequence. |
| [celld](https://celld.dev/) | A lease taken by a conditional write on object storage with epoch fencing; SQLite replicated to object storage as base plus increments. Became the R2 head object with `onlyIf` on its etag. |

## Map

| Path | What |
|---|---|
| `engine/shim.c`, `engine/heappi.wasm` | The host contract: init, eval, call, resume, snapshot, restore, apply, anchor, CPU budget, memory limit, script contexts. |
| `embed/` | Pi Durable in the heap: `prelude.js`, `pi-glue.js`, `build.sh`, the Pi-versus-small benchmark, the restore benchmark, `pi-tests.mjs`. |
| `host/` | The Node cell: `engine.mjs`, `store.mjs` (SQLite, layers, HMAC, journal, mailbox), `cell.mjs` (activation, folds, templates, rebuild check, upgrade), `tests.mjs`. |
| `cf/` | The Cloudflare Worker: `src/cell.mjs` the cell as a Durable Object, `src/demo.mjs` the demo on it, `src/gateway.mjs`, `src/mcp.mjs`. |
| `app/` | The Node chat environment: pi-ai gateway, MCP clients, signals, the page shared with the Cloudflare demo. |
| `heap/harness.js` | The small harness: 300 lines, same host contract, used by the benchmarks. |
| `baselines/`, `bench/` | Pi Durable as published and a transcript store on the same scenario; the benchmark and its report. |

## Mechanics

**Activation.** Events from the mailbox go through `__deliver`; Pi's timers are pumped until nothing fires; `__drain` returns the outbox: host calls, mirrored Pi entries, progress records, replies. The checkpoint is written, then the calls are dispatched. Results come back as events. A model call, a tool call and a reply are three parks per step.

**Chained checkpoints.** A checkpoint holds the cells that changed since the previous one, plus tombstones for freed cells. A chain is read in order onto its base. Checkpoints are HMAC'd; a tampered one is refused.

**Layers and folds.** Two layers are built once per harness source and stored by content: the engine layer (natives only, 30 KB) and the harness layer (Pi evaluated, 1.6 MB). A new agent is a restore of the two, no eval. The engine keeps the harness layer as an *anchor*; a fold is a checkpoint relative to the anchor that replaces the chain, so the 1.6 MB layer stays shared forever and a fold with Pi inside is 48 to 139 KB. The writer folds when the chain outweighs the last fold or the last restore was slow.

**Template restore.** One idle instance per harness version holds the layers. A restore clones its wasm memory (1.9 ms for 12 MB) and applies only the agent's checkpoints onto the live runtime: the engine resets the changed cells in place and reads them again. Full restore is the fallback. On Cloudflare the layers are keyed by the hash of the harness source and stored once in R2, because Pi's bundle does not evaluate to the same bytes in every isolate.

**Code mode.** The `run` tool compiles the script in one of two script contexts inside the heap, with their own intrinsics. Only strings cross the boundary. `tools.<name>()` for every catalog tool including MCP and agent messaging, `remember()` and `mem[handle]` for working memory, `Promise.all` works, no nested scripts. A script awaiting a tool is parked with its frame. A script that never ends is terminated by the CPU budget after a grace period.

**Crashes.** Every catalog tool says whether it may run twice (`replay: "safe"` or `"unsafe"`, Pi's own word; MCP tools take it from their `readOnlyHint` and `idempotentHint`). A call is marked as sent before it starts and done when it answers, so after a restart `Cell.recover` finds the calls that were cut off. A model call or a safe tool runs again under the same idempotency key, `<agent>:<call id>`, which reaches MCP servers as `_meta["heappi/idempotency-key"]`. Any other tool is never run again: the model gets an `interrupted` error and decides. The key is what makes `send_to_agent` safe, because the recipient drops a repeat. The heap needs no help with any of this, since a checkpoint and its outbox commit together. A tool that may not run twice also gets a tool round of its own (Pi's `executionMode: "sequential"`).

**Steering and stop.** A message sent while the agent works becomes a `steer` event, Pi's `whenBusy: "steer"`, and joins the run after the current tool round. `cancel` aborts what the host has out, answers the heap's pending calls at once, calls Pi's `abort`, and leaves a note in the transcript so the next message does not answer the stopped request. An input may carry a `requestId`, Pi's own idempotency key: a repeat gets no second answer.

**Tokens.** With more than eight catalog tools, MCP tools are `deferred` and the model finds them through Pi's `tool_search`; scripts can call them without loading. The catalog is sorted, so the start of a prompt stays the same across restarts and the provider's cache holds. In a three-message chat with 25 tools, the first request fell from 5,227 to 1,284 prompt tokens and the six requests together from 33,661 to 19,723.

**The readable shadow.** Every event and call is journaled with bodies interned by hash, and every committed Pi entry is mirrored. At every fold the host rebuilds the agent from the layers plus the journal in a scratch heap and compares a canonical hash. A journal missing one event hashes differently.

## Interfaces

**The heap contract.** The host knows six globals the harness defines, strings in and out: `__deliver(eventJson)`, `__drain()` returning the outbox as JSON, `__stats()`, `__stateHash()` for the rebuild check, `__migrate()` after a harness upgrade, `__peek(handle)` for debugging. Events in: `{type:"user", text, requestId?}`, `{type:"steer", text, requestId?}`, `{type:"cancel"}`, `{type:"signal", name, payload}`, `{type:"config", config}`, `{type:"result", id, value | error}`. Outbox out: `{kind:"call", id, name:"model"|"tool", args}`, `{kind:"reply", turn, text, cancelled?}`, `progress` (tool starts and ends, `busy`, `steered`), `pi.entry`, `error`. A call is answered by a result event with the same id in a later activation; the heap keeps the pending promise, the host keeps nothing. Any harness that implements this runs in the cell; Pi does through `embed/pi-glue.js`, the small harness natively.

**What a script sees.** Four names and nothing else: `tools.<name>(args)` for any catalog tool (`run` refused), `mem[handle]` and `remember(value, label)` for working memory, `log(...)`. Only strings cross the boundary; results come back as JSON copies.

**The demo API** (the same on Node and Cloudflare; on Cloudflare behind the token):

| | |
|---|---|
| `GET/POST /api/agents` | list agents; create one `{id?, name?}` |
| `POST /api/agents/:id/messages {text, requestId?, mode?}` | a user turn; `mode: "steer"` joins the running one |
| `POST /api/agents/:id/cancel` | stop the running turn |
| `POST /api/agents/:id/signals {name, payload, afterMs?, key?}` | an external signal, now or after a delay; on Cloudflare a repeated `key` is dropped |
| `GET /api/agents/:id/events` | SSE: deltas, tool starts and ends, replies, activations |
| `/api/agents/:id/transcript`, `info`, `evict`, `rename`, `DELETE` | read, inspect, drop the hot copy, rename, remove |
| `POST /mcp/notes` | the notes MCP server, for outside clients |

**Adding a tool:** on Node, a case in the broker in `app/server.mjs`; on Cloudflare, in `DemoCell.broker` in `cf/src/demo.mjs`, plus a catalog entry with a JSON schema and `replay: "safe"` if running it twice is harmless. **Adding an MCP server:** a line in `app/mcp.json` (stdio or streamable HTTP) or in `mcpFor()` in `cf/src/demo.mjs` (`{url}` or `{handler}`). Tools reach the heap as `mcp__<server>__<tool>`.

## Open problems

The journal as Pi's storage adapter instead of a mirror, so there is one copy of the transcript. Scripts that run several unsafe tools with `Promise.all` are not kept apart. A Cloudflare object evicted mid-call recovers on the next message, signal or opened page, not by itself. Agents created before harness 102 keep their old harness and do not understand steer or cancel. Releasing strings and atoms pinned in the anchor. One script context per script instead of a pool of two. WebSocket hibernation on the Cloudflare cell (SSE keeps the object alive while a page is open). Encryption at rest. Publishing the engine fork.

## License

MIT. [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) covers QuickJS, dquickts and Pi.
