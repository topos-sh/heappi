# heappi

**[Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable) running unchanged inside a snapshottable QuickJS heap. The heap is the agent; chained heap checkpoints are the durability.**

- **Engine:** [dquickts](https://github.com/stencil-hq/dquickts), a QuickJS fork with heap snapshots and safepoint suspension, plus seven patches from this project: chained checkpoints (deltas against a base), tombstones, folds relative to an anchor layer, and applying checkpoints onto a live runtime. Compiled to a wasm32-wasi reactor, 1.7 MB.
- **Harness:** `@earendil-works/pi-durable` 1.4.0 bundled with esbuild (minified, 413 KB of source), evaluated inside the heap on a 20-line web-platform prelude and 150 lines of glue. Its model calls and tool calls go out as host calls; its `MemoryStorage` lives in the heap and is what the checkpoint carries. Pi's own replay journal and task graph ride along unused.
- **Host:** an engine, a lease and a journal. It delivers events, runs the heap until everything is parked on a host call, commits one checkpoint plus journal rows in one transaction, and emits the calls. It never parses agent state. Two hosts: Node with SQLite, and a Cloudflare Durable Object per agent with R2 as the cold store.
- **What that buys:** a park at any `await`, including inside a running script, in 2 to 25 KB; a restore in a few milliseconds on any machine with the same engine build; working memory as handles the model refers to without seeing; no replay and no determinism rule; an idle agent that is storage only.

A prototype, not a product. Benchmarked against Pi Durable on its own storage and against a transcript store; deployed as a demo on Cloudflare with a real model, MCP servers and agents that message each other.

## Where it comes from

| Source | What was taken |
|---|---|
| Smalltalk images | The idea: the program and its state are one file that resumes where it stopped. |
| dquickts (Stencil Labs) | `JS_WriteSnapshot` / `JS_ReadSnapshot`, base plus delta format, `JS_RequestSuspend` at safepoints, the host-call pattern of a promise whose resolver lives in the heap. Forked; nothing pushed upstream yet. |
| Pi Durable (Earendil Works) | Three uses: the vocabulary and discipline of the small benchmark harness (commit before anything is shown, tools as durable units, code mode as a nested call); the baseline, run as published; and the harness itself, embedded. |
| [Infinity](https://infinity.hydro.run/) (Hydro, UC Berkeley) | The slice discipline: an execution never blocks on anything external. One FIFO per agent, stable event ids, dedup by sender sequence. |
| [celld](https://celld.dev/) | A lease taken by a conditional write on object storage with epoch fencing; SQLite replicated to object storage as base plus increments. Became the R2 head object with `onlyIf` on its etag. |

## How it differs

The field makes agents durable in three ways: a journal of steps to replay (Temporal, Restate, DBOS, Cloudflare Workflows, Pi's code mode), a checkpoint of declared state at step boundaries (LangGraph, Infinity, Pi Durable turns, fx, Cloudflare Agents SDK), or a snapshot of a whole machine (E2B, Modal, Vercel Sandbox, CRIU). heappi stores an image of a language heap: it parks where a machine snapshot can, at the size a turn checkpoint has, with no replay. The price is that the checkpoint is an engine-specific binary rather than rows, so every engine or harness change needs a layer that stays deployable or a rebuild from the journal.

## Mechanics

**Activation.** Events from the mailbox go through `__deliver`; Pi's timers are pumped until nothing fires; `__drain` returns the outbox: host calls, mirrored Pi entries, progress records, replies. The checkpoint is written, then the calls are dispatched. Results come back as events. A model call, a tool call and a reply are three parks per step.

**Chained checkpoints.** A checkpoint holds the cells that changed since the previous one, plus tombstones for freed cells. A chain is read in order onto its base. Checkpoints are HMAC'd; a tampered one is refused.

**Layers and folds.** Two layers are built once per harness source and stored by content: the engine layer (natives only, 30 KB) and the harness layer (Pi evaluated, 1.6 MB). A new agent is a restore of the two, no eval. The engine keeps the harness layer as an *anchor*; a fold is a checkpoint relative to the anchor that replaces the chain, so the 1.6 MB layer stays shared forever and a fold with Pi inside is 48 to 139 KB. The writer folds when the chain outweighs the last fold or the last restore was slow.

**Template restore.** One idle instance per harness version holds the layers. A restore clones its wasm memory (1.9 ms for 12 MB) and applies only the agent's checkpoints onto the live runtime: the engine resets the changed cells in place and reads them again. Full restore is the fallback. On Cloudflare the layers are keyed by the hash of the harness source and stored once in R2, because Pi's bundle does not evaluate to the same bytes in every isolate.

**Code mode.** The `run` tool compiles the script in one of two script contexts inside the heap, with their own intrinsics. Only strings cross the boundary. `tools.<name>()` for every catalog tool including MCP and agent messaging, `remember()` and `mem[handle]` for working memory, `Promise.all` works, no nested scripts. A script awaiting a tool is parked with its frame. A script that never ends is terminated by the CPU budget after a grace period.

**The readable shadow.** Every event and call is journaled with bodies interned by hash, and every committed Pi entry is mirrored. At every fold the host rebuilds the agent from the layers plus the journal in a scratch heap and compares a canonical hash. A journal missing one event hashes differently.

## Benchmarks

All numbers are from this repo's scripts on an M-series Mac, Node 24, 28 Oct 2026, except the Cloudflare rows. The scenario is a research agent: 50 turns, each a model call, a code-mode script that fetches a 19 KB source and keeps it as a handle, a tool call, a reply. The model is scripted so every system gives identical answers; all got 50 of 50.

**Pi Durable inside the heap against the small harness** (`embed/pi-bench.mjs`):

| | Small harness | Pi Durable in the heap |
|---|---|---|
| Harness layer, stored once | 15 KB | 1.6 MB |
| Checkpoint on a park, median / max | 2.3 / 29.7 KB | 24 / 53 KB |
| Checkpoint on a reply | 0.4 KB | 15 KB |
| Written per turn, folds included | 70 KB | 199 KB |
| Live chain after 50 turns | 1.4 MB | 2.2 MB |
| Journal | 110 KB | 361 KB |
| Time per turn, rebuild check at every fold | 29 ms | 108 ms |
| Warm restore, full chain at turn 50 | 13.5 ms, 62 deltas | 25.5 ms, 26 deltas, 3.8 MB |
| Warm restore through the template, 8 turns in | 2.3 ms | 4 ms |
| Engine heap | 2.3 MB | 5.7 MB |
| Wasm linear memory after 50 turns | 15 MB | 29 MB |
| Model context, last request | 5 KB | 73 KB |

Pi costs about twice the disk, ten times the checkpoint bytes and three and a half times the time of the small harness, because every Pi step commits task, live and usage documents that the delta carries, and because Pi sends its whole transcript to the model where the small harness keeps a window. In return it is a complete harness.

**Against the other ways of storing the same agent** (small harness, `bench/run.mjs`, before the anchored folds):

| | Disk per agent | Model context, last request | Time per turn |
|---|---|---|---|
| Agent image | 1.52 MB | 5 KB | 32 ms, four parks and commits |
| Pi Durable on its own SQLite storage | 2.34 MB | whole transcript | 115 ms |
| Transcript store, payloads kept | 1.54 MB | 1.49 MB | 2.6 ms |
| Transcript store, payloads dropped | 64 KB | 33 KB | 0.4 ms |

The image holds the same information as the transcript that keeps its data, in the same space, and sends the model 300 times less. The transcript store is faster per turn because it only appends; the image pays for four snapshot-and-commit cycles, and half of its 32 ms is the rebuild check at every fold, which production would sample.

**Memory** (40 agents, 10 turns each): a hot image is 2.7 MB RSS, almost all wasm linear memory; the same loop in a `node:vm` context is 0.9 MB of V8 heap before the cost of a real isolate; a Pi Durable harness process open and idle is 62 MB, shared by many conversations in practice. A parked image is 0.

**Cloudflare** (Durable Object in SJC, R2 in WNAM, small harness, `cf/bench.mjs`): a hot turn with four parks is about 50 ms of work and 150 ms at the client including a 73 ms round trip; warm restore from the object's SQLite about 10 ms above the round trip; cold restore from R2 300 to 500 ms; one R2 operation from the object costs 100 to 500 ms, so replication is batched after the response. The demo with Pi inside and a real model: creating an agent 3.9 s the first time (layers built and put to R2 once), template restore after eviction with two checkpoints applied, a 37 KB MCP result filtered inside a script and passed to another tool while the model received 25 characters.

**What is not good:**

- Write amplification: 199 KB per turn with Pi inside for a few KB of real change. The fold cadence is the lever and it trades against restore latency.
- Wasm linear memory grows and never shrinks: 29 MB after 50 turns. The host drops the hot copy past a threshold and pays a warm restore.
- QuickJS is an interpreter, an order of magnitude or more behind V8. Scripts are glue; heavy work is a tool.
- In-object timings on Workers read zero for CPU work because the clock is frozen; the Cloudflare numbers are client-side.
- Each agent on Cloudflare also keeps a 1.7 MB copy of the layers in its own SQLite so a full restore needs no R2 read.
- The rebuild check is run at every fold here, which halves turn throughput; it exists to prove the journal is sufficient, not to run in production at that rate.

## Run it

```bash
npm install
cp .env.example .env         # ANTHROPIC_API_KEY
npm run app                  # Pi in the heap, Sonnet through pi-ai, MCP servers, signals, chat page at :8788
npm run app:demo             # same with the scripted model, no key
npm test                     # twelve checks: realm isolation, budget kill, rebuild hash, upgrade, tamper, dedup, eviction, memory limit, template restore
node --expose-gc bench/run.mjs --turns 50 --agents 3 --mem-agents 40 && node bench/report.mjs
node embed/pi-bench.mjs      # Pi in the heap against the small harness
```

Cloudflare: `./cf/sync.sh`, create an R2 bucket and name it in `cf/wrangler.demo.jsonc`, `npx wrangler@4 deploy --config cf/wrangler.demo.jsonc`, then `secret put` for `ANTHROPIC_API_KEY` and `DEMO_TOKEN`. The demo exposes `fetch`, `now`, `run`, `list_agents`, `send_to_agent`, a notes MCP server hosted in the Worker over R2, and the public Cloudflare docs and DeepWiki MCP servers; signals with a delay become Durable Object alarms. Rebuilding the engine needs wasi-sdk 25 and the dquickts fork; the binary is committed.

## Map

| Path | What |
|---|---|
| `engine/shim.c`, `engine/heappi.wasm` | The host contract: init, eval, call, resume, snapshot, restore, apply, anchor, CPU budget, memory limit, script contexts. |
| `embed/` | Pi Durable in the heap: `prelude.js`, `pi-glue.js`, `build.sh`, the Pi-versus-small benchmark, the restore benchmark. |
| `host/` | The Node cell: `engine.mjs`, `store.mjs` (SQLite, layers, HMAC, journal, mailbox), `cell.mjs` (activation, folds, templates, rebuild check, upgrade), `tests.mjs`. |
| `cf/` | `src/cell.mjs` the Durable Object base, `src/demo.mjs` the demo Worker, `src/index.mjs` the benchmark Worker, `bench.mjs`. |
| `app/` | The Node chat environment: pi-ai gateway, MCP clients, signals, the page shared with the Cloudflare demo. |
| `heap/harness.js` | The small harness: 300 lines, same host contract, used by the benchmarks. |
| `baselines/`, `bench/` | Pi Durable as published and a transcript store on the same scenario; the benchmark and its report. |

## Open problems

The journal as Pi's storage adapter instead of a mirror, so there is one copy of the transcript. Releasing strings and atoms pinned in the anchor. One script context per script instead of a pool of two. WebSocket hibernation on the Cloudflare cell (SSE keeps the object alive while a page is open). Encryption at rest. Publishing the engine fork.

## License

MIT. [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) covers QuickJS, dquickts and Pi.
