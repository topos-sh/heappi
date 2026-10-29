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

The same scenario on the closest systems from the list above, run from this repository on an M-series Mac, Node 24, 28 Oct 2026. The scenario is a research agent: 50 turns, each one a model call, a code-mode script (or the system's equivalent) that fetches a 19 KB source and keeps it as working memory, a tool call, a reply. The model is scripted, so every system should give identical answers, and the answers are checked. Systems that could not be run locally are marked.

| | heappi, Pi inside the heap | Pi Durable 1.4 on its SQLite storage | LangGraph 1.5 + SqliteSaver | DBOS 5.4 on Postgres | Transcript store (SQLite) |
|---|---|---|---|---|---|
| Family | heap image | turn checkpoints, scripts replayed | state checkpoint per superstep | journal of steps, replay | conversation rows |
| Where it can park | any `await`, inside a script | between turns | between graph nodes | between steps | between messages |
| Correct turns | 50 / 50 | **39 / 50** | 50 / 50 | 50 / 50 | 50 / 50 |
| Disk after 50 turns | 2.2 MB chain + 0.4 MB journal; 1.6 MB harness layer shared by all agents | 2.5 MB | **170 MB** | 1.4 MB of step outputs (1.5 MB on disk) | 1.5 MB kept, 64 KB if payloads are dropped |
| One durable write | 24 KB median park, 53 KB max, 15 KB reply | document revisions and entries per step | the whole state: 674 KB median, 1.35 MB by turn 50 | the step's output, the payload once | one message row |
| Resume after a restart | 4 ms through the template early on; 25 ms at turn 50 with 26 checkpoints since the last fold | 4.6 ms to open; next turn 175 ms | 15 ms to load the last checkpoint; next turn 93 ms | 1.1 s to launch and find the workflow, then 141 ms to replay 49 turns | one query |
| Time per turn | 108 ms with a journal rebuild check at each of ten folds; about 60 ms without | 115 ms | 47 ms | 21 ms | 2.6 ms |
| Model context at turn 50 | 75 KB | 78 KB | 27 KB | 26 KB | 1.49 MB kept, 33 KB dropped |

What the table says, honestly:

- **Pi Durable on its own storage answered wrong from turn 40**, in the earlier benchmark too. Pi's `store()` for script data is capped at 1 MB of JSON; 40 sources fill it, every later script throws, and the answer becomes -1. The earlier benchmark only checked the image's answers and reported Pi as correct; that was wrong. Inside the heap, Pi's data lives in heappi's handles, which have no such cap.
- **LangGraph's SQLite checkpointer writes the whole state at every superstep**, 250 times for 50 turns, which is where 170 MB comes from. The payload sits in a state channel the model does not see, so its context is small; the cost moved to disk. The Postgres checkpointer stores channel values by version and would not rewrite unchanged channels; the SQLite one does.
- **DBOS is the cheapest per turn and the smallest on disk** because it journals only step outputs and keeps the agent's state in workflow memory. The price is in the resume: a restart replays every step from the journal, and the workflow code between steps must be deterministic. Replay here is fast because the steps are cheap stubs; the time grows with history.
- **The transcript store wins on speed** because it only appends rows. It either sends the model 1.5 MB per request or forgets what it fetched.
- **heappi is in the middle on disk and slow per turn** because a turn is four checkpoint-and-commit cycles, and because the rebuild check runs at every fold here to prove the journal is sufficient; production would sample. What it alone does is the first row: it parks anywhere, with the script's stack, and the model context stays flat without dropping the data.
- Pi Durable and heappi send the model the full transcript; LangGraph and DBOS send the messages without the payload for the same reason heappi does, by reference. The transcript numbers are the ends of the range.
- **Not measured:** Infinity (no public runtime), Temporal and Restate (need their servers; same family as DBOS), Cloudflare Workflows and Agents SDK (the Agents SDK stores state as rows in the object's SQLite, the transcript store's shape), and microVM snapshots (E2B, Modal, Vercel Sandbox: a snapshot is the guest memory, hundreds of MB, and restore is hundreds of milliseconds to seconds per the vendors' documentation).

**Cloudflare** (Durable Object in SJC, R2 in WNAM, `cf/bench.mjs`): a hot turn with four parks is about 50 ms of work and 150 ms at the client including a 73 ms round trip; warm restore from the object's SQLite about 10 ms above the round trip; cold restore from R2 300 to 500 ms; one R2 operation from the object costs 100 to 500 ms, so replication is batched after the response. In-object CPU timings read zero because Workers freeze the clock.

**Memory**: a hot heappi agent with Pi inside holds 5.7 MB of engine heap in 29 MB of wasm linear memory after 50 turns, because linear memory grows for snapshot buffers and never shrinks; the host drops the hot copy past a threshold and pays a warm restore. A parked agent is zero. A Pi Durable harness process open and idle is 62 MB, shared by many conversations in practice.

**Other costs:** 204 KB written per turn with Pi inside for a few KB of real change, because every Pi step commits task, live and usage documents that the delta carries; the fold cadence is the lever and it trades against restore latency. QuickJS is an interpreter, an order of magnitude or more behind V8; scripts are glue and heavy work is a tool. On Cloudflare each agent also keeps a 1.7 MB copy of the layers in its own SQLite so that a full restore needs no R2 read.

Scripts: `node embed/pi-bench.mjs` (heappi), `node baselines/pi-resume.mjs` (Pi), `node baselines/langgraph.mjs` (LangGraph), `DBOS_SYSTEM_DATABASE_URL=postgresql://... node baselines/dbos.mjs` (DBOS, needs a Postgres), `node bench/run.mjs` (transcript store and the memory table). Raw outputs are in `bench/out/` and `embed/out/`; every number above is in

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
