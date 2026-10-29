# heappi

**An AI agent that is a saved program, not a saved conversation.**

heappi runs each agent inside its own small JavaScript heap. Between messages the whole heap, including the agent loop, its tools, any script that is halfway through running, and the data it is holding, is written to storage as a checkpoint of a few kilobytes. When the next message arrives, any machine can load that checkpoint and the agent continues from the exact instruction where it stopped. While it waits, it uses no memory and no CPU.

## Why

Most agent runtimes save a transcript and rebuild the agent from it on every message. That has three costs:

- **Everything the agent learned goes back through the model.** A 20 KB tool result stays in the prompt forever, or is thrown away.
- **Code that the agent is running cannot pause.** A tool call in the middle of a script means either keeping a process alive or replaying the script later, which requires the script to be deterministic.
- **An idle agent is either a running process or a cold start.**

heappi keeps the agent's state as live objects instead. A tool result becomes a handle the model can refer to without seeing the data. A script that awaits a tool call is checkpointed with its stack and resumes later, anywhere, with no replay. A parked agent costs storage only, and a wake-up is a few milliseconds.

The harness inside the heap is [Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable), unchanged. The engine is [QuickJS](https://bellard.org/quickjs/) through the [dquickts](https://github.com/stencil-hq/dquickts) fork, compiled to WebAssembly, with a patch that writes checkpoints as chained deltas.

## How a turn runs

1. An event lands in the agent's mailbox: a user message, a tool result, a timer, a message from another agent.
2. The host loads the heap if it is not already in memory. A **restore** clones a shared template that holds the engine and the harness, then applies only this agent's checkpoints. Warm restore is about 4 ms.
3. The event is delivered into the heap. The agent runs until every runnable piece of code is waiting on something outside: a model call, a tool call. This is a **park**.
4. The heap is checkpointed. A checkpoint holds only what changed since the last one, typically 2 to 25 KB. Now and then the host writes a **fold**, a checkpoint relative to the shared template that replaces the chain so far, and sweeps the rest.
5. The pending calls go out. Their results come back as new events, and the cycle repeats. A reply to the user is one of the things the heap emits.

Large tool results never enter the prompt. The model receives a **handle** with a digest, and can run a script that works on the data by handle, calls other tools with it, and returns only what matters.

## Try it

**Locally, with a real model** (Node 24):

```bash
npm install
cp .env.example .env     # put your ANTHROPIC_API_KEY in it
npm run app              # http://localhost:8788
```

`npm run app:demo` runs the same page with a scripted model and no key.

**On Cloudflare** (one Durable Object per agent, R2 as the cold store):

```bash
./cf/sync.sh
npx wrangler@4 r2 bucket create <your-bucket>      # then put its name in cf/wrangler.demo.jsonc
npx wrangler@4 deploy --config cf/wrangler.demo.jsonc
npx wrangler@4 secret put ANTHROPIC_API_KEY --config cf/wrangler.demo.jsonc
npx wrangler@4 secret put DEMO_TOKEN --config cf/wrangler.demo.jsonc   # the page asks for this once
```

The page shows the conversation, streamed replies, tool calls, and a panel with the agent's state: whether it is in memory or parked, the size of its checkpoint chain, and the last restores. Try sending a message, then "Evict hot copy", then another message: the agent comes back from storage and continues. Send an external signal with a delay to see a timer wake it up. Create two agents: they can message each other.

Tools in the demo: `fetch`, `now`, `list_agents`, `send_to_agent`, a `run` tool for scripts, and three MCP servers: a shared notebook on R2, the Cloudflare docs, and DeepWiki.

## What is in the repository

| Path | What |
|---|---|
| `engine/` | The WebAssembly engine (committed binary) and the C shim that is the host's contract with the heap. |
| `embed/` | Pi Durable bundled to run inside the heap, with 20 lines of web platform and 150 lines of glue. |
| `host/` | The Node cell: SQLite as the warm store, layers, folds, template restore, journal, rebuild check. |
| `cf/` | The same cell as a Cloudflare Durable Object, the demo Worker, and the benchmark Worker. |
| `app/` | The chat environment on Node: pi-ai gateway, MCP servers, signals, the page. |
| `heap/` | A small alternative harness, used by the benchmarks. |
| `bench/`, `baselines/` | Benchmarks against Pi Durable on its own storage and against a transcript store. |

[USAGE.md](USAGE.md) describes every interface.

## Numbers

Measured on Node, 50 turns per agent, with Pi Durable inside the heap.

| | |
|---|---|
| Checkpoint per park | 24 KB median |
| Shared harness layer, stored once | 1.6 MB |
| Warm restore | 4 ms |
| Model context after 50 turns | 73 KB, against 1.5 MB for a transcript that keeps the same data |
| Idle agent | storage only |

## What it is not

- **Not fast at compute.** QuickJS is an interpreter. Heavy work belongs in a tool, not in a script.
- **Not readable state.** A checkpoint loads only into the same engine build. The journal of events beside it is the readable copy, and the agent can be rebuilt from it; the host checks that at every fold.
- **Not shareable.** One heap is one agent. Agents share things through tools and messages.
- **Not production.** A prototype with a patched fork of a fork, no encryption at rest, and one demo deployment.

## License

MIT. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for QuickJS, dquickts and Pi.
