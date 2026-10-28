// Node wrapper over engine/agent-image.wasm (a wasm32-wasi reactor).
// One Engine = one wasm instance = one heap. The module is compiled once.
import { readFile } from "node:fs/promises";
import { WASI } from "node:wasi";
import { fileURLToPath } from "node:url";

export const SNAPSHOT_BASE = 1 << 0;
export const SNAPSHOT_DELTA = 1 << 1;
export const SNAPSHOT_CHAIN = 1 << 2;

const WASM_PATH = fileURLToPath(new URL("../engine/agent-image.wasm", import.meta.url));
let compiled; // Promise<WebAssembly.Module>

export function compileEngine() {
  compiled ??= readFile(WASM_PATH).then((b) => WebAssembly.compile(b));
  return compiled;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

export class Engine {
  static async create() {
    const mod = await compileEngine();
    const wasi = new WASI({ version: "preview1", args: ["agent-image"], env: {}, returnOnExit: false });
    const inst = await WebAssembly.instantiate(mod, wasi.getImportObject());
    wasi.initialize(inst);
    return new Engine(inst);
  }

  constructor(inst) {
    this.inst = inst;
    this.x = inst.exports;
    this.live = false;
  }

  get memory() { return this.x.memory; }
  /** bytes of wasm linear memory currently reserved by this instance */
  get linearBytes() { return this.x.memory.buffer.byteLength; }

  #u8() { return new Uint8Array(this.x.memory.buffer); }

  #putString(s) {
    const bytes = enc.encode(s);
    const ptr = this.x.ai_malloc(bytes.length + 1);
    this.#u8().set(bytes, ptr);
    this.#u8()[ptr + bytes.length] = 0;
    return { ptr, len: bytes.length };
  }
  #putBytes(b) {
    const ptr = this.x.ai_malloc(b.length || 1);
    this.#u8().set(b, ptr);
    return ptr;
  }
  #cstr(ptr) {
    if (!ptr) return "";
    const u = this.#u8();
    let end = ptr;
    while (u[end] !== 0) end++;
    return dec.decode(u.subarray(ptr, end));
  }
  error() { return this.#cstr(this.x.ai_error()); }
  result() {
    const ptr = this.x.ai_result_ptr();
    const len = this.x.ai_result_len();
    if (!ptr || !len) return undefined;
    return dec.decode(this.#u8().subarray(ptr, ptr + len));
  }
  #status(rc, what) {
    if (rc < 0) throw new Error(`${what}: ${this.error()}`);
    return rc === 1 ? "suspended" : "done";
  }

  init() {
    if (this.x.ai_init() !== 0) throw new Error(`init: ${this.error()}`);
    this.live = true;
  }
  drop() { if (this.live) { this.x.ai_drop(); this.live = false; } }

  setBudgetMs(ms) { this.x.ai_set_budget_ms(ms); }
  ticks() { return this.x.ai_ticks(); }

  /** evaluate a script in the global scope; returns "done" | "suspended" */
  eval(src, name = "<eval>") {
    const s = this.#putString(src), n = this.#putString(name);
    const rc = this.x.ai_eval(s.ptr, s.len, n.ptr);
    this.x.ai_free(s.ptr); this.x.ai_free(n.ptr);
    return this.#status(rc, "eval");
  }
  /** call a global function with one string argument; returns { status, result } */
  call(fn, arg) {
    const f = this.#putString(fn);
    const a = arg === undefined ? null : this.#putString(arg);
    const rc = this.x.ai_call(f.ptr, a ? a.ptr : 0, a ? a.len : 0);
    this.x.ai_free(f.ptr); if (a) this.x.ai_free(a.ptr);
    const status = this.#status(rc, `call ${fn}`);
    return { status, result: status === "done" ? this.result() : undefined };
  }
  resume() {
    const rc = this.x.ai_resume();
    const status = this.#status(rc, "resume");
    return { status, result: status === "done" ? this.result() : undefined };
  }
  isSuspended() { return !!this.x.ai_is_suspended(); }
  run() { return this.#status(this.x.ai_run(), "run"); }

  /** returns a copy of the snapshot bytes */
  snapshot(flags) {
    if (this.x.ai_snapshot(flags) !== 0) throw new Error(`snapshot: ${this.error()}`);
    const ptr = this.x.ai_snap_ptr(), len = this.x.ai_snap_len();
    const out = new Uint8Array(len);
    out.set(this.#u8().subarray(ptr, ptr + len));
    this.x.ai_snap_free();
    return out;
  }

  /** restore base + chained deltas into a fresh runtime inside this instance */
  restore(base, deltas = []) {
    const bptr = this.#putBytes(base);
    const n = deltas.length;
    const ptrs = deltas.map((d) => this.#putBytes(d));
    const arr = this.x.ai_malloc(4 * Math.max(n, 1));
    const lens = this.x.ai_malloc(4 * Math.max(n, 1));
    const dv = new DataView(this.x.memory.buffer);
    for (let i = 0; i < n; i++) {
      dv.setUint32(arr + 4 * i, ptrs[i], true);
      dv.setUint32(lens + 4 * i, deltas[i].length, true);
    }
    const rc = this.x.ai_restore(bptr, base.length, arr, lens, n);
    this.x.ai_free(bptr); ptrs.forEach((p) => this.x.ai_free(p));
    this.x.ai_free(arr); this.x.ai_free(lens);
    if (rc !== 0) throw new Error(`restore: ${this.error()}`);
    this.live = true;
  }

  baseId() { this.x.ai_base_id(); return this.result(); }
  mem() { this.x.ai_mem(); return JSON.parse(this.result()); }
  gc() { this.x.ai_gc(); }
}
