// Wrapper over engine/agent-image.wasm (a wasm32-wasi reactor), runtime
// agnostic: give it an instantiated module. One Engine = one instance = one
// heap. Node loading lives in engine-node.mjs; Workers use cf/src/wasi.mjs.

export const SNAPSHOT_BASE = 1 << 0;
export const SNAPSHOT_DELTA = 1 << 1;
export const SNAPSHOT_CHAIN = 1 << 2;

const enc = new TextEncoder();
const dec = new TextDecoder();

/** the five WASI imports the engine needs, over a memory getter */
export function wasiImports(getMemory, log = (s) => console.error(s)) {
  const u8 = () => new Uint8Array(getMemory().buffer);
  const dv = () => new DataView(getMemory().buffer);
  let pending = "";
  return { wasi_snapshot_preview1: {
    clock_time_get(_id, _precision, out) { dv().setBigUint64(out, BigInt(Math.round(nowNs())), true); return 0; },
    fd_write(fd, iovs, iovsLen, nwritten) {
      let n = 0; const d = dv(); const u = u8();
      for (let i = 0; i < iovsLen; i++) { const p = d.getUint32(iovs + 8 * i, true), l = d.getUint32(iovs + 8 * i + 4, true); pending += dec.decode(u.subarray(p, p + l)); n += l; }
      d.setUint32(nwritten, n, true);
      let nl; while ((nl = pending.indexOf("\n")) >= 0) { log(pending.slice(0, nl)); pending = pending.slice(nl + 1); }
      return 0;
    },
    fd_close() { return 0; },
    fd_seek() { return 8; },
    random_get(ptr, len) { crypto.getRandomValues(u8().subarray(ptr, ptr + len)); return 0; },
  } };
}
function nowNs() { return (typeof performance !== "undefined" ? performance.timeOrigin + performance.now() : Date.now()) * 1e6; }

export class Engine {
  /** instantiate synchronously from a compiled module (Workers, or Node after compile) */
  static fromModule(mod, log) {
    let inst;
    const imports = wasiImports(() => inst.exports.memory, log);
    inst = new WebAssembly.Instance(mod, imports);
    inst.exports._initialize();
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
    return rc === 1 ? "suspended" : rc === 2 ? "terminated" : "done";
  }

  init() {
    if (this.x.ai_init() !== 0) throw new Error(`init: ${this.error()}`);
    this.live = true;
  }
  drop() { if (this.live) { this.x.ai_drop(); this.live = false; } }

  setBudgetMs(ms) { this.x.ai_set_budget_ms(ms); }
  setBudgetTicks(n) { this.x.ai_set_budget_ticks(n); }
  setMemoryLimit(bytes) { this.x.ai_set_memory_limit(bytes); }
  contextCount() { return this.x.ai_context_count(); }
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
