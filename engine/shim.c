/*
 * Host shim for the agent image: a wasm32-wasi reactor exporting the
 * host operations of the design (deliver / run / snapshot / restore)
 * over the dquickts-chain engine.
 *
 * The host never parses an agent's state. It calls global functions the
 * heap defines (`__deliver(json)`, `__drain()`), reads the strings they
 * return, and moves checkpoint bytes in and out. Everything else, the
 * agent loop included, is JavaScript inside the heap.
 *
 * Contexts: context 0 is the harness. A small pool of script contexts
 * (own global object, own intrinsics) exists for code-mode scripts; the
 * harness compiles a script there with __evalIn(index, source) and only
 * ever hands it JSON strings and realm-local wrappers, so a script cannot
 * reach the harness realm. The pool is never freed (the snapshot reader
 * rejects a freed context), so a context is reused across scripts.
 *
 * CPU budget: at the budget the interrupt handler asks for a suspension
 * (checkpointable, resumed later). If the stack is not suspendable, e.g.
 * inside a promise job, the handler terminates the execution after a
 * grace period with an uncatchable error; the host then reports
 * "terminated" and the harness fails the running script.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "quickjs.h"

#define EXPORT(name) __attribute__((export_name(name)))
#define MAX_CTX 8
#define GRACE_TICKS 32

static JSRuntime *rt;
static JSContext *ctx;             /* context 0, the harness */
static JSContext *ctxs[MAX_CTX];   /* all contexts in creation order */
static int ctx_count;
static int script_pool = 2;        /* script contexts created by ai_init */
static char errbuf[1024];
static char *result_buf;
static size_t result_len;
static uint8_t *snap_buf;
static size_t snap_len;
static double budget_ms;
static long budget_ticks;
static double run_start_ms;
static long run_start_ticks;
static long interrupt_ticks;
static long over_ticks;            /* ticks since the budget was hit */
static int terminated;             /* last run ended by the budget, unsuspendable */
static size_t memory_limit;

static double now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e3 + t.tv_nsec / 1e6;
}

/* ---- natives: the only host code pointers that may end up in a heap ---- */

static JSValue js_log(JSContext *c, JSValueConst this_val, int argc, JSValueConst *argv) {
    for (int i = 0; i < argc; i++) {
        const char *s = JS_ToCString(c, argv[i]);
        fprintf(stderr, "%s%s", i ? " " : "", s ? s : "?");
        JS_FreeCString(c, s);
    }
    fprintf(stderr, "\n");
    return JS_UNDEFINED;
}

/* __evalIn(index, source): compile `source` in script context `index`
   (1..pool) and return its completion value, typically a function */
static JSValue js_eval_in(JSContext *c, JSValueConst this_val, int argc, JSValueConst *argv) {
    int idx;
    if (argc < 2 || JS_ToInt32(c, &idx, argv[0]) < 0) return JS_ThrowTypeError(c, "__evalIn(index, source)");
    if (idx <= 0 || idx >= ctx_count) return JS_ThrowRangeError(c, "no script context %d", idx);
    size_t len;
    const char *src = JS_ToCStringLen(c, &len, argv[1]);
    if (!src) return JS_EXCEPTION;
    JSValue v = JS_Eval(ctxs[idx], src, len, "<script>", JS_EVAL_TYPE_GLOBAL);
    JS_FreeCString(c, src);
    if (JS_IsException(v)) {
        /* move the exception to the caller's context */
        JSValue e = JS_GetException(ctxs[idx]);
        return JS_Throw(c, e);
    }
    return v;
}

/* __hostCall(op, json) in a script context: forwards (contextIndex, op, json)
   as strings to the harness global __scriptCall and returns its string
   result. Natives carry no realm, so this is the only way a script reaches
   the harness, and only strings cross. */
static JSValue js_script_call(JSContext *c, JSValueConst this_val, int argc, JSValueConst *argv) {
    int idx = -1;
    for (int i = 1; i < ctx_count; i++) if (ctxs[i] == c) idx = i;
    if (idx < 0 || argc < 2) return JS_ThrowTypeError(c, "__hostCall(op, json)");
    const char *op = JS_ToCString(c, argv[0]);
    size_t jlen; const char *json = JS_ToCStringLen(c, &jlen, argv[1]);
    if (!op || !json) { JS_FreeCString(c, op); JS_FreeCString(c, json); return JS_EXCEPTION; }
    JSContext *h = ctxs[0];
    JSValue g = JS_GetGlobalObject(h);
    JSValue f = JS_GetPropertyStr(h, g, "__scriptCall");
    JS_FreeValue(h, g);
    JSValue args[3] = { JS_NewInt32(h, idx), JS_NewString(h, op), JS_NewStringLen(h, json, jlen) };
    JS_FreeCString(c, op); JS_FreeCString(c, json);
    JSValue r = JS_Call(h, f, JS_UNDEFINED, 3, (JSValueConst *)args);
    JS_FreeValue(h, f); JS_FreeValue(h, args[1]); JS_FreeValue(h, args[2]);
    if (JS_IsException(r)) {
        JSValue e = JS_GetException(h);
        const char *msg = JS_ToCString(h, e);
        JSValue out = JS_ThrowInternalError(c, "%s", msg ? msg : "harness error");
        JS_FreeCString(h, msg); JS_FreeValue(h, e);
        return out;
    }
    size_t rlen; const char *rs = JS_ToCStringLen(h, &rlen, r);
    JS_FreeValue(h, r);
    JSValue out = rs ? JS_NewStringLen(c, rs, rlen) : JS_UNDEFINED;
    JS_FreeCString(h, rs);
    return out;
}

static const void *const natives[] = { (const void *)js_log, (const void *)js_eval_in, (const void *)js_script_call };
static const JSSnapshotHost host = { .natives = natives, .native_count = 3 };

/* ---- helpers ---- */

static void set_error_from_exception(JSContext *c) {
    JSValue e = JS_GetException(c);
    const char *s = JS_ToCString(c, e);
    snprintf(errbuf, sizeof errbuf, "%s", s ? s : "unknown error");
    if (JS_IsError(c, e)) {
        JSValue st = JS_GetPropertyStr(c, e, "stack");
        if (!JS_IsUndefined(st)) {
            const char *ss = JS_ToCString(c, st);
            if (ss) { size_t n = strlen(errbuf); snprintf(errbuf + n, sizeof errbuf - n, "\n%s", ss); JS_FreeCString(c, ss); }
        }
        JS_FreeValue(c, st);
    }
    JS_FreeCString(c, s);
    JS_FreeValue(c, e);
}

static void set_result_str(const char *s) {
    free(result_buf);
    result_len = strlen(s);
    result_buf = malloc(result_len + 1);
    memcpy(result_buf, s, result_len + 1);
}

static void set_result(JSValue v) {
    free(result_buf); result_buf = NULL; result_len = 0;
    if (JS_IsUndefined(v) || JS_IsNull(v)) return;
    const char *s = JS_ToCString(ctx, v);
    if (!s) return;
    set_result_str(s);
    JS_FreeCString(ctx, s);
}

static int interrupt_handler(JSRuntime *r, void *opaque) {
    interrupt_ticks++;
    int over = (budget_ms > 0 && now_ms() - run_start_ms > budget_ms) ||
               (budget_ticks > 0 && interrupt_ticks - run_start_ticks >= budget_ticks);
    if (!over) return 0;
    if (over_ticks++ == 0) JS_RequestSuspend(r);
    if (over_ticks > GRACE_TICKS) { terminated = 1; return 1; } /* not suspendable here: terminate */
    return 0;
}

static void start_run(void) { run_start_ms = now_ms(); run_start_ticks = interrupt_ticks; over_ticks = 0; terminated = 0; }

static void install_globals(void) {
    JSValue g = JS_GetGlobalObject(ctx);
    JS_SetPropertyStr(ctx, g, "__log", JS_NewCFunction(ctx, js_log, "__log", 1));
    JS_SetPropertyStr(ctx, g, "__evalIn", JS_NewCFunction(ctx, js_eval_in, "__evalIn", 2));
    JS_FreeValue(ctx, g);
}

static void new_runtime(void) {
    rt = JS_NewRuntime();
    JS_SetInterruptHandler(rt, interrupt_handler, NULL);
    if (memory_limit) JS_SetMemoryLimit(rt, memory_limit);
    ctx_count = 0;
}

/* 0 = quiescent, -1 = a job threw, 2 = a job was terminated by the budget */
static int run_jobs(void) {
    JSContext *c;
    int r;
    while ((r = JS_ExecutePendingJob(rt, &c)) != 0) {
        if (r < 0) {
            if (terminated) { set_error_from_exception(c); return 2; }
            set_error_from_exception(c);
            return -1;
        }
    }
    return 0;
}

static int finish(JSValue v) {
    if (JS_IsException(v)) {
        if (JS_IsSuspended(rt)) return 1;
        set_error_from_exception(ctx);
        return terminated ? 2 : -1;
    }
    set_result(v);
    JS_FreeValue(ctx, v);
    return run_jobs();
}

/* ---- exports ---- */

EXPORT("ai_malloc") void *ai_malloc(size_t n) { return malloc(n); }
EXPORT("ai_free") void ai_free(void *p) { free(p); }
EXPORT("ai_error") const char *ai_error(void) { return errbuf; }
EXPORT("ai_result_ptr") const char *ai_result_ptr(void) { return result_buf; }
EXPORT("ai_result_len") size_t ai_result_len(void) { return result_len; }
EXPORT("ai_ticks") long ai_ticks(void) { return interrupt_ticks; }
EXPORT("ai_set_budget_ms") void ai_set_budget_ms(double ms) { budget_ms = ms; }
EXPORT("ai_set_budget_ticks") void ai_set_budget_ticks(long n) { budget_ticks = n; }
EXPORT("ai_set_memory_limit") void ai_set_memory_limit(size_t n) { memory_limit = n; if (rt) JS_SetMemoryLimit(rt, n); }
EXPORT("ai_context_count") int ai_context_count(void) { return ctx_count; }

/* fresh runtime: the harness context plus `script_pool` script contexts */
EXPORT("ai_init") int ai_init(void) {
    new_runtime();
    if (!rt) { snprintf(errbuf, sizeof errbuf, "JS_NewRuntime failed"); return -1; }
    ctx = JS_NewContext(rt);
    if (!ctx) { snprintf(errbuf, sizeof errbuf, "JS_NewContext failed"); return -1; }
    ctxs[ctx_count++] = ctx;
    for (int i = 0; i < script_pool; i++) {
        JSContext *sc = JS_NewContext(rt);
        ctxs[ctx_count++] = sc;
        JSValue g = JS_GetGlobalObject(sc);
        JS_SetPropertyStr(sc, g, "__hostCall", JS_NewCFunction(sc, js_script_call, "__hostCall", 2));
        JS_FreeValue(sc, g);
    }
    install_globals();
    return 0;
}

EXPORT("ai_drop") void ai_drop(void) {
    if (snap_buf) { js_free_rt(rt, snap_buf); snap_buf = NULL; snap_len = 0; }
    for (int i = 0; i < ctx_count; i++) JS_FreeContext(ctxs[i]);
    ctx_count = 0; ctx = NULL;
    if (rt) { JS_FreeRuntime(rt); rt = NULL; }
}

/* evaluate a script in the harness context; 0 done, 1 suspended, 2 terminated, -1 error */
EXPORT("ai_eval") int ai_eval(const char *src, size_t len, const char *name) {
    start_run();
    return finish(JS_Eval(ctx, src, len, name ? name : "<eval>", JS_EVAL_TYPE_GLOBAL));
}

/* call harness global `fn` with one string argument; the string result is
   left in the result buffer. Same statuses as ai_eval. */
EXPORT("ai_call") int ai_call(const char *fn, const char *arg, size_t arglen) {
    JSValue g = JS_GetGlobalObject(ctx);
    JSValue f = JS_GetPropertyStr(ctx, g, fn);
    JS_FreeValue(ctx, g);
    if (!JS_IsFunction(ctx, f)) { JS_FreeValue(ctx, f); snprintf(errbuf, sizeof errbuf, "global %s is not a function", fn); return -1; }
    JSValue a = arg ? JS_NewStringLen(ctx, arg, arglen) : JS_UNDEFINED;
    start_run();
    JSValue v = JS_Call(ctx, f, JS_UNDEFINED, arg ? 1 : 0, (JSValueConst *)&a);
    JS_FreeValue(ctx, a);
    JS_FreeValue(ctx, f);
    return finish(v);
}

EXPORT("ai_resume") int ai_resume(void) {
    if (!JS_IsSuspended(rt)) { snprintf(errbuf, sizeof errbuf, "nothing suspended"); return -1; }
    start_run();
    return finish(JS_Resume(ctx));
}

EXPORT("ai_is_suspended") int ai_is_suspended(void) { return rt && JS_IsSuspended(rt); }
EXPORT("ai_run") int ai_run(void) { start_run(); return run_jobs(); }

EXPORT("ai_snapshot") int ai_snapshot(int flags) {
    if (snap_buf) { js_free_rt(rt, snap_buf); snap_buf = NULL; snap_len = 0; }
    snap_buf = JS_WriteSnapshot(ctx, &snap_len, &host, NULL, 0, flags);
    if (!snap_buf) { set_error_from_exception(ctx); return -1; }
    return 0;
}
EXPORT("ai_snap_ptr") const uint8_t *ai_snap_ptr(void) { return snap_buf; }
EXPORT("ai_snap_len") size_t ai_snap_len(void) { return snap_len; }
EXPORT("ai_snap_free") void ai_snap_free(void) { if (snap_buf) { js_free_rt(rt, snap_buf); snap_buf = NULL; snap_len = 0; } }

/* restore base + n chained deltas into a fresh runtime (replaces the
   current one); anchor >= 0 makes the state after that delta the anchor
   of the folds */
EXPORT("ai_restore") int ai_restore(const uint8_t *base, size_t base_len,
                                    const uint8_t *const *deltas, const size_t *lens, int n, int anchor) {
    ai_drop();
    new_runtime();
    JSSnapshotResult res;
    int rc = n == 0
        ? JS_ReadSnapshot(rt, base, base_len, NULL, 0, &host, &res, errbuf, sizeof errbuf)
        : JS_ReadSnapshotChainAnchored(rt, base, base_len, deltas, lens, n, anchor, &host, &res, errbuf, sizeof errbuf);
    if (rc) { JS_FreeRuntime(rt); rt = NULL; return -1; }
    ctx_count = res.context_count < MAX_CTX ? res.context_count : MAX_CTX;
    for (int i = 0; i < ctx_count; i++) ctxs[i] = res.contexts[i];
    ctx = ctxs[0];
    JS_FreeSnapshotResult(rt, &res);
    return 0;
}

/* the current base becomes the anchor of the folds; 0 on success */
EXPORT("ai_set_anchor") int ai_set_anchor(void) {
    if (JS_SetSnapshotAnchor(ctx)) { set_error_from_exception(ctx); return -1; }
    return 0;
}
EXPORT("ai_anchor_id") void ai_anchor_id(void) {
    char tmp[32];
    snprintf(tmp, sizeof tmp, "%llu", (unsigned long long)JS_GetSnapshotAnchorId(rt));
    set_result_str(tmp);
}

EXPORT("ai_base_id") void ai_base_id(void) {
    char tmp[32];
    snprintf(tmp, sizeof tmp, "%llu", (unsigned long long)JS_GetSnapshotBaseId(rt));
    set_result_str(tmp);
}

EXPORT("ai_mem") void ai_mem(void) {
    JSMemoryUsage u;
    JS_ComputeMemoryUsage(rt, &u);
    char tmp[256];
    snprintf(tmp, sizeof tmp,
             "{\"malloc_size\":%lld,\"memory_used\":%lld,\"obj_count\":%lld,\"str_count\":%lld,\"atom_count\":%lld}",
             (long long)u.malloc_size, (long long)u.memory_used_size, (long long)u.obj_count,
             (long long)u.str_count, (long long)u.atom_count);
    set_result_str(tmp);
}

EXPORT("ai_gc") void ai_gc(void) { JS_RunGC(rt); }
