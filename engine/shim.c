/*
 * Host shim for the agent image: a wasm32-wasi reactor exporting the
 * four host operations of the design (deliver / run / snapshot / restore)
 * over the dquickts-chain engine.
 *
 * The host never parses an agent's state. It calls global functions the
 * heap defines (`__deliver(json)`, `__drain()`), reads the strings they
 * return, and moves checkpoint bytes in and out. Everything else, the
 * agent loop included, is JavaScript inside the heap.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "quickjs.h"

#define EXPORT(name) __attribute__((export_name(name)))

static JSRuntime *rt;
static JSContext *ctx;
static char errbuf[1024];
static char *result_buf;      /* last string result, malloc'd */
static size_t result_len;
static uint8_t *snap_buf;     /* last snapshot, js_malloc'd on rt */
static size_t snap_len;
static double budget_ms;      /* 0 = no budget */
static double run_start_ms;
static long interrupt_ticks;

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

static const void *const natives[] = { (const void *)js_log };
static const JSSnapshotHost host = { .natives = natives, .native_count = 1 };

/* ---- helpers ---- */

static void set_error_from_exception(void) {
    JSValue e = JS_GetException(ctx);
    const char *s = JS_ToCString(ctx, e);
    snprintf(errbuf, sizeof errbuf, "%s", s ? s : "unknown error");
    if (JS_IsError(ctx, e)) {
        JSValue st = JS_GetPropertyStr(ctx, e, "stack");
        if (!JS_IsUndefined(st)) {
            const char *ss = JS_ToCString(ctx, st);
            if (ss) {
                size_t n = strlen(errbuf);
                snprintf(errbuf + n, sizeof errbuf - n, "\n%s", ss);
                JS_FreeCString(ctx, ss);
            }
        }
        JS_FreeValue(ctx, st);
    }
    JS_FreeCString(ctx, s);
    JS_FreeValue(ctx, e);
}

static void set_result(JSValue v) {
    free(result_buf);
    result_buf = NULL;
    result_len = 0;
    if (JS_IsUndefined(v) || JS_IsNull(v)) return;
    const char *s = JS_ToCString(ctx, v);
    if (!s) return;
    result_len = strlen(s);
    result_buf = malloc(result_len + 1);
    memcpy(result_buf, s, result_len + 1);
    JS_FreeCString(ctx, s);
}

static int interrupt_handler(JSRuntime *r, void *opaque) {
    interrupt_ticks++;
    if (budget_ms > 0 && now_ms() - run_start_ms > budget_ms)
        JS_RequestSuspend(r);
    return 0;
}

static void install_globals(void) {
    JSValue g = JS_GetGlobalObject(ctx);
    JS_SetPropertyStr(ctx, g, "__log", JS_NewCFunction(ctx, js_log, "__log", 1));
    JS_FreeValue(ctx, g);
}

/* run the job queue; 0 = quiescent, -1 = a job threw */
static int run_jobs(void) {
    JSContext *c;
    int r;
    while ((r = JS_ExecutePendingJob(rt, &c)) != 0) {
        if (r < 0) {
            JSValue e = JS_GetException(c);
            const char *s = JS_ToCString(c, e);
            snprintf(errbuf, sizeof errbuf, "job: %s", s ? s : "?");
            JS_FreeCString(c, s);
            JS_FreeValue(c, e);
            return -1;
        }
    }
    return 0;
}

/* ---- exports ---- */

EXPORT("ai_malloc") void *ai_malloc(size_t n) { return malloc(n); }
EXPORT("ai_free") void ai_free(void *p) { free(p); }
EXPORT("ai_error") const char *ai_error(void) { return errbuf; }
EXPORT("ai_result_ptr") const char *ai_result_ptr(void) { return result_buf; }
EXPORT("ai_result_len") size_t ai_result_len(void) { return result_len; }
EXPORT("ai_ticks") long ai_ticks(void) { return interrupt_ticks; }

EXPORT("ai_set_budget_ms") void ai_set_budget_ms(double ms) { budget_ms = ms; }

/* fresh runtime with the natives installed; 0 on success */
EXPORT("ai_init") int ai_init(void) {
    rt = JS_NewRuntime();
    if (!rt) { snprintf(errbuf, sizeof errbuf, "JS_NewRuntime failed"); return -1; }
    JS_SetInterruptHandler(rt, interrupt_handler, NULL);
    ctx = JS_NewContext(rt);
    if (!ctx) { snprintf(errbuf, sizeof errbuf, "JS_NewContext failed"); return -1; }
    install_globals();
    return 0;
}

EXPORT("ai_drop") void ai_drop(void) {
    if (snap_buf) { js_free_rt(rt, snap_buf); snap_buf = NULL; snap_len = 0; }
    if (ctx) { JS_FreeContext(ctx); ctx = NULL; }
    if (rt) { JS_FreeRuntime(rt); rt = NULL; }
}

/* evaluate a script in the global scope; 0 = done, 1 = suspended, -1 = error */
EXPORT("ai_eval") int ai_eval(const char *src, size_t len, const char *name) {
    run_start_ms = now_ms();
    JSValue v = JS_Eval(ctx, src, len, name ? name : "<eval>", JS_EVAL_TYPE_GLOBAL);
    if (JS_IsException(v)) {
        if (JS_IsSuspended(rt)) return 1;
        set_error_from_exception();
        return -1;
    }
    set_result(v);
    JS_FreeValue(ctx, v);
    return run_jobs();
}

/* call global function `fn` with one string argument; the string result
   (if any) is left in the result buffer. 0 = done, 1 = suspended, -1 = error */
EXPORT("ai_call") int ai_call(const char *fn, const char *arg, size_t arglen) {
    JSValue g = JS_GetGlobalObject(ctx);
    JSValue f = JS_GetPropertyStr(ctx, g, fn);
    JS_FreeValue(ctx, g);
    if (!JS_IsFunction(ctx, f)) {
        JS_FreeValue(ctx, f);
        snprintf(errbuf, sizeof errbuf, "global %s is not a function", fn);
        return -1;
    }
    JSValue a = arg ? JS_NewStringLen(ctx, arg, arglen) : JS_UNDEFINED;
    run_start_ms = now_ms();
    JSValue v = JS_Call(ctx, f, JS_UNDEFINED, arg ? 1 : 0, (JSValueConst *)&a);
    JS_FreeValue(ctx, a);
    JS_FreeValue(ctx, f);
    if (JS_IsException(v)) {
        if (JS_IsSuspended(rt)) return 1;
        set_error_from_exception();
        return -1;
    }
    set_result(v);
    JS_FreeValue(ctx, v);
    return run_jobs();
}

/* continue a suspended execution; same statuses as ai_call */
EXPORT("ai_resume") int ai_resume(void) {
    if (!JS_IsSuspended(rt)) { snprintf(errbuf, sizeof errbuf, "nothing suspended"); return -1; }
    run_start_ms = now_ms();
    JSValue v = JS_Resume(ctx);
    if (JS_IsException(v)) {
        if (JS_IsSuspended(rt)) return 1;
        set_error_from_exception();
        return -1;
    }
    set_result(v);
    JS_FreeValue(ctx, v);
    return run_jobs();
}

EXPORT("ai_is_suspended") int ai_is_suspended(void) { return rt && JS_IsSuspended(rt); }

/* run the job queue only; 0 = quiescent, -1 = error */
EXPORT("ai_run") int ai_run(void) { run_start_ms = now_ms(); return run_jobs(); }

/* write a snapshot; flags = JS_SNAPSHOT_BASE / DELTA / CHAIN or 0; 0 on success */
EXPORT("ai_snapshot") int ai_snapshot(int flags) {
    if (snap_buf) { js_free_rt(rt, snap_buf); snap_buf = NULL; snap_len = 0; }
    snap_buf = JS_WriteSnapshot(ctx, &snap_len, &host, NULL, 0, flags);
    if (!snap_buf) { set_error_from_exception(); return -1; }
    return 0;
}
EXPORT("ai_snap_ptr") const uint8_t *ai_snap_ptr(void) { return snap_buf; }
EXPORT("ai_snap_len") size_t ai_snap_len(void) { return snap_len; }
EXPORT("ai_snap_free") void ai_snap_free(void) {
    if (snap_buf) { js_free_rt(rt, snap_buf); snap_buf = NULL; snap_len = 0; }
}

/* restore base + n chained deltas into a fresh runtime (replaces the
   current one); `deltas` is an array of n pointers, `lens` of n sizes */
EXPORT("ai_restore") int ai_restore(const uint8_t *base, size_t base_len,
                                    const uint8_t *const *deltas, const size_t *lens, int n) {
    ai_drop();
    rt = JS_NewRuntime();
    JS_SetInterruptHandler(rt, interrupt_handler, NULL);
    JSSnapshotResult res;
    int rc;
    if (n == 0)
        rc = JS_ReadSnapshot(rt, base, base_len, NULL, 0, &host, &res, errbuf, sizeof errbuf);
    else
        rc = JS_ReadSnapshotChain(rt, base, base_len, deltas, lens, n, &host, &res, errbuf, sizeof errbuf);
    if (rc) { JS_FreeRuntime(rt); rt = NULL; return -1; }
    ctx = res.contexts[0];
    JS_FreeSnapshotResult(rt, &res);
    return 0;
}

/* id of the attached base (0 if none), as a decimal string in the result buffer */
EXPORT("ai_base_id") void ai_base_id(void) {
    char tmp[32];
    snprintf(tmp, sizeof tmp, "%llu", (unsigned long long)JS_GetSnapshotBaseId(rt));
    free(result_buf);
    result_len = strlen(tmp);
    result_buf = malloc(result_len + 1);
    memcpy(result_buf, tmp, result_len + 1);
}

/* memory usage as JSON in the result buffer */
EXPORT("ai_mem") void ai_mem(void) {
    JSMemoryUsage u;
    JS_ComputeMemoryUsage(rt, &u);
    char tmp[256];
    snprintf(tmp, sizeof tmp,
             "{\"malloc_size\":%lld,\"memory_used\":%lld,\"obj_count\":%lld,\"str_count\":%lld,\"atom_count\":%lld}",
             (long long)u.malloc_size, (long long)u.memory_used_size, (long long)u.obj_count,
             (long long)u.str_count, (long long)u.atom_count);
    free(result_buf);
    result_len = strlen(tmp);
    result_buf = malloc(result_len + 1);
    memcpy(result_buf, tmp, result_len + 1);
}

EXPORT("ai_gc") void ai_gc(void) { JS_RunGC(rt); }
