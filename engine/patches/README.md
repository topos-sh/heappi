# Engine patches

`heappi.wasm` is built from [stencil-hq/dquickts](https://github.com/stencil-hq/dquickts) at commit `f9a3efa` with these seven patches applied in order:

```bash
git clone https://github.com/stencil-hq/dquickts.git && cd dquickts
git checkout f9a3efa
git am ../heappi/engine/patches/*.patch
make check            # the engine's test suite, including the chained-checkpoint hosts
```

Then `WASI_SDK=/path/to/wasi-sdk-25 ENGINE=../dquickts ./engine/build.sh` from the heappi directory. What the patches add: chained checkpoints (deltas against a base), fast-array elements written before properties, a base that does not pin its cells plus tombstones, two test hosts, the wasm32-wasi build, folds relative to an anchor layer, and applying chained checkpoints onto a live runtime.
