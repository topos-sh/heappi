#!/bin/sh
# Copy the engine and the harness next to the Worker source (wrangler bundles
# the wasm as CompiledWasm and the harness as Text).
set -e
cd "$(dirname "$0")"
cp ../engine/heappi.wasm src/heappi.wasm
cp ../heap/harness.js src/harness.txt
ls -la src/heappi.wasm src/harness.txt
