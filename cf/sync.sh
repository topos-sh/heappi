#!/bin/sh
# Copy the engine and the harness next to the Worker source (wrangler bundles
# the wasm as CompiledWasm and the harness as Text).
set -e
cd "$(dirname "$0")"
cp ../engine/agent-image.wasm src/agent-image.wasm
cp ../heap/harness.js src/harness.txt
ls -la src/agent-image.wasm src/harness.txt
