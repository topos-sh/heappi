#!/bin/sh
# Copy the engine, the Pi harness and the chat page next to the Worker
# sources (wrangler bundles the wasm as CompiledWasm, the rest as Text).
set -e
cd "$(dirname "$0")"
cp ../engine/heappi.wasm src/heappi.wasm
[ -f ../embed/pi-harness.js ] || (cd ../embed && ./build.sh)
cp ../embed/pi-harness.js src/pi-harness.txt
cp ../app/ui/index.html src/ui.html
ls -la src/heappi.wasm src/pi-harness.txt src/ui.html
