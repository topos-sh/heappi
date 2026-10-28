#!/bin/sh
# Bundle Pi Durable (with pi-ai's faux provider) into one script the heap can evaluate.
set -e
cd "$(dirname "$0")"
npx -y esbuild@0.25.0 entry.mjs --bundle --platform=neutral --format=iife --target=es2022 \
  --main-fields=module,main --outfile=pi-bundle.js --log-level=error '--external:node:*'
cat prelude.js pi-bundle.js pi-glue.js > pi-harness.js
ls -la pi-bundle.js pi-harness.js
