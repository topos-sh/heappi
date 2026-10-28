#!/bin/sh
# Build the engine (dquickts-chain, branch chained-checkpoints) plus the host
# shim as a wasm32-wasi reactor module: engine/agent-image.wasm
#
#   ENGINE=../dquickts-chain WASI_SDK=/path/to/wasi-sdk ./engine/build.sh
set -e
cd "$(dirname "$0")"
ENGINE="${ENGINE:-../../dquickts-chain}"
WASI_SDK="${WASI_SDK:?set WASI_SDK to a wasi-sdk root}"
VERSION="$(cat "$ENGINE/VERSION")"
CC="$WASI_SDK/bin/clang"
SRCS="$ENGINE/quickjs.c $ENGINE/dtoa.c $ENGINE/libregexp.c $ENGINE/libunicode.c $ENGINE/cutils.c shim.c"
"$CC" --target=wasm32-wasi -O2 -flto -mexec-model=reactor \
  -D_GNU_SOURCE -DCONFIG_VERSION="\"$VERSION\"" -funsigned-char -fwrapv \
  -I"$ENGINE" \
  -Wl,--initial-memory=2097152 -Wl,--max-memory=1073741824 -Wl,-z,stack-size=1048576 \
  -Wl,--export=malloc -Wl,--export=free \
  -o agent-image.wasm $SRCS
ls -la agent-image.wasm
