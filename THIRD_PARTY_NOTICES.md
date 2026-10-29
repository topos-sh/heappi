# Third-party notices

heappi is MIT licensed (see LICENSE). It redistributes or bundles the following
software, each under its own MIT license.

## QuickJS and dquickts

`engine/heappi.wasm` is a build of [dquickts](https://github.com/stencil-hq/dquickts),
a fork of [QuickJS](https://bellard.org/quickjs/), with the chained-checkpoint
patches of this project, compiled to wasm32-wasi. The binary therefore contains
QuickJS and dquickts code under the following notice.

    QuickJS Javascript Engine

    Copyright (c) 2017-2021 Fabrice Bellard
    Copyright (c) 2017-2021 Charlie Gordon
    Copyright (c) 2026 Stencil Labs, Inc.

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.

The binary also links the compiler-rt builtins and wasi-libc from the
[wasi-sdk](https://github.com/WebAssembly/wasi-sdk), licensed under Apache 2.0
with the LLVM exception, which places no attribution requirement on compiled
output.

## Pi Durable, pi-ai, pi-codemode (Earendil Works)

`embed/build.sh` bundles `@earendil-works/pi-durable` and `@earendil-works/pi-ai`
from [earendil-works/pi](https://github.com/earendil-works/pi) into the harness
layer, and the Cloudflare Worker bundles pi-ai's Anthropic provider. Both are
MIT licensed by Earendil Works; the generated bundles are not committed to this
repository, so the notice applies to builds and deployments made from it.

## Model Context Protocol SDK

`app/mcp.mjs` uses `@modelcontextprotocol/sdk`, MIT licensed by Anthropic, PBC.
It is not bundled into anything committed here.
