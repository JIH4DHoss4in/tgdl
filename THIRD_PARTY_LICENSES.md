# Third-party notices

`tgdl` is built from Rust dependencies resolved by `Cargo.lock`. Package metadata records the license expression and source location for each resolved version. Anyone redistributing the application is responsible for retaining all notices required by those licenses.

## Locally modified grammers crates

This repository redistributes modified source copies of:

- `grammers-mtproto` 0.10.0
- `grammers-mtsender` 0.10.0

The root manifest selects these copies through `[patch.crates-io]`. Their upstream project is [grammers](https://codeberg.org/Lonami/grammers), and the recorded upstream revision is `5c6d44ff30e02d6c9295bcf1fcb51403ad77c981`. Both crates are available under `MIT OR Apache-2.0`.

The local changes provide bounded handling for 1 MiB media responses, prevent malformed response lengths from causing a panic, and add HTTP CONNECT proxy support with bounded headers, fallible DNS initialization, safe credential decoding, and credential-redacted errors. See [vendor/README.md](vendor/README.md) for patch scope and maintenance instructions.

## Pinned core2 source

`hermes-tdata` 0.1.1 depends on the yanked `core2` 0.4.0 release. To keep clean builds reproducible, the root manifest pins the corresponding upstream source revision: `545e84bcb0f235b12e21351e0c69767958efe2a7`. `core2` is available under `MIT OR Apache-2.0` and is fetched as source during the initial dependency build.

## MIT license for redistributed grammers source

```text
MIT License

Copyright (c) 2021 Lonami <gh@lonami.dev>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
