# Direct dependencies

This document summarizes the direct dependencies declared by the vendored crate manifest.

| Dependency | Purpose |
| --- | --- |
| `bytes` | Input and output buffers |
| `grammers-crypto` | Protocol ring-buffer implementation |
| `grammers-mtproto` | MTProto implementation |
| `grammers-session` | Persistent update and data-center state |
| `grammers-tl-types` | Telegram request and response types |
| `hickory-resolver` | Proxy hostname resolution |
| `locate-locale` | Default system-locale detection |
| `log` | Sender diagnostics |
| `os_info` | Telegram client system information |
| `tokio` | Asynchronous TCP, tasks, and channels |
| `tokio-socks` | SOCKS5 transport |
| `url` | Proxy URL parsing |

Test-only dependencies include `simple_logger` and `socks5-server`. The `toml` crate supports manifest and dependency consistency tests.

Refer to the crate manifest and root `Cargo.lock` for exact versions, features, and source locations.
