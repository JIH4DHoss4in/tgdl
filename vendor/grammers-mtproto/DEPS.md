# Direct dependencies

This document summarizes the direct dependencies declared by the vendored crate manifest.

| Dependency | Purpose |
| --- | --- |
| `bytes` | Input and output buffers |
| `crc32fast` | CRC-32 calculations for full transport mode |
| `flate2` | Gzip message compression and decompression |
| `getrandom` | Secure authorization-key values and nonces |
| `grammers-crypto` | Encryption, decryption, and integer factorization |
| `grammers-tl-types` | Telegram TL serialization and deserialization |
| `log` | Protocol diagnostics |
| `num-bigint` | Authorization-key generation |
| `sha1` | Authorization-key generation |
| `toml` | Manifest and dependency consistency tests |

Refer to the crate manifest and root `Cargo.lock` for exact versions, features, and source locations.
