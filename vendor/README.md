# Vendored grammers patches

`tgdl` requires three narrowly scoped changes to `grammers` 0.10.0. The root `Cargo.toml` selects these local source copies through `[patch.crates-io]`.

| Crate | Local modification |
| --- | --- |
| `grammers-mtproto` | Accept bounded incoming MTProto responses large enough to contain a complete 1 MiB `upload.getFile` result, and return an error instead of panicking when a declared length is malformed. |
| `grammers-mtsender` | Add HTTP CONNECT transport, optional Basic authentication, safe userinfo decoding, credential-redacted errors, fallible DNS initialization, bounded proxy response headers, and focused tests. |
| `grammers-tl-types` | Accept Telegram Layer 228+ constructors `channel#d49f34c6` and `user#b1b8cc83` alongside legacy constructor IDs, consuming trailing `linked_community_id` fields while maintaining 0.10.0 struct compatibility. |

The patches do not change SOCKS5 behavior or outgoing MTProto container limits.

## Provenance

- Upstream project: [codeberg.org/Lonami/grammers](https://codeberg.org/Lonami/grammers)
- Crate version: `0.10.0`
- Recorded upstream revision: `5c6d44ff30e02d6c9295bcf1fcb51403ad77c981`
- License: `MIT OR Apache-2.0`

## Updating the vendored source

1. Obtain the exact source for the new upstream crate versions.
2. Compare both local directories with their upstream equivalents.
3. Reapply only the modifications documented above.
4. Run the root quality gate and the applicable vendored-crate tests.
5. Update crate versions, provenance, patch notes, and third-party notices together.

Cargo registry extraction markers and normalized manifest backups are intentionally excluded from version control. See [THIRD_PARTY_LICENSES.md](../THIRD_PARTY_LICENSES.md) for licensing details.
