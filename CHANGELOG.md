# Changelog

## 0.1.1 — 2026-10-12

### Fixed
- Fixed MTProto deserialization failure (`unexpected constructor: d49f34c6`) by adding support for Telegram Layer 228+ `channel` (`0xd49f34c6`) and `user` (`0xb1b8cc83`) constructors with `linked_community_id`.
- Reclassified MTProto deserialization errors as transient with backoff and retry instead of immediately failing jobs permanently.
- Added automatic fallback to search account dialogs when resolving public channels if username lookup returns empty but `peer_id` is configured.

## 0.1.0 — 2026-08-01

Initial public release.
