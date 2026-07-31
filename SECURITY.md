# Security policy

## Supported versions

Before version 1.0, security updates are provided only for the latest release line.

| Version | Supported |
| --- | --- |
| Latest `0.1.x` release | Yes |
| Earlier releases | No |

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue. Use **Security → Report a vulnerability** in the GitHub repository. Maintainers should enable GitHub private vulnerability reporting before publishing the first release.

Include the affected version or commit, operating system, potential impact, minimal reproduction steps, and any suggested mitigation. Allow reasonable time for confirmation, remediation, and coordinated disclosure before publishing details.

## Sensitive information

Never attach or paste real:

- Telegram API hashes, phone numbers, login codes, or two-step verification passwords;
- authorization keys, session databases, Telegram Desktop `tdata`, or Telethon sessions;
- proxy credentials or canonical proxy URLs containing credentials;
- configuration or state databases, `.part` files, or private channel and group identifiers;
- unredacted logs or screenshots containing account, source, or filesystem details.

If credentials or session material may have been exposed, revoke or replace them before submitting a report.

## Security boundaries

`tgdl` stores Telegram user sessions locally and operates with the authority granted to those accounts. Protect the operating system user account and application data directory. Never share session files or run an untrusted build with real accounts.

The application requests owner-only permissions for local configuration, session, state, and lock files. Environment overrides apply only to the current process and are not persisted during configuration or metadata updates.

Release binaries are currently unsigned. Download releases only from the official repository and verify each archive against `SHA256SUMS` before use.

Telegram, configured proxies, the local filesystem, and imported third-party session files are external trust boundaries. `tgdl` cannot guarantee their availability, integrity, or behavior.
