# tgdl

A fast, resumable, multi-account Telegram media downloader for Windows, Linux, and macOS.

`tgdl` is a terminal application that monitors configured Telegram channels and groups, queues newly available media, and downloads files through one or more authorized user accounts. After account setup, it does not require Telegram Desktop, a bot token, or a browser.

> [!IMPORTANT]
> `tgdl` is an independent project and is not affiliated with or endorsed by Telegram. Use only accounts you own and content you are authorized to access. Your use of the application remains subject to the [Telegram API Terms](https://core.telegram.org/api/terms).

## Key features

- Monitor multiple public or private channels and groups in one process.
- Reserve the primary monitor for monitoring, or allow it to participate in downloads.
- Prefer premium accounts while retaining normal-account fallback.
- Download up to two files concurrently, with no more than one active file per account.
- Use between one and eight adaptive MTProto transfer lanes per active account.
- Configure HTTP CONNECT or SOCKS5 proxies globally or per account.
- Authorize accounts by phone, Telegram Desktop `tdata`, or a Telethon SQLite session.
- Resume 1 MiB ranges from crash-safe SQLite checkpoints.
- Import complete source history or an inclusive local date and time range.
- Filter media by file extension and inclusive minimum or maximum file size.
- Reuse an existing job when Telegram identifies forwarded media as the same remote file.
- Keep incomplete downloads under `.part` filenames until finalization succeeds.

Telegram determines final throughput and request limits. Additional lanes can improve connection utilization, but they cannot guarantee a particular speed or prevent server-requested waits.

## Install a release

Download the archive for your platform from the repository's **Releases** page:

| Platform | Release asset |
| --- | --- |
| Windows x64 | `tgdl-<version>-windows-x86_64.zip` |
| Linux x64 (glibc) | `tgdl-<version>-linux-x86_64.tar.gz` |
| macOS on Apple Silicon | `tgdl-<version>-macos-aarch64.tar.gz` |
| macOS on Intel | `tgdl-<version>-macos-x86_64.tar.gz` |

Each release includes a `SHA256SUMS` manifest. Release binaries are currently unsigned, so Windows SmartScreen or macOS Gatekeeper may request confirmation. Verify the archive checksum before running the application.

## Quick start

1. Create a Telegram API application at [my.telegram.org](https://my.telegram.org).
2. Extract the release archive.
3. Run `tgdl.exe` on Windows or `./tgdl` on Linux or macOS.
4. Complete first-time setup by entering the API ID, API hash, output directory, first account, and first monitored source.
5. Add any additional accounts and sources from the console interface.
6. Select **Start monitoring and downloads**.

Press `Ctrl+C` on the Downloads page to stop safely after active jobs have been checkpointed.

## Sources and live monitoring

One active account monitors every configured source. `monitor_primary` identifies the preferred monitor. If that account repeatedly fails or loses authorization, another enabled account takes over.

- Public channels and groups are resolved by username. Telegram may allow the monitor to observe them without joining.
- The monitor account must already be a member of each private channel or group.
- `tgdl` never joins sources, sends or deletes messages, or marks messages as read.
- Live monitoring intentionally ignores messages posted while the application is stopped.
- Previously accepted queued or partial jobs resume during the next run.

The **Download channel or group history** workflow is independent of live monitoring. It can scan one, several, or all configured sources, either across all accessible history or within an inclusive date and time range. Existing jobs, duplicate media, and configured media filters are respected.

## Accounts and scheduling

Each file is assigned to one account at a time. If a job must be reassigned, the replacement account refetches the source message through its own session and downloads only ranges that are missing from the durable checkpoint.

Supported authorization methods are:

- phone number, login code, and optional two-step verification password;
- Telegram Desktop `tdata` folder;
- Telethon SQLite `.session` file.

Telethon `StringSession` values are not supported. Close Telegram Desktop or any Telethon process before importing its session. Do not use copied authorization sessions concurrently from different applications or networks; Telegram may revoke duplicated authorization keys.

When an account cannot access a private source message, the scheduler offers the job to another eligible account. Telegram cooldown deadlines are stored in the state database and remain effective after a restart.

## Proxy configuration

The console accepts HTTP or SOCKS5 proxies in either shorthand form:

```text
127.0.0.1:1080
127.0.0.1:1080:username:password
```

Hostnames and bracketed IPv6 addresses are also supported. A per-account proxy overrides the global route and remains in effect during authorization, monitoring, transfer reconnections, and media data-center migrations.

Manual TOML configuration uses canonical URLs:

```toml
proxy_url = "socks5://127.0.0.1:1080"
# proxy_url = "http://username:password@proxy.example.com:8080"
```

## Media filters and filenames

Supported media includes documents, videos, audio, voice notes, animations, stickers, and the largest available version of a photo. The application ignores text-only messages, polls, locations, contacts, and web previews without directly downloadable media.

- An empty extension list accepts every file extension.
- Minimum and maximum file sizes are inclusive.
- Media with an unknown size is accepted only when no size filter is configured.
- Original filenames are preserved when Telegram supplies them.
- Different media items with the same filename receive `_1`, `_2`, and subsequent suffixes.
- Repeated forwards of the same Telegram document or photo identity reuse the existing job and output path.

Deduplication uses Telegram's remote media identity rather than a full content hash. Files uploaded separately are treated as different media even when their bytes are identical.

## Recovery and verification

Known-size files are divided into 1 MiB ranges and written at their final offsets. Completed ranges are synchronized and recorded every 16 MiB or two seconds. After a restart, the application requests only ranges that are absent from the most recent durable checkpoint.

The completed file is synchronized and renamed from `<filename>.part` only after all ranges finish successfully. Telegram-provided hash ranges can be used for targeted recovery. Optional full-file SHA-256 verification performs an additional disk pass and is disabled by default to keep finalization immediate.

## Console controls

| Key | Action |
| --- | --- |
| Up / Down or `j` / `k` | Move the selection or scroll data |
| Page Up / Page Down | Move by one page |
| Home / End | Jump to the beginning or end |
| Enter | Open or confirm the selected item |
| Esc or `q` | Return to the parent page |
| Number key | Select the corresponding numbered action |
| `Ctrl+C` | Stop monitoring or history processing safely |

## Configuration and private data

Use **Settings** and **Show application paths** to inspect the active configuration and data locations. See [config.example.toml](config.example.toml) for the complete configuration structure.

| Environment variable | Purpose |
| --- | --- |
| `TGDL_API_ID` | Override the Telegram API ID for the current process |
| `TGDL_API_HASH` | Override the Telegram API hash for the current process |
| `TGDL_PROXY_URL` | Override the global proxy for the current process; an empty value selects a direct connection |
| `TGDL_CONFIG` | Use an alternate `config.toml` path |

Environment overrides are process-local and are never written to the configuration file when the application saves account metadata or unrelated settings.

Session databases contain Telegram authorization material. Configuration files may contain an API hash and proxy credentials. Never publish real configuration, session files, `tdata`, state databases, private source identifiers, or unredacted logs.

`tgdl` requests owner-only permissions for application data: `0700` for directories and `0600` for files on Unix, and a current-user-only ACL on Windows. Configuration changes are written atomically. See the [security policy](SECURITY.md) for additional guidance.

## Build from source

Requirements:

- Rust 1.96 or newer with Rust 2024 edition support;
- Git and internet access for the initial dependency fetch;
- native build tools for Windows, Linux, or macOS.

Clone the repository, open its root directory, and run:

```console
cargo build --locked --release
```

The resulting executable is `target/release/tgdl.exe` on Windows and `target/release/tgdl` on Linux and macOS.

`Cargo.lock` and the patched crates under `vendor/` are required source inputs. The project is not published to crates.io because its local `grammers` patches add bounded handling for 1 MiB media responses and HTTP CONNECT support.

Run the complete quality gate before submitting a change:

```console
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked --release
```

## Current limitations

Version 0.1 supports one output directory, multiple sources, any number of configured accounts, and no more than two simultaneously active download accounts. It does not provide a graphical desktop interface, background-service installer, automatic source joining, automatic message deletion, bandwidth limits, encrypted Telegram CDN redirects, or byte-level content deduplication.

Monitoring uses Telegram update streams and [`updates.getChannelDifference`](https://core.telegram.org/api/updates) where applicable. File transfers use Telegram's [file API](https://core.telegram.org/api/files) through dedicated media sessions with `cdn_supported=false`.

## Documentation

- [Changelog](CHANGELOG.md)
- [Contributing guide](CONTRIBUTING.md)
- [Release guide](RELEASING.md)
- [Security policy](SECURITY.md)
- [Third-party notices](THIRD_PARTY_LICENSES.md)

`tgdl` is available under the [MIT License](LICENSE).
