# Contributing to tgdl

Contributions that improve correctness, tests, documentation, usability, or platform compatibility are welcome. Please keep each change focused and preserve the application's safety and recovery guarantees.

## Security and responsible use

- Use only accounts and content you are authorized to access.
- Never include API hashes, phone numbers, login codes, two-step verification passwords, proxy credentials, authorization keys, session databases, `tdata`, state databases, or private source identifiers in an issue or pull request.
- Report suspected vulnerabilities privately according to [SECURITY.md](SECURITY.md).
- Automated tests must not require real Telegram credentials.
- Any live test must be explicit, opt-in, and limited to dedicated accounts and sources.

## Development environment

Install Rust 1.96 or newer, Git, and the native build tools for your operating system. Clone the repository, open its root directory, and fetch the locked dependency set by running:

```console
cargo build --locked
```

`Cargo.lock` and the two patched crates under `vendor/` are required source files. Do not regenerate the lockfile or replace vendored source as part of an unrelated change.

## Change requirements

- Keep behavior changes small enough to review and test independently.
- Add or update a regression test when fixing deterministic behavior.
- Preserve existing configuration, SQLite state, chunk checkpoints, `.part` files, and completed output paths.
- Keep each active file assigned to only one account at a time.
- Preserve the configured proxy route during authorization, monitoring, reconnections, and media data-center migrations.
- Respect Telegram cooldowns. Do not introduce behavior that ignores or bypasses server-requested waits.
- Never log secrets, raw session content, or canonical proxy URLs that contain credentials.
- Update the README and changelog when a change affects users.

Changes under `vendor/` must remain narrowly scoped. Update [vendor/README.md](vendor/README.md) and [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md) whenever provenance, purpose, version, or licensing information changes.

## Quality gate

Run the complete quality gate before opening a pull request:

```console
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked --release
```

GitHub Actions repeats formatting and lint checks and performs native tests and release builds on Windows, Linux, macOS on Intel, and macOS on Apple Silicon.

## Pull requests

A pull request should explain:

1. the problem being addressed;
2. the resulting user-visible behavior;
3. the validation that was performed;
4. any effect on Telegram requests, account access, monitored sources, proxies, cooldowns, scheduling, state migrations, partial files, filenames, or security.

Keep broad refactoring separate from behavior changes whenever practical. This separation makes regressions easier to identify and review.
