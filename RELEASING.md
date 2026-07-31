# Release guide

Pushing a tag in the form `v<package-version>` starts the release workflow in `.github/workflows/release.yml`. The workflow validates the tag and source tree, builds on native GitHub-hosted runners, and publishes the following assets:

- `tgdl-<version>-windows-x86_64.zip`
- `tgdl-<version>-linux-x86_64.tar.gz`
- `tgdl-<version>-macos-aarch64.tar.gz`
- `tgdl-<version>-macos-x86_64.tar.gz`
- `SHA256SUMS`

Release binaries are currently unsigned.

## 1. Prepare the release

1. Select a version that follows [Semantic Versioning](https://semver.org/).
2. Update the package `version` in `Cargo.toml`.
3. Run `cargo check` once to update `Cargo.lock`, and confirm that only the root package version changed in the lockfile.
4. Move completed entries from **Unreleased** to a dated section in `CHANGELOG.md`.
5. Verify that the README, configuration example, security policy, third-party notices, and release notes describe the implemented behavior accurately.
6. Run the complete quality gate:

   ```console
   cargo fmt --all -- --check
   cargo clippy --locked --all-targets -- -D warnings
   cargo test --locked --all-targets
   cargo build --locked --release
   ```

7. Review the exact publication set:

   ```console
   git status --short --ignored
   git ls-files
   git ls-files -ci --exclude-standard
   ```

   The final command must produce no output.

8. Confirm that Git tracks no real configuration, session material, `tdata`, state database, `.part` file, credential, private identifier, log, build output, or release archive.
9. Merge the release preparation into the default branch and wait for every required CI check to pass.

## 2. Publish the tag

Create and push an annotated tag that exactly matches the package version:

```console
git tag -a v0.1.0 -m "tgdl v0.1.0"
git push origin v0.1.0
```

The release workflow rejects malformed tags and tags that do not match `Cargo.toml`. Versions containing a prerelease suffix are published as GitHub prereleases.

## 3. Verify the release

After the Release workflow completes:

1. Confirm that all four platform archives and `SHA256SUMS` are attached to the GitHub release.
2. Download every asset and verify its SHA-256 checksum.
3. Extract each archive and confirm that it contains the executable, README, changelog, license, third-party notices, security policy, and configuration example.
4. Smoke-test first launch, keyboard navigation, setup cancellation, application path display, and clean exit on each operating system family.
5. Perform a real download test only with dedicated authorized accounts and content.
6. Treat published tags as immutable. Publish corrections under a new patch version.

## Checksum verification

Linux:

```console
sha256sum --check SHA256SUMS
```

macOS:

```console
shasum -a 256 -c SHA256SUMS
```

Windows PowerShell:

```powershell
Get-FileHash .\tgdl-0.1.0-windows-x86_64.zip -Algorithm SHA256
```
