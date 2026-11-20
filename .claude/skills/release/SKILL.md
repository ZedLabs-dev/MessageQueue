---
name: release
description: Prepare and tag an ZedLabs.MessageQueue release - version bump, CHANGELOG section, PublicAPI Unshipped-to-Shipped move, full gate, pack check, then the tag that triggers .github/workflows/release.yml.
argument-hint: "<version, e.g. 1.0.0-rc.2 or 1.0.0>"
disable-model-invocation: true
---

Prepare release $ARGUMENTS. Stop and ask before tagging or pushing; the owner pushes.

1. **Blockers.** Read `CHANGELOG.md` → "Release blockers". A final (non-prerelease) version is not allowed while a
   blocker is open (today: the Azure Service Bus integration specs have never run against a real broker).
2. **Version.** In `Directory.Build.props` set `VersionPrefix` and `VersionSuffix` (empty for a final release).
   The tag will be `v<VersionPrefix>[-<VersionSuffix>]`; `release.yml` refuses a mismatch.
3. **Public API.** For each package in `src/*/`: append every line of `PublicAPI.Unshipped.txt` (except
   `#nullable enable`) to `PublicAPI.Shipped.txt`, keep the shipped file sorted, and leave Unshipped with only
   `#nullable enable`. A `*REMOVED*<entry>` line deletes `<entry>` from Shipped instead of being appended, and a
   shipped `~` (oblivious) entry whose annotated form is being appended is deleted too
   (`Packaging/Given_the_public_api_files` checks that every shipped `~` entry is retired that way).
   `release.yml` refuses a non-empty Unshipped file. After the first stable release, set
   `PackageValidationBaselineVersion` in `src/Directory.Build.props` to the previous version.
4. **Changelog.** Rename `## [Unreleased]` to `## [<version>] - <yyyy-mm-dd>` and add a fresh empty
   `## [Unreleased]` above it. The release notes on GitHub are that section, verbatim.
5. **Gate.**
   ```bash
   dotnet restore ZedLabs.MessageQueue.sln --locked-mode
   dotnet build ZedLabs.MessageQueue.sln -warnaserror && dotnet build ZedLabs.MessageQueue.sln -warnaserror -c Release
   dotnet format ZedLabs.MessageQueue.sln --verify-no-changes
   MQ_REQUIRE_DOCKER=true scripts/coverage.sh
   dotnet list ZedLabs.MessageQueue.sln package --vulnerable --include-transitive
   dotnet pack ZedLabs.MessageQueue.sln -c Release -o artifacts
   ```
   Check `artifacts/`: three `.nupkg` and three `.snupkg` with the new version; the provider packages depend on
   exactly that core version (`unzip -p artifacts/ZedLabs.MessageQueue.RabbitMQ.*.nupkg '*.nuspec'`).
6. **Commit and tag** (owner's go-ahead): commit on `main`, then `git tag v<version>` and `git push origin main v<version>`.
   `release.yml` reruns the CI gates, pushes to nuget.org (trusted publishing when the `NUGET_USER` variable is set,
   else the `NUGET_API_KEY` secret) and creates the GitHub release with the changelog section.
