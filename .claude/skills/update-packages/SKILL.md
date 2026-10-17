---
name: update-packages
description: Update this repo's NuGet packages through Central Package Management - list outdated, bump, regenerate lock files, read release notes for majors, adapt code test-first, run the full gate and summarize breaking changes.
argument-hint: "[package-id ...] (default: all outdated)"
---

Update NuGet packages for ESE.MessageQueue. Arguments (optional package ids): $ARGUMENTS

1. **Inventory.**
   ```bash
   dotnet list ESE.MessageQueue.sln package --outdated --include-transitive
   dotnet list ESE.MessageQueue.sln package --vulnerable --include-transitive
   ```
   Build a table: package, current, latest stable, major?, where used (Library / Samples / Benchmarks / Tests /
   Build groups in `Directory.Packages.props`). Skip prereleases. Keep `Microsoft.Extensions.*` on the runtime's
   major (10.x) and on one version together.
2. **Bump** only in `Directory.Packages.props` (`<PackageVersion Include=... Version=...>`). Never add `Version` to a
   `PackageReference`, never suppress NU19xx.
3. **Majors:** read the release notes or migration guide (WebFetch the GitHub releases page or the project's docs)
   before building. List each breaking change that touches this code. A behavior change gets a failing spec first
   (CLAUDE.md "Testing"); a public signature change goes into `PublicAPI.Unshipped.txt` and `CHANGELOG.md`.
4. **Lock files:** `dotnet restore ESE.MessageQueue.sln --force-evaluate`, then confirm
   `dotnet restore ESE.MessageQueue.sln --locked-mode` passes. Never hand-edit `packages.lock.json`.
5. **Gate** (all must pass):
   ```bash
   dotnet build ESE.MessageQueue.sln -warnaserror
   dotnet build ESE.MessageQueue.sln -warnaserror -c Release
   dotnet format ESE.MessageQueue.sln --verify-no-changes
   MQ_REQUIRE_DOCKER=true scripts/coverage.sh
   dotnet list ESE.MessageQueue.sln package --vulnerable --include-transitive
   dotnet pack ESE.MessageQueue.sln -c Release -o artifacts
   ```
   The core package must still not reference RabbitMQ.Client or Azure.* (`Packaging/Given_the_package_split`).
   Remove containers you started (`docker ps -a` before and after).
6. **Changelog:** add the bumps under `## [Unreleased]` → `### Changed` in `CHANGELOG.md`.
7. **Summary** for the commit message or pull request: the version table, breaking changes and what was changed for
   them, packages held back and why, and the gate results (test counts, coverage per assembly).
