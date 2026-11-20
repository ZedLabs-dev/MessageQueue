# 0016. Public API lock, documentation and packaging

- Status: Accepted (2026-10-17); amended 2026-11-20 (ZedLabs rebrand, Apache-2.0, one-time Shipped baseline rewrite)
- Context: OPTIMIZATION-PLAN §6.2, §6.4, Phase 7; ADR 0015

## Decision

- **API lock:** Microsoft.CodeAnalysis.PublicApiAnalyzers on the three packages (`src/Directory.Build.props`).
  `PublicAPI.Shipped.txt` is the 1.0 baseline; `PublicAPI.Unshipped.txt` holds only `#nullable enable`, so
  RS0016/RS0017 fail the build on any unrecorded change. RS0026/RS0027 (several overloads with optional parameters)
  are off: every async overload takes a trailing `CancellationToken = default`, the BCL convention.
- **Review before the lock:** internal or sealed where nothing outside needs more (health check classes,
  `RetryExecutor`, `OpenTelemetryActivity`, validators internal; queues, configs, `RetryPolicy`, telemetry providers,
  hosted service, `ConfigurationException` sealed); unused `QueueHealthStatus` and `TransientMessageException`
  removed; `TelemetryActivity` gets the standard `Dispose()` / `protected virtual Dispose(bool)` pattern (CA1816
  enforced again); extension classes renamed `MessageQueueServiceCollectionExtensions` /
  `MessageQueueHealthCheckExtensions` to match the provider ones. Options keep `set` accessors: the options pattern
  (`Configure` delegates, `AddInMemoryMessageQueue(configure)`, the binder) assigns them; types nothing mutates
  already use `init`.
- **Docs:** `GenerateDocumentationFile` with CS1591 as an error on the packages, except the options files listed in
  `.editorconfig` (CLAUDE.md "Code conventions").
- **Packaging:** metadata in `Directory.Build.props` (Authors and Company ZedLabs, Apache-2.0, repository, tags,
  release notes link; until the 2026-11-20 amendment below: Eric Ewing and MIT); a README, `LICENSE` and `NOTICE` per
  package; deterministic CI builds, embedded untracked sources, `.snupkg` symbols, SourceLink. The SDK already bundles
  SourceLink; the explicit `Microsoft.SourceLink.GitHub` reference pins it under CPM.
- **Version** `1.0.0-rc.1`: 1.0.0 is blocked until the ASB integration specs run on a real namespace (CHANGELOG).
  `EnablePackageValidation` checks each package alone; set `PackageValidationBaselineVersion` after 1.0.0 ships.
- **Release:** `.github/workflows/release.yml` on `v*` tags reruns `ci.yml` (now `workflow_call`), checks the tag
  equals the project version, Unshipped files are empty and the CHANGELOG has the section, pushes with NuGet trusted
  publishing (`NuGet/login@v1`, OIDC) when the `NUGET_USER` variable is set, else the `NUGET_API_KEY` secret, then
  creates the GitHub release from that CHANGELOG section. Moving Unshipped to Shipped is a manual pre-release step
  (`/release` skill), so API changes are reviewed when they ship.
- **InternalsVisibleTo:** the packed assemblies grant it only to the two provider packages; the test and benchmark
  grants are conditioned on `_IsPacking`, which `dotnet pack` sets, so CI packs with a rebuild (no `--no-build`).
- **Release safety:** the tag must be on main, the `release` environment needs required reviewers, and the actions
  that see secrets or mint OIDC tokens (NuGet/login, claude-code-action) are pinned to commit SHAs.

## Consequences

- Every public change is visible in a reviewed text diff; accidental exposure fails the build.

## Amendment (2026-11-20): ZedLabs rebrand and relicensing

The repository moved to the ZedLabs organization (https://github.com/ZedLabs/MessageQueue) before any package was
published. The owner decided:

- **Names:** package IDs, assemblies, namespaces, project folders and the solution were renamed from `ESE.MessageQueue*`
  to `ZedLabs.MessageQueue*`; the ActivitySource and Meter from `ESE.MessageQueue` to `ZedLabs.MessageQueue`; metric and
  tag names from `ese.messagequeue.*` to `zedlabs.messagequeue.*`; library-owned headers and application properties from
  `x-ese-*` to `x-zedlabs-*`; the in-memory `messaging.system` from `ese.inmemory` to `zedlabs.inmemory`. The full map
  is in the CHANGELOG ("Breaking: ZedLabs rebrand").
- **License:** Apache-2.0 replaces MIT (`LICENSE` holds the full Apache License 2.0 text, `NOTICE` the attribution);
  Authors, Company and copyright holder are ZedLabs. Both files are packed into every package, and
  `Packaging/Given_the_packed_packages` packs each package and checks its nuspec (license expression, authors,
  copyright, project and repository URL) and the packed `LICENSE` and `NOTICE`.
- **One-time exception to "Shipped changes only in `/release`":** every entry of `PublicAPI.Shipped.txt` names the
  old namespaces, so the rename rewrote the Shipped baselines in place (same entries, new namespaces) and
  `PublicAPI.Unshipped.txt` the same way, keeping each `*REMOVED*` line matched to its Shipped entry. Nothing had been
  published, so no consumer compiled against the old baseline. The rule applies again from here on.
