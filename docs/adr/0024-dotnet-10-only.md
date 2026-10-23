# 0024. .NET 10 only

- Status: Accepted (2026-10-23)
- Context: PRODUCTION-READINESS-PLAN §10 ("support only .NET 10.x and forward"); OPTIMIZATION-PLAN Phase 2; ADR 0016

## Context

The three packages were moved to `net10.0` in Phase 2 and the SDK is pinned in `global.json`, but no record said
which runtimes a 1.x release supports, or why the library does not multi-target. Applications on .NET 8 or 9 would
otherwise expect a `net8.0` asset sooner or later.

## Decision

- **One target framework, `net10.0`**, set once in `Directory.Build.props` for every project (packages, tests,
  samples, benchmarks). No `netstandard2.0`, `net8.0` or `net9.0` assets, no `#if NET...` branches.
- **Support window:** .NET 10 is an LTS release, supported by Microsoft until 2028-11-14. The 1.x line targets it for
  that whole window. Applications on .NET 11 and later consume the `net10.0` assets through forward compatibility.
- **Moving on:** a later target (the next LTS, .NET 12) is a deliberate, recorded decision: a new major version if
  the older target is dropped, never a silent bump. Patch releases never change the target framework.
- **SDK:** `global.json` pins the 10.0 feature band with `rollForward: latestFeature`; CI builds with that SDK.

## Rationale

- The library already depends on APIs newer than .NET 8 (`Guid.CreateVersion7` for message ids) and on the
  `Microsoft.Extensions.*` 10.0 packages (options validation on start, source-generated logging, hosting); a lower
  target would need polyfills or older package lines.
- Multi-targeting would multiply the test matrix (and Testcontainers runs) per framework for a library whose users
  are new services; the cost is real, the demand is not.
- One LTS target gives operators a single date to plan around.

## Consequences

- Applications on .NET 8 or 9 cannot use 1.x; they stay on their own messaging code or upgrade the runtime.
- Dependencies are kept on their .NET 10-compatible lines (`Microsoft.Extensions.*` 10.0.x, Central Package
  Management); Dependabot bumps within them.
- Revisit before 2028-11-14, or earlier when .NET 12 (LTS) ships.
