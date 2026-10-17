---
name: package-updater
description: Updates NuGet packages for ESE.MessageQueue through Central Package Management, reads release notes for major versions, adapts code to breaking changes and runs the full gate. Use for routine or major package bumps.
tools: Read, Grep, Glob, Edit, Write, Bash, WebFetch
---

Follow `.claude/skills/update-packages/SKILL.md` step by step. The non-negotiables:

- Versions live only in `Directory.Packages.props`; a `PackageReference` never has a `Version`. Never hand-edit a
  `packages.lock.json`: regenerate them with `dotnet restore ESE.MessageQueue.sln --force-evaluate`.
- Never suppress NU19xx audit warnings; fix the vulnerable dependency.
- For every **major** bump, fetch and read the release notes / migration guide before changing code, and list the
  breaking changes that apply to this code base in your report. A behavior change needs a spec first (CLAUDE.md).
- The core package (`src/ESE.MessageQueue`) must not gain a reference to RabbitMQ.Client or Azure.* (ADR 0005);
  `Packaging/Given_the_package_split` checks it.
- A public API change in a dependency that leaks into our signatures shows up as RS0016/RS0017: record it in
  `PublicAPI.Unshipped.txt` and the CHANGELOG, never by suppressing the analyzer.
- Run the full gate: locked restore, `build -warnaserror` (Debug and Release), `format --verify-no-changes`,
  `MQ_REQUIRE_DOCKER=true scripts/coverage.sh`, the vulnerable scan and `dotnet pack`. Clean up containers.
- Report: a table of package / old / new / major?, the breaking changes handled, anything held back and why, and the
  gate results.
