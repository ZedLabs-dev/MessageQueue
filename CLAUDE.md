# ESE.MessageQueue

Provider-agnostic event messaging library for .NET 10. Applications code against `IMessageQueue<T>`;
RabbitMQ, Azure Service Bus and an in-memory queue are interchangeable implementations.
Roadmap: `OPTIMIZATION-PLAN.md` (phased work), `PRODUCTION-READINESS-PLAN.md` (backlog).

## Commands

```bash
dotnet restore ESE.MessageQueue.sln --locked-mode          # CI restore; lock files must not drift
dotnet build ESE.MessageQueue.sln -warnaserror              # warnings are errors (also via Directory.Build.props)
dotnet test ESE.MessageQueue.sln --blame-hang-timeout 2m                             # full suite (RabbitMQ via Testcontainers; needs Docker)
dotnet test ESE.MessageQueue.sln --blame-hang-timeout 2m --filter Category=Unit      # fast loop: no Docker, no broker
dotnet test ESE.MessageQueue.sln --blame-hang-timeout 2m --filter Category=Integration
scripts/coverage.sh                                         # full suite + coverage report + gates (TestResults/coverage-report/index.html)
dotnet format ESE.MessageQueue.sln                          # apply .editorconfig style
dotnet format ESE.MessageQueue.sln --verify-no-changes      # what CI runs
dotnet list ESE.MessageQueue.sln package --vulnerable --include-transitive   # must report none
dotnet pack ESE.MessageQueue.sln -c Release -o artifacts    # the three packages + .snupkg symbols
scripts/mutate.py scripts/mutants/<set>.json [ids]          # mutation testing in a scratch copy (never the working tree)

dotnet run --project samples/ESE.MessageQueue.Samples                         # sample worker, in-memory (append `-- --environment RabbitMq|AzureServiceBus`)
dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*' --job short   # docs/benchmarks.md

cp docker/.env.example docker/.env                          # once; .env is git-ignored
docker compose -f docker/docker-compose.dev.yml up -d --wait                       # RabbitMQ 4.x
docker compose -f docker/docker-compose.dev.yml --profile servicebus up -d          # + ASB emulator (ACCEPT_EULA=Y in .env)
```

The compose file is for manual experiments only; the tests do not use it (see Testing below).

## Layout and architecture

| Path | Contents |
|---|---|
| `src/ESE.MessageQueue` | Core package, no broker SDK. `Abstractions/` (`IMessageQueue<T>` = `IMessagePublisher<T>` + `IMessageSubscriber<T>` + `Capabilities`, `MessageContext`, `MessageResult`, `PublishOptions`), `Providers/` (public provider SPI: `IMessageQueueProvider`, `MessageQueueRegistration<T>`, `MessageDispatcher<T>`, `IDeliveryActions`, `PublishTelemetry`, `PublishValidation`, `MessageSerializer`, `MessageQueueConfigValidator<T>`), `Processing/` (internal consumer helpers `HandlerScope`, `TransportBackoff`, `LogThrottle`, `BackgroundWork`; `DeadLetterReasons`, `IMessageProcessor<T>`), `Configuration/`, `Extensions/` (`AddMessageQueue<T>`, `AddMessageQueueProvider`, `AddInMemoryMessageQueue<T>`), `Retry/`, `Hosting/` (`MessageQueueHostedService<T>`, DI scope per message), `Telemetry/` (`ITelemetryProvider`, `OpenTelemetryProvider`, W3C `TraceContext`; ADR 0013), `HealthChecks/` (generic check), `InMemory/` (built-in provider, ADR 0003) |
| `src/ESE.MessageQueue.RabbitMQ` | RabbitMQ.Client 7 provider: lazy connect, `PublisherPool`, per-delay `RetryQueues`, TLS rules, health check, `AddRabbitMqProvider()` (ADRs 0006-0008) |
| `src/ESE.MessageQueue.AzureServiceBus` | Service Bus provider: SDK seams in `ServiceBusTransport.cs`, scheduled-copy retries, Entra ID, peek health check, `AddAzureServiceBusProvider()` (ADRs 0009-0012) |
| `tests/ESE.MessageQueue.Tests` | One test project for all three packages (ADR 0015) |
| `samples/` | `ESE.MessageQueue.Samples` (worker; the only OpenTelemetry exporter reference, ADR 0014) and `ESE.MessageQueue.Samples.LoopbackProvider` (a provider built on the public SPI only, no InternalsVisibleTo) |
| `benchmarks/` | BenchmarkDotNet; never run by `dotnet test`, CI only builds it |
| `docker/` | Dev compose file, `.env.example`, ASB emulator config |
| `docs/` | `README.md` (index), `getting-started/` (one guide per provider), `configuration.md` (settings reference), `operations/` (failure modes, monitoring, alerts, runbooks), `release-checklist.md`, `production-readiness-status.md` (tracking view of PRODUCTION-READINESS-PLAN), `adr/` (decisions: read before changing retry, config, dispatcher, packaging), API docs, `benchmarks.md` |

- Providers are selected by the `Provider` setting among registered providers (`InMemory` built in; brokers via
  `Add…Provider()` **before** `AddMessageQueue<T>`). Core binds, validates and registers for every provider (ADR 0015).
- Every provider consumes through `MessageDispatcher<T>` and implements only `IDeliveryActions` plus metadata
  mapping; retry, backoff, size limit and dead-letter decisions are never re-implemented in a provider (ADR 0004).
- Provider-specific settings live on the provider config (`RabbitMqConfig`, `AzureServiceBusConfig`, `InMemoryConfig`),
  never on `MessageQueueConfig` (ADR 0002).
- First-party providers use core internals through `InternalsVisibleTo` and pin the exact core version; a third-party
  provider must work with the public SPI alone.

## Code conventions (enforced by `.editorconfig` + analyzers)

- File-scoped namespaces; `using` directives outside the namespace.
- Omit default accessibility: no `private` on members, no `internal` on top-level types.
- Fields: `readonly Type name;` camelCase, **no underscore prefix**. `static readonly`/`const`: PascalCase.
- `var` for locals. `this.` only to disambiguate (constructor assignments).
- Options/config classes: no XML comments; `[Required]`/`[Range]` attributes; a separate
  `IValidateOptions<T>` validator; `const` section names and defaults; register with `ValidateOnStart()`.
- Make only the requested change. No speculative features. Validate at system boundaries, trust internal code.
  Reuse existing abstractions. Mock only external dependencies.
- This file supersedes `messagequeue-refactoring-instructions.md` (the owner's original brief, kept unedited) where
  they disagree: assertions use AwesomeAssertions, not FluentAssertions (decision D1), and provider settings live on
  the provider configs, not on `MessageQueueConfig` (ADR 0002).

## Testing: behavior-driven, spec first

1. Write the spec first. Run it and **see it fail** for the right reason before writing implementation.
2. Test classes are named for the behavior or context under test: `Given_a_rabbitmq_queue`, `PublishingMessages`.
3. Test methods: `When_<Condition_Or_Action>`, e.g. `When_Message_Is_Valid`, `When_ConnectionString_Is_Null`.
4. Assertions use AwesomeAssertions (Apache-2.0 fork of FluentAssertions) with a `because` that starts with
   `"it should"` / `"it should not"` and describes behavior, so output reads "When X, it should Y":
   ```csharp
   result.Should().BeTrue("it should acknowledge a valid message");
   await act.Should().ThrowAsync<MessageQueueException>("it should not leak the connection password");
   ```
5. No `// Arrange` / `// Act` / `// Assert` comments.
6. Every test class has `[Trait("Category","Unit")]` or `[Trait("Category","Integration")]`. Unit tests must not
   need a broker, Docker or the network.
7. Layout: one folder per area in `tests/ESE.MessageQueue.Tests` (`Serialization/`, `Retry/`, `Telemetry/`,
   `HealthChecks/`, `InMemory/`, `RabbitMQ/`, `AzureServiceBus/`, `Configuration/`, `Processing/`, `Providers/`,
   `Hosting/`, `Abstractions/`, `Packaging/`, `Samples/`), one class under test per file. Shared fixtures and helpers live in `Support/` (`SpanCapture` / `MetricCapture` listen to one
   ActivitySource / Meter: give each test its own source name).
8. Pass `TestContext.Current.CancellationToken` to anything that takes a token (analyzer xUnit1051 fails the build).
   Async tests that could hang get `[Fact(Timeout = ...)]`; CI also runs with `--blame-hang-timeout 2m`.
9. Before asserting an exact count of asynchronously delivered messages, use `Wait.ForCountAsync`: it waits for the
   count, then requires it to stay stable for 500 ms so late duplicates fail the test.
10. The default retry policy takes about 60 s to give up. Any test whose handler fails sets a millisecond `Retry`
    policy (see `RabbitMqTestQueues.FastRetry`). For the in-memory provider, await `WaitForIdleAsync` instead of polling.

### Test infrastructure

- **Runner:** xunit.v3 run through VSTest (`xunit.runner.visualstudio`), not Microsoft Testing Platform
  (`IsTestingPlatformApplication=false` in the test project). That keeps `--filter Category=Unit` and
  `--collect:"XPlat Code Coverage"` working with plain `dotnet test`.
- **RabbitMQ:** `Support/RabbitMqBrokerFixture` is an xunit collection fixture (`[Collection(RabbitMqCollection.Name)]`)
  that starts one RabbitMQ container (image `tag@sha256:` digest in `docker/test-broker/Dockerfile`, where Dependabot bumps both) via Testcontainers for the whole run and hands tests its
  connection string (Testcontainers supplies the credentials; test configs set `RequireTls = false`). It starts only when an integration test in that
  collection runs, so the unit loop never touches Docker. `Support/RabbitMqTestQueues` creates per-test queues and
  deletes them (and their dead-letter queues, lazily created `{queue}.retry.*` queues and exchanges) in the test
  class's `IAsyncLifetime.DisposeAsync`. RabbitMQ queues connect lazily: call `InitializeAsync` before asserting on
  topology.
- **Azure Service Bus:** integration tests run only when `MQ_ASB_CONNECTION_STRING` is set (a real namespace, or the
  emulator from the compose file after the owner accepts its EULA). They use the predeclared queue
  `integration-test-queue` and `integration-test-topic`/`integration-test-subscription` from
  `docker/servicebus-emulator-config.json` (`MQ_ASB_QUEUE_NAME`, `MQ_ASB_TOPIC_NAME`, `MQ_ASB_SUBSCRIPTION_NAME`
  override them; `MQ_ASB_TRANSACTIONAL_RETRIES=false` for entities without transactions).
  Without a namespace, the provider's behavior is unit-tested through its internal SDK seams with
  `Support/FakeServiceBus` (async sends and settlements that enlist in the ambient transaction, a processor stop that
  waits for running handlers, deliveries that refuse a second settlement, like the SDK) and `ServiceBusModelFactory` messages. Never accept the emulator's EULA on the owner's behalf.
- **Skips are skips:** no Docker means the RabbitMQ tests report **Skipped** (`Assert.SkipWhen` in the test class's
  `InitializeAsync`); no `MQ_ASB_CONNECTION_STRING` means the ASB tests report **Skipped**. `MQ_REQUIRE_DOCKER=true` (or `1`;
  set in CI) turns missing Docker into a failure. Only Testcontainers' `DockerUnavailableException` is turned into a
  skip; any other container start failure fails the tests.
- **Coverage:** coverlet.collector writes Cobertura; `scripts/coverage.sh` merges every
  `TestResults/**/coverage.cobertura.xml` with ReportGenerator (local tool in `.config/dotnet-tools.json`, run
  `dotnet tool restore` once) for the three package assemblies, writes an HTML report and fails when their combined
  line coverage is below `COVERAGE_THRESHOLD` or any one of them is below `COVERAGE_ASSEMBLY_FLOOR`. The gate is measured on the merged unit + integration run with
  Docker on and no ASB connection string; a unit-only run is lower. `--skip-tests` only merges existing results (CI).

## Hard rules

- **Never** early-`return`, catch-and-ignore, or loosen an assertion to make a test pass. A test that cannot
  run must be a real skip that shows up as skipped (`Assert.Skip`/`SkipWhen`/`SkipUnless`), never a vacuous pass.
- **Never** add or bump a package outside Central Package Management: versions live only in
  `Directory.Packages.props`; `PackageReference` has no `Version`. Commit the updated `packages.lock.json`.
- Docs are specs too: changing a setting, default, startup message, `[LoggerMessage]` or metric fails
  `Packaging/Given_the_configuration_reference` / `Given_the_monitoring_reference` until `docs/configuration.md` or
  `docs/operations/monitoring.md` follows. Guide code lives in `samples/ESE.MessageQueue.Samples/Docs/`
  (`#region snippet:<name>`), copied verbatim into `docs/getting-started/` (`Given_the_documentation_snippets`).
- **Never** suppress NU19xx (NuGet audit) warnings. Fix the vulnerable dependency instead.
- **Never** blanket-disable analyzers. Deferring a rule needs an `.editorconfig` entry with a comment naming the phase.
- Public API changes must update `src/<package>/PublicAPI.Unshipped.txt` (RS0016/RS0017 fail the build otherwise)
  and `CHANGELOG.md`. Every public type and member gets XML docs (CS1591 is an error), except the options classes
  listed in `.editorconfig`. `PublicAPI.Shipped.txt` changes only in the release step (`/release`).
- The core package never references RabbitMQ.Client or Azure.* (`Packaging/Given_the_package_split`).
- Work on `main` only (owner's directive): phases land as commits on `main`. No feature branches, worktrees or PRs
  unless the owner asks.
- Do not edit `.env` files or hand-edit lock files (regenerate them with `dotnet restore`).
- Do not modify `PRODUCTION-READINESS-PLAN.md` or `messagequeue-refactoring-instructions.md`. The untracked
  `MessageQueue/docs/azure-service-bus-implementation-guide.md` is the owner's unmaintained draft left at the
  pre-Phase 7 path: leave it alone, do not stage it, and do not treat it as current documentation.

## Definition of done

- [ ] Specs written first and observed failing; now green.
- [ ] `dotnet build ESE.MessageQueue.sln -warnaserror` is clean.
- [ ] `dotnet format ESE.MessageQueue.sln --verify-no-changes` passes.
- [ ] `dotnet test` reports 0 failed and **0 vacuous passes**; skips appear as skips.
- [ ] `scripts/coverage.sh` passes: combined line coverage of the three packages at or above the gate (89%) and each
      package at or above its floor (87%); never lower either.
- [ ] Vulnerable-package scan reports none.
- [ ] `CHANGELOG.md` Unreleased updated for any behavior or public-API change.
- [ ] Adversarial review done for changes to `src/` (below) and its survivors turned into specs.
- [ ] Ticked the item in `OPTIMIZATION-PLAN.md` if the change completes one.

## Claude Code tooling (`.claude/`, ADR 0017)

- **Hooks:** every edited `.cs` file is formatted on the spot (`.claude/hooks/format-cs.sh`). The Stop hook
  (`.claude/hooks/stop-gate.sh`) builds with `-warnaserror` and runs the Unit tests whenever C# or build files differ
  from HEAD; while red, the turn cannot end (it lets go when `stop_hook_active` is set, and Claude Code caps repeats).
  It does not run integration tests, coverage or the scan: the definition of done still applies.
- **Agents:** `test-writer` (spec first, red shown), `security-reviewer` (section 2 checklist, scan, secrets grep),
  `package-updater`, `adversarial-reviewer`. **Skills:** `/update-packages`, `/add-tests <file>`, `/release <version>`,
  `/mutation-test`.
- **Adversarial review before commit** (the owner's workflow): before committing a change to `src/`, run the
  `adversarial-reviewer` agent (or `/mutation-test`). It lists the rules the change promises, mutates each one with
  `scripts/mutate.py` in a scratch copy, probes concurrency, shutdown, hostile input and secret leaks there, and
  reports survivors; each survivor becomes a spec before the commit. Never mutate or probe in the working tree.
- Unattended work (weekly `update-packages.yml`, `@claude` in `claude.yml`) arrives as a pull request: the owner works
  on `main`, and that pull request is the review vehicle.
