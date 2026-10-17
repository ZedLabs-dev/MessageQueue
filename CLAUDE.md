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
scripts/coverage.sh                                         # full suite + coverage report + gate (TestResults/coverage-report/index.html)
dotnet format ESE.MessageQueue.sln                          # apply .editorconfig style
dotnet format ESE.MessageQueue.sln --verify-no-changes      # what CI runs
dotnet list ESE.MessageQueue.sln package --vulnerable --include-transitive   # must report none

cp MessageQueue/.env.example MessageQueue/.env              # once; .env is git-ignored
docker compose -f MessageQueue/docker-compose.dev.yml up -d --wait                       # RabbitMQ 4.x
docker compose -f MessageQueue/docker-compose.dev.yml --profile servicebus up -d          # + ASB emulator (ACCEPT_EULA=Y in .env)
```

The compose file is for manual experiments only; the tests do not use it (see Testing below).

## Architecture

| Layer | Folder | Key types |
|---|---|---|
| Abstractions | `Abstractions/` | `IMessageQueue<T>` (= `IMessagePublisher<T>` + `IMessageSubscriber<T>` + `Capabilities`), `MessageContext`, `MessageResult`, `PublishOptions`, `MessageQueueCapabilities` |
| Consumer pipeline | `Processing/` | internal `MessageDispatcher<T>` + `IDeliveryActions` transport seam (ADR 0004), `DeadLetterReasons`, `IMessageProcessor<T>`; shared consumer helpers `HandlerScope` (re-entrancy), `TransportBackoff`, `LogThrottle`, `BackgroundWork` (ADR 0010) |
| Configuration | `Configuration/`, `Extensions/` | `MessageQueueConfig` (portable base record), `MessageQueueProvider`, per-provider `IValidateOptions` validators, `ServiceCollectionExtensions.AddMessageQueue<T>` / `AddInMemoryMessageQueue<T>` |
| Providers | `RabbitMQ/`, `AzureServiceBus/`, `InMemory/` | `RabbitMqMessageQueue<T>` (RabbitMQ.Client 7, lazy async connect, `PublisherPool`, per-delay `RetryQueues`, `RabbitMqConnections` TLS rules; ADRs 0006–0008); `AzureServiceBusMessageQueue<T>` (thin SDK seams in `ServiceBusTransport.cs`, one sender, scheduled-copy retries on queues and lock-held retries on subscriptions in `ServiceBusMessages`, Entra ID identity; ADRs 0009–0012); `InMemoryMessageQueue<T>` (single-process, non-durable, ADR 0003) |
| Cross-cutting | `Serialization/`, `Retry/`, `Exceptions/` | `MessageSerializer` (JSON / MessagePack, `UntrustedData`), `RetryPolicy` + `RetryExecutor` (`MaxRetries` semantics, ADR 0001), `MessageQueueException` |
| Hosting & ops | `Hosting/`, `HealthChecks/`, `Telemetry/` | `MessageQueueHostedService<T>` (DI scope per message), health checks per broker (`AddRabbitMqHealthCheck<T>` / `AddAzureServiceBusHealthCheck<T>` reuse the queue's connection or client; no secrets or exception text in results), `ITelemetryProvider` (`System.Diagnostics` Activity/Meter) |

Abstractions never reference a broker SDK. Provider-specific settings live on the provider config
(`RabbitMqConfig`, `AzureServiceBusConfig`, `InMemoryConfig`), not on the base (ADR 0002). Every provider consumes
through `MessageDispatcher<T>` and implements only `IDeliveryActions` (complete / reschedule / dead-letter) plus
metadata mapping; retry, backoff, size limit and dead-letter decisions are never re-implemented in a provider.
Design decisions live in `docs/adr/`; read them before changing retry, config, in-memory or dispatcher behavior.

## Code conventions (enforced by `.editorconfig` + analyzers)

- File-scoped namespaces; `using` directives outside the namespace.
- Omit default accessibility: no `private` on members, no `internal` on top-level types.
- Fields: `readonly Type name;` camelCase, **no underscore prefix**. `static readonly`/`const`: PascalCase.
- `var` for locals. `this.` only to disambiguate (constructor assignments).
- Options/config classes: no XML comments; `[Required]`/`[Range]` attributes; a separate
  `IValidateOptions<T>` validator; `const` section names and defaults; register with `ValidateOnStart()`.
- Make only the requested change. No speculative features. Validate at system boundaries, trust internal code.
  Reuse existing abstractions. Mock only external dependencies.

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
7. Layout: one folder per area (`Serialization/`, `Retry/`, `Telemetry/`, `HealthChecks/`, `InMemory/`, `RabbitMQ/`,
   `AzureServiceBus/`, `Configuration/`, `Processing/`, `Hosting/`, `Abstractions/`), one class under test per file. Shared
   fixtures and helpers live in `Support/`.
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
  that starts one `rabbitmq:4.3.6-management` container via Testcontainers for the whole run and hands tests its
  connection string (Testcontainers supplies the credentials; test configs set `RequireTls = false`). It starts only when an integration test in that
  collection runs, so the unit loop never touches Docker. `Support/RabbitMqTestQueues` creates per-test queues and
  deletes them (and their dead-letter queues, lazily created `{queue}.retry.*` queues and exchanges) in the test
  class's `IAsyncLifetime.DisposeAsync`. RabbitMQ queues connect lazily: call `InitializeAsync` before asserting on
  topology.
- **Azure Service Bus:** integration tests run only when `MQ_ASB_CONNECTION_STRING` is set (a real namespace, or the
  emulator from the compose file after the owner accepts its EULA). They use the predeclared queue
  `integration-test-queue` and `integration-test-topic`/`integration-test-subscription` from
  `MessageQueue/servicebus-emulator-config.json` (`MQ_ASB_QUEUE_NAME`, `MQ_ASB_TOPIC_NAME`, `MQ_ASB_SUBSCRIPTION_NAME`
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
  `dotnet tool restore` once), prints line coverage for `ESE.MessageQueue`, writes an HTML report and fails below the
  gate (`COVERAGE_THRESHOLD`, default in the script). The gate is measured on the merged unit + integration run with
  Docker on and no ASB connection string; a unit-only run is lower. `--skip-tests` only merges existing results (CI).

## Hard rules

- **Never** early-`return`, catch-and-ignore, or loosen an assertion to make a test pass. A test that cannot
  run must be a real skip that shows up as skipped (`Assert.Skip`/`SkipWhen`/`SkipUnless`), never a vacuous pass.
- **Never** add or bump a package outside Central Package Management: versions live only in
  `Directory.Packages.props`; `PackageReference` has no `Version`. Commit the updated `packages.lock.json`.
- **Never** suppress NU19xx (NuGet audit) warnings. Fix the vulnerable dependency instead.
- **Never** blanket-disable analyzers. Deferring a rule needs an `.editorconfig` entry with a comment naming the phase.
- Public API changes must update `PublicAPI.Unshipped.txt`. Those files and PublicApiAnalyzers arrive in Phase 7;
  until then, record every public-surface change in `CHANGELOG.md` under Unreleased.
- Work on `main` only (owner's directive): phases land as commits on `main`. No feature branches, worktrees or PRs
  unless the owner asks.
- Do not edit `.env` files or hand-edit lock files (regenerate them with `dotnet restore`).
- Do not modify `PRODUCTION-READINESS-PLAN.md` or `MessageQueue/docs/azure-service-bus-implementation-guide.md`.

## Definition of done

- [ ] Specs written first and observed failing; now green.
- [ ] `dotnet build ESE.MessageQueue.sln -warnaserror` is clean.
- [ ] `dotnet format ESE.MessageQueue.sln --verify-no-changes` passes.
- [ ] `dotnet test` reports 0 failed and **0 vacuous passes**; skips appear as skips.
- [ ] `scripts/coverage.sh` passes: line coverage of `ESE.MessageQueue` at or above the gate (84% since the Azure
      Service Bus review fixes; never lower it).
- [ ] Vulnerable-package scan reports none.
- [ ] `CHANGELOG.md` Unreleased updated for any behavior or public-API change.
- [ ] Ticked the item in `OPTIMIZATION-PLAN.md` if the change completes one.
