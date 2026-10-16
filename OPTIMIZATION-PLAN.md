# ESE.MessageQueue — Optimization, Security & Agentic Workflow Plan

Reviewed 2026-10-16 against `main` @ `846883c`. This plan complements [PRODUCTION-READINESS-PLAN.md](PRODUCTION-READINESS-PLAN.md) (the long-term backlog) and adopts the conventions in [messagequeue-refactoring-instructions.md](messagequeue-refactoring-instructions.md). Each phase below leaves the build green and lands as one or more commits directly on `main` (owner's directive; no feature branches or worktrees).

---

## 0. Current state (verified)

| Check | Result |
|---|---|
| `dotnet build` | ✅ 0 errors, **28 warnings** (all NU1902/NU1903 vulnerability warnings) |
| `dotnet test` (no brokers) | "103 passed". **That number is misleading, see below.** |
| `dotnet test` (with RabbitMQ 4.x running) | 103 passed, and the RabbitMQ tests really ran |
| Vulnerable packages | **MessagePack 3.1.4: 3 high and 10 moderate advisories.** OpenTelemetry.Api 1.14.0: 1 moderate |
| Deprecated packages | `xunit` 2.9.2 is marked **Legacy**; the replacement is `xunit.v3` |
| SDKs installed | 9.0.203, 10.0.300. TFM is `net9.0`, and **.NET 9 support ends 2026-11-10** (per Microsoft's release metadata). Latest .NET 10 is 10.0.13, supported until 2028-11-14. |

### Why "103 passed" isn't trustworthy
- `MessageQueue.Tests/Skip.cs` is a no-op: the `throw` is commented out. When there is no broker, the RabbitMQ and ASB tests hit `if (queue == null) return;` and get reported as **passed**.
  - Without brokers, about 19 tests pass without testing anything.
  - Even with RabbitMQ running, all ~7 Azure Service Bus tests still pass vacuously, because there is no ASB emulator.
- `RabbitMqMessageQueueTests.Dispose()` is marked `[Fact]`, so a cleanup method runs as a test.
- `TelemetryTests.cs` (753 lines) holds 7 unrelated test classes: serializer, retry, health, in-memory, and others.
- `docker-compose.dev.yml` starts **Azurite**, which emulates Azure *Storage*, not Service Bus. The real emulator is `mcr.microsoft.com/azure-messaging/servicebus-emulator`, which needs a SQL Server container alongside it.

---

## 1. NuGet packages

### Library (`MessageQueue.csproj`)

| Package | Current | Action | Notes |
|---|---|---|---|
| MessagePack | 3.1.4 | **→ 3.1.12** (latest) | **Verified in a scratch copy:** `dotnet list package --vulnerable --include-transitive` reports no vulnerable packages once this bump and the removals below are applied. Also needs code hardening (§2). |
| Azure.Messaging.ServiceBus | 7.20.1 | → 7.21.0 | Minor version |
| RabbitMQ.Client | 6.8.1 | → 7.2.2 | **Major rewrite: async-only API, `IChannel`, built-in confirm tracking.** Own phase (Phase 5). |
| Microsoft.Extensions.Diagnostics.HealthChecks | 9.0.4 | → 10.0.x | Together with the net10 retarget |
| OpenTelemetry.Api | 1.14.0 | **Remove** | Vulnerable. The code only uses `System.Diagnostics` `ActivitySource`/`Meter`. |
| OpenTelemetry | 1.7.0 | **Remove** | Unused |
| OpenTelemetry.Extensions.Hosting | 1.7.0 | **Remove** | Unused |
| OpenTelemetry.Exporter.OpenTelemetryProtocol | 1.7.0 | **Remove** | Unused. A library should not choose exporters for its host. |
| OpenTelemetry.Exporter.Prometheus.AspNetCore | 1.6.0-rc.1 | **Remove** | Unused, prerelease, and pulls in ASP.NET Core |
| Microsoft.AspNetCore.OpenApi | 9.0.4 | **Remove** | Unused. Pulls the ASP.NET Core framework into a class library. |
| *(add)* Microsoft.Extensions.Configuration.Binder | — | **Add (required)** | Tested in a scratch copy: after the removals, `GetValue`/`Bind` in `ServiceCollectionExtensions` no longer compile without it. Hosting, Logging and DI abstractions still arrive through HealthChecks; reference them explicitly anyway. |

### Tests (`MessageQueue.Tests.csproj`)

| Package | Current | Target |
|---|---|---|
| xunit | 2.9.2 (Legacy) | **xunit.v3** (currently 4.0.2 on NuGet). Gives a native `Assert.Skip`/`SkipUnless`, which replaces the fake `Skip.cs`. |
| xunit.runner.visualstudio | 2.8.2 | 4.0.1 |
| Microsoft.NET.Test.Sdk | 17.12.0 | 18.x |
| coverlet.collector | 6.0.2 | latest (10.x) |
| NSubstitute | 5.3.0 | 6.x |
| FluentAssertions | 8.8.0 | **Replace with AwesomeAssertions 9.6.0** (D1). FA v8 needs a paid Xceed license; the Apache-2.0 fork keeps the same `.Should()...because` API. |
| *(add)* Testcontainers.RabbitMq, Testcontainers.ServiceBus (4.16.0) | — | Self-contained integration tests |
| *(add)* Microsoft.Extensions.DependencyInjection, .Logging, .Configuration | — | **Required** once the OTel/OpenApi packages are removed. The tests use `BuildServiceProvider`, `AddLogging` and `ConfigurationBuilder`, which were only arriving transitively. |

### Make package hygiene permanent
- `Directory.Packages.props` for **Central Package Management**: one place for versions.
- `Directory.Build.props` with `NuGetAudit=true`, `NuGetAuditMode=all`, `NuGetAuditLevel=moderate`, and `<WarningsAsErrors>NU1902;NU1903;NU1904</WarningsAsErrors>`. A vulnerable package then **fails the build**, so neither a human nor an agent can miss it.
- `RestorePackagesWithLockFile=true`, with `--locked-mode` in CI.
- `global.json` pins SDK `10.0.x` with `rollForward: latestFeature`.
- Dependabot (`.github/dependabot.yml`) for NuGet, GitHub Actions and Docker, grouped weekly.

---

## 2. Security review

| # | Severity | Finding | Fix |
|---|---|---|---|
| S1 | **High** | MessagePack 3.1.4 has known high-severity CVEs, and the code deserializes broker input with `ContractlessStandardResolver` but **without `MessagePackSecurity.UntrustedData`**, which is the attack surface those CVEs cover (hash-collision DoS, deep-nesting stack overflow). | Upgrade to 3.1.12 **and** use `.WithSecurity(MessagePackSecurity.UntrustedData)` in [MessageSerializer.cs](MessageQueue/Serialization/MessageSerializer.cs). Add a test that a hostile, deeply nested payload throws instead of crashing the process. |
| S2 | Medium | OpenTelemetry.Api 1.14.0 has a moderate advisory | Remove the package (it's unused) |
| S3 | Medium | ASB only supports SAS connection strings. There is no managed identity or Entra ID. | Add `FullyQualifiedNamespace` and an optional `TokenCredential` (`DefaultAzureCredential`). Prefer identity and document it as the production default. |
| S4 | Medium | `AzureServiceBusHealthCheck` uses `ServiceBusAdministrationClient`, which **requires Manage rights**. That forces the app to hold over-privileged credentials. It also creates a new client on every probe. | Default to a receiver `PeekMessageAsync` liveness check, which needs only Listen. Make the admin-metrics check opt-in. Reuse a singleton client. |
| S5 | Medium | Health checks put `ex.Message` and the broker host name/port in `HealthCheckResult.Data`. Health endpoints are often unauthenticated. | Remove raw exception text and topology from `Data`. Log the details instead. |
| S6 | Medium | Exception text leaks into the broker and telemetry: ASB `AbandonMessageAsync` stores `OriginalException` (the message text) as an application property, dead-letter descriptions include `ex.Message`, and `OpenTelemetryActivity` records the **stack trace** on spans. | Store the exception *type* only. Make message and stack-trace capture opt-in. Truncate long values. |
| S7 | Low/Med | RabbitMQ has no TLS configuration and no enforcement. `amqp://` is accepted silently. | Add `RequireTls` (default `true` outside Development). Reject plain `amqp://` when it's set. Expose `SslOption` (server name, cert validation). |
| S8 | Low | `docker-compose.dev.yml` and the tests hardcode `admin/password123`. The compose file uses floating tags (`3-management`, `latest`), and RabbitMQ 3.x is EOL. | Move credentials to `.env` (git-ignored) with a committed `.env.example`. Tests read env vars or use Testcontainers. Pin `rabbitmq:4.x-management` by digest. |
| S9 | Low | No message-size limit before deserialization | Add `MaxMessageSizeBytes` (default 1 MB) and dead-letter anything larger without deserializing it |
| S10 | Low | Connection-failure exceptions wrap `ex.Message` from the client. Check that no client library echoes the URI with credentials. | Add a test asserting that the `MessageQueueException` message never contains the password from a URI like `amqp://u:SECRET@host`. |

**Process:** add CodeQL (C#) and `dotnet list package --vulnerable --include-transitive` to CI, and run the Claude Code `/security-review` on every PR (see §5).

---

## 3. Correctness bugs (fix test-first)

Write a failing test for each bug, then fix it.

| # | Where | Bug |
|---|---|---|
| B1 | [RabbitMQMessageQueue.cs:138](MessageQueue/RabbitMQ/RabbitMQMessageQueue.cs:138), [AzureServiceBusMessageQueue.cs:71](MessageQueue/AzureServiceBus/AzureServiceBusMessageQueue.cs:71) | `Interlocked.Increment(...) % length` goes **negative after int overflow**, which throws `IndexOutOfRangeException` after about 2.1 billion publishes. Use `(int)((uint)Interlocked.Increment(ref i) % (uint)len)`. |
| B2 | RabbitMQ consumer | `EventingBasicConsumer` with an `async` lambda is an **async void** handler, and it calls `BasicAck`/`BasicNack` concurrently on a shared `IModel`, which isn't thread-safe. Fixed by the v7 migration (`AsyncEventingBasicConsumer` + `IChannel`). |
| B3 | RabbitMQ consumer | **`RetryAttempts` is ignored.** Any failure does `BasicNack(requeue:false)` and sends the message straight to the DLQ, or **drops it** if the DLQ is disabled. Implement **real backoff** (D2): on failure, ack the original and republish to a per-queue retry queue with a per-message TTL that dead-letters back to the main exchange. Carry the attempt count in a header and send to the DLQ after the last attempt. |
| B4 | RabbitMQ `SubscribeAsync` | `BasicQos` is only applied for quorum queues, so classic queues get **unbounded prefetch**. Always apply it. |
| B5 | ASB `HandleRetry` | The computed exponential delay is **never applied**. `Abandon` redelivers immediately, and `NextRetryAt` is fiction. Implement **real backoff** (D2): schedule a clone with `ScheduledEnqueueTime` and an attempt-count property, then complete the original. Dead-letter after the last attempt. Keep `MaxDeliveryCount` as a crash-safety net only. |
| B6 | ASB `SubscribeAsync` | A second call adds a second `ProcessMessageAsync` handler, and the SDK throws. `ProcessErrorAsync` **swallows every error silently**. Guard against re-subscribe, and log and record telemetry in the error handler. |
| B7 | `AzureServiceBusMessageQueue` | Has `DisposeAsync()` but **doesn't implement `IAsyncDisposable`**, so DI calls the sync `Dispose()`, which blocks on async (sync-over-async). Declare the interface. Make both providers' dispose idempotent. |
| B8 | ASB batch publish | `Chunk(MaxBatchSize)` ignores the byte-size limit (256 KB/1 MB), so large batches fail at runtime. Use `CreateMessageBatchAsync` + `TryAddMessage`. |
| B9 | [OpenTelemetryProvider.cs](MessageQueue/Telemetry/OpenTelemetry/OpenTelemetryProvider.cs) | `OpenTelemetryActivity.SetError` uses `new`, not `override`. Callers holding a `TelemetryActivity` never set the span status. Make the base method `virtual`. |
| B10 | Telemetry | `ITelemetryProvider` is registrable but **no queue implementation uses it**: no spans, no metrics, no `traceparent` propagation. Inject it (default `NullTelemetryProvider`) and wire publish, consume, retry and dead-letter. |
| B11 | [MessageQueueHostedService.cs](MessageQueue/Hosting/MessageQueueHostedService.cs) | Several problems: <ul><li>The **scoped processor is resolved once and held for the app's lifetime** (a captive dependency, e.g. one DbContext forever).</li><li>`MessageContext` is fabricated (new GUID, `DeliveryCount = 1`).</li><li>`Retry` and `DeadLetter` both collapse to `false`.</li></ul> Fix: create a scope per message, pass the real broker metadata, and support all three `MessageResult` values (implement `IMessageSubscriber<T>`, which is declared but never used). |
| B12 | [InMemoryMessageQueue.cs](MessageQueue/Testing/InMemoryMessageQueue.cs) | `ProcessAllAsync` **loops forever** when a handler always returns `false`, because it requeues inside the drain loop. Snapshot the count, or add a max-attempts setting. |
| B13 | `RetryExecutor` | `new Random()` is not thread-safe, so use `Random.Shared`. Exponential backoff casts `double` → `long` ticks *before* the cap, which overflows for large attempt counts. Clamp in `double` first. |
| B14 | `AddMessageQueue` | A missing `Provider` key binds to enum value 0 and **silently selects RabbitMQ**. Missing validation: pool sizes > 0, `MaxOutstandingConfirms` > 0, ASB `SubscriptionName` required when `TopicName` is set. Move to `IOptions<T>` + `IValidateOptions<T>` + `ValidateOnStart()` (matches §5 of the refactoring instructions). |
| B15 | RabbitMQ `PublisherChannel` | With `AutomaticRecoveryEnabled`, a recovered channel resets `NextPublishSeqNo`, so stale `pendingConfirms` keys can collide. Obsolete after the v7 migration, which tracks confirms natively. |
| B16 | Config | These config properties are **never used**: `DeadLetterQueueName` (ASB), `EnablePartitioning`, `RequiresDuplicateDetection`, `DuplicateDetectionHistoryTimeWindow`, `BatchFlushInterval`. Resolve them through the config split in §3a (D4). |

---

## 3a. Provider-agnostic architecture

The library's main purpose is event messaging **without the application knowing which broker it runs on**. RabbitMQ and Azure Service Bus are just two implementations, and AWS SNS/SQS or MQTT may follow. Changes that follow from this:

1. **In-memory becomes a first-class provider** (D3).
   - Add `MessageQueueProvider.InMemory`, selectable from config just like RabbitMQ, so local development and simple deployments need no broker.
   - Move it out of the `Testing` namespace into `ESE.MessageQueue.InMemory`, still in the main package.
   - Give it the same semantics as the brokers:
     - background dispatch to the subscriber
     - bounded concurrency (`MaxConcurrentCalls`)
     - the shared retry/backoff pipeline
     - an in-memory dead-letter list
     - `MessageContext` with a real message ID and delivery count
   - This also fixes B12.
   - Keep the test helpers (`ProcessAllAsync`, `PublishedMessages`) on it.
   - Document it clearly as **single-process and non-durable**: messages are lost on restart.
2. **One retry pipeline, many transports** (D2). Retry policy, attempt counting, backoff, dead-letter decisions and telemetry live in a shared consumer pipeline driven by `RetryPolicy`/`RetryExecutor`. Each provider implements only a small transport seam, roughly:
   - `CompleteAsync`
   - `RescheduleAsync(delay)`
   - `DeadLetterAsync(reason)`
   - `DeliveryAttempt`

   The readiness-plan default is 5 attempts, about 60 s total. The same pipeline feeds `MessageResult.Retry`/`DeadLetter` (B11).
3. **Base config holds only portable concepts** (D4):
   - Base: `QueueName`, `RetryPolicy`, `SerializationFormat`, `EnableDeadLetterQueue`.
   - Provider-specific settings move to provider configs. `DeadLetterQueueName` moves to RabbitMQ (and a future SQS redrive target); ASB's DLQ is the fixed `$deadletterqueue` subqueue.
   - Entity-shape settings (`EnablePartitioning`, `RequiresDuplicateDetection`, `DuplicateDetectionHistoryTimeWindow`) are removed. They are set when the queue is created, belong in infrastructure-as-code, and would require Manage rights (conflicts with S4).
   - `BatchFlushInterval` is removed until buffered publishing is actually built.
4. **Portable idempotency instead of broker dedup flags.** Let publishers supply a message key or ID (`PublishAsync(message, options)`). Each provider maps it to its native mechanism:
   - ASB duplicate detection uses `MessageId`.
   - SQS FIFO uses `MessageDeduplicationId`.
   - RabbitMQ and MQTT carry it as a header so the consumer can dedupe.
5. **Capability honesty.** Each provider declares what it guarantees: durability, DLQ, scheduled delivery, ordering. `ValidateOnStart` fails fast when config asks for something the provider can't do. MQTT, for example, has no broker-side DLQ or delay, so the pipeline would emulate them or the config is rejected.

---

## 4. Performance optimizations

Benchmark before and after each change using a new `MessageQueue.Benchmarks` project (BenchmarkDotNet, `[MemoryDiagnoser]`). Don't merge "optimizations" without numbers.

1. **Serializer allocations:**
   - Use `JsonSerializer.SerializeToUtf8Bytes` and `Deserialize<T>(ReadOnlySpan<byte>)` instead of the UTF-8 string round trip.
   - Accept `ReadOnlyMemory<byte>` so consumers skip `Body.ToArray()`. On RabbitMQ use `ea.Body` directly; on ASB use `Body.ToMemory()`.
   - Optionally offer a `JsonSerializerContext` overload for source-generated, AOT-friendly serialization.
2. **RabbitMQ batch publish** `PublishAsync(IEnumerable<T>)` awaits each confirm serially, so throughput is bound by round-trip time. Publish all messages, then await their confirms together (bounded by `MaxOutstandingConfirms`).
3. **ASB sender pool:** `ServiceBusSender` is thread-safe, and every pooled sender shares one AMQP connection. Benchmark the pool against a single sender. If the pool doesn't help, remove `PublisherSender`, the pool and the semaphores.
4. **Message IDs:** use `Guid.CreateVersion7()` (sortable, better for broker indexes and logs). Use native `DateTimeOffset` application properties instead of `ToString("O")`.
5. **Hosted service:** add `LoggerMessage` source-generated logging. Create a scope per message (B11).
6. **Health checks:** reuse the queue's existing connection or client instead of opening a new TCP/AMQP connection on every probe (also fixes S4).
7. **Sample code in the shipped package:** move `Examples/` to a `samples/` project. The in-memory queue stays in the main package as a real provider (see §3a).

---

## 5. Test plan

### Infrastructure
- Migrate to **xunit.v3**.
  - Delete `Skip.cs`.
  - Unavailable brokers become **real skips** via `Assert.SkipUnless`, or the tests run via Testcontainers.
  - **Rule: a test must never `return` early to pass.**
- Add **Testcontainers** for RabbitMQ 4.x and the Azure Service Bus emulator, so integration tests need no manual Docker setup and can run in CI.
- Add traits `[Trait("Category","Unit")]` and `[Trait("Category","Integration")]` so there's a fast loop: `dotnet test --filter Category=Unit`.
- Split `TelemetryTests.cs` into one file per class under test. Remove the `[Fact]` from `RabbitMqMessageQueueTests.Dispose`, and implement `IAsyncLifetime` cleanup.
- Add a coverage gate: coverlet line coverage on `MessageQueue/`, enforced in CI, with a ReportGenerator summary. The gate starts at the measured Phase 3 baseline (**59%**), is ratcheted up each phase and never lowered, and targets **80%**.

### Missing tests to add
| Area | Tests |
|---|---|
| `ServiceCollectionExtensions` | Provider selection, missing provider fails (B14), `ValidateOnStart` failures for each rule, telemetry registration |
| `MessageQueueHostedService` | Scope per message, `Retry`/`DeadLetter`/`Acknowledge` mapping, no processor registered, cancellation |
| Serializer | Untrusted MessagePack payload rejected (S1), oversize payload (S9), `ReadOnlyMemory` overloads, null/empty body, unknown format |
| Round-robin | Index stays in range across `int.MaxValue` wrap (B1) |
| RabbitMQ consumer | Extract ack/nack/retry decision logic into a pure, testable class. Cover success, handler false, handler throws, deserialization failure, retry limit reached → DLQ (B3). |
| ASB consumer | Use `ServiceBusModelFactory` and an `IServiceBusMessageActions` seam to test complete, abandon, dead-letter, retry delay (B5) and re-subscribe guard (B6) **without a broker** |
| `PublisherSender` | Timeout → `MessageQueueException`, caller cancellation propagates, semaphore released on failure |
| Telemetry | `SetError` through the base reference sets the span status (B9). `traceparent` round-trip through publish → consume (B10). Malformed `traceparent`. |
| `InMemoryMessageQueue` | Handler that always fails terminates (B12) |
| `RetryExecutor` | Large attempt count doesn't overflow (B13), concurrent use |
| Health checks | No secrets or exception text in `Data` (S5), least-privilege ASB check (S4) |
| Security | Connection-string password never appears in exception messages (S10) |
| Integration (Testcontainers) | Publish/consume round trip, DLQ after N retries, large message, deserialization failure → DLQ, connection recovery, topic/subscription for ASB |

**Definition of done for every phase:**
- `dotnet build -warnaserror` passes clean.
- `dotnet test` reports **0 failed and 0 vacuous passes**, and skips appear as skips.
- Coverage is at or above the gate.

---

## 6. Agentic-engineering setup

The goal: any agent (Claude Code locally, in GitHub Actions, or on a schedule) can pick up a task, verify its own work deterministically, and can't silently regress quality or security.

### 6.1 Repo context for agents
- **`CLAUDE.md`** (root). Short and imperative. Contents:
  - Build, test and format commands, including the fast unit filter.
  - Architecture map: abstractions → providers → hosting/telemetry.
  - The conventions from `messagequeue-refactoring-instructions.md`: `When_<Condition>` test names, assertions with `because` text that starts "it should", no AAA comments, options + `IValidateOptions` pattern, no underscore-prefixed fields.
  - Hard rules:
    - Never early-return in tests.
    - Never add a package without CPM.
    - Never suppress NU19xx.
    - Public API changes require updating `PublicAPI.Unshipped.txt`.
  - "Definition of done" checklist (from §5).
- Fold `messagequeue-refactoring-instructions.md` into `CLAUDE.md`. Keep `PRODUCTION-READINESS-PLAN.md` as the backlog, and turn its unchecked items into GitHub issues so agents can be assigned to them.
- `docs/adr/` with short Architecture Decision Records: retry semantics per provider, serializer security, identity-first auth, the RabbitMQ 7 migration. Agents read these instead of re-deciding.

### 6.2 Deterministic guardrails (the most important part)
Agents work best when the build itself rejects bad output:
- `Directory.Build.props`:
  - `TreatWarningsAsErrors`
  - `Nullable=enable`
  - `AnalysisLevel=latest-recommended`
  - `EnforceCodeStyleInBuild`
  - `GenerateDocumentationFile` (library only)
  - the NuGetAudit settings from §1
- `.editorconfig` encoding the house style, so `dotnet format --verify-no-changes` enforces it.
- **`Microsoft.CodeAnalysis.PublicApiAnalyzers`**: any accidental public-surface change fails the build. This directly supports PRODUCTION-READINESS-PLAN item 1 (locking the v1 API).
- Lock files + `--locked-mode`, so agents can't drift dependency versions.

### 6.3 Claude Code project config (`.claude/`)
- `settings.json`:
  - Permission allowlist for `dotnet build|test|format|list package`, `docker compose -f MessageQueue/docker-compose.dev.yml *`, `git status|diff|log`.
  - Deny edits to `.env` and lock files except through restore.
- **Hooks:**
  - `PostToolUse` on Edit/Write of `*.cs` → `dotnet format --include <file>`.
  - `Stop` → `dotnet build -warnaserror && dotnet test --filter Category=Unit`, so the agent can't finish with a red build.
- **Subagents** (`.claude/agents/`):
  - `test-writer`: follows the naming and assertion conventions and must show the failing test first.
  - `security-reviewer`: checks against §2 and runs the vulnerable-package scan.
  - `package-updater`: bumps through CPM, reads release notes for majors, runs the full suite.
- **Skills / slash commands** (`.claude/skills/`):
  - `/update-packages`: outdated → bump → build → test → summarize breaking changes.
  - `/add-tests <file>`: coverage gaps for a file → tests.
  - `/release`: version bump, changelog, PublicAPI shipped/unshipped move.

### 6.4 CI/CD (`.github/workflows/`)
- `ci.yml` on every push to `main` (and on PRs, if PRs are ever adopted):
  - restore (locked)
  - `build -warnaserror`
  - `format --verify-no-changes`
  - unit tests
  - integration tests (Testcontainers, or RabbitMQ and ASB-emulator service containers)
  - coverage gate
  - `list package --vulnerable` → fail
- `codeql.yml` (C#), weekly and on PR.
- **Claude Code GitHub Action**: `@claude` on issues and PRs to implement backlog items, plus an automatic review pass (`/code-review` + `/security-review`) on each PR.
- **Scheduled agent** (weekly routine): run `/update-packages` and open a PR. Dependabot covers the plain bumps; the agent handles majors that need code changes.
- *Optional, future:* branch protection on `main` with required CI checks and human approval for agent-authored PRs, if the owner moves from commits-on-main to a PR flow.
- Release workflow: `dotnet pack` with SourceLink and deterministic builds, then push to the NuGet feed on tag.

---

## 7. Execution order

Each phase lands as commits on `main` with a green CI. Claude Code hooks (`PostToolUse` format, `Stop` build+unit-test gate, §6.3) land in Phase 7, not Phase 1.

| Phase | Scope | Exit criteria |
|---|---|---|
| **1. Guardrails & honest baseline** — implemented 2026-10-16; exit criteria still open: CI has not run yet, and ~19 broker tests still pass vacuously until Phase 3. The ASB emulator compose is validated with `docker compose config` only (EULA left for the owner), and its predeclared entity names do not yet match the random queue names in `AzureServiceBusIntegrationTests`. | `global.json`, `Directory.Build.props` (audit settings, but warnings not yet errors), CPM, `CLAUDE.md`, `.claude/settings.json`, CI skeleton, `.env` for the compose file and the real ASB emulator | CI runs; test counts are reported truthfully |
| **2. Security patch, package diet & net10** ✅ done 2026-10-16 (106/106 pass on net10 with RabbitMQ 4.3.6 running; vulnerable scan clean; S1 specs `When_Configuring_MessagePack` and `When_Deserializing_A_MessagePack_Dictionary` were seen failing without `UntrustedData`. The deep-nesting spec is a regression guard only: MessagePack enforces the 500-level depth limit even for `TrustedData`.) | MessagePack 3.1.12 + `UntrustedData` (S1); remove the 6 unused packages and add Configuration.Binder plus the 3 test-project references (S2); minor bumps; **retarget to `net10.0`** with Microsoft.Extensions.* 10.0.x; **turn on `TreatWarningsAsErrors`** | 0 NU19xx warnings; vulnerable scan clean. Already proven on net9 in a scratch copy: build OK, 103/103 pass, scan clean. |
| **3. Test infrastructure** — implemented 2026-10-16; exit criterion "integration tests run in CI" still open until CI runs (107 tests: 89 Unit + 18 Integration. Docker on, no ASB connection string: 101 passed, 6 ASB skipped, and one `rabbitmq:4.3.6-management` container was created and destroyed. Docker socket blocked: 89 passed, 18 skipped, no container. **Coverage gate: 59%** line on `ESE.MessageQueue`, the measured 59.9% (Debug) / 60.6% (Release) on the merged unit + integration run, rounded down; ratchet to 80% in later phases. xunit.v3 runs through VSTest, not MTP. Testcontainers.ServiceBus was not added: the emulator needs the owner to accept its EULA, so ASB integration tests are gated on `MQ_ASB_CONNECTION_STRING`. CI wiring: unit and integration steps, `MQ_REQUIRE_DOCKER=true`, and the coverage gate.) | xunit.v3, delete `Skip.cs`, Testcontainers, traits, split test files, coverage gate, FluentAssertions → AwesomeAssertions | No vacuous passes; integration tests run in CI |
| **4. Bug fixes & provider model** | B1, B3–B7, B9–B14, each test-first; §3a (in-memory provider, shared retry pipeline, config split, resolving B16) | All new regression tests pass |
| **5. RabbitMQ.Client 7 migration** | Async API, `IChannel`, native confirms, `AsyncEventingBasicConsumer`; delete `PublisherChannel` (fixes B2, B15) | Integration suite passes on RabbitMQ 4.x; benchmark at least matches 6.x |
| **6. Hardening & perf** | S3–S10, §4 optimizations with benchmarks, telemetry wiring (B10) | Benchmarks recorded in `docs/benchmarks.md` |
| **7. API lock** | PublicApiAnalyzers, XML docs, samples project, package metadata, `v1.0.0` | `PublicAPI.Shipped.txt` committed |

Phases 1–3 make every later phase safe to hand to an agent. Phases 4, 5 and 6 split into independent tasks that land as sequential commits on `main`.

---

## 8. Decisions (2026-10-16)

| # | Decision | Effect |
|---|---|---|
| D1 | Use **AwesomeAssertions** instead of FluentAssertions 8 | Phase 3: swap the package and `using` lines; the assertion style is unchanged |
| D2 | **Real backoff** for every provider | B3/B5 unblocked; shared retry pipeline (§3a.2) |
| D3 | The in-memory queue is a **first-class provider**, not just a test double | §3a.1; stays in the main package |
| D4 | Portable base config; `DeadLetterQueueName` moves to provider configs that use it; remove `EnablePartitioning`, `RequiresDuplicateDetection`, `DuplicateDetectionHistoryTimeWindow` and `BatchFlushInterval`; add a portable idempotency key. Opt-in ASB auto-provisioning and buffered publishing were considered and deferred to the backlog. | §3a.3–4, resolves B16 |
