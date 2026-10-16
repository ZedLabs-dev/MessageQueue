# Changelog

All notable changes to ESE.MessageQueue are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Provider-agnostic core (OPTIMIZATION-PLAN Phase 4: §3a, B9, B11–B14, S9, §4.1)

Design records: `docs/adr/0001`–`0004`. Pre-v1, so these breaking changes ship without a deprecation period.

#### Breaking
- `IMessageQueue<T>` now composes `IMessagePublisher<T>` and `IMessageSubscriber<T>` and adds
  `MessageQueueCapabilities Capabilities { get; }`. Custom implementations must add the new members.
- `IMessagePublisher<T>` adds `PublishAsync(T, PublishOptions, CancellationToken)` (message id as the portable
  idempotency key, correlation id, string headers).
- `IMessageSubscriber<T>`'s rich handler is now `Func<T, MessageContext, CancellationToken, Task<MessageResult>>`
  (was `Func<T, MessageContext, Task<MessageResult>>`). Every provider implements it.
- `MessageContext` is a `sealed record`: `MessageId` (required), new `CorrelationId`, `Timestamp` is now
  `DateTimeOffset`, `DeliveryCount` renamed to `DeliveryAttempt` (1-based), `Properties` replaced by read-only
  `Headers` (`IReadOnlyDictionary<string, string>`). `MessageContext` and `MessageResult` moved to their own files
  (same namespace).
- `IMessageProcessor<T>.ProcessAsync` gains a `CancellationToken` parameter.
- `RetryPolicy.MaxAttempts` renamed to `MaxRetries` and now means retries *after* the first attempt
  (at most `MaxRetries + 1` deliveries). `RetryExecutor.ShouldRetry`/`ExecuteAsync` follow suit: `ExecuteAsync` makes
  `MaxRetries + 1` attempts (it made `MaxAttempts`). XML comments removed from `RetryPolicy` (it is now config).
- `RetryExecutor.CalculateDelay(int attemptNumber)` parameter renamed to `retryNumber` (source-breaking for named
  arguments). `RetryPolicy.Aggressive` keeps `MaxRetries = 5` and therefore now makes up to 6 deliveries (was 5).
- New default retry policy: 5 retries, exponential from 2 s, multiplier 2, max 30 s, 10% jitter (about 60 s in total).
  Was 3 attempts from 1 s, max 5 min.
- `MessageQueueConfig`: `RetryAttempts` and `RetryDelay` replaced by `Retry` (a `RetryPolicy`, section `Retry`);
  `ConnectionString` moved to `RabbitMqConfig` and `AzureServiceBusConfig`; `DeadLetterQueueName` moved to
  `RabbitMqConfig`; new `MaxMessageSizeBytes` (default 1 MB) and `MaxConcurrentCalls` (default 10, moved from
  `AzureServiceBusConfig`); new `const SectionName = "MessageQueue"`.
- `AzureServiceBusConfig`: removed the unused `EnablePartitioning`, `RequiresDuplicateDetection`,
  `DuplicateDetectionHistoryTimeWindow` and `BatchFlushInterval` (B16). Use `PublishOptions.MessageId` for idempotency.
- `AddMessageQueue<T>`: a missing, unknown or numeric `Provider` now throws `ConfigurationException` instead of
  silently selecting RabbitMQ. Setting validation moved from registration time to host start: invalid values throw
  `OptionsValidationException` from `ValidateOnStart` (or on first resolve) instead of `ConfigurationException`.
  New rules: pool sizes, `MaxOutstandingConfirms`, `MaxConcurrentCalls`, `MaxMessageSizeBytes` > 0, `PrefetchCount`
  1..65535, timeouts > 0, ASB `SubscriptionName` required when `TopicName` is set, retry values sane.
- Binding is strict: unknown keys (typos, removed settings such as `RetryAttempts`, `RetryDelay`,
  `EnablePartitioning`) fail at host start with `InvalidOperationException`, as do values the binder cannot convert
  (`"PrefetchCount": "ten"`). `Provider`, `SerializationFormat` and `Retry:Strategy` accept only enum *names*
  (case-insensitive): numbers (`"1"`, `"+1"`) and combinations (`"A,B"`) are rejected.
- Registering a second queue for the same message type (`AddMessageQueue<T>` or `AddInMemoryMessageQueue<T>`) throws
  `ConfigurationException`.
- Azure Service Bus: `SubscriptionName` without `TopicName` is rejected. In-memory: `QueueName` is required in
  configuration (no default; `AddInMemoryMessageQueue<T>()` and `new InMemoryMessageQueue<T>()` use the type name).
- Provider constructors validate their config with the same rules as startup and throw `OptionsValidationException`.
- `PublishAsync` validates its arguments on every provider: null message/options throw `ArgumentNullException`;
  a blank or over-128-character `MessageId`, more than 64 headers, blank or long header names, values over 4096
  characters, and reserved header names (`x-*`, `traceparent`, `tracestate`) throw `ArgumentException`.
- `MessageContext.Headers` is immutable: the given dictionary is copied into a frozen dictionary.
- The in-memory queue moved from `ESE.MessageQueue.Testing` to `ESE.MessageQueue.InMemory` and is now a provider
  (`Provider = InMemory`). `AddInMemoryMessageQueue<T>()` moved to `ESE.MessageQueue.Extensions.ServiceCollectionExtensions`
  and takes an optional `Action<InMemoryConfig>`. Removed `ProcessAllAsync`, `ProcessOneAsync`, `Count` and
  `PendingMessages`: delivery is now automatic on a background loop; use `WaitForIdleAsync`, `PendingCount`,
  `ProcessedMessages` and the new `DeadLetters`. `Clear()` now clears only the recorded lists.
- `MessageQueueHostedService<T>` constructor is now `(IMessageSubscriber<T>, IServiceScopeFactory,
  ILogger<MessageQueueHostedService<T>>, IServiceProviderIsService? = null)` (was `(IServiceProvider, ILogger<...>)`).
  It overrides `StartAsync`/`StopAsync`: it subscribes during host start (a subscribe failure now fails the host
  instead of being logged and ignored), unsubscribes on stop, and does nothing but log a warning when no
  `IMessageProcessor<T>` is registered.
- `RabbitMqMessageQueue<T>` and `AzureServiceBusMessageQueue<T>` constructors take optional `ITelemetryProvider` and
  `ILogger`. Both implement `IAsyncDisposable` and `IDisposable` with idempotent dispose. Subscribing twice to the
  RabbitMQ or in-memory queue throws `InvalidOperationException`.
- `TelemetryActivity.SetError` is now `virtual` (`OpenTelemetryActivity` overrides it). `OpenTelemetryActivity`'s
  constructor gains an optional `recordExceptionDetails` parameter.
- `InMemoryMessageQueue<T>` is `sealed`.
- Design decision recorded for Phase 7: split into ESE.MessageQueue (core), ESE.MessageQueue.RabbitMQ and
  ESE.MessageQueue.AzureServiceBus with a public provider-registration extension point (ADR 0005). Not implemented yet.

#### Fixed
- B9: `SetError` called through a `TelemetryActivity` reference now sets the span's error status.
- B11: the hosted service creates a DI scope per message, passes the broker's real `MessageContext`, and applies
  `Retry` and `DeadLetter` instead of collapsing both to "not acknowledged".
- `AddInMemoryMessageQueue(instance)` registers the caller's instance (and its aliases) as instances, so disposing
  the container does not dispose a queue the caller owns.
- B12: an always-failing handler on the in-memory queue no longer loops forever; it is dead-lettered after `MaxRetries`.
- B13: `RetryExecutor` uses `Random.Shared` and clamps in `double`, so huge attempt numbers cannot overflow into
  negative delays, and jitter never exceeds `MaxDelay`.
- B14: see `AddMessageQueue<T>` above.
- RabbitMQ: failures are now retried with the policy's backoff (republished with an `x-delivery-attempt` header after
  an in-process wait) before going to the dead-letter queue, instead of being dead-lettered on the first failure.
  Acks and nacks are serialized on the consumer channel, and `UnsubscribeAsync` now cancels the token passed to
  running handlers (as the in-memory and ASB providers do).
- RabbitMQ/ASB: the publisher round-robin index no longer goes negative after `int` overflow (B1).
- ASB: abandon no longer copies the exception message into application properties; the dead-letter description
  holds the exception type only (S6).

#### Added
- Shared consumer pipeline used by every provider (ADR 0004): size limit, deserialization, handler, retry/backoff,
  dead-letter or drop, consume span and consumed/retried/dead-lettered metrics.
- S9: `MaxMessageSizeBytes` (default 1 MB); larger bodies are dead-lettered (`MessageTooLarge`) without deserializing.
- `PublishOptions` limits (`MaxMessageIdLength`, `MaxHeaders`, `MaxHeaderNameLength`, `MaxHeaderValueLength`),
  `DeadLetterReasons.DeliveryFailed`, `InMemoryConfig.ShutdownTimeout`, `OpenTelemetryActivity.MaxRecordedLength`.
- `PublishOptions`, `MessageQueueCapabilities`, `DeadLetterReasons`, `InMemoryConfig`, `InMemoryDeadLetter<T>`,
  `MessageQueueProvider.InMemory`, `RabbitMqConfigValidator`, `AzureServiceBusConfigValidator`, `InMemoryConfigValidator`.
- Constants: `RetryPolicy.MaxRetriesLimit` (1000), `DefaultMaxRetries`, `DefaultInitialDelay`, `DefaultMaxDelay`,
  `MaxAllowedDelay` (1 day); `MessageQueueConfig.SectionName`, `DefaultMaxMessageSizeBytes`, `DefaultMaxConcurrentCalls`,
  `MaxConcurrentCallsLimit`; `InMemoryConfig.DefaultShutdownTimeout`. `RetryPolicy`'s `[Range]` attributes were dropped:
  the provider validators enforce the retry ranges (DataAnnotations do not validate nested options).
- Multiple queues per container: options are named per message type, so each `T` can bind its own section and provider.
  `IMessagePublisher<T>` and `IMessageSubscriber<T>` are registered as aliases of `IMessageQueue<T>`.
- Message ids are version 7 GUIDs unless `PublishOptions.MessageId` is set.
- `OpenTelemetryProvider(sourceName, recordExceptionDetails)` and `AddMessageQueueTelemetry(sourceName, recordExceptionDetails)`.
- Packages: Microsoft.Extensions.Options.ConfigurationExtensions and Microsoft.Extensions.Options.DataAnnotations 10.0.13.

- Shared pipeline robustness: telemetry and logging failures never affect a message; an `IsRetryable` that throws
  counts as non-retryable; a `CustomDelayCalculator` that throws falls back to `InitialDelay` (logged). The in-memory
  queue dead-letters (`DeliveryFailed`) a message whose delivery failed before it was settled.
- In-memory lifecycle: `WaitForIdleAsync` throws `ObjectDisposedException` after dispose instead of hanging;
  unsubscribe and dispose wait for running handlers for at most `InMemoryConfig.ShutdownTimeout` (default 30 s).
- Hosted service: an exception while disposing a message's DI scope is logged and no longer turns a successful
  result into a retry.

#### Changed
- RabbitMQ and Azure Service Bus log a startup warning when the retry policy has a backoff they cannot honour yet
  (`Capabilities.DelayedRedelivery = false`); see ADR 0001 for the interim behaviour.
- Config records' `ToString()` prints `ConnectionString = ***`.
- S6: spans record `exception.type` only; the exception message and stack trace are recorded only with
  `recordExceptionDetails: true`, truncated to 4096 characters. The span status description is the exception type by default.
- §4.1: JSON serializes straight to UTF-8 (`SerializeToUtf8Bytes`) and deserializes from `ReadOnlySpan<byte>`;
  consumers pass `ReadOnlyMemory<byte>`. A JSON body that is not valid UTF-8 now fails deserialization
  (it was silently repaired with U+FFFD). ASB passes the body without copying; RabbitMQ.Client 6.x still needs one copy.
- Logging in the library uses source-generated `LoggerMessage`.

### Changed
- **Breaking:** retargeted from `net9.0` to `net10.0`. Consumers must target .NET 10.
- **Behavior:** MessagePack deserialization now uses `MessagePackSecurity.UntrustedData`.
  Dictionaries and hash sets are rebuilt with hash-collision-resistant comparers, and object graphs
  deeper than 500 levels are rejected. Message types whose dictionary/hash-set keys are custom types
  without a supported collision-resistant hash may now fail to deserialize with
  `MessagePackSerializationException`.
- **Source-breaking:** `InMemoryMessageQueue<T>.SubscribeAsync` parameter renamed from `messageHandler`
  to `handler` to match `IMessageQueue<T>`. Only callers that pass the argument by name are affected.
- MessagePack 3.1.4 → 3.1.12 (fixes known high/moderate advisories).
- Azure.Messaging.ServiceBus 7.20.1 → 7.21.0.
- Microsoft.Extensions.Diagnostics.HealthChecks 9.0.4 → 10.0.13.
- Local development compose file: RabbitMQ 4.3, real Azure Service Bus emulator (replaces Azurite),
  credentials from `MessageQueue/.env`.

### Added
- Explicit references to Microsoft.Extensions.Configuration.Binder, DependencyInjection.Abstractions,
  Hosting.Abstractions, Logging.Abstractions and Options (10.0.13). These were previously only transitive.

### Removed
- Unused package references that leaked into consumers: Microsoft.AspNetCore.OpenApi, OpenTelemetry,
  OpenTelemetry.Api (vulnerable), OpenTelemetry.Extensions.Hosting,
  OpenTelemetry.Exporter.OpenTelemetryProtocol, OpenTelemetry.Exporter.Prometheus.AspNetCore.
  Telemetry still uses `System.Diagnostics` `ActivitySource`/`Meter`; hosts choose their own exporters.
  Apps that relied on these packages arriving transitively must reference them directly.
- Stale `MessageQueue/scripts/*.sh` (referenced docker-compose v1, RabbitMQ 3 and a non-existent test project).

### Development
Test infrastructure only; no change to the library or its public API.
- Tests migrated to xunit.v3 4.0.2 (run through VSTest with xunit.runner.visualstudio 4.0.1) and from
  FluentAssertions 8 to AwesomeAssertions 9.6.0 (Apache-2.0).
- No more vacuous passes: the no-op `Skip.cs` and the `Helper.IsRabbitMqAvailable` early returns are gone. RabbitMQ
  integration tests run against a `rabbitmq:4.3.6-management` container started by Testcontainers and report
  **skipped** when Docker is unavailable (`MQ_REQUIRE_DOCKER=true` makes that a failure). Azure Service Bus integration
  tests run only when `MQ_ASB_CONNECTION_STRING` is set (replaces `AZURE_SERVICEBUS_CONNECTION_STRING`) and are
  skipped otherwise.
- Every test class carries `[Trait("Category","Unit")]` or `[Trait("Category","Integration")]`;
  `dotnet test --filter Category=Unit` is the fast loop.
- Tests reorganized into one folder per area and one behavior-named class per file (`Given_a_...`), and
  `TelemetryTests.cs` split by class under test. The cleanup method that ran as a `[Fact]` is now `IAsyncLifetime`
  cleanup. Hardcoded broker credentials removed from the tests.
- Coverage gate: `scripts/coverage.sh` (coverlet + ReportGenerator local tool) fails CI below 59% line coverage of
  `ESE.MessageQueue`. CI now runs unit and integration tests as separate steps.
