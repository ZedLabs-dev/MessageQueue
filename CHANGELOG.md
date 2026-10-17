# Changelog

All notable changes to ESE.MessageQueue are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### RabbitMQ provider on RabbitMQ.Client 7 (OPTIMIZATION-PLAN Phase 5: B1–B4, B15, S7, S10, §4.2)

Design records: `docs/adr/0006`–`0008`.

#### Breaking
- RabbitMQ.Client 6.8.1 → 7.2.2 (async-only API). Apps that reference RabbitMQ.Client themselves must move to 7.x.
- `RabbitMqMessageQueue<T>` no longer connects in its constructor. It connects, and declares its exchange and queues,
  on the first `PublishAsync`/`SubscribeAsync` or on the new `InitializeAsync(CancellationToken)`. New
  `RabbitMqMessageQueue.CreateAsync<T>(config, telemetry, logger, cancellationToken)` constructs and connects in one
  call. Connection failures surface there as `MessageQueueException`, whose message names only the exception type
  (no client text, no credentials). A failed connect is retried on the next call.
- TLS is required by default for any host other than `localhost`/`127.0.0.1`/`::1` (S7): a plain `amqp://`
  connection string to a remote host now fails validation at startup and in the constructor. Use `amqps://` or set
  the new `RequireTls = false`. Connect-time re-checks throw `ConfigurationException`.
- `RabbitMqConfig.ConnectionString` is no longer `[Required]` alone: exactly one of `ConnectionString` or the new
  `HostName` must be set. `ConnectionString` must be an absolute `amqp://`/`amqps://` URI.
- Retries are no longer waited out in process: they are published to per-delay retry queues
  `{QueueName}.retry.{ms}` (declared on first use, same queue type as the main queue) and come back to the main
  queue when their TTL expires. `Capabilities.DelayedRedelivery` is now `true`. Delete the retry queues together
  with the main queue.
- Dead letters are published by the library to the dead-letter queue, with headers `x-ese-dead-letter-reason`
  (a `DeadLetterReasons` code) and `x-ese-dead-letter-detail` (exception type), then the original is acked. Before,
  the original was nacked into the broker DLX without a reason. The main queue's DLX arguments are unchanged.
- `MessageContext.DeliveryAttempt` now also counts broker redeliveries (`x-delivery-count` on quorum queues, the
  redelivered flag on classic queues). A delivery interrupted by a crash or by `UnsubscribeAsync` uses up an attempt.
- `MessageContext.Headers` no longer contains broker or library headers (`x-*`, `traceparent`, `tracestate`).
- `PrefetchCount` now applies to classic queues too (it applied only to quorum queues). `MaxConcurrentCalls` now
  bounds concurrent RabbitMQ handlers (it was ignored).
- `RabbitMqMessageQueue<T>.Dispose()` blocks on `DisposeAsync()`; prefer `DisposeAsync`.
- Removed the unused `RabbitMqConfig.EnableClusterMode` (it was never read).
- `QueueName` is limited to 240 UTF-8 bytes, and `DeadLetterQueueName` and `ExchangeName` to 255. This leaves room for
  the derived `.retry.{ms}` and `.deadletter` queues.
- `UnsubscribeAsync` returns after `ShutdownTimeout` (or when its token is cancelled) even if handlers are still
  running; they can still settle, and the channel closes once they finish. `DisposeAsync` waits one
  `ShutdownTimeout` for all handlers, then closes the connection (at most 1 s more), requeueing what is unsettled.
- `RabbitMqHealthCheck(connectionString)` applies the queue's TLS rules: plain `amqp://` to a non-local host is
  reported Unhealthy unless the new `requireTls: false` is passed (constructor and `AddRabbitMqHealthCheck`).
- After a failed connection, the next attempts back off (1 s, doubling, at most 30 s); calls in between fail fast
  with `MessageQueueException`.
- Retry queues now follow the retry policy's schedule (at most three per step for jitter; 14 for the default policy)
  and, when classic, expire when unused (`x-expires`, at least one hour; quorum retry queues never expire, because
  RabbitMQ 4.3.6 does not reset a quorum queue's expiry on redeclare). Zero-delay retries go through `{QueueName}.retry.0`.
- A publisher can no longer set the attempt: `x-delivery-attempt` is honoured only on messages coming back from the
  queue's retry queues, and `x-delivery-count` only on broker redeliveries from quorum queues.
- Removed the internal `PublisherChannel` (custom confirm tracking, B15); confirms are tracked by RabbitMQ.Client.

#### Added
- `RabbitMqConfig`: `RequireTls` (`bool?`), `Tls` (`Enabled`, `ServerName`, `AcceptablePolicyErrors`,
  `AllowInsecureCertificates`; accepting certificate errors requires `AllowInsecureCertificates = true`),
  `HostName`/`Port`/`UserName`/`Password`/`VirtualHost` as an alternative to `ConnectionString` (`Password` is
  redacted from `ToString()`), `ShutdownTimeout` (default 30 s).
- New public record `RabbitMqTlsConfig`; new static class `RabbitMqMessageQueue` (factory).
- `AddRabbitMqHealthCheck<T>()` reuses the registered RabbitMQ queue's connection. It also reports Unhealthy when the subscribed consumer's
  channel is closed or the broker cancelled the consumer.
- A startup warning when `MaxConcurrentCalls` is above `PrefetchCount`.

#### Fixed
- B1: publisher channels are picked with the shared overflow-safe `RoundRobin` helper.
- B2: the consumer uses `AsyncEventingBasicConsumer`; handlers are awaited (no `async void`) and settle on a
  channel that is safe for concurrent use.
- B3: retries follow `RetryPolicy` with real, broker-side backoff; the original is acked only after the retry copy
  is confirmed, so a retry is neither lost nor duplicated (barring a crash between the two).
- B4: `BasicQos(PrefetchCount)` is applied to every queue type.
- B15: no custom sequence-number tracking left to collide after recovery.
- Unsubscribe requeues deliveries whose handlers were cancelled, waits up to `ShutdownTimeout`, and closes the
  consumer channel, so the messages are redelivered and a later `SubscribeAsync` works.
- Batch `PublishAsync(IEnumerable<T>)` publishes all messages, then awaits their confirms together (§4.2), instead
  of one round trip per message. Publishes beyond `MaxOutstandingConfirms` wait instead of failing.
- S5 (RabbitMQ): `RabbitMqHealthCheck` puts no exception text, host or port in its result data or description,
  and no longer blocks a thread while it connects.
- A malformed delivery (such as an out-of-range timestamp) is settled: it is handled, or dead-lettered as
  `DeliveryFailed`. Before, it could stay unacknowledged and block the consumer.
- Retry or dead-letter queues deleted at runtime are declared again (only on an explicit nack or return; a confirm
  timeout is not republished). Persistent transport failures requeue after a per-message backoff (100 ms, doubling,
  at most 30 s, cut short by unsubscribe) with a throttled error log, instead of a hot loop.
- Unsubscribe or dispose called from inside a handler no longer deadlocks (tasks a handler spawns are not treated
  as the handler once it returned). Deliveries the client had prefetched but
  not handled at shutdown go back without using up an attempt.
- Cancelling a batch publish waits for the publishes already started, so none fails unobserved.
- The consumer no longer copies the message body: RabbitMQ.Client 7 keeps it valid until the awaited handler
  completes, and the size limit is checked first.

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
