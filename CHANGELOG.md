# Changelog

All notable changes to ZedLabs.MessageQueue are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Dependencies

- Microsoft.Extensions.* and System.Threading.RateLimiting 10.0.13 → 10.0.15 (the packages' minimum dependency
  versions), Microsoft.SourceLink.GitHub 10.0.404. The library stays on the 10.0.x line while it targets .NET 10.
- Samples, benchmarks and tests: OpenTelemetry 1.21.0, Testcontainers 4.17.0 (the mutual TLS fixture moves off the
  `WithResourceMapping(byte[], string, ...)` overload it obsoleted), xunit.v3 4.0.3, Microsoft.NET.Test.Sdk 18.12.0,
  coverlet.collector 10.1.1, ReportGenerator 5.6.1.

### Tests

- The Azure Service Bus integration specs can run against the Service Bus emulator, which
  `Support/ServiceBusEmulatorFixture` starts with Testcontainers once the owner sets `MQ_ASB_ACCEPT_EULA=Y` (accepting
  the licences of the emulator and its SQL Server). Images are pinned in `docker/test-servicebus/Dockerfile`, where
  Dependabot bumps them. All 11 specs passed against emulator 2.0.1 on 2026-11-21. The 1.0.0 release blocker stays open
  until they pass against a real namespace, because the emulator cannot exercise `TransactionalRetries`.

## [1.0.0-rc.1] - 2026-11-20

The first release of ZedLabs.MessageQueue, a release candidate for 1.0.0: provider-agnostic event messaging for
.NET 10, published by ZedLabs under the Apache-2.0 license. Applications code against `IMessageQueue<T>`
(`IMessagePublisher<T>` + `IMessageSubscriber<T>`); RabbitMQ, Azure Service Bus and an in-memory queue are
interchangeable providers chosen in configuration. Every provider runs deliveries through one pipeline: size limit,
deserialization, the handler, then retry with real backoff and dead-lettering. The public API is locked for 1.0.0;
one release blocker remains open (below).

### Packages

| Package | Contents |
|---|---|
| `ZedLabs.MessageQueue` | Abstractions, the delivery pipeline, hosting, configuration and validation, telemetry, health checks, dead-letter operations, the in-memory provider and the public provider SPI. No broker SDK. |
| `ZedLabs.MessageQueue.RabbitMQ` | RabbitMQ provider on RabbitMQ.Client 7.2.2. |
| `ZedLabs.MessageQueue.AzureServiceBus` | Azure Service Bus provider on Azure.Messaging.ServiceBus 7.21.0 and Azure.Identity 1.21.0. |

- All three target `net10.0` only (ADR 0024).
- The provider packages depend on exactly `[1.0.0-rc.1]` of `ZedLabs.MessageQueue`, because they share its
  internals, so upgrade all `ZedLabs.MessageQueue*` packages together.
- Every package ships with SourceLink, a deterministic build, a `.snupkg` symbol package, a README, `LICENSE` and
  `NOTICE`.

### Release blockers for 1.0.0

These apply to 1.0.0 final only. This release candidate ships with them open.

- **Open: the Azure Service Bus integration specs have never run against a real broker.** Running them needs a
  namespace, or the emulator, whose EULA is the owner's to accept. Without `MQ_ASB_CONNECTION_STRING` they skip, so
  the provider is verified only by its unit specs over the SDK seams. To close this, run `Category=Integration` with a
  namespace or the emulator, then remove this item.

### Features

- **One API, three providers.**
  - Register brokers before `AddMessageQueue<T>(configuration)` with `services.AddRabbitMqProvider()` or
    `services.AddAzureServiceBusProvider()`. `InMemory` is built in, and the `Provider` setting selects one.
  - A container can hold one queue per message type, each with its own section and provider.
  - `IMessagePublisher<T>`, `IMessageSubscriber<T>` and `IDeadLetterQueue<T>` are registered next to
    `IMessageQueue<T>`.
  - `Capabilities` reports what a queue supports: durable, dead letter, delayed redelivery and dead-letter
    operations.
- **Publishing.**
  - Publish one message or a batch, optionally with `PublishOptions`:
    - a message id, the portable idempotency key (default: a version 7 GUID)
    - a correlation id
    - up to 64 string headers
  - Arguments are validated the same way on every provider.
  - A message whose serialized body exceeds `MaxMessageSizeBytes` (default 1 MB) is refused before anything is sent
    (ADR 0026).
- **Consuming.**
  - Implement `IMessageProcessor<T>` and register `AddMessageProcessor<T, TProcessor>()` with
    `MessageQueueHostedService<T>`. The service subscribes at host start, creates a DI scope per message and drains
    on stop.
  - Or call `SubscribeAsync` with a handler that returns `MessageResult.Acknowledge`, `Retry` or `DeadLetter`.
  - Handlers receive a `MessageContext`: message id, correlation id, 1-based `DeliveryAttempt`, read-only
    `Headers`, timestamp.
  - Concurrency is bounded by `MaxConcurrentCalls` (default 10).
  - Shutdown is bounded by `ShutdownTimeout` (default 25 s, below the host's default 30 s). Startup warning 5005
    flags a value that is not below the host's (ADR 0025).
- **Retry and dead-lettering** (ADRs 0001, 0004, 0006, 0009).
  - `RetryPolicy` defaults to 5 retries, exponential from 2 s, multiplier 2, at most 30 s, with 10% jitter.
  - Backoff happens on the broker:
    - RabbitMQ: per-delay retry queues.
    - Service Bus queues: scheduled copies, transactional by default.
    - Service Bus topic subscriptions: the retry waits under the message lock.
  - Dead letters carry a reason code and the exception type, never exception text.
  - `RequireDelayedRedelivery` fails startup when a provider cannot honor the backoff (ADR 0020).
- **Dead-letter operations** (ADR 0023).
  - `IDeadLetterQueue<T>` has three operations:
    - `PeekAsync`: read-only snapshots.
    - `ReplayAsync`: back to the queue as a first delivery with the same message id and trace; at-least-once;
      `DryRun`.
    - `PurgeAsync`: an explicit `MaxCount`, no purge-all.
  - Replay and purge select by reason code and filter.
  - All providers support them (Service Bus topic subscriptions: peek and purge only).
  - Telemetry counts replays and purges; operator guide: `docs/dead-letter-operations.md`.
- **Serialization.**
  - JSON (System.Text.Json: camelCase, depth 64, strict UTF-8).
  - MessagePack with `MessagePackSecurity.UntrustedData` (ADR 0018).
  - Optional source-generated JSON metadata through `AddMessageQueueJsonMetadata<T>(context)`, with no reflection
    fallback (ADR 0019).
- **Configuration** (ADRs 0002, 0015).
  - Binding is strict: unknown keys and numeric enum values fail at startup.
  - Every setting is validated with `ValidateOnStart`.
  - Provider settings live on `RabbitMqConfig`, `AzureServiceBusConfig` and `InMemoryConfig`.
  - Every setting, default and startup error is listed in `docs/configuration.md`.
- **RabbitMQ.**
  - Connects lazily (`InitializeAsync`, `RabbitMqMessageQueue.CreateAsync`), with reconnect backoff (ADR 0007).
  - Publisher confirms and a publisher channel pool.
  - Quorum or classic queues, with `PrefetchCount` applied to both.
  - TLS is required for any non-loopback host (`RequireTls`), with server-name and policy-error rules.
  - Mutual TLS (PFX, PEM or the certificate store) and a private CA trust anchor (`Tls:CaCertificatePath`)
    (ADRs 0008, 0022).
- **Azure Service Bus.**
  - Queues and topic subscriptions.
  - Authentication by Entra ID (`FullyQualifiedNamespace` with a passed, registered or `DefaultAzureCredential`) or
    by connection string.
  - Batches are split by size.
  - Every message is settled exactly once; shutdown is bounded (ADRs 0009-0012).
- **Telemetry** (`AddMessageQueueTelemetry`, ADR 0013).
  - Spans and metrics follow the OpenTelemetry messaging semantic conventions.
  - W3C trace context runs from publisher to consumer on every provider.
  - The `zedlabs.messagequeue.*` counters cover retries, dead letters, transport errors and dead-letter
    operations.
  - The default ActivitySource and Meter are named `ZedLabs.MessageQueue`.
  - Logging uses source-generated `LoggerMessage` with documented event ids.
- **Health checks.**
  - `AddMessageQueueHealthCheck`.
  - `AddRabbitMqHealthCheck`: the queue's own connection or a standalone connection string with TLS settings.
  - `AddAzureServiceBusHealthCheck`: peek, Listen rights.
  - The opt-in `AddAzureServiceBusMetricsHealthCheck`.
  - Results never carry exception text; failures are logged as event 6001.
- **Provider SPI** (ADR 0015).
  - Build a third-party provider on public types alone: `IMessageQueueProvider`, `MessageQueueRegistration<T>`,
    `MessageDispatcher<T>`, `IDeliveryActions`, `PublishValidation`, `MessageSerializer`, and the dead-letter store
    interfaces.
  - `samples/ZedLabs.MessageQueue.Samples.LoopbackProvider` is an example.
- **Hardening.**
  - No passwords, keys or SAS tokens in exceptions, logs, health results or `ToString()`.
  - Consumed ids, headers, reason details and tags are bounded (ADR 0021).
  - A throwing telemetry or logging provider never fails a publish or a delivery.

### Documentation

- Getting-started guides per provider (`docs/getting-started/`).
- The configuration reference (`docs/configuration.md`).
- Operations (`docs/operations/`): failure modes, monitoring, alerts and runbooks.
- Dead-letter operations, the API references, benchmarks (`docs/benchmarks.md`) and the design records
  (`docs/adr/`).

### Changes since the pre-release development history

Nothing was published before this release. If you built the library from source before 1.0.0-rc.1, these are the
changes to act on. The full development log, kept further down in `CHANGELOG.md`, has every item with its design
record.

- **Rebrand.**
  - `ESE.MessageQueue*` became `ZedLabs.MessageQueue*`: package ids, assemblies, namespaces and the solution.
  - The default ActivitySource and Meter are `ZedLabs.MessageQueue`.
  - Metric and tag names use the `zedlabs.messagequeue.*` prefix (Prometheus: `zedlabs_messagequeue_*`).
  - The in-memory `messaging.system` is `zedlabs.inmemory`.
  - Library headers are named `x-zedlabs-*`. Drain retry and dead-letter queues written with the old `x-ese-*`
    headers first.
  - The license is Apache-2.0 instead of MIT.
- **Package split.**
  - Broker providers are separate packages and are registered before `AddMessageQueue<T>`.
  - The core no longer brings RabbitMQ.Client or Azure SDKs.
  - `MessageQueueConfig.Provider` is a string, and the `MessageQueueProvider` enum is gone.
- **Reshaped API.**
  - `IMessageQueue<T>` composes publisher and subscriber and adds `Capabilities`.
  - The rich handler and `IMessageProcessor<T>.ProcessAsync` take a `CancellationToken`.
  - `MessageContext` is a sealed record with `DeliveryAttempt` and read-only `Headers`.
  - `ITelemetryProvider` is built around `TelemetryTarget`.
  - `RetryPolicy.MaxAttempts` became `MaxRetries`, which counts retries after the first attempt.
  - `RetryAttempts`/`RetryDelay` became `Retry`.
  - `ConnectionString` and `DeadLetterQueueName` moved to the provider configs.
  - `DeadLetterReason` is a struct with a constructor.
  - `InMemoryDeadLetter<T>` is a sealed class (`with` no longer compiles).
  - The in-memory queue moved to `ZedLabs.MessageQueue.InMemory` and delivers on its own; use `WaitForIdleAsync`.
  - Renamed: `MessageQueueServiceCollectionExtensions`, `MessageQueueHealthCheckExtensions`, and the per-provider
    health check extension classes.
  - Several types became internal, removed or sealed; the full log lists them.
- **Removed settings** (they now fail at startup as unknown keys):
  - Service Bus: `SenderPoolSize`, `MaxConcurrentSends`, `EnablePartitioning`, `RequiresDuplicateDetection`,
    `DuplicateDetectionHistoryTimeWindow`, `BatchFlushInterval`.
  - RabbitMQ: `EnableClusterMode`.
- **Changed behavior.**
  - The retry defaults above.
  - `ShutdownTimeout` is 25 s.
  - Oversized publishes, bad correlation ids and unsupported RabbitMQ `ExchangeType` values are refused.
  - RabbitMQ requires TLS for remote hosts.
  - Retries wait on the broker rather than in process.
  - Dead letters carry reason headers.
  - Health results attach no exception.
  - Metric names follow the semantic conventions.
  - `TraceContext` parsing follows strict W3C rules.
  - RabbitMQ.Client is 7.x (async-only).
  - The target is .NET 10.

## [Pre-release development history]

Not released. This is the development log from before 1.0.0-rc.1, kept for anyone who built from source, newest
first. Some entries were later superseded (the MIT license, the 30 s `ShutdownTimeout`, earlier coverage gates and
paths); where they disagree with 1.0.0-rc.1 above, the 1.0.0-rc.1 section is current.

### RabbitMQ health check right after subscribing

#### Fixed
- RabbitMQ: a health probe right after `SubscribeAsync` could report Unhealthy. RabbitMQ.Client 7 completes
  `BasicConsumeAsync` once the broker's `basic.consume-ok` arrived, but sets the consumer's `IsRunning` only when its
  dispatcher hands consume-ok to the consumer, later; the probe read `IsRunning`. The subscription's consumer now
  counts as running from the consumer tag `BasicConsumeAsync` returns until the broker cancels it (automatic recovery
  registers it again), its own cancel is confirmed or its channel shuts down. With `MaxConcurrentCalls` above 1 the
  client handles consume-ok and cancels on several dispatcher workers, in any order: a late consume-ok no longer
  revives a consumer the broker cancelled, which still reports Unhealthy.
  `Given_a_rabbitmq_queue.When_The_Consumer_Is_Cancelled_By_The_Broker` failed intermittently in CI.

### ZedLabs rebrand and Apache-2.0 relicensing (ADR 0016 amended)

The repository moved to https://github.com/ZedLabs-dev/MessageQueue. Nothing had been published, so the old names were
never on NuGet; this section is the map for anyone who built from source.

#### Breaking: ZedLabs rebrand
- Package IDs and assemblies: `ESE.MessageQueue` → `ZedLabs.MessageQueue`, `ESE.MessageQueue.RabbitMQ` →
  `ZedLabs.MessageQueue.RabbitMQ`, `ESE.MessageQueue.AzureServiceBus` → `ZedLabs.MessageQueue.AzureServiceBus`.
- Namespaces `ESE.MessageQueue.*` → `ZedLabs.MessageQueue.*` (so logger categories, which are the type names, follow).
- Project folders, project files and the solution (`ESE.MessageQueue.sln` → `ZedLabs.MessageQueue.sln`), including the
  test, sample and benchmark projects; `InternalsVisibleTo` grants name the new assemblies.
- Default ActivitySource and Meter name (`AddMessageQueueTelemetry(sourceName)`): `ESE.MessageQueue` →
  `ZedLabs.MessageQueue`. Update `AddSource`/`AddMeter` in the host's OpenTelemetry setup.
- Metric and tag names `ese.messagequeue.*` → `zedlabs.messagequeue.*`: the counters `messages.retried`,
  `messages.dead_lettered`, `transport.errors`, `dead_letters.replayed`, `dead_letters.purged`, and the tags
  `message_type`, `delivery_attempt`, `dead_letter.reason`, `retry.next_attempt`, `retry.delay_ms`. Prometheus names
  follow (`ese_messagequeue_*` → `zedlabs_messagequeue_*`); update dashboards and alert rules.
- `messaging.system` of the in-memory provider: `ese.inmemory` → `zedlabs.inmemory` (the loopback sample's
  `ese.loopback` → `zedlabs.loopback`).
- Library-owned message headers and application properties `x-ese-*` → `x-zedlabs-*`: `x-zedlabs-delivery-attempt`,
  `x-zedlabs-delivery-count`, `x-zedlabs-message-id`, `x-zedlabs-dead-letter-reason`, `x-zedlabs-dead-letter-detail`,
  `x-zedlabs-dead-letter-attempt`, `x-zedlabs-dead-lettered-at`. Messages written with the old headers (retry copies,
  dead letters) are read without them: drain queues and dead-letter queues built from source before switching.
- License: Apache-2.0 replaces MIT; authors, company and copyright holder are ZedLabs (`Copyright 2026 ZedLabs`).

#### Changed
- `LICENSE` holds the full Apache License 2.0 text; a new `NOTICE` file; both are packed into every package. Package
  metadata: `PackageLicenseExpression` Apache-2.0, Authors and Company ZedLabs, `RepositoryUrl` and
  `PackageProjectUrl` https://github.com/ZedLabs-dev/MessageQueue.
- **One-time exception to the API lock rule:** `PublicAPI.Shipped.txt` of each package was rewritten with the renamed
  entries (and `PublicAPI.Unshipped.txt` to match) outside `/release`, because every Shipped entry named the old
  namespaces and nothing had shipped (ADR 0016 amendment).

#### Added
- `Packaging/Given_the_packed_packages`: packs each package and checks its nuspec (license expression Apache-2.0,
  authors ZedLabs, copyright, ZedLabs project and repository URL) and that `LICENSE` and `NOTICE` are packed.

### Mutual TLS against a real broker (ADR 0022 amended)

#### Added
- `RabbitMqTlsConfig.CaCertificatePath` (`Tls:CaCertificatePath`): a trust anchor for a broker with a private CA. A PEM
  file (one or more CA certificates) or a DER certificate; the broker's certificate must chain to those CAs, and only
  to them, instead of the OS trust store, and be issued for server authentication. The server name is still checked,
  revocation is not, and `AcceptablePolicyErrors` (with `AllowInsecureCertificates`) still decides what may be
  overlooked. The file must contain the root (an intermediate alone is not an anchor); certificates the broker sends
  are never anchors; issuers are never downloaded from a certificate's AIA location; basic constraints, path lengths
  and name constraints are enforced; a leaf whose only usage is `anyExtendedKeyUsage` is refused, one with no extended
  key usage is accepted. Needs TLS; loaded and checked at startup (`Tls:CaCertificatePath '...' does not exist or cannot be
  read`, `... holds no certificate`, `Tls:CaCertificatePath needs TLS: ...`); loaded once per queue and disposed with it.
- `AddRabbitMqHealthCheck(connectionString, RabbitMqTlsConfig tls, name, failureStatus, tags, timeout, requireTls)`:
  the standalone connection-string check with the queue's TLS settings and rules (client certificate, CA, server name,
  acceptable errors). The settings are copied at registration; the certificates are loaded once and disposed with the
  container. Invalid settings report Unhealthy and log event 6001.
- Integration specs against RabbitMQ with `ssl_options.verify = verify_peer` and `fail_if_no_peer_cert = true`
  (`Support/RabbitMqMutualTlsBrokerFixture`, its own collection; CA, server and client certificates generated per run,
  never committed): round trip with PFX and PEM client certificates, a client without a certificate refused with a
  `MessageQueueException` that carries no password, an untrusted CA, another CA and a wrong server name refused unless
  `AllowInsecureCertificates` and `AcceptablePolicyErrors` accept them, and the standalone health check. Mutants:
  `scripts/mutants/mutual-tls.json` and the adversarial review's `review-mutual-tls.json`.

#### Changed
- `AddRabbitMqHealthCheck(connectionString, ...)` (without `RabbitMqTlsConfig`) registers one check instance per
  registration, resolved as a keyed singleton named after the check, like the standalone Service Bus checks; its
  probes are unchanged.

### Code gaps found while documenting (PRODUCTION-READINESS-PLAN §4, §5, §10; ADRs 0024-0027)

#### Breaking
- `PublishAsync` refuses a message whose serialized body is larger than `MaxMessageSizeBytes`, on every provider:
  it throws `MessageQueueException` naming the size, the limit and the destination (never the content) and sends
  nothing. A batch is checked whole first and names the message's index; none of it is sent. Before, the message was
  sent and every consumer dead-lettered it as `MessageTooLarge` (ADR 0026). Retry and dead-letter copies and replays
  are not checked; the consumer keeps its own check for other publishers.
- `ShutdownTimeout` defaults to 25 s (was 30 s) on `InMemoryConfig`, `RabbitMqConfig` and `AzureServiceBusConfig`, so
  one queue's drain ends before the host's default 30 s `HostOptions.ShutdownTimeout` (ADR 0025). Set it explicitly
  to keep 30 s, and raise the host's timeout with it.
- RabbitMQ `ExchangeType` must be `direct`, `fanout`, `topic`, `headers` (exact, lowercase: RabbitMQ is
  case-sensitive) or a plugin type starting with `x-`; any other value fails at startup
  (`ExchangeType '...' is not supported: ...`) instead of on first connect.

#### Added
- `PublishValidation.ValidateBodySize(bodyLength, maxMessageSizeBytes, destination, batchIndex)`: the publish-side size
  check for providers, third-party ones included.
- RabbitMQ records `zedlabs.messagequeue.transport.errors` with `error.type` `ConnectionFailed`, `ConnectionShutdown`,
  `RecoveryFailed`, `ChannelShutdown` or `ConsumerCancelled`; closes the library makes itself are not counted
  (ADR 0027). Log event 2008 (Warning) when telemetry throws while counting one.
- Debug log events 1010 (`Published message {MessageId} to {Destination}`, the id without control characters) and
  1011 (`Published a batch of {MessageCount} messages to {Destination}`) after each successful publish, on every
  provider. Publishing still logs nothing at Information (ADR 0027).
- Startup warning 5005 from `MessageQueueHostedService<T>` when a queue's `ShutdownTimeout` is not below the host's
  `HostOptions.ShutdownTimeout` (not when the host waits forever), naming the queue's configuration key; Debug event
  5006 when the host options cannot be read (ADR 0025).
- Load specs `RabbitMQ/Given_a_rabbitmq_queue_under_load` (5,000 messages, 32 handlers, prefetch 64, 5% transient
  failures) and `InMemory/Given_an_in_memory_queue_under_load`; RabbitMQ consume benchmark
  (`RabbitMqConsumeBenchmarks`, `MaxConcurrentCalls` 1 and 16) in `docs/benchmarks.md`.
- ADR 0024: .NET 10 only (`net10.0`, the LTS window to 2028-11-14, no multi-targeting).

#### Fixed
- In-memory: `WaitForIdleAsync` could return while a delivery's pipeline was still recording its telemetry: a
  completed or dead-lettered message was settled inside the pipeline, before its metrics, and a retried message's
  redelivery could finish before the first delivery had recorded the retry. Deliveries now settle once their pipeline
  has returned, and a reschedule keeps the queue busy until then. `Given_an_in_memory_queue.When_Recording_Telemetry`
  failed whenever it ran alone.
- In-memory and RabbitMQ wrap their logger like Service Bus does: a throwing logging provider can no longer fail a
  publish that succeeded (or mark it failed in metrics), a delivery, or the RabbitMQ client's event handlers.

### Documentation (PRODUCTION-READINESS-PLAN §2, §7, §8, §9, §10)

#### Added
- Getting-started guides per provider (`docs/getting-started/`: in-memory, RabbitMQ, Azure Service Bus with Entra ID):
  install, registration, `appsettings.json` and environment variables, publish, `IMessageProcessor<T>` with the hosted
  service, health checks, telemetry, dead-letter peek and replay. RabbitMQ least-privilege vhost permissions verified
  against RabbitMQ 4.3.6 (write on `amq.default` is required).
- `docs/configuration.md`: every setting of every config class with type, default, range and notes; production vs
  development behavior; when each kind of error surfaces; 58 startup errors quoted as the library produces them.
- `docs/operations/`: `failure-modes.md` (guarantees, duplicates and idempotency, poison and oversized messages,
  outages, lock loss, shutdown), `monitoring.md` (every metric, span attribute and log event with its level, health
  checks), `alerts.md` (Prometheus rules and an Azure Monitor rule, with the OTel-to-Prometheus naming assumption),
  `runbooks.md` (connection failures, retries, dead-letter backlog, certificates, credential rotation, stuck
  consumers, configuration rollouts and `PRECONDITION_FAILED`, retry-queue cleanup, per-provider troubleshooting).
- `docs/release-checklist.md` (the regression suite and release record) and `docs/production-readiness-status.md`
  (every readiness item with status and evidence); `docs/README.md` index.
- Specs that keep the docs honest (`Packaging/`): `Given_the_documentation_snippets` (guide code is compiled from
  `samples/ZedLabs.MessageQueue.Samples/Docs/` and must match verbatim; the in-memory guide is executed; every JSON and
  environment-variable example binds strictly and validates), `Given_the_configuration_reference` (every setting and
  default; every quoted startup error is reproduced), `Given_the_monitoring_reference` (every log event id and level,
  metric and unit, attribute name).

#### Changed
- `docs/benchmarks.md`: release-grade numbers from BenchmarkDotNet's default job (the ShortRun table is kept for
  comparison). No regression; the in-memory path measures 1.3-2.0 µs per message instead of 3.0 µs.
- `docs/azure-service-bus-api.md`: the package is `ZedLabs.MessageQueue.AzureServiceBus`, and health results carry no
  exception (it is logged as event 6001); `docs/message-queue-api.md` lists the transport-error and dead-letter
  operation metrics.

### Dead-letter operations (PRODUCTION-READINESS-PLAN "drain and replay dead-letter queues", ADR 0023)

#### Added
- **`IDeadLetterQueue<T>`**: `PeekAsync(maxCount)` (read-only `DeadLetterMessage<T>` snapshots: ids, reason code and
  detail, enqueue and dead-letter time, attempt, headers, the message or the body length), `ReplayAsync(options)` (back to
  the queue as a first delivery with the full retry budget, same message id, correlation id, headers and trace;
  at-least-once: confirmed send, then removal; `DryRun`; `DeadLetterReplayResult` with bounded failures) and
  `PurgeAsync(options)` (explicit `MaxCount`, no parameterless purge-all). Options select by `ReasonCodes` and a
  `Filter`; replay skips `MessageTooLarge` and `DeserializationFailed` unless they are named. Implemented by the
  in-memory, RabbitMQ and Azure Service Bus queues and registered in DI next to `IMessageQueue<T>`.
- `MessageQueueCapabilities.DeadLetterOperations` (`DeadLetterOperations` flags, an `init` property): all on in-memory,
  RabbitMQ (none with `EnableDeadLetterQueue = false`) and Service Bus queues; peek and purge on Service Bus topic
  subscriptions, where a resend would reach every subscription.
- Provider SPI: `IDeadLetterStore`, `IDeadLetterScan`, `IDeadLetterEntry`, `DeadLetterMetadata` and `DeadLetterQueue<T>`
  (the shared selection, limits, counting, telemetry and logging); the loopback sample provider opts in with them.
- `DeadLetterLimits` (peek 1000, 10,000 examined per call, 100 listed failures).
- Telemetry: `ITelemetryProvider.RecordDeadLettersReplayed`/`RecordDeadLettersPurged` (default no-op) and the metrics
  `zedlabs.messagequeue.dead_letters.replayed` / `.purged`. Log events 1201-1205, 2012, 3018, 3019.
- The sample worker's `Sample:DeadLetters` (`None`, `Peek`, `Replay`); operator how-to `docs/dead-letter-operations.md`.

#### Changed (tooling)
- Coverage gate raised to 89% combined and the per-package floor to 87% (`scripts/coverage.sh`; measured 90.1%, lowest
  package 88.1%).

#### Changed
- `MessageQueueCapabilities` equality and `ToString()` include `DeadLetterOperations`: a first-party queue's
  `Capabilities` no longer equals `new(Durable, DeadLetter, DelayedRedelivery)` built without it.
- Dead letters record when they were dead-lettered (`x-zedlabs-dead-lettered-at`, RabbitMQ and Service Bus) and, on Service
  Bus, the attempt (`x-zedlabs-dead-letter-attempt`).
- **RabbitMQ:** a new quorum dead-letter queue is declared with `x-delivery-limit = -1`, so a dead letter returned by
  lost connections is never dropped by the default delivery limit (20). An existing one cannot be redeclared: the
  queue starts with warning 2013; apply a `delivery-limit: -1` policy (`docs/dead-letter-operations.md`).
- **Service Bus:** only a genuine refusal (`InvalidOperationException`, `NotSupportedException`, `TransactionException`)
  switches retries and replays to send-then-complete; `ObjectDisposedException`, `TransactionAbortedException` and
  `TransactionInDoubtException` now fail that message (a retry is handed back) and keep transactions on.
- A dead-letter call whose pass fails counts and logs what it did (event 1206) and throws `MessageQueueException`
  naming only the failure's type; a throwing `Filter` keeps its exception. Messages without an id are replayed once per
  call (keyed by body, correlation id and headers). RabbitMQ passes keep only delivery tags of held dead letters and
  end when their channel is lost; a Service Bus pass ends once it holds 1000 skipped dead letters.

### Deferred items closed (OPTIMIZATION-PLAN §9)

Design records: `docs/adr/0019` (source-generated JSON metadata), `0020` (`RequireDelayedRedelivery`), `0021`
(bounded values), `0022` (client certificates); `0001`, `0008` and `0018` amended.

#### Breaking
- `PublishOptions.CorrelationId` longer than 128 characters (`MaxMessageIdLength`) or containing control characters
  is refused with `ArgumentException` on every provider (it was sent as is, and failed only on Service Bus).
- Enum settings of every config type, nested ones included (`Tls:AcceptablePolicyErrors`, the new store settings),
  must be names (or, for flags, comma-separated names); numbers such as `"2"` now fail at startup, as they already did
  for `SerializationFormat` and `Retry:Strategy`.
- `InMemoryDeadLetter<T>` is a sealed class instead of a positional record, and `DeadLetterReason` a plain readonly
  struct instead of a record struct: equality, `==`/`!=`, `ToString()` and (for `InMemoryDeadLetter<T>`)
  `Deconstruct` and `init` properties are unchanged, but `with` expressions on `InMemoryDeadLetter<T>` no longer
  compile. Reason: the compiler's synthesized record members were nullable-oblivious (`~`) in the public API.
  The old entries are retired in `PublicAPI.Unshipped.txt` (`*REMOVED*~` lines, or annotated replacements where the
  analyzer treats both as the same member); `/release` drops them from Shipped.

#### Added
- **Source-generated JSON** (ADR 0019): `services.AddMessageQueueJsonMetadata<T>(MyJsonContext.Default)` and
  `MessageQueueConfig.JsonTypeInfoResolver` (code only). Serialization then uses only that metadata (no reflection
  fallback), with the same camelCase names, depth limit and UTF-8 rules; startup fails if the resolver lacks `T` or
  the format is not `Json`. Provider SPI: `MessageSerializer.Serialize/Deserialize` overloads taking the resolver
  (the two-argument ones stay the reflection path). Refused with `ConfigurationException` for a queue registered as
  an instance, in either registration order.
- **Mutual TLS for RabbitMQ** (ADR 0022): `Tls:ClientCertificatePath` (PFX or PEM), `Tls:ClientCertificateKeyPath`,
  `Tls:ClientCertificatePassword` (redacted from `ToString()`), or `Tls:ClientCertificateThumbprint` with
  `Tls:ClientCertificateStoreName`/`StoreLocation`. Checked at startup; loaded once per queue, reused across reconnects
  and disposed with the queue; presented on both endpoint forms.
- **`MessageQueueConfig.RequireDelayedRedelivery`** (ADR 0020, default false): fail at startup (or, for third-party
  providers, when the queue is resolved) instead of warning when the retry backoff cannot be honored.

#### Changed
- **Bounded values** (S6, ADR 0021): dead-letter reason codes and details (`DeadLetterReason` constructor), exception
  type names, `error.type` span and metric tags, span status descriptions and the dead-letter reason metric tag are
  cut to 4096 characters (never inside a surrogate pair). A consumed `CorrelationId` loses control characters and is
  cut to 128 characters, like the message id. Consumed ids lose C1 control characters (U+0080-U+009F) too, and a cut
  never ends in half a surrogate pair.
- Coverage gate raised to 88% combined and the per-package floor to 86% (`scripts/coverage.sh`).
- Container images are pinned by digest (`tag@sha256:...`) in `docker/test-broker/Dockerfile` and the dev compose file.
- CI: `ci.yml` job timeout 30 minutes; `release.yml` checks that the tag is on `main` from the checkout's own
  `origin/main` (fetch-depth 0), with no second unauthenticated fetch, so private repositories work.

### Phase 7: package split, API lock, packaging, agentic tooling (OPTIMIZATION-PLAN Phase 7)

Design records: `docs/adr/0015` (provider SPI and package split), `0016` (API lock and packaging), `0017` (tooling).

#### Breaking
- Three packages (ADR 0005): `ZedLabs.MessageQueue` (core, in-memory provider; no broker SDK), `ZedLabs.MessageQueue.RabbitMQ`
  and `ZedLabs.MessageQueue.AzureServiceBus`. Broker providers must be registered before `AddMessageQueue<T>`:
  `services.AddRabbitMqProvider()` / `services.AddAzureServiceBusProvider()`. An unregistered `Provider` now fails with
  "`<section>:Provider '<name>' is not registered. Registered providers: InMemory, ...`" (was "is not supported.
  Supported providers: ..."). A provider registered as a type or factory instead of `AddMessageQueueProvider(instance)`
  fails registration with a message saying so.
- The core package no longer depends on RabbitMQ.Client, Azure.Messaging.ServiceBus or Azure.Identity. An app that
  used those SDKs' types directly while getting them through `ZedLabs.MessageQueue` must reference them itself (or the
  provider package that brings them).
- **Lockstep versions:** `ZedLabs.MessageQueue.RabbitMQ` and `ZedLabs.MessageQueue.AzureServiceBus` depend on the exact
  `ZedLabs.MessageQueue` version they were built with (`[1.0.0-rc.1]`), because they share its internals (ADR 0015).
  Upgrade all `ZedLabs.MessageQueue*` packages together.
- `ZedLabs.MessageQueue.Testing.TestingExtensions` (removed in Phase 4, listed here for completeness): use
  `AddInMemoryMessageQueue<T>()` from `MessageQueueServiceCollectionExtensions`.
- **Health checks attach no exception any more** (S5): `HealthCheckResult.Exception` is null and `Data` is empty on
  failure (the generic check no longer puts the queue name in `Data`); the exception is logged with event id 6001
  ("Health check {HealthCheck} failed with {ExceptionType}") through the container's `ILoggerFactory`.
  `MessageQueueHealthCheck` takes an optional `ILogger`. The standalone Service Bus checks validate the connection
  string or namespace at registration (`ArgumentException`, fixed message).
- `DeadLetterReason` is a struct with a constructor (`new DeadLetterReason(code, detail)`, code required);
  `default(DeadLetterReason).Code` reads `DeliveryFailed`. Positional deconstruction is gone.
- `RetryStrategy` moved to its own file (same namespace); `RabbitMqTlsConfig` likewise.
- The `MessageQueueProvider` enum is removed; `MessageQueueConfig.Provider` is a `string` (`InMemoryConfig.ProviderName`,
  `RabbitMqConfig.ProviderName`, `AzureServiceBusConfig.ProviderName`).
- Renamed: `ServiceCollectionExtensions` → `MessageQueueServiceCollectionExtensions`, `HealthCheckExtensions` →
  `MessageQueueHealthCheckExtensions` (extension-method call sites are unchanged). The RabbitMQ and Service Bus health
  check registrations moved to `RabbitMqHealthCheckExtensions` / `AzureServiceBusHealthCheckExtensions` in the provider
  packages (same namespace, same method names).
- No longer public: `RetryExecutor`, `OpenTelemetryActivity`, `RabbitMqHealthCheck`, `AzureServiceBusHealthCheck`,
  `AzureServiceBusMetricsHealthCheck` (register them with the `Add…HealthCheck` extensions), and the config validators
  (`RabbitMqConfigValidator`, `AzureServiceBusConfigValidator`, `InMemoryConfigValidator`).
- Removed (unused): `QueueHealthStatus`, `TransientMessageException`.
- Sealed: `RabbitMqMessageQueue<T>`, `AzureServiceBusMessageQueue<T>`, `MessageQueueHostedService<T>`,
  `MessageQueueHealthCheck`, `OpenTelemetryProvider`, `NullTelemetryProvider`, `ConfigurationException`, and the
  records `RabbitMqConfig`, `RabbitMqTlsConfig`, `AzureServiceBusConfig`, `InMemoryConfig`, `RetryPolicy`.
- `TelemetryActivity.Dispose()` is no longer virtual: override `protected virtual Dispose(bool disposing)` instead
  (standard dispose pattern; CA1816 is enforced again).
- Moved to namespace `ZedLabs.MessageQueue.Providers` and made public as the provider SPI: `MessageDispatcher<T>`,
  `IDeliveryActions`, `DeadLetterReason`, `DispatchOutcome`, `PublishTelemetry`, `PublishValidation`,
  `MessageSerializer` (was `ZedLabs.MessageQueue.Serialization`, internal).

#### Added
- Provider SPI for third-party providers (ADR 0015): `IMessageQueueProvider`, `MessageQueueRegistration<T>` (strict
  binding, shared validation and `ValidateOnStart` done by core), `QueueFactoryContext<TConfig>`,
  `MessageQueueConfigValidator<TConfig>`, `MessageContextExtensions.WithTraceHeaders`, public `TraceHeaders`, and
  `services.AddMessageQueueProvider(provider)`. `samples/ZedLabs.MessageQueue.Samples.LoopbackProvider` is a provider
  built on that SPI alone.
- Public API lock: PublicApiAnalyzers with `PublicAPI.Shipped.txt` baselines; XML documentation on every public type
  and member (options classes excepted, per the house rules).
- NuGet packaging: MIT license (new `LICENSE`; Apache-2.0 since the ZedLabs rebrand above), README per package,
  SourceLink, deterministic builds, `.snupkg` symbols, package validation; provider packages depend on the exact core version. `release.yml` publishes on `v*` tags (NuGet
  trusted publishing or `NUGET_API_KEY`) and creates the GitHub release from this file.
- Claude Code tooling: format-on-edit and build/unit-test Stop hooks, `test-writer`, `security-reviewer`,
  `package-updater` and `adversarial-reviewer` agents, `update-packages`, `add-tests`, `release` and `mutation-test`
  skills, and `scripts/mutate.py` (mutation testing in a scratch copy) with definitions in `scripts/mutants/`.
- GitHub: optional `claude.yml` (`@claude` and pull request review), weekly `update-packages.yml` opening a pull request;
  CI now packs and uploads the packages.

#### Changed
- Consumed messages (RabbitMQ, Service Bus, and anything through `MessageDispatcher<T>`): a broker-supplied message
  id loses control characters and is cut to 128 characters before it reaches logs or handlers; application headers
  get the publish-side caps on consume too (at most 64, names up to 128 and values up to 4096 characters; others are
  dropped).
- RabbitMQ: the client buffers at most `max(MaxMessageSizeBytes, 16 MiB)` per inbound body (was 64 MiB). The retry
  attempt header counts only when the broker's `x-death` history says the message expired out of this queue's retry
  queue (ADR 0006 note: a publisher can still forge both, for its own messages).
- Service Bus processor errors are counted: `ITelemetryProvider.RecordTransportError` (default interface method, so
  custom providers keep compiling) and the metric `zedlabs.messagequeue.transport.errors` with `error.type` (B6).
- `MessageDispatcher<T>` validates its arguments and snapshots the config it is given; JSON deserialization states
  `MaxDepth = 64` explicitly (ADR 0018).
- Packed assemblies grant `InternalsVisibleTo` only to the first-party provider packages; tests and benchmarks see
  internals in development builds only.
- Samples: settings are read from the output folder (`dotnet run --project samples/ZedLabs.MessageQueue.Samples` works
  from the repository root), and the broker is chosen with `-- --environment RabbitMq`.
- The RabbitMQ test and benchmark image tag lives in `docker/test-broker/Dockerfile`, watched by Dependabot.
- Repository layout: `MessageQueue/` → `src/ZedLabs.MessageQueue*/`, `MessageQueue.Tests/` → `tests/ZedLabs.MessageQueue.Tests/`,
  the compose file, `.env.example` and emulator config → `docker/`, the API docs → `docs/`. The local `.env` now lives in
  `docker/.env`.
- Coverage is gated on the three packages together (87%) plus a per-assembly floor (`scripts/coverage.sh`).
- §4.4: the library writes no `DateTimeOffset` application properties any more (the Service Bus rework removed
  `PublishedAt` / `NextRetryAt`; `MessageContext.Timestamp` is the broker's `EnqueuedTime`), so nothing is left to
  convert to native properties.

### Phase 6: telemetry, samples, benchmarks (OPTIMIZATION-PLAN B10, S8, §4, §4.5, §4.7)

Design records: `docs/adr/0013` (telemetry), `docs/adr/0014` (samples and benchmarks). Numbers: `docs/benchmarks.md`.

#### Breaking
- `ITelemetryProvider` is redesigned (B10). Every member takes a `TelemetryTarget(System, Destination, MessageType)`
  (with optional `Subscription`, `RoutingKey`, `PublishOperation`).
  - `StartPublishActivity(target, messageCount, messageId)`
  - `StartConsumeActivity(target, MessageContext, TraceContext? parent)`
  - `RecordMessagePublished(target, count, duration, errorType)`
  - `RecordMessageConsumed(target, duration, errorType)`
  - `RecordMessageRetried(target, nextAttempt)`
  - `RecordMessageDeadLettered(target, reason)`
  - `ExtractTraceContext(IReadOnlyDictionary<string, string>)`
  - `InjectTraceContext(IDictionary<string, string>)`

  Custom providers must be updated. `NullTelemetryProvider` and `OpenTelemetryProvider` implement the new shape.
- `OpenTelemetryProvider` metric names follow the OpenTelemetry messaging semantic conventions:
  - `messaging.client.sent.messages` (was `messagequeue.messages.published`)
  - `messaging.client.consumed.messages` (was `messagequeue.messages.consumed`; `error.type` replaces `success`)
  - `zedlabs.messagequeue.messages.retried` and `zedlabs.messagequeue.messages.dead_lettered` (with
    `zedlabs.messagequeue.dead_letter.reason`)
  
  Attributes are now `messaging.system`, `messaging.destination.name`, `messaging.operation.name`,
  `zedlabs.messagequeue.message_type`, `error.type` (and `messaging.destination.subscription.name` on Service Bus
  subscriptions), replacing `queue`, `message_type` and `success`. Span names are `{operation} {destination}`
  (`send` on Service Bus), and `messaging.system` is the broker (`rabbitmq`, `servicebus`, `zedlabs.inmemory`) instead of
  `messagequeue`. The RabbitMQ destination is `{exchange}:{queue}` (exchange and routing key), and Service Bus
  subscription consumers report the topic, so publish and consume telemetry join.
- `OpenTelemetryProvider` implements `IDisposable` (disposes its `ActivitySource` and `Meter`);
  `AddMessageQueueTelemetry` registers it with a factory so the container disposes it.
- `TraceContext` parsing is strict W3C Trace Context. Values that used to "parse" (`00-abc-def-01`, uppercase hex,
  all-zero ids) are now ignored.
- In-memory `PublishAsync(IEnumerable<T>)` validates and serializes every message before queueing any (as RabbitMQ
  and Service Bus do) and traces the batch as one span.
- The sample classes in `ZedLabs.MessageQueue.Examples` (`OrderCreated`, `OrderCreatedProcessor`, `OrderService`, ...) are
  removed from the library; see `samples/ZedLabs.MessageQueue.Samples` (§4.7).
- `MessageQueue/.env.example` and `docker-compose.dev.yml` no longer have default RabbitMQ credentials or a working
  SQL Server password (S8). Compose refuses to start until `RABBITMQ_USER` / `RABBITMQ_PASS` / `MSSQL_SA_PASSWORD`
  are set in `MessageQueue/.env`; the example ships placeholders to replace. Ports bind to `127.0.0.1` only.

#### Added
- End-to-end tracing on every provider (B10):
  - Every publish (batch: one span) creates an `ActivityKind.Producer` span and writes W3C `traceparent` /
    `tracestate` to the message. Those header names stay reserved.
  - Every delivery creates an `ActivityKind.Consumer` span parented on the publisher's context, with the same trace ID
    across RabbitMQ, Azure Service Bus and in-memory. Retry copies keep the original trace. Deliveries that are not
    acknowledged get Error status and the metrics' `error.type`.
  - Trace headers match case-insensitively and may be strings or UTF-8 bytes; on Service Bus the SDK's `Diagnostic-Id`
    is the fallback for `traceparent`.
  - Retries and dead letters are tagged on the span (`zedlabs.messagequeue.retry.next_attempt`,
    `zedlabs.messagequeue.dead_letter.reason`).
  - The consume span is `Activity.Current` inside the handler.
  - A message without valid trace headers starts a new trace instead of joining the subscriber's ambient span.
- Duration histograms `messaging.client.operation.duration` (publish) and `messaging.process.duration` (receipt to
  settlement), in seconds, with the conventions' bucket advice.
- `TraceContext.Parse(traceparent, tracestate)` (strict, bounded: traceparent at most 256 characters, tracestate 512
  characters / 32 members; never throws), `TraceContext.ToTraceParent()`, `TraceFlags` (sampled and random flags
  kept), `Sampled`, `TraceState`, and `TelemetryActivity.SetTag` / `SetErrorType` / `ErrorType`.
- Publish telemetry is guarded like the consumer pipeline: a throwing provider is logged (event 1009) and never fails
  or changes a publish. Without a telemetry provider, publishing allocates nothing for telemetry; with one, nothing is
  parsed, formatted or tagged while no listener is attached.
- `samples/ZedLabs.MessageQueue.Samples`: a worker on the in-memory provider by default (RabbitMQ / Azure Service Bus via
  `--environment`), a processor showing Acknowledge / Retry / DeadLetter, and OpenTelemetry console-exporter wiring.
  The sample only references `OpenTelemetry.Extensions.Hosting` / `OpenTelemetry.Exporter.Console` 1.20.0 and
  `Microsoft.Extensions.Hosting` 10.0.13. A Unit smoke test boots it.
- `benchmarks/ZedLabs.MessageQueue.Benchmarks` (BenchmarkDotNet 0.15.8): serializer, dispatcher overhead, in-memory
  throughput, RabbitMQ confirms. CI builds it, and `dotnet test` does not run it.

#### Changed
- CA1848 / CA1873 are errors again (the Phase 6 deferral in `.editorconfig` is removed). All library logging uses
  `LoggerMessage` source generation.

### Azure Service Bus provider rework (OPTIMIZATION-PLAN B5–B8, S3–S6, S10, §4.3)

Design records: `docs/adr/0009`–`0012`.

#### Breaking
- Removed `AzureServiceBusConfig.SenderPoolSize` and `MaxConcurrentSends` and the internal `PublisherSender` pool
  (§4.3, ADR 0012): one thread-safe `ServiceBusSender` per queue. Configurations that still set them fail at startup
  (unknown key). `SendTimeout` still bounds every send.
- `AzureServiceBusConfig.ConnectionString` is no longer `[Required]`: exactly one of `ConnectionString` or the new
  `FullyQualifiedNamespace` must be set. A malformed connection string now fails validation at startup and in the
  constructor (`OptionsValidationException`) instead of `MessageQueueException` from the constructor.
- Queue retries are scheduled copies (`ScheduledEnqueueTime` = now + backoff) sent with a new `MessageId` and the
  original id in `x-zedlabs-message-id`, then the original is completed, in one transaction by default (B5).
  `Capabilities.DelayedRedelivery` is `true` for queues; the backoff startup warning no longer applies to them.
  Partitioned entities, the Basic tier and the emulator refuse the transaction: on the first refusal the queue switches
  to send-then-complete for good (one warning; the backoff still applies). `TransactionalRetries = false` skips the
  attempt. The commit blocks a thread-pool thread; a startup warning flags `MaxConcurrentCalls` above 100.
- Topic subscriptions: a retry waits under the message lock (when the delay fits the lock-renewal budget), then the
  message is abandoned with the next attempt recorded; longer delays are abandoned at once with a warning.
  `DelayedRedelivery` stays `false` there. A startup warning appears when `Retry.MaxRetries + 1` exceeds Service Bus's
  default `MaxDeliveryCount` (10), and another when a subscription uses `PrefetchCount > 0` (prefetched locks run down
  while retries hold handler slots).
- `MessageContext.DeliveryAttempt` comes from library properties (`x-zedlabs-delivery-attempt`, `x-zedlabs-delivery-count`)
  when they agree with the broker's `DeliveryCount`, else from `DeliveryCount`; values saturate (always 1 to
  `int.MaxValue`). Dead letters set the marker out of reach, so a message resubmitted from the DLQ starts at attempt 1. `MessageContext.MessageId` is the
  original id across retries, and empty (never null) for messages without a `MessageId`. `MessageContext.Headers` no longer contains `x-*`, `traceparent`, `tracestate` or
  the SDK's `Diagnostic-Id`.
- Dead letters carry the reason code and exception type in the reason, description and the application properties
  `x-zedlabs-dead-letter-reason` / `x-zedlabs-dead-letter-detail`; never exception text. `OriginalException` is gone (S6).
- A second `SubscribeAsync` throws `InvalidOperationException` (it used to fail inside the SDK, wrapped).
  `UnsubscribeAsync` and `DisposeAsync` are bounded by the new `ShutdownTimeout` (default 30 s); cancelled handlers'
  messages are abandoned for redelivery without using an attempt. `Dispose()` runs `DisposeAsync` on the thread
  pool. Publishing after dispose throws `ObjectDisposedException`.
- Publish errors name only the exception type (no client text); a message too large for any batch throws
  `MessageQueueException` naming its index. Client-creation failures carry no inner exception (it can echo the
  connection string; S10).
- `AzureServiceBusHealthCheck` no longer uses the administration client (Manage rights) or creates a client per
  probe: it peeks (Listen rights) through one cached receiver, and its result carries no data and no exception text.
  It gained an optional `subscriptionName` parameter and implements `IAsyncDisposable`/`IDisposable` (S4, S5).
- `MessageQueueHealthCheck` no longer puts the exception message in `Data["error"]` (S5).

- A `TokenCredential` passed to the constructor together with a `ConnectionString` configuration throws
  `ConfigurationException`; a container-registered credential is ignored for connection-string queues.

#### Added
- Entra ID identity (S3): `AzureServiceBusConfig.FullyQualifiedNamespace`; the credential is the one passed to the
  new constructor `AzureServiceBusMessageQueue<T>(config, TokenCredential, telemetry, logger)`, else a
  `TokenCredential` registered in the container, else one `DefaultAzureCredential` shared per process. New dependency: Azure.Identity 1.21.0.
  Required roles: Azure Service Bus Data Sender / Data Receiver.
- `AzureServiceBusConfig.ShutdownTimeout` (30 s) and `TransactionalRetries` (true).
- `AddAzureServiceBusHealthCheck<T>()` reuses the registered queue's client (Unhealthy while its processor is not
  running). `AddAzureServiceBusMetricsHealthCheck` / `AzureServiceBusMetricsHealthCheck`: the opt-in admin-metrics
  check (needs Manage), one administration client for every probe. Both standalone checks (and their registration
  extensions) also accept `FullyQualifiedNamespace` + `TokenCredential`.

#### Fixed
- B5: the retry backoff is applied (queues: on the broker).
- B6: re-subscribing after unsubscribe works (one processor per subscription); processor errors are logged with
  error source, entity path and exception type (LoggerMessage), throttled per distinct error, instead of swallowed.
- B7: dispose is idempotent and bounded; unsubscribe waits for in-flight handlers; unsubscribe or dispose from inside a
  handler no longer deadlocks.
- B8: batch publish splits by byte size with `CreateMessageBatchAsync` + `TryAddMessage`.
- Every message is settled exactly once. A message whose body or metadata cannot be read is dead-lettered as
  `DeliveryFailed`; failed settlements back off per message before abandoning; lost locks are logged, not thrown.
- S10 (ASB): no SharedAccessKey in any exception or log line from configuration or client creation.
- A handler's cancellation callback that throws no longer breaks `UnsubscribeAsync`/`DisposeAsync` (Azure Service Bus
  and RabbitMQ): it is logged, and the processor or channel is still stopped and the client closed.
- A message without a `MessageId` is no longer treated as a settlement failure after it was handled.

#### Internal
- Shared consumer helpers in `Processing/` (`HandlerScope`, `TransportBackoff`, `LogThrottle`, `BackgroundWork`,
  `MessageDispatcher.RejectUnreadableAsync`), used by RabbitMQ and Azure Service Bus. RabbitMQ behavior is unchanged
  except that an unreadable delivery is now logged by the dispatcher (event 1007) instead of RabbitMQ event 2008, and
  counted as a failed consume.
- Thin internal seams over the Service Bus SDK; the provider is unit-tested without a namespace.
- The startup warning for configurations without delayed redelivery no longer says "yet".

### RabbitMQ provider on RabbitMQ.Client 7 (OPTIMIZATION-PLAN Phase 5: B1–B4, B15, S7, S10, §4.2)

Design records: `docs/adr/0006`–`0008`.

#### Breaking
- RabbitMQ.Client 6.8.1 → 7.2.2 (async-only API). Apps that reference RabbitMQ.Client themselves must move to 7.x.
- `RabbitMqMessageQueue<T>` no longer connects in its constructor. It connects, and declares its exchange and queues,
  on the first `PublishAsync`/`SubscribeAsync` or on the new `InitializeAsync(CancellationToken)`. New
  `RabbitMqMessageQueue.CreateAsync<T>(config, telemetry, logger, cancellationToken)` constructs and connects in one
  call. Connection failures surface there as `MessageQueueException`, whose message names only the exception type
  (no client text, no credentials). A failed connect is retried on the next call.
- TLS is required by default for any host other than a loopback host (`localhost`, `127.0.0.0/8`, `::1`) (S7): a plain `amqp://`
  connection string to a remote host now fails validation at startup and in the constructor. Use `amqps://` or set
  the new `RequireTls = false`. Connect-time re-checks throw `ConfigurationException`.
- `RabbitMqConfig.ConnectionString` is no longer `[Required]` alone: exactly one of `ConnectionString` or the new
  `HostName` must be set. `ConnectionString` must be an absolute `amqp://`/`amqps://` URI.
- Retries are no longer waited out in process: they are published to per-delay retry queues
  `{QueueName}.retry.{ms}` (declared on first use, same queue type as the main queue) and come back to the main
  queue when their TTL expires. `Capabilities.DelayedRedelivery` is now `true`. Delete the retry queues together
  with the main queue.
- Dead letters are published by the library to the dead-letter queue, with headers `x-zedlabs-dead-letter-reason`
  (a `DeadLetterReasons` code) and `x-zedlabs-dead-letter-detail` (exception type), then the original is acked. Before,
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
- The in-memory queue moved from `ZedLabs.MessageQueue.Testing` to `ZedLabs.MessageQueue.InMemory` and is now a provider
  (`Provider = InMemory`). `AddInMemoryMessageQueue<T>()` moved to `ZedLabs.MessageQueue.Extensions.ServiceCollectionExtensions`
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
- Design decision recorded for Phase 7: split into ZedLabs.MessageQueue (core), ZedLabs.MessageQueue.RabbitMQ and
  ZedLabs.MessageQueue.AzureServiceBus with a public provider-registration extension point (ADR 0005). Not implemented yet.

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
  `ZedLabs.MessageQueue`. CI now runs unit and integration tests as separate steps.
