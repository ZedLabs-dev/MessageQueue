# Production readiness status

Where each item of [PRODUCTION-READINESS-PLAN.md](../PRODUCTION-READINESS-PLAN.md) stands, with the evidence. The
plan file itself is the owner's and is left unchanged; this page is its tracking view. Status as of 2026-10-22,
version `1.0.0-rc.1`.

**Done** = implemented, specified and documented. **Partial** = done in part; the remaining gap is named and can be
closed without the owner. **Open** = not done. **Blocked** = whatever remains can only be closed by the owner (an
account, a license or a decision).

| | Done | Partial | Open | Blocked |
|---|---:|---:|---:|---:|
| Items (42) | 36 | 4 | 0 | 2 |

Open and blocked items, at a glance:

- **Blocked:** the Service Bus edge-case integration specs exist (RabbitMQ and in-memory ones pass) but have never
  run, because there is no namespace (§7; also the 1.0.0 release blocker); running the Service Bus emulator from the
  compose file (§7): starting it means accepting its EULA, which is the owner's decision.
- **Partial:** publish has no log line (§4, by design, needs the owner's sign-off); throughput benchmarks lack Service
  Bus, RabbitMQ consumption and a networked broker (§5); high-concurrency behavior is specified but not load-tested
  (§5); .NET 10-only support is decided but has no design record (§10).

## 1. Functional and API surface

| Item | Status | Evidence |
|---|---|---|
| Review public interfaces and lock the public API for v1 | Done | [ADR 0016](adr/0016-public-api-lock-and-packaging.md); `src/*/PublicAPI.Shipped.txt` with PublicApiAnalyzers (RS0016/RS0017 fail the build); spec `Packaging/Given_the_public_api_files`. `/release` moves Unshipped to Shipped at each release |
| Document planned breaking changes; apply now or defer | Done | Breaking changes were applied before 1.0 and listed under "Breaking" in [CHANGELOG.md](../CHANGELOG.md) (enum names only, correlation-id limits, `InMemoryDeadLetter<T>` shape); none are pending. After 1.0 the API lock makes every public change a reviewed diff |
| All public types and members intentional | Done | [ADR 0016](adr/0016-public-api-lock-and-packaging.md) "review before the lock" (internal or sealed by default); provider SPI deliberately public ([ADR 0015](adr/0015-provider-spi-and-package-split.md)); CS1591 as an error; spec `Packaging/Given_the_documentation_exemptions` |

## 2. Configuration and environment

| Item | Status | Evidence |
|---|---|---|
| Required and optional settings for RabbitMQ and Azure Service Bus | Done | [configuration.md](configuration.md): every setting with type, default, range, required and notes; spec `Packaging/Given_the_configuration_reference` checks every setting is listed with its real default |
| Validate configuration at startup | Done | [ADR 0002](adr/0002-provider-agnostic-config-and-options-validation.md), `ValidateOnStart` in `Providers/MessageQueueRegistration.cs`; specs `Configuration/Given_every_startup_rule`, `Given_a_message_queue_configuration`; 58 quoted errors in [configuration.md](configuration.md#startup-errors), each produced by the spec |
| Standardize binding and section names | Done | Section `MessageQueue` (`MessageQueueConfig.SectionName`), one section per queue, strict binding (unknown keys fail), enum names only; [ADR 0002](adr/0002-provider-agnostic-config-and-options-validation.md), [ADR 0015](adr/0015-provider-spi-and-package-split.md); [configuration.md](configuration.md#how-configuration-is-read) |
| Missing or invalid configuration in production vs development | Done | Decided: the same fail-fast behavior in every environment, no default provider or endpoint; TLS required off-loopback ([ADR 0008](adr/0008-rabbitmq-tls-and-consumer-shutdown.md)). [configuration.md](configuration.md#production-and-development) |

## 3. Error handling and resilience

| Item | Status | Evidence |
|---|---|---|
| Exception flows use `MessageQueueException` consistently | Done | `Exceptions/MessageQueueException.cs` (`ConfigurationException` derives from it); publish, subscribe and connect failures are wrapped without leaking broker text; argument and option errors use the .NET types. Listed in [failure-modes.md](operations/failure-modes.md) and [configuration.md](configuration.md#production-and-development) |
| Retry behavior per provider (max attempts, backoff, dead-letter) | Done | [ADR 0001](adr/0001-retry-semantics-and-default-policy.md), [0006](adr/0006-rabbitmq-broker-side-backoff.md), [0009](adr/0009-azure-service-bus-backoff-and-attempt-counting.md); [configuration.md](configuration.md#retry-section-retrypolicy); specs `Retry/`, `Processing/Given_a_message_dispatcher`, provider retry specs |
| Transient vs fatal errors defined and tested | Done | `MessageResult` (`Retry` / `DeadLetter`), `RetryPolicy.IsRetryable` (`NonRetryableError`), immediate dead-letter for `DeserializationFailed`/`MessageTooLarge`/`DeliveryFailed`; [failure-modes.md](operations/failure-modes.md#the-delivery-pipeline); specs `Processing/Given_a_message_dispatcher` |
| Dead-letter queues created and used consistently | Done | One pipeline for every provider ([ADR 0004](adr/0004-dispatcher-and-transport-seam.md)); `{QueueName}.deadletter` on RabbitMQ, the entity's subqueue on Service Bus, in-memory list; `IDeadLetterQueue<T>` on all three ([ADR 0023](adr/0023-dead-letter-operations.md)); specs `RabbitMQ/Given_a_rabbitmq_dead_letter_queue`, `AzureServiceBus/Given_an_azure_service_bus_dead_letter_queue`, `InMemory/Given_an_in_memory_dead_letter_queue` |

## 4. Telemetry, logging and observability

| Item | Status | Evidence |
|---|---|---|
| Microsoft.Extensions.Logging for structured logging | Done | 55 source-generated `[LoggerMessage]` events with unique ids (spec `Processing/Given_the_library_log_messages`); [monitoring.md](operations/monitoring.md#log-events), checked by `Packaging/Given_the_monitoring_reference` |
| Logs for publish, subscribe, retries, dead-lettering, connection failures | Partial | Subscribe (5001/5002), retries (1001, 1002), dead-lettering (1003, 1004), connection failures (2003, 3009) are logged. **Publish has no log line**: its outcome is in metrics and spans, and failures throw to the caller. A per-message publish log was left out as noise; the owner should confirm that decision |
| `ITelemetryProvider` abstraction | Done | `Telemetry/ITelemetryProvider.cs`; [ADR 0013](adr/0013-telemetry-trace-propagation-and-metrics.md) |
| OpenTelemetry provider (traces and metrics) | Done | `Telemetry/OpenTelemetry/OpenTelemetryProvider.cs`: publish and consume spans, 9 metrics (throughput, latency, retries, dead letters, transport errors, replay, purge); [monitoring.md](operations/monitoring.md) |
| W3C trace context propagation | Done | `traceparent`/`tracestate` headers (`Telemetry/TraceHeaders.cs`, `TraceContext.cs`); specs `InMemory/Given_a_traced_in_memory_queue`, `RabbitMQ/Given_a_traced_rabbitmq_queue`, `AzureServiceBus/Given_a_traced_azure_service_bus_queue` |
| Document how to register custom telemetry providers | Done | [monitoring.md](operations/monitoring.md#wiring) (`AddMessageQueueTelemetry<TProvider>()`), [getting started](getting-started/in-memory.md#telemetry), [message-queue-api.md](message-queue-api.md#telemetry-traces-and-metrics-adr-0013) |

## 5. Performance and scalability

| Item | Status | Evidence |
|---|---|---|
| Benchmark publish/subscribe throughput for RabbitMQ and Azure Service Bus under realistic workloads | Partial | [benchmarks.md](benchmarks.md): serializer, dispatcher, in-memory end to end, RabbitMQ publish (one by one and batched), default job. **Missing:** Service Bus (no namespace), RabbitMQ consumption, and a broker over a real network (the RabbitMQ numbers are loopback) |
| Behavior under high concurrency (`MaxConcurrentCalls`, prefetch, batch sizes) | Partial | Specified: `InMemory/Given_an_in_memory_queue` (bounded concurrency), `RabbitMQ/Given_a_rabbitmq_queue_with_a_subscriber` (`When_Handlers_Run_Concurrently`, `When_Publishing_Concurrently_With_Publisher_Confirms`), `RabbitMQ/Given_a_publisher_pool`, `AzureServiceBus/Given_an_azure_service_bus_subscriber`, `Given_an_azure_service_bus_batch_publish`; startup warnings 2010, 3016. **Missing:** a sustained load test at high concurrency |
| No synchronous blocking on async paths | Done | Async throughout (RabbitMQ.Client 7, [ADR 0007](adr/0007-rabbitmq-client-7-and-lazy-initialization.md)). Known, documented exceptions: the Service Bus retry transaction commits synchronously ([ADR 0009](adr/0009-azure-service-bus-backoff-and-attempt-counting.md), warning 3016), and `Dispose()` wraps `DisposeAsync()` for callers that only know `IDisposable` |
| Connection pooling and client reuse | Done | RabbitMQ: one connection per queue with a `PublisherPool` of channels ([ADR 0007](adr/0007-rabbitmq-client-7-and-lazy-initialization.md)); Service Bus: one client and sender per queue ([ADR 0012](adr/0012-azure-service-bus-single-sender-and-batching.md)); queue-bound health checks reuse them |

## 6. Security and compliance

| Item | Status | Evidence |
|---|---|---|
| Secure handling of connection strings and credentials | Done | Redacted `ToString()`, fixed error messages, no exception text in logs or health results (OPTIMIZATION-PLAN §2 S5, S10; [ADR 0011](adr/0011-azure-service-bus-identity-and-health.md)); Entra ID by default on Service Bus; [configuration.md](configuration.md#production-and-development) |
| TLS requirements and defaults | Done | RabbitMQ: TLS required for every non-loopback host unless `RequireTls = false`, insecure certificate acceptance needs a second flag, mutual TLS ([ADR 0008](adr/0008-rabbitmq-tls-and-consumer-shutdown.md), [ADR 0022](adr/0022-rabbitmq-client-certificates.md)); Service Bus is always TLS. Specs `RabbitMQ/Given_a_rabbitmq_connection_config`, `Given_a_rabbitmq_client_certificate` |
| Recommended IAM/permission scopes | Done | RabbitMQ least-privilege vhost permissions, verified against RabbitMQ 4.3.6 ([RabbitMQ guide](getting-started/rabbitmq.md#2-a-broker)); Service Bus Data Sender/Receiver only, Manage only for the opt-in metrics check ([Service Bus guide](getting-started/azure-service-bus.md#2-entities-and-permissions), [ADR 0011](adr/0011-azure-service-bus-identity-and-health.md)) |
| Serialization settings reviewed for leakage or injection | Done | [ADR 0018](adr/0018-serializer-security.md) (MessagePack `UntrustedData`, depth limits, strict UTF-8), [ADR 0019](adr/0019-source-generated-json-metadata.md); dead-letter records carry exception types only ([ADR 0021](adr/0021-bounded-values-in-brokers-and-telemetry.md)) |

## 7. Testing strategy

| Item | Status | Evidence |
|---|---|---|
| Unit tests cover core behaviors per provider and the pipeline | Done | One test project, behavior-driven specs per area; coverage gate 89% combined, 87% per package (`scripts/coverage.sh`); 275 mutants in `scripts/mutants/` |
| Docker Compose for local integration testing (RabbitMQ and Service Bus emulator) | Blocked | `docker/docker-compose.dev.yml` has both. RabbitMQ is used; the Service Bus emulator has only been checked with `docker compose config`, because starting it requires accepting its EULA, which is the owner's decision |
| Integration tests for edge cases (large, invalid, deserialization failures, max retries, dead-lettering) | Blocked | RabbitMQ and in-memory: done (`RabbitMQ/Given_a_rabbitmq_queue_with_malformed_messages`, `Given_a_rabbitmq_queue_with_a_failing_handler`, `Given_a_rabbitmq_dead_letter_queue`, `Given_a_rabbitmq_queue_on_an_unreliable_broker`). Service Bus: `AzureServiceBus/Given_a_running_azure_service_bus` (11 specs) has never run; it needs a namespace or the emulator, and is the 1.0.0 release blocker ([release checklist](release-checklist.md#release-blockers)) |
| Regression suite to run before releases | Done | [release-checklist.md](release-checklist.md), used by `/release` |

## 8. Documentation

| Item | Status | Evidence |
|---|---|---|
| Getting started per provider | Done | [in-memory](getting-started/in-memory.md), [RabbitMQ](getting-started/rabbitmq.md), [Azure Service Bus](getting-started/azure-service-bus.md); their code is compiled from `samples/ESE.MessageQueue.Samples/Docs/` and the in-memory guide is executed (`Packaging/Given_the_documentation_snippets`) |
| Configuration examples for appsettings.json and environment variables | Done | Every guide and [configuration.md](configuration.md); every example is bound and validated by the same spec |
| Usage samples: publisher, consumer, processor | Done | Guides; runnable worker `samples/ESE.MessageQueue.Samples`; provider sample `samples/ESE.MessageQueue.Samples.LoopbackProvider` |
| Failure modes and how to monitor and operate | Done | [failure-modes.md](operations/failure-modes.md), [monitoring.md](operations/monitoring.md) |
| Telemetry integration (OpenTelemetry setup, custom providers) | Done | [monitoring.md](operations/monitoring.md#wiring), guides' telemetry step (compiled) |

## 9. Operational runbooks

| Item | Status | Evidence |
|---|---|---|
| SOPs for connection failures, high retry rates, dead-letter backlog | Done | [runbooks.md](operations/runbooks.md) |
| Recommended alert rules | Done | [alerts.md](operations/alerts.md): Prometheus rules for dead-letter rate, retries, p95 latency, publish failures, transport errors, stalled consumers, RabbitMQ dead-letter depth; Azure Monitor rule for Service Bus dead letters |
| Safe configuration changes and rollbacks | Done | [runbooks.md](operations/runbooks.md#rolling-out-configuration-changes): restart-only settings, queue argument changes (`PRECONDITION_FAILED`), retry-queue cleanup |
| Troubleshooting per provider | Done | [runbooks.md](operations/runbooks.md#rabbitmq) and [#azure-service-bus](operations/runbooks.md#azure-service-bus) |
| Drain and replay dead-letter queues | Done | `IDeadLetterQueue<T>` ([ADR 0023](adr/0023-dead-letter-operations.md)); [dead-letter-operations.md](dead-letter-operations.md); [runbooks.md](operations/runbooks.md#dead-letter-backlog) |

## 10. Open questions and design decisions

| Question | Status | Decision |
|---|---|---|
| Expose provider-specific advanced options directly, or via extension points only? | Done | Both, deliberately split: provider-specific **settings** live on each provider's config class (`RabbitMqConfig`, `AzureServiceBusConfig`), never on the portable base ([ADR 0002](adr/0002-provider-agnostic-config-and-options-validation.md)); broker SDK clients are not exposed; new brokers and behaviors plug in through the public provider SPI ([ADR 0005](adr/0005-provider-packaging-and-extensibility.md), [ADR 0015](adr/0015-provider-spi-and-package-split.md)). Entity settings (partitioning, duplicate detection) stay in infrastructure (OPTIMIZATION-PLAN decision D4) |
| Support only .NET 10.x and forward | Partial | Decided and implemented: `net10.0` only (`Directory.Build.props`), SDK pinned in `global.json`, done in OPTIMIZATION-PLAN Phase 2. No ADR records it; one would make the support policy (which .NET versions a 1.x release targets) explicit |
| Retry and backoff configurable; start with 5 retries and about 60 seconds | Done | [ADR 0001](adr/0001-retry-semantics-and-default-policy.md): `RetryPolicy` (`Retry` section), default 5 retries, exponential 2 s to 30 s with jitter, about 60 s in total; per-provider backoff in ADRs [0006](adr/0006-rabbitmq-broker-side-backoff.md) and [0009](adr/0009-azure-service-bus-backoff-and-attempt-counting.md) |
