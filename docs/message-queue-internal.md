# Message Queue internals

How the library is put together, for contributors. Decisions and their reasons are in `docs/adr/`; this page is the
map. Public usage is in `docs/message-queue-api.md` and `docs/azure-service-bus-api.md`.

## Packages

| Package | Contents | Depends on |
|---|---|---|
| `ESE.MessageQueue` | Abstractions, the shared consumer pipeline, retry, serialization, registration and validation, hosting, telemetry, the generic health check, the in-memory provider, and the public provider SPI (`ESE.MessageQueue.Providers`) | Microsoft.Extensions.*, MessagePack |
| `ESE.MessageQueue.RabbitMQ` | RabbitMQ provider and health check | core (exact version), RabbitMQ.Client 7 |
| `ESE.MessageQueue.AzureServiceBus` | Service Bus provider and health checks | core (exact version), Azure.Messaging.ServiceBus, Azure.Identity |

The provider packages use core internals (`HandlerScope`, `TransportBackoff`, `LogThrottle`, `BackgroundWork`,
`Cancellation`, `SafeLogger`, `DeliveryText`, `RetryExecutor`) through `InternalsVisibleTo`, which is why they pin the
exact core version (ADR 0015). A third-party provider uses only the public SPI; the loopback sample proves that.

## Registration (ADR 0002, ADR 0015)

1. `AddRabbitMqProvider()` / `AddAzureServiceBusProvider()` / `AddMessageQueueProvider(p)` add an
   `IMessageQueueProvider` instance to the service collection. `InMemory` is built in.
2. `AddMessageQueue<T>(configuration, section)` reads `Provider`, finds the provider by name and calls its
   `AddQueue<T>`. The provider calls `MessageQueueRegistration<T>.AddQueue<TConfig, TValidator, TQueue>(factory)`.
3. Core binds the section to `TConfig` (unknown keys fail, enum settings must be names), registers DataAnnotations,
   the shared rules (`MessageQueueConfigValidator<TConfig>`) and the provider validator with `ValidateOnStart`, and
   registers the queue as a singleton exposed as `IMessageQueue<T>`, `IMessagePublisher<T>` and
   `IMessageSubscriber<T>`. Options are named by `typeof(T).FullName`, so each message type has its own section.

## Publishing

`PublishValidation` checks the message and `PublishOptions` (id, header limits, reserved `x-*` / trace header names),
`MessageSerializer` serializes (ADR 0018), and `PublishTelemetry` starts the producer span and supplies the
`traceparent` / `tracestate` headers (ADR 0013). Then each provider sends its own way:

- RabbitMQ: a pool of confirm-tracking channels; batches publish everything, then await the confirms (ADR 0007).
- Service Bus: one sender; batches are built with `CreateMessageBatchAsync` within the byte limit (ADR 0012).
- In-memory: an unbounded channel in the process (ADR 0003).

## Consuming (ADR 0004)

Each provider maps a broker delivery to a `MessageContext` (sanitized id, capped headers, 1-based `DeliveryAttempt`,
trace headers) and hands the body to `MessageDispatcher<T>.DispatchAsync` with an `IDeliveryActions` for that
delivery. The dispatcher: size limit, deserialize, handler, then `RetryPolicy` (ADR 0001), and calls exactly one of
complete, reschedule (delay, next attempt) or dead-letter, or none when shutdown cancelled the handler. Only a
failing transport action propagates; telemetry and logging failures never change the outcome.

| Provider | Reschedule | Dead-letter | Attempt counting |
|---|---|---|---|
| RabbitMQ | copy to `{queue}.retry.{ms}` (TTL, dead-letters back), then ack (ADR 0006) | copy to `{queue}.deadletter` with reason headers, then ack | `x-delivery-attempt` when `x-death` says it expired out of a retry queue; plus broker redeliveries |
| Service Bus queue | scheduled copy, then complete, in a transaction (ADR 0009) | the entity's dead-letter subqueue | library properties consistent with `DeliveryCount` |
| Service Bus subscription | wait under the lock, then abandon | dead-letter subqueue | `DeliveryCount` |
| In-memory | timer, back into the channel | `DeadLetters` list | in-process counter |

## Shutdown and settlement (ADR 0008, ADR 0010)

`UnsubscribeAsync` / `DisposeAsync` cancel the handlers' token, wait at most `ShutdownTimeout`, and close the
connection or client. A call from inside a handler (`HandlerScope`) finishes in the background instead of waiting for
itself. Each delivery is settled at most once; a failing settlement backs off per message (`TransportBackoff`) before
the message is handed back.

## Hosting, health and telemetry

- `MessageQueueHostedService<T>` subscribes at host start and resolves the scoped `IMessageProcessor<T>` in a new DI
  scope per message; stopping the host cancels in-flight processors.
- Health checks (ADRs 0008, 0011): the provider checks reuse the queue's connection or client. Results carry no
  exception, exception text, host or namespace; failures are logged (event 6001).
- Telemetry (ADR 0013): `ITelemetryProvider`; `OpenTelemetryProvider` uses `System.Diagnostics` only. Spans for publish
  and consume, semantic-convention metrics, retries, dead letters and transport errors.

## Tests

One test project, `tests/ESE.MessageQueue.Tests`, Unit and Integration traits (CLAUDE.md "Testing"). RabbitMQ
integration specs use Testcontainers; Service Bus behavior is specified through the SDK seams with
`Support/FakeServiceBus`, and the broker specs run only with `MQ_ASB_CONNECTION_STRING`.
