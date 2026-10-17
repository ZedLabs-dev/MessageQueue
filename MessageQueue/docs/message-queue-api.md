# Message Queue API

Provider-agnostic event messaging for .NET. Applications code against `IMessageQueue<T>`; RabbitMQ, Azure Service Bus
and an in-memory provider are interchangeable, chosen by configuration.

## Library Overview

**Package / namespace root:** `ESE.MessageQueue`
**Target:** .NET 10
**Design records:** `docs/adr/` (retry semantics, configuration, in-memory guarantees, consumer pipeline)

**Core concepts:**
- `IMessageQueue<T>` = `IMessagePublisher<T>` + `IMessageSubscriber<T>` + `Capabilities`.
- One consumer pipeline for every provider: size limit, deserialization, your handler, then the retry policy
  (backoff, dead-letter after the last retry).
- Configuration through the options pattern with startup validation; one queue per message type, each with its own
  section and provider.

## Usage Examples

### Basic Configuration

```csharp
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;

services.AddMessageQueue<OrderCreated>(configuration);                     // binds the "MessageQueue" section
services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();       // scoped: one instance per message
services.AddHostedService<MessageQueueHostedService<OrderCreated>>();      // consumes for the host's lifetime
```

Several message types can use different sections (and providers):

```csharp
services.AddMessageQueue<OrderCreated>(configuration, "Orders");
services.AddMessageQueue<PaymentReceived>(configuration, "Payments");
```

### Configuration File Setup

`Provider` is required: `RabbitMq`, `AzureServiceBus` or `InMemory` (names only, case-insensitive). A missing or
unknown value throws `ConfigurationException` at registration; there is no default broker. Other settings are
validated when the host starts (`OptionsValidationException`); unknown keys such as the removed `RetryAttempts` fail
startup too. Constructing a provider directly applies the same validation. One queue per message type.

**RabbitMQ:**
```json
{
  "MessageQueue": {
    "Provider": "RabbitMq",
    "ConnectionString": "amqps://user:password@rabbit.example.com:5671/",
    "QueueName": "order-created",
    "EnableDeadLetterQueue": true,
    "Retry": { "MaxRetries": 5, "InitialDelay": "00:00:02", "MaxDelay": "00:00:30" },
    "UseQuorumQueues": true,
    "QuorumInitialGroupSize": 3,
    "EnablePublisherConfirms": true,
    "PrefetchCount": 10
  }
}
```

**Azure Service Bus:**
```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "your-namespace.servicebus.windows.net",
    "QueueName": "order-created",
    "MaxConcurrentCalls": 10,
    "MessageTimeToLive": "7.00:00:00",
    "PrefetchCount": 20,
    "MaxAutoLockRenewalDuration": "00:05:00"
  }
}
```

**In-memory** (local development, tests, single-process deployments; no broker, nothing is durable):
```json
{ "MessageQueue": { "Provider": "InMemory", "QueueName": "order-created", "ShutdownTimeout": "00:00:30" } }
```
`QueueName` is required. `ShutdownTimeout` bounds how long unsubscribe and dispose wait for running handlers.
or in code: `services.AddInMemoryMessageQueue<OrderCreated>(config => config.MaxConcurrentCalls = 4);`

### Publishing Messages

```csharp
public class OrderService(IMessagePublisher<OrderCreated> publisher)
{
    public Task CreateOrderAsync(OrderCreated order, CancellationToken ct) =>
        publisher.PublishAsync(order, new PublishOptions
        {
            MessageId = order.OrderId,             // portable idempotency key
            CorrelationId = Activity.Current?.Id,
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, ct);
}
```

### Message Processing

```csharp
public class OrderCreatedProcessor(ILogger<OrderCreatedProcessor> logger) : IMessageProcessor<OrderCreated>
{
    public async Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context, CancellationToken ct)
    {
        try
        {
            await ProcessOrderLogic(message, ct);
            return MessageResult.Acknowledge;
        }
        catch (ValidationException)
        {
            return MessageResult.DeadLetter;       // permanent: no retry
        }
        // Any other exception counts as MessageResult.Retry: redelivered after the backoff,
        // dead-lettered once Retry.MaxRetries retries have failed.
    }
}
```

## API Reference

### `IMessagePublisher<T>`

| Member | Purpose |
|---|---|
| `PublishAsync(T message, CancellationToken ct = default)` | Publish with a generated version 7 GUID message id |
| `PublishAsync(T message, PublishOptions options, CancellationToken ct = default)` | Publish with message id, correlation id and headers |
| `PublishAsync(IEnumerable<T> messages, CancellationToken ct = default)` | Publish several messages |

Throws `MessageQueueException` when publishing (or, in-memory, serializing) fails.

### `IMessageSubscriber<T>`

| Member | Purpose |
|---|---|
| `SubscribeAsync(Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken ct = default)` | Rich handler: metadata in, `MessageResult` out. The token is cancelled when the subscription stops. A thrown exception counts as `Retry`. |
| `SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken ct = default)` | Convenience: `true` → `Acknowledge`, `false` → `Retry` |
| `UnsubscribeAsync(CancellationToken ct = default)` | Stop delivering; undelivered messages stay queued |

One subscriber per queue instance.

### `IMessageQueue<T>`

Both of the above plus `MessageQueueCapabilities Capabilities`:

| Provider | `Durable` | `DeadLetter` | `DelayedRedelivery` |
|---|---|---|---|
| RabbitMQ | yes | yes (`{QueueName}.deadletter`) | yes: per-delay retry queues `{QueueName}.retry.{ms}` (ADR 0006) |
| Azure Service Bus (queue) | yes | yes (`$deadletterqueue`) | yes: scheduled retry copies (ADR 0009) |
| Azure Service Bus (topic subscription) | yes | yes (`$deadletterqueue`) | no: a retry waits under the message lock, then is abandoned (ADR 0009) |
| In-memory | **no** | yes (`DeadLetters`) | yes |

The retry *count* behaves the same everywhere. RabbitMQ, in-memory and Azure Service Bus queues honour the backoff
*delay* without holding the message; an Azure Service Bus topic subscription holds it under its lock (when the delay
fits the lock-renewal budget) and logs a startup warning when the retry policy has a backoff.

### `PublishOptions` Record

- `MessageId` (string?): message id and idempotency key; consumers see it as `MessageContext.MessageId`.
  Non-blank, at most `PublishOptions.MaxMessageIdLength` (128) characters.
- `CorrelationId` (string?)
- `Headers` (IReadOnlyDictionary<string, string>?): delivered as `MessageContext.Headers`. At most `MaxHeaders` (64),
  names up to 128 characters, values up to 4096. Reserved names are rejected with `ArgumentException`: anything
  starting with `x-` (broker and library headers such as `x-delivery-attempt`), `traceparent` and `tracestate`.

Every provider validates the message and options the same way before sending (`ArgumentNullException` /
`ArgumentException`).

### `IMessageProcessor<T>` Interface

`Task<MessageResult> ProcessAsync(T message, MessageContext context, CancellationToken cancellationToken)`.
Registered as scoped with `AddMessageProcessor<T, TProcessor>()`; `MessageQueueHostedService<T>` creates a DI scope
for every message, so scoped dependencies (a `DbContext`, say) live for one message.

### `MessageContext` Record

- `MessageId` (string): the publisher's message id (stays the same across retries)
- `CorrelationId` (string?)
- `Timestamp` (DateTimeOffset): publish time (ASB: enqueue time)
- `DeliveryAttempt` (int): 1 for the first delivery, 2 for the first retry, and so on
- `Headers` (IReadOnlyDictionary<string, string>): publisher headers, immutable (a handler cannot change what a retry sees)

### `MessageResult` Enum

- `Acknowledge`: processed; remove the message
- `Retry`: transient failure; redeliver after the backoff, dead-letter when retries are exhausted
- `DeadLetter`: permanent failure; dead-letter now

### Provider classes

`RabbitMqMessageQueue<T>(RabbitMqConfig, ITelemetryProvider? = null, ILogger? = null)`,
`AzureServiceBusMessageQueue<T>(AzureServiceBusConfig, ITelemetryProvider? = null, ILogger? = null)` (or with a
`TokenCredential` after the config) and
`InMemoryMessageQueue<T>(InMemoryConfig? = null, ITelemetryProvider? = null, ILogger? = null)`. All are
`IAsyncDisposable`; `AddMessageQueue<T>` registers them as container-owned singletons.

`InMemoryMessageQueue<T>` also exposes `PublishedMessages`, `ProcessedMessages`, `DeadLetters`, `PendingCount`,
`WaitForIdleAsync()` (completes when nothing is queued, running or waiting to retry) and `Clear()`.

## Configuration Options

### Base settings (every provider)

| Property | Type | Default | Description |
|---|---|---|---|
| Provider | string | (required) | `RabbitMq`, `AzureServiceBus` or `InMemory` |
| QueueName | string | (required) | Queue to publish to and consume from |
| Retry | RetryPolicy | see below | Retry and backoff policy |
| SerializationFormat | `Json` / `MessagePack` | Json | Wire format |
| EnableDeadLetterQueue | bool | true | When false, messages that would be dead-lettered are dropped with a warning |
| MaxMessageSizeBytes | int | 1048576 | Larger bodies are dead-lettered (`MessageTooLarge`) without being deserialized |
| MaxConcurrentCalls | int | 10 | Handlers running at once (1..1000), on every provider. RabbitMQ: also bounded by `PrefetchCount` |

### `Retry` section

| Property | Default | Description |
|---|---|---|
| Strategy | Exponential | `Fixed`, `Linear`, `Exponential` (`Custom` needs a code-supplied calculator) |
| MaxRetries | 5 | Retries after the first attempt; at most `MaxRetries + 1` deliveries |
| InitialDelay | 00:00:02 | Wait before the first retry |
| BackoffMultiplier | 2.0 | Exponential growth factor (1..100) |
| MaxDelay | 00:00:30 | Cap for a single wait (at most 1 day) |
| UseJitter / JitterFactor | true / 0.1 | ±10% jitter, never above `MaxDelay` |

The default waits 2 + 4 + 8 + 16 + 30 ≈ 60 s across five retries, then dead-letters.

### RabbitMQ settings (`RabbitMqConfig`)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| ConnectionString | string | (one endpoint form required) | `amqps://user:password@host:port/vhost` (`amqp://` only without TLS, see `RequireTls`) |
| HostName / Port / UserName / Password / VirtualHost | string / int | empty / 0 (5672, or 5671 with TLS) | Alternative to `ConnectionString`; set exactly one of `ConnectionString` and `HostName`. `Password` is redacted from `ToString()` |
| RequireTls | bool? | unset: false for `localhost`/`127.0.0.1`/`::1`, true for any other host | Plain `amqp://` (or `HostName` without `Tls:Enabled`) is rejected at startup when TLS is required |
| Tls:Enabled | bool | false | TLS for the `HostName` form (`amqps://` enables it for a connection string) |
| Tls:ServerName | string | the host | Name the server certificate must match |
| Tls:AcceptablePolicyErrors | `SslPolicyErrors` flags | None | Certificate errors to accept; anything but `None` also needs `Tls:AllowInsecureCertificates = true` |
| DeadLetterQueueName | string | `{QueueName}.deadletter` | Dead-letter queue |
| UseQuorumQueues | bool | true | Quorum queues (main, dead-letter and retry queues) |
| QuorumInitialGroupSize | int | 3 | Initial quorum group size |
| PrefetchCount | int | 10 | Unacknowledged deliveries per consumer (1..65535), applied to every queue type |
| ExchangeName | string | QueueName | Exchange to publish to |
| ExchangeType | string | "direct" | `direct`, `fanout`, `topic` or `headers` |
| EnablePublisherConfirms | bool | true | Wait for broker confirms on `PublishAsync` (retry and dead-letter copies are always confirmed) |
| PublisherChannelPoolSize | int | 2 | Publisher channels (1..64), used round-robin |
| MaxOutstandingConfirms | int | 1000 | Unconfirmed publishes per channel; further publishes wait (backpressure) |
| ConfirmTimeout | TimeSpan | 30 seconds | Longest wait for capacity plus the confirm (> 0) |
| ShutdownTimeout | TimeSpan | 30 seconds | Longest wait for running handlers on unsubscribe or dispose |
| HeartbeatInterval / NetworkRecoveryInterval | TimeSpan | 60 s / 10 s | Connection settings |

#### RabbitMQ behavior (RabbitMQ.Client 7; ADRs 0006–0008)

- **Lazy connection.** Constructing the queue (or resolving it from DI) does not connect. The first publish,
  subscribe or `InitializeAsync()` connects and declares the exchange and queues. A failure throws
  `MessageQueueException` (without credentials) and the next call tries again. To fail fast, use
  `await queue.InitializeAsync()` or `await RabbitMqMessageQueue.CreateAsync<T>(config)`.
- **Broker-side backoff.** A retry is published, with a confirm, to `{QueueName}.retry.{ms}`, a durable queue with
  that TTL that dead-letters back to the main queue; then the original is acked. Buckets follow the policy's
  schedule, with at most three per step for jitter (14 for the default policy, 5 without jitter); custom calculators
  are rounded to two significant digits. Retry queues are created on first use; classic ones expire when unused for
  max(1 h, 4 x TTL), quorum ones never do (clean them up with an operator policy if needed). A deleted retry or dead-letter queue is declared again; persistent broker failures requeue the
  delivery after a growing backoff.
- **Dead letters** are published to the dead-letter queue with `x-ese-dead-letter-reason` (a `DeadLetterReasons`
  code) and, for exceptions, `x-ese-dead-letter-detail` (the exception type). Then the original is acked.
- **Attempts** come from the library's `x-delivery-attempt` header (honoured only on messages coming back from the
  queue's retry queues) plus broker redeliveries: the redelivered flag adds 1, and on quorum queues
  `x-delivery-count` adds every crash. A process that crashes mid-handler uses up an attempt.
- **Headers**: `MessageContext.Headers` holds application headers only; `x-*` and trace headers are stripped.
- **Unsubscribe/dispose** cancels the handlers' token and requeues their deliveries. Prefetched deliveries that
  were never handled go back without using up an attempt. The call returns once nothing is in flight, or after
  `ShutdownTimeout`; handlers still running after that can settle normally, and the channel closes after them.
  `DisposeAsync` is bounded by `ShutdownTimeout` (plus 1 s for the close).
  Calling it from inside a handler returns immediately. You can subscribe again afterwards.
- **Malformed deliveries** (for example an out-of-range timestamp) are handled or dead-lettered as `DeliveryFailed`;
  they never block the consumer.
- **Batches** (`PublishAsync(IEnumerable<T>)`) publish everything, then await the confirms together. Order is not
  guaranteed.

### Azure Service Bus settings (`AzureServiceBusConfig`)

| Property | Default | Description |
|---|---|---|
| FullyQualifiedNamespace | (one of the two) | Namespace host name for Entra ID identity (production default; `DefaultAzureCredential` unless a `TokenCredential` is supplied or registered) |
| ConnectionString | (one of the two) | Shared access connection string (local development, emulator) |
| TopicName / SubscriptionName | null | Publish to a topic and consume from a subscription; `SubscriptionName` is required when `TopicName` is set |
| MessageTimeToLive | 14 days | Message TTL |
| PrefetchCount | 10 | Receiver prefetch |
| MaxAutoLockRenewalDuration | 5 minutes | Lock renewal while a handler runs (also a subscription retry's wait budget) |
| EnableBatchedOperations / MaxBatchSize | true / 100 | Batch publishing, split by byte size and at most `MaxBatchSize` messages per batch |
| SendTimeout | 30 s | Bounds every send and settlement |
| ShutdownTimeout | 30 s | How long unsubscribe and dispose wait for running handlers |
| TransactionalRetries | true | Queue retries send the copy and complete the original in one transaction |

Identity needs the **Azure Service Bus Data Sender** and **Data Receiver** roles, never Owner or Manage. Entity settings
(partitioning, duplicate detection, max delivery count) belong to the queue's infrastructure definition, not to this
library. On a topic subscription keep the entity's `MaxDeliveryCount` above `Retry.MaxRetries + 1`. Details:
`azure-service-bus-api.md`.

## Error Handling

### Exception Types

- `MessageQueueException`: publishing, subscribing or connection failures (RabbitMQ: on first use, see above)
- `ConfigurationException`: a missing or unsupported `Provider`; RabbitMQ connection settings that break the TLS rules
  when the queue connects
- `OptionsValidationException` (Microsoft.Extensions.Options): invalid settings, raised at host start

### Retry Logic and Dead-lettering

Every provider runs deliveries through the same pipeline:

1. Body larger than `MaxMessageSizeBytes` → dead-letter `MessageTooLarge`.
2. Body cannot be deserialized → dead-letter `DeserializationFailed`.
3. Handler returns `Acknowledge` → removed. `DeadLetter` → dead-letter `Rejected`.
4. Handler returns `Retry` or throws → redelivered as the next `DeliveryAttempt` after the backoff delay, until
   `MaxRetries` retries have been made; then dead-letter `MaxRetriesExceeded`.

Dead-letter records carry the reason code and, for exceptions, the exception **type** only; exception messages are
never written to the broker. Reason codes are listed in `DeadLetterReasons`.

## Threading and Concurrency

- Publishing is thread-safe; RabbitMQ uses a publisher channel pool, ASB one thread-safe sender per queue.
- One subscriber per queue instance. Every provider runs up to `MaxConcurrentCalls` handlers at once.
- Handlers must be idempotent: delivery is at least once. Use `MessageContext.MessageId` to de-duplicate.

## Observability

- `services.AddMessageQueueTelemetry("MyApp.Messaging")` registers an `ActivitySource`/`Meter`-based provider.
  Consume spans and the `messagequeue.messages.consumed|retried|deadlettered` counters come from the pipeline.
  Spans record `exception.type`; pass `recordExceptionDetails: true` to also record messages and stack traces.
- Logging uses `Microsoft.Extensions.Logging` with source-generated `LoggerMessage` methods.
- Health checks: `AddRabbitMqHealthCheck<T>()` (reuses the registered RabbitMQ queue's connection),
  `AddRabbitMqHealthCheck(connectionString)` (opens a connection per probe), `AddAzureServiceBusHealthCheck<T>()`
  (reuses the registered queue's client; a peek, Listen rights only), `AddAzureServiceBusHealthCheck(connectionString,
  queueName)` (one client for every probe), the opt-in `AddAzureServiceBusMetricsHealthCheck` (needs Manage), and
  `AddMessageQueueHealthCheck`. No check puts exception text, host, port or namespace in the result.
