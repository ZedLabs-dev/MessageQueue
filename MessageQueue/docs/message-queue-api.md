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
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;...",
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
| RabbitMQ | yes | yes (`{QueueName}.deadletter`) | no: retries currently wait in process, then are republished |
| Azure Service Bus | yes | yes (`$deadletterqueue`) | no: retries are currently abandoned for immediate redelivery |
| In-memory | **no** | yes (`DeadLetters`) | yes |

**Interim:** the retry *count* behaves the same everywhere, but the backoff *delay* is honoured broker-side only by
the in-memory provider today. RabbitMQ and Azure Service Bus log a startup warning when the retry policy has a
backoff; broker-side delayed redelivery arrives with their provider rework.

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

- `MessageId` (string): broker message id
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
`AzureServiceBusMessageQueue<T>(AzureServiceBusConfig, ITelemetryProvider? = null, ILogger? = null)` and
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
| MaxConcurrentCalls | int | 10 | Handlers running at once. Honoured by ASB and in-memory; RabbitMQ honours it after its provider rework (until then prefetch bounds concurrency) |

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
| ConnectionString | string | (required) | `amqp://user:password@host:port/vhost` (`amqps://` for TLS) |
| DeadLetterQueueName | string | `{QueueName}.deadletter` | Dead-letter queue |
| UseQuorumQueues | bool | true | Quorum queues for replication |
| QuorumInitialGroupSize | int | 3 | Initial quorum group size |
| PrefetchCount | int | 10 | Unacknowledged deliveries per consumer (1..65535) |
| ExchangeName | string | QueueName | Exchange to publish to |
| ExchangeType | string | "direct" | `direct`, `fanout`, `topic` or `headers` |
| EnablePublisherConfirms | bool | true | Wait for broker confirms |
| PublisherChannelPoolSize | int | 2 | Publisher channels (1..64) |
| MaxOutstandingConfirms | int | 1000 | Pending confirms per channel (backpressure) |
| ConfirmTimeout | TimeSpan | 30 seconds | Confirm timeout (> 0) |
| HeartbeatInterval / NetworkRecoveryInterval | TimeSpan | 60 s / 10 s | Connection settings |

### Azure Service Bus settings (`AzureServiceBusConfig`)

| Property | Default | Description |
|---|---|---|
| ConnectionString | (required) | SAS connection string |
| TopicName / SubscriptionName | null | Publish to a topic and consume from a subscription; `SubscriptionName` is required when `TopicName` is set |
| MessageTimeToLive | 14 days | Message TTL |
| PrefetchCount | 10 | Receiver prefetch |
| MaxAutoLockRenewalDuration | 5 minutes | Lock renewal while a handler runs |
| EnableBatchedOperations / MaxBatchSize | true / 100 | Batch publishing |
| SenderPoolSize / MaxConcurrentSends / SendTimeout | 2 / 1000 / 30 s | Sender pool |

Entity settings (partitioning, duplicate detection, max delivery count) belong to the queue's infrastructure
definition, not to this library. Keep the entity's `MaxDeliveryCount` above `Retry.MaxRetries + 1`.

## Error Handling

### Exception Types

- `MessageQueueException`: publishing, subscribing or connection failures
- `ConfigurationException`: a missing or unsupported `Provider`
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

- Publishing is thread-safe; RabbitMQ and ASB use publisher pools.
- One subscriber per queue instance. ASB and in-memory run up to `MaxConcurrentCalls` handlers at once.
- Handlers must be idempotent: delivery is at least once. Use `MessageContext.MessageId` to de-duplicate.

## Observability

- `services.AddMessageQueueTelemetry("MyApp.Messaging")` registers an `ActivitySource`/`Meter`-based provider.
  Consume spans and the `messagequeue.messages.consumed|retried|deadlettered` counters come from the pipeline.
  Spans record `exception.type`; pass `recordExceptionDetails: true` to also record messages and stack traces.
- Logging uses `Microsoft.Extensions.Logging` with source-generated `LoggerMessage` methods.
- Health checks: `AddRabbitMqHealthCheck`, `AddAzureServiceBusHealthCheck`, `AddMessageQueueHealthCheck`.
