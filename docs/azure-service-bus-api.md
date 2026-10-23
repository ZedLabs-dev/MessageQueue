# Azure Service Bus Provider

`AzureServiceBusMessageQueue<T>` implements `IMessageQueue<T>` on Azure Service Bus: a queue, or a topic with one
subscription. It runs every delivery through the shared consumer pipeline (`MessageDispatcher<T>`, ADR 0004), so
retry counting, backoff decisions and dead-lettering behave as on every other provider.

**Namespace:** `ESE.MessageQueue.AzureServiceBus` (package `ESE.MessageQueue.AzureServiceBus`, .NET 10)
**SDK:** Azure.Messaging.ServiceBus 7.21, Azure.Identity 1.21
**Design records:** ADR 0009 (backoff and attempts), 0010 (lifecycle and settlement), 0011 (identity, health,
secrets), 0012 (one sender, batching)

Step-by-step setup: [getting started with Azure Service Bus](getting-started/azure-service-bus.md). Settings:
[configuration reference](configuration.md#azure-service-bus-azureservicebusconfig). Operations:
[runbooks](operations/runbooks.md#azure-service-bus).

## Usage

### Registration

```csharp
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using ESE.MessageQueue.HealthChecks;

services.AddAzureServiceBusProvider();                                     // package ESE.MessageQueue.AzureServiceBus
services.AddMessageQueue<OrderCreated>(configuration);                     // binds the "MessageQueue" section
services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();       // scoped: one instance per message
services.AddHostedService<MessageQueueHostedService<OrderCreated>>();      // consumes for the host's lifetime
services.AddHealthChecks().AddAzureServiceBusHealthCheck<OrderCreated>();  // reuses the queue's client
```

### Identity (production default)

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "contoso.servicebus.windows.net",
    "QueueName": "order-created",
    "Retry": { "MaxRetries": 5, "InitialDelay": "00:00:02", "MaxDelay": "00:00:30" }
  }
}
```

The queue authenticates with Entra ID. The credential is, in order: one passed to the constructor
(`new AzureServiceBusMessageQueue<T>(config, credential)`), a `TokenCredential` registered in the container
(`services.AddSingleton<TokenCredential>(new ManagedIdentityCredential(...))`), else one `DefaultAzureCredential`
shared by the process. Passing a credential together with a `ConnectionString` configuration throws
`ConfigurationException`.

Grant the application's identity, on the queue or topic/subscription (never Owner or Manage):

| Role | Needed for |
|---|---|
| Azure Service Bus Data Sender | publishing; queue-mode retries (they send a scheduled copy) |
| Azure Service Bus Data Receiver | consuming; the default health check (peek) |

### Connection string (local development, emulator)

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "ConnectionString": "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
    "QueueName": "order-created",
    "TransactionalRetries": false
  }
}
```

Set exactly one of `ConnectionString` and `FullyQualifiedNamespace`. A connection string must have `Endpoint` and
either `SharedAccessKeyName` + `SharedAccessKey` or `SharedAccessSignature`. Invalid settings fail at host start with
a fixed message; no error or log line ever contains the key.

### Topic and subscription

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "contoso.servicebus.windows.net",
    "QueueName": "billing-orders",
    "TopicName": "order-events",
    "SubscriptionName": "billing"
  }
}
```

The queue publishes to `TopicName` and consumes from `SubscriptionName` (required with a topic). `QueueName` then only
names the queue in logs, metrics and telemetry.

### Processing

```csharp
public sealed class OrderCreatedProcessor : IMessageProcessor<OrderCreated>
{
    public async Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context, CancellationToken cancellationToken)
    {
        // context.MessageId stays the publisher's id across retries; context.DeliveryAttempt is 1, 2, 3, ...
        try
        {
            await Handle(message, cancellationToken);
            return MessageResult.Acknowledge;
        }
        catch (TransientException)
        {
            return MessageResult.Retry;        // or just throw: an exception counts as Retry
        }
        catch (InvalidOrderException)
        {
            return MessageResult.DeadLetter;   // permanent: dead-letter now (reason Rejected)
        }
    }
}
```

The handler's token is cancelled on shutdown; a handler that stops because of it has its message abandoned for
redelivery, without using up an attempt.

## Retries and dead letters

| Mode | How a retry waits | `Capabilities.DelayedRedelivery` |
|---|---|---|
| Queue | A copy is scheduled (`ScheduledEnqueueTime = now + delay`) and the original completed, in one transaction | true |
| Subscription | The message is held under its lock for the delay (if it fits the lock-renewal budget), then abandoned | false |

- **Queue mode**: the copy has a new `MessageId` (so duplicate detection cannot drop it) and carries the original id in
  `x-ese-message-id`; handlers still see the original `MessageContext.MessageId`. With `TransactionalRetries`
  (default true) the send and the complete are atomic. Partitioned entities, the Basic tier and the emulator refuse
  the transaction: on the first refusal the queue switches to sending the copy first, then completing (the worst case
  is a duplicate, never a loss), and logs one warning. `TransactionalRetries = false` skips the attempt. Each commit
  blocks a thread-pool thread for a round trip; a startup warning flags `MaxConcurrentCalls` above 100.
- **Subscription mode**: a copy sent to the topic would reach every subscription, so none is sent. Delays longer than
  the remaining lock budget (`MaxAutoLockRenewalDuration` since receipt, or the current lock, minus 2 s) are abandoned
  at once with a warning. A waiting retry occupies a handler slot. A startup warning says the backoff cannot be
  honoured on the broker, and another one appears when `MaxRetries + 1` exceeds Service Bus's default
  `MaxDeliveryCount` (10): the entity's own value cannot be read without Manage rights, so keep it above
  `MaxRetries + 1`, or the broker dead-letters first (reason `MaxDeliveryCountExceeded`). With `PrefetchCount > 0`
  a startup warning notes that prefetched messages' locks run down while retries hold handler slots; consider
  `PrefetchCount = 0` on subscriptions with long backoffs.
- **Attempts**: `MessageContext.DeliveryAttempt` comes from the library's `x-ese-delivery-attempt` and
  `x-ese-delivery-count` properties when they agree with the broker's `DeliveryCount`, else from `DeliveryCount`.
  Publishers cannot set `x-` properties through this library. Dead letters set the marker out of reach, so a message
  resubmitted from the dead-letter queue starts again at attempt 1.
- **Dead letters** go to the entity's `$deadletterqueue` with `DeadLetterReason` = a `DeadLetterReasons` code,
  `DeadLetterErrorDescription` = the exception type (or empty), and the same in the application properties
  `x-ese-dead-letter-reason` / `x-ese-dead-letter-detail`. Exception messages are never stored.
- **Poison messages**: a message whose body or metadata cannot be read is dead-lettered as `DeliveryFailed`. A
  settlement that fails is retried after a per-message backoff (100 ms, doubling, at most 30 s) by abandoning the
  message; abandons count towards `MaxDeliveryCount`, which ends a persistent failure in the dead-letter queue.

- **Dead-letter operations** (`IDeadLetterQueue<T>`, ADR 0023): `PeekAsync` browses the dead-letter subqueue without
  locking (Listen); `PurgeAsync` completes selected dead letters (Listen); `ReplayAsync` (queues only, Send and Listen)
  sends each as a copy with a new broker id and the original id in `x-ese-message-id` (so `MessageContext.MessageId` is
  unchanged and duplicate detection cannot drop it), attempt 1, then completes the dead letter, in one transaction while
  `TransactionalRetries` is on and the entity accepts it. On a topic subscription replay throws
  `NotSupportedException`: a resend to the topic would reach every subscription. Dead letters also record
  `x-ese-dead-lettered-at` and `x-ese-dead-letter-attempt`. See [dead-letter-operations.md](dead-letter-operations.md).

## Configuration (`AzureServiceBusConfig`)

| Property | Default | Description |
|---|---|---|
| FullyQualifiedNamespace | (one of the two) | Namespace host name, used with Entra ID identity |
| ConnectionString | (one of the two) | Shared access connection string (redacted from `ToString()`) |
| TopicName / SubscriptionName | null | Publish to a topic and consume from one subscription |
| MessageTimeToLive | 14 days | TTL of published messages |
| PrefetchCount | 10 | Receiver prefetch |
| MaxAutoLockRenewalDuration | 5 minutes | Lock renewal while a handler runs; also the subscription-mode retry budget |
| EnableBatchedOperations / MaxBatchSize | true / 100 | Batch publishing; a batch closes when full by bytes or at `MaxBatchSize` messages |
| SendTimeout | 30 s | Bounds every send, batch and settlement |
| ShutdownTimeout | 25 s | How long unsubscribe and dispose wait for running handlers |
| TransactionalRetries | true | Send the retry copy and complete the original in one transaction (queue mode) |

Base settings (`QueueName`, `Retry`, `SerializationFormat`, `EnableDeadLetterQueue`, `MaxMessageSizeBytes`,
`MaxConcurrentCalls`) are described in `message-queue-api.md`. Entity settings (partitioning, duplicate detection,
`MaxDeliveryCount`, lock duration) belong to the infrastructure definition, not to this library.

## Publishing

- One thread-safe `ServiceBusSender` per queue on the client's single connection; there is no sender pool.
- `PublishAsync(IEnumerable<T>)` builds batches with `CreateMessageBatchAsync` + `TryAddMessage`. A message too large
  for an empty batch throws `MessageQueueException` naming its index; batches before it stay sent.
- Each message gets `MessageId` (`PublishOptions.MessageId` or a version 7 GUID), `CorrelationId`, `ContentType`,
  `Subject` = the message type name, `TimeToLive`, and the headers as application properties.
- Failures throw `MessageQueueException` naming the exception type; a send longer than `SendTimeout` throws one that
  says it timed out; the caller's cancellation throws `OperationCanceledException`.

## Lifecycle

- The client connects on first use. Constructing the queue validates the configuration only.
- One subscriber at a time; a second `SubscribeAsync` throws `InvalidOperationException`. Each subscription gets its
  own processor, so unsubscribe → subscribe works.
- `UnsubscribeAsync` cancels the handlers' token and waits for them at most `ShutdownTimeout`; handlers still
  running afterwards may settle normally, and their processor is disposed once they finish. Messages that arrive
  after shutdown began are handed back untouched.
- `DisposeAsync` is idempotent, gives all handlers one `ShutdownTimeout`, then closes the client (at most 1 s
  more). `Dispose()` runs `DisposeAsync` on the thread pool; prefer `DisposeAsync`.
- Calling `UnsubscribeAsync` or `DisposeAsync` from inside a handler returns at once; the rest finishes in the
  background after that handler.
- Processor errors are logged with their error source, entity path and exception type, throttled per distinct error.
- A handler's cancellation callback that throws during shutdown is logged; the shutdown still completes.

## Health checks

| Registration | Rights | Behaviour |
|---|---|---|
| `AddAzureServiceBusHealthCheck<T>()` | Listen | Reuses the queue's client: Unhealthy while its processor is not running, else peeks the consumed entity |
| `AddAzureServiceBusHealthCheck(connectionString, queueName, subscriptionName: null)` or `(fullyQualifiedNamespace, credential, queueName, ...)` | Listen | Peeks through one client and receiver kept for every probe |
| `AddAzureServiceBusMetricsHealthCheck(connectionString, queueName)` or `(fullyQualifiedNamespace, credential, queueName)` | **Manage** | Opt-in: adds active, dead-letter and scheduled counts to the result data |

Results have fixed descriptions and carry no exception, exception text, host or namespace; a failed probe's exception
goes to the application's log instead (event 6001, category `ESE.MessageQueue.HealthChecks`).

## Troubleshooting

- **`UnauthorizedAccessException` in the processor error log**: the identity lacks Data Receiver (consume) or Data
  Sender (publish, queue-mode retries).
- **"The entity refused a transaction" warning**: the tier, emulator or a partitioned entity does not support the
  retry transaction; the queue already fell back to send-then-complete. Set `TransactionalRetries = false` to skip it.
- **Messages dead-lettered with `MaxDeliveryCountExceeded`** on a subscription: the entity's `MaxDeliveryCount` is
  below `MaxRetries + 1`.
- **Lock-lost warnings**: a handler ran longer than `MaxAutoLockRenewalDuration`; the message is redelivered.
