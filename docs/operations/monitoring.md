# Monitoring

What the library reports, so you can build dashboards and [alerts](alerts.md) on it: metrics, spans, log events and
health checks. Everything below is checked against the code by
`tests/ESE.MessageQueue.Tests/Packaging/Given_the_monitoring_reference` (every log event id and level, every metric and
unit, every attribute name). Design: [ADR 0013](../adr/0013-telemetry-trace-propagation-and-metrics.md) (telemetry),
[ADR 0021](../adr/0021-bounded-values-in-brokers-and-telemetry.md) (bounded values).

## Wiring

The library emits through `System.Diagnostics` only (`ActivitySource` and `Meter`); it references no OpenTelemetry
package or exporter. `services.AddMessageQueueTelemetry("MyApp.Messaging")` registers the `OpenTelemetryProvider`,
whose source and meter both carry that name; your OpenTelemetry pipeline subscribes with
`AddSource("MyApp.Messaging")` and `AddMeter("MyApp.Messaging")` (see the telemetry step of any
[getting-started guide](../getting-started/in-memory.md#telemetry)). Without `AddMessageQueueTelemetry` there are no
spans, no metrics and no trace propagation; logs are always written. A custom backend implements `ITelemetryProvider`
and is registered with `services.AddMessageQueueTelemetry<TProvider>()`.

Telemetry never changes what happens to a message: a throwing provider is logged (event 1005 or 1009) and the publish
or delivery proceeds. Nothing is formatted or tagged while nobody listens.

## Metrics

| Metric | Type | Unit | Recorded | Meaning |
|---|---|---|---|---|
| `messaging.client.sent.messages` | counter | `{message}` | per publish call, by the number of messages | Messages published. `error.type` = the exception type when the publish failed |
| `messaging.client.operation.duration` | histogram | `s` | per publish call | Duration of a publish call (a batch is one call), including the broker confirm |
| `messaging.client.consumed.messages` | counter | `{message}` | per delivery | Deliveries handed to the pipeline. Unless acknowledged, `error.type` is the outcome: a dead-letter reason code, `Retry` (the handler asked for one) or the type of the exception the handler threw |
| `messaging.process.duration` | histogram | `s` | per delivery | Receipt to settlement: deserialization, your handler and the ack, retry copy or dead-letter |
| `ese.messagequeue.messages.retried` | counter | `{message}` | per retry scheduled | Deliveries rescheduled for another attempt |
| `ese.messagequeue.messages.dead_lettered` | counter | `{message}` | per dead letter | Deliveries dead-lettered, tagged `ese.messagequeue.dead_letter.reason`. Not counted when `EnableDeadLetterQueue = false` (the message is dropped: event 1004) |
| `ese.messagequeue.transport.errors` | counter | `{error}` | per transport error | Broker trouble outside a delivery. **Azure Service Bus:** errors the SDK processor reported (authentication, entity not found, network, throttling); `error.type` = the `ServiceBusFailureReason` or the exception type. **RabbitMQ:** `error.type` = `ConnectionFailed` (connecting or declaring the topology failed, event 2003; calls refused during the reconnect backoff are not counted), `ConnectionShutdown` (the broker closed the connection or it was lost), `RecoveryFailed` (one automatic reconnect attempt failed), `ChannelShutdown` (the consumer channel was closed while the connection stayed open, for example by `consumer_timeout`), `ConsumerCancelled` (the broker cancelled the consumer: its queue was deleted). Closes the library makes itself are not counted. Not recorded by the in-memory provider |
| `ese.messagequeue.dead_letters.replayed` | counter | `{message}` | per replay call | Dead letters moved back to their queue |
| `ese.messagequeue.dead_letters.purged` | counter | `{message}` | per purge call | Dead letters deleted |

Duration histograms use the OpenTelemetry messaging buckets: 5 ms, 10, 25, 50, 75, 100, 250, 500, 750 ms, 1 s, 2.5,
5, 7.5 and 10 s.

**Tags on every metric** (bounded: no message ids or other per-message values):

| Tag | Values |
|---|---|
| `messaging.system` | `rabbitmq`, `servicebus` or `ese.inmemory` |
| `messaging.destination.name` | in-memory and Service Bus queue: the queue; Service Bus topic mode: the topic; RabbitMQ: `{exchange}:{queue}` (`order-placed:order-placed` by default) |
| `messaging.operation.name` | publish: `publish` (`send` on Service Bus); consume metrics: `process`; dead-letter operations: `replay`, `purge` |
| `ese.messagequeue.message_type` | the message type's name (`OrderPlaced`) |
| `error.type` | when the operation did not succeed (above); cut to 4096 characters |
| `messaging.destination.subscription.name` | consumer metrics of a Service Bus topic subscription |
| `ese.messagequeue.dead_letter.reason` | `ese.messagequeue.messages.dead_lettered` only: `MessageTooLarge`, `DeserializationFailed`, `MaxRetriesExceeded`, `NonRetryableError`, `DeliveryFailed`, `Rejected` |

A delivery interrupted by shutdown is not counted (it is handed back and counted when it is delivered again).

## Spans

| Span | Kind | When |
|---|---|---|
| `publish {destination}` (`send {destination}` on Service Bus) | Producer | one per `PublishAsync` call; a batch is one span |
| `process {destination}` | Consumer | one per delivery, a child of the publish span that sent the message |

The publish span's W3C context travels in the message headers `traceparent` and `tracestate` (reserved: applications
can neither set nor read them). Every attempt of a retried message is a child of the same publish span. While your
handler runs, its consume span is `Activity.Current`, so your own spans nest under it. A message without valid trace
headers starts a new trace; on Service Bus the SDK's `Diagnostic-Id` is used when `traceparent` is absent.

**Attributes**

| Attribute | On | Value |
|---|---|---|
| `messaging.system` | both | as on metrics |
| `messaging.destination.name` | both | as on metrics |
| `messaging.operation.name` | both | `publish` / `send` / `process` |
| `messaging.operation.type` | both | `send` on publish, `process` on consume |
| `ese.messagequeue.message_type` | both | the message type's name |
| `messaging.rabbitmq.destination.routing_key` | both, RabbitMQ | the queue name |
| `messaging.message.id` | publish of one message; consume | the message id |
| `messaging.batch.message_count` | publish of a batch | number of messages |
| `messaging.message.conversation_id` | consume | the correlation id, when set |
| `messaging.destination.subscription.name` | consume, Service Bus subscription | the subscription |
| `ese.messagequeue.delivery_attempt` | consume | 1 for the first delivery, 2 for the first retry, ... |
| `ese.messagequeue.retry.next_attempt` | consume, when retried | the next attempt number |
| `ese.messagequeue.retry.delay_ms` | consume, when retried | the backoff before it, in milliseconds |
| `ese.messagequeue.dead_letter.reason` | consume, when dead-lettered | the reason code |
| `error.type` | both, unless the outcome was a success | as on metrics; the span status is then Error |

When the handler throws, the consume span also gets an `exception` event with `exception.type`. Exception messages and
stack traces are recorded only with `AddMessageQueueTelemetry(name, recordExceptionDetails: true)` (cut to 4096
characters): they can carry personal or secret data.

## Logs

The library logs through `Microsoft.Extensions.Logging` with source-generated messages and fixed event ids. Logger
categories start with `ESE.MessageQueue` (the queue classes, `ESE.MessageQueue.Hosting.MessageQueueHostedService` and
`ESE.MessageQueue.HealthChecks`), so one filter covers them:

```json
{ "Logging": { "LogLevel": { "ESE.MessageQueue": "Information" } } }
```

Publishing logs nothing at Information: a line per message would drown everything else, its outcome is in the publish
metrics and spans, and a failure throws to the caller. To trace publishes, raise the queue's category to Debug for a
while (events 1010 and 1011), for example `"ESE.MessageQueue.RabbitMQ": "Debug"`.

Messages never contain connection strings, passwords or exception messages from a broker client; exceptions are
attached to the log entry (except event 3003, where even the exception could carry the connection string). Repeating
transport errors (2001, 3001, 3008, 3009, 3010, 3011) are throttled to one entry per 10 seconds per kind, with the
count of suppressed entries in `{SuppressedLogs}`.

Ids: 1000s core pipeline and dead-letter operations, 2000s RabbitMQ, 3000s Azure Service Bus, 4000s in-memory, 5000s
hosted service, 6000s health checks.

### Log events

| EventId | Level | Name | Message template | Meaning and action |
|---|---|---|---|---|
| 1001 | Warning | HandlerFailed | Handler for queue {QueueName} failed on message {MessageId} (attempt {DeliveryAttempt}) with {ExceptionType} | The handler threw; the delivery is retried (or dead-lettered once retries are used up). The exception is attached. A burst means a failing dependency or a bug: see the high-retry runbook |
| 1002 | Debug | Rescheduled | Message {MessageId} on queue {QueueName} rescheduled as attempt {NextAttempt} after {Delay} | A retry was scheduled with its delay. Debug level: enable it to trace one message |
| 1003 | Warning | DeadLettered | Message {MessageId} on queue {QueueName} dead-lettered: {Reason} {Detail} | A message went to the dead-letter queue with its reason code and detail (exception type). Alert on the rate, not on single lines |
| 1004 | Warning | Dropped | Message {MessageId} on queue {QueueName} dropped because dead-lettering is disabled: {Reason} {Detail} | `EnableDeadLetterQueue = false`: a message that would be dead-lettered was acknowledged and is gone |
| 1005 | Warning | TelemetryFailed | Telemetry for queue {QueueName} failed; the message is processed regardless | The registered telemetry provider threw; the message was processed regardless. Fix the provider |
| 1006 | Warning | DelayCalculatorFailed | Retry delay calculator for queue {QueueName} failed; retrying after {FallbackDelay} instead | `Retry:CustomDelayCalculator` threw; the retry used the fallback delay. Fix the calculator |
| 1007 | Error | DeliveryUnreadable | Message {MessageId} on queue {QueueName} could not be read ({ExceptionType}); dead-lettering it as DeliveryFailed | A delivery's metadata or body could not be read (broker-side corruption, a foreign publisher); it is dead-lettered as `DeliveryFailed` |
| 1008 | Warning | CallbackFailed | Queue {QueueName}: a cancellation callback registered by a handler threw during shutdown; the shutdown continues | A cancellation callback registered by a handler threw during shutdown; the shutdown continued |
| 1009 | Warning | PublishTelemetryFailed | Publish telemetry for queue {QueueName} failed; the message is published regardless | Telemetry threw while publishing; the message was published regardless |
| 1010 | Debug | Published | Published message {MessageId} to {Destination} | A `PublishAsync` of one message reached the broker (confirmed, on RabbitMQ). Debug level: off by default, enable it to trace a message from its publisher. Never content or headers; control characters are removed from the id. A failed or refused publish throws to the caller instead |
| 1011 | Debug | PublishedBatch | Published a batch of {MessageCount} messages to {Destination} | A batch `PublishAsync` reached the broker. Debug level, like 1010 |
| 1101 | Warning | BackoffUnsupported | Queue {QueueName}: this provider or entity has no broker-side delayed redelivery (Capabilities.DelayedRedelivery = false), so the retry backoff ({InitialDelay} to {MaxDelay}) is applied in process or not at all | At startup: the provider or entity cannot delay a retry (a Service Bus subscription) but the policy has a backoff. Set `RequireDelayedRedelivery = true` to make this fatal |
| 1201 | Information | Replayed | Dead-letter replay on queue {QueueName}: {Replayed} replayed, {Failed} failed, {Skipped} skipped (dry run: {DryRun}) | Summary of a dead-letter replay (also for a dry run) |
| 1202 | Warning | Purged | Dead-letter purge on queue {QueueName}: {Purged} dead letter(s) deleted | Summary of a dead-letter purge: messages were permanently deleted |
| 1203 | Warning | ReplayFailed | Replaying dead letter {MessageId} on queue {QueueName} failed ({ExceptionType}); it stays in the dead-letter queue | One dead letter could not be replayed; it stays a dead letter and is listed in the result |
| 1204 | Warning | Cancelled | Dead-letter {Operation} on queue {QueueName} was cancelled after {Done} message(s); the rest stay in the dead-letter queue | A dead-letter operation was cancelled; work done so far stays done |
| 1205 | Warning | ReturnFailed | Queue {QueueName}: handing back the dead letters a pass held failed; the broker returns them when their lock or channel ends | Dead letters held by an operation could not be handed back; the broker returns them when the lock or channel ends |
| 1206 | Warning | Aborted | Dead-letter {Operation} on queue {QueueName} aborted after {Done} message(s) ({ExceptionType}); the rest stay in the dead-letter queue | A dead-letter operation ended early (lost connection, failing broker call); counts so far are logged and the call throws `MessageQueueException` |
| 2001 | Error | DeliveryFailed | Delivery from RabbitMQ queue {QueueName} could not be settled ({ConsecutiveFailures} failure(s) in a row, {SuppressedLogs} similar error(s) not logged); requeued after a backoff | RabbitMQ: a delivery could not be settled (ack, retry copy or dead-letter copy failed). It is requeued after a growing backoff; throttled to one line per 10 s. Look for broker trouble, a deleted queue or missing permissions |
| 2002 | Debug | DeliveryAbandoned | Delivery from RabbitMQ queue {QueueName} was interrupted by shutdown and requeued for redelivery | RabbitMQ: a handler was cancelled by shutdown; its delivery was requeued without using an attempt |
| 2003 | Error | ConnectFailed | Could not connect to RabbitMQ for queue {QueueName} | RabbitMQ: connecting (or declaring the topology) failed. The exception names the cause: unreachable host, `ACCESS_REFUSED`, `PRECONDITION_FAILED` (a queue argument changed), TLS. The next attempt backs off up to 30 s |
| 2004 | Warning | ShutdownTimedOut | RabbitMQ queue {QueueName}: handlers did not finish within {ShutdownTimeout}; their channel stays open until they do | RabbitMQ: handlers ignored cancellation for longer than `ShutdownTimeout`; their channel stays open until they finish |
| 2005 | Warning | RequeueFailed | RabbitMQ queue {QueueName}: could not requeue a delivery; the broker requeues it when the channel closes | RabbitMQ: a requeue failed; the broker requeues the delivery when the channel closes |
| 2006 | Warning | CancelFailed | RabbitMQ queue {QueueName}: cancelling the consumer failed | RabbitMQ: cancelling the consumer during shutdown failed (usually the channel was already closed) |
| 2007 | Debug | CloseFailed | RabbitMQ queue {QueueName}: closing a channel failed | RabbitMQ: closing a channel failed. Debug level; harmless during shutdown or after a connection loss |
| 2008 | Warning | TelemetryFailed | RabbitMQ queue {QueueName}: telemetry failed while counting a transport error; the error is handled regardless | RabbitMQ: the telemetry provider threw while counting a transport error |
| 2009 | Debug | UnhandledRequeueFailed | RabbitMQ queue {QueueName}: an unhandled delivery could not be returned without using an attempt; requeued instead | RabbitMQ: a prefetched delivery that was never handled could not be returned without using an attempt, so it was requeued (it then counts as a redelivery) |
| 2010 | Warning | ConcurrencyAbovePrefetch | RabbitMQ queue {QueueName}: MaxConcurrentCalls ({MaxConcurrentCalls}) is above PrefetchCount ({PrefetchCount}), so at most PrefetchCount handlers run at once | RabbitMQ, at startup: `MaxConcurrentCalls` is above `PrefetchCount`, so only `PrefetchCount` handlers can run. Raise `PrefetchCount` |
| 2011 | Warning | CloseAbandoned | RabbitMQ queue {QueueName}: the connection did not close in time (a handler is still running); it closes in the background | RabbitMQ: the connection did not close within a second of shutdown because a handler still runs; it closes in the background |
| 2012 | Warning | DeadLetterReturnFailed | RabbitMQ queue {QueueName}: handing back the dead letters a pass held failed; closing its channel returns them (on a quorum queue that counts as a failed delivery) | RabbitMQ: dead letters held by an operation could not be handed back; closing the channel returns them (a quorum queue counts that as a failed delivery) |
| 2013 | Warning | DeadLetterQueueWithoutDeliveryLimit | RabbitMQ queue {QueueName}: the quorum dead-letter queue {DeadLetterQueueName} exists without x-delivery-limit = -1 and cannot be redeclared with it; unless a policy sets delivery-limit: -1 for it, a dead letter returned by a lost channel more than 20 times is dropped (see docs/dead-letter-operations.md) | RabbitMQ, at startup: a quorum dead-letter queue created by an earlier version lacks `x-delivery-limit = -1`. Apply the `delivery-limit: -1` policy (runbook) |
| 3001 | Error | DeliveryFailed | Message from Azure Service Bus queue {QueueName} could not be settled ({ConsecutiveFailures} failure(s) in a row, {SuppressedLogs} similar error(s) not logged); abandoned after a backoff | Service Bus: a message could not be settled (complete, retry copy, dead-letter). It is abandoned after a growing backoff; throttled. Look for missing Data Sender rights, throttling or an outage |
| 3002 | Debug | DeliveryAbandoned | Message from Azure Service Bus queue {QueueName} was interrupted by shutdown and abandoned for redelivery | Service Bus: a handler was cancelled by shutdown; the message was abandoned without using an attempt |
| 3003 | Error | ClientCreationFailed | Azure Service Bus client for queue {QueueName} could not be created ({ExceptionType}); the exception is not logged because it can contain the connection string | Service Bus: the client could not be created (bad connection string or namespace). The exception is deliberately not logged: it can contain the connection string |
| 3004 | Warning | ShutdownTimedOut | Azure Service Bus queue {QueueName}: handlers did not finish within {ShutdownTimeout}; the processor is disposed once they do | Service Bus: handlers ignored cancellation for longer than `ShutdownTimeout`; the processor is disposed when they finish |
| 3005 | Warning | AbandonFailed | Azure Service Bus queue {QueueName}: abandoning a message failed; it is redelivered when its lock expires | Service Bus: abandoning a message failed; it is redelivered when its lock expires |
| 3006 | Warning | StopFailed | Azure Service Bus queue {QueueName}: stopping the processor failed | Service Bus: stopping the processor failed |
| 3007 | Debug | CloseFailed | Azure Service Bus queue {QueueName}: closing a client resource failed | Service Bus: closing a client resource failed. Debug level; harmless during shutdown |
| 3008 | Warning | LockLost | Azure Service Bus queue {QueueName}: a message lock was lost ({SuppressedLogs} similar warning(s) not logged); the broker redelivers the message | Service Bus: a message lock was lost (the handler outlived the lock or `MaxAutoLockRenewalDuration`); the broker redelivers it, and `DeliveryCount` rises. Throttled |
| 3009 | Error | ProcessorError | Azure Service Bus processor error on {EntityPath} from {ErrorSource} ({ExceptionType}; {SuppressedLogs} similar error(s) not logged) | Service Bus: the SDK processor reported an error (authentication, entity not found, network, throttling). The SDK keeps retrying; also counted in `ese.messagequeue.transport.errors`. Throttled per distinct error |
| 3010 | Warning | ProcessorWarning | Azure Service Bus processor warning on {EntityPath} from {ErrorSource} ({ExceptionType}; {SuppressedLogs} similar warning(s) not logged): the message is redelivered | Service Bus: a processor error about a lost lock; the message is redelivered |
| 3011 | Warning | DelayExceedsLock | Azure Service Bus queue {QueueName}: retry delay {Delay} exceeds the remaining lock budget {LockBudget} of a subscription message, so it is abandoned for immediate redelivery ({SuppressedLogs} similar warning(s) not logged) | Service Bus subscription: a retry delay exceeds the remaining lock budget, so the message is abandoned for immediate redelivery instead of waiting |
| 3012 | Warning | RetriesExceedMaxDeliveryCount | Azure Service Bus queue {QueueName}: the retry policy allows {Deliveries} deliveries, more than Service Bus's default MaxDeliveryCount ({MaxDeliveryCount}); make sure the subscription's MaxDeliveryCount is higher, or the broker dead-letters first | Service Bus subscription, at startup: the retry policy allows more deliveries than the default `MaxDeliveryCount` (10). Raise the subscription's `MaxDeliveryCount` |
| 3013 | Warning | CloseAbandoned | Azure Service Bus queue {QueueName}: the client did not close in time (a handler is still running); it closes in the background | Service Bus: the client did not close within a second of shutdown because a handler still runs; it closes in the background |
| 3014 | Warning | TransactionsRefused | Azure Service Bus queue {QueueName}: the entity refused a transaction ({ExceptionType}); retries now send the copy, then complete the original (at-least-once: a crash in between can duplicate). Partitioned entities, the Basic tier and the emulator do this; set TransactionalRetries = false to skip the attempt | Service Bus: the entity refused the retry transaction (partitioned entity, Basic tier, emulator). Retries switched to send-then-complete for this queue; set `TransactionalRetries = false` to skip the attempt |
| 3015 | Warning | SubscriptionPrefetches | Azure Service Bus queue {QueueName}: subscription with PrefetchCount {PrefetchCount}: prefetched messages' locks run down while retries wait under the lock in handler slots, so they can expire and be redelivered; consider PrefetchCount = 0 | Service Bus subscription, at startup: `PrefetchCount > 0`; prefetched locks can expire while retries wait. Consider `PrefetchCount = 0` |
| 3016 | Warning | TransactionalConcurrency | Azure Service Bus queue {QueueName}: TransactionalRetries with MaxConcurrentCalls {MaxConcurrentCalls} (above {Threshold}): each retry's transaction commit blocks a thread-pool thread for a broker round trip | Service Bus, at startup: `TransactionalRetries` with more than 100 concurrent handlers; each retry's commit blocks a thread-pool thread |
| 3017 | Warning | TelemetryFailed | Azure Service Bus queue {QueueName}: telemetry failed while counting a processor error; the error is handled regardless | Service Bus: the telemetry provider threw while counting a processor error |
| 3018 | Warning | DeadLetterAbandonFailed | Azure Service Bus queue {QueueName}: handing back a dead letter a pass held failed; it is returned when its lock expires | Service Bus: a dead letter held by an operation could not be abandoned; it is returned when its lock expires |
| 3019 | Warning | ReplayTransactionsRefused | Azure Service Bus queue {QueueName}: the entity refused a transaction for a dead-letter replay ({ExceptionType}); replays now send the copy, then complete the dead letter (at-least-once: a failure in between leaves a duplicate) | Service Bus: the entity refused a transaction for a dead-letter replay; replays switched to send-then-complete |
| 4001 | Error | DeliveryFailed | Delivery of message {MessageId} on in-memory queue {QueueName} failed before it was settled; the message was dead-lettered | In-memory: the pipeline failed before settling a delivery; the message was dead-lettered |
| 4002 | Error | AfterDeliveryFailed | Message {MessageId} on in-memory queue {QueueName} was settled, but a step after settling failed | In-memory: a step after settling a message failed |
| 4003 | Warning | ShutdownTimedOut | In-memory queue {QueueName}: handlers did not finish within {ShutdownTimeout}; stopped waiting for them | In-memory: handlers ignored cancellation for longer than `ShutdownTimeout` |
| 5001 | Information | Starting | Starting message queue hosted service for {MessageType} | The hosted service subscribed for this message type |
| 5002 | Information | Stopping | Stopping message queue hosted service for {MessageType} | The hosted service is unsubscribing (host shutdown) |
| 5003 | Warning | NoProcessor | No IMessageProcessor<{MessageType}> is registered; the hosted service will not consume messages | No `IMessageProcessor<T>` is registered, so this hosted service consumes nothing. A registration bug |
| 5004 | Warning | ScopeDisposalFailed | Disposing the DI scope for {MessageType} message {MessageId} failed; the processor's result is kept | Disposing the per-message DI scope failed; the processor's result stands |
| 5005 | Warning | ShutdownTimeoutNotBelowHost | The {MessageType} queue may wait up to its ShutdownTimeout ({ShutdownTimeout}) for running handlers, not less than the host's HostOptions.ShutdownTimeout ({HostShutdownTimeout}): the host can stop waiting first and lose their results. Lower {ShutdownTimeoutSetting} or raise HostOptions.ShutdownTimeout | At startup: the queue's drain can outlast the host's shutdown wait (never when the host waits forever: `ShutdownTimeout` zero or less), so a slow handler's result can be lost on shutdown. Keep the queue's `ShutdownTimeout` below the host's ([failure modes](failure-modes.md#shutdown-with-handlers-in-flight)) |
| 5006 | Debug | HostOptionsUnreadable | Could not read HostOptions.ShutdownTimeout for the {MessageType} queue; the shutdown budget is not checked | At startup: the host's options could not be read, so warning 5005 is not checked. Harmless |
| 6001 | Warning | Failed | Health check {HealthCheck} failed with {ExceptionType} | A health check probe failed; the exception (with its details) is here, never in the health result |

The levels to page on are Error (2001, 2003, 3001, 3003, 3009, 4001, 4002, 1007) and the dead-letter and retry rates
from metrics; see [alerts](alerts.md). Warning events at startup (1101, 2010, 2013, 3012, 3015, 3016, 5005) are
configuration advice: review them on every deployment.

## Health checks

| Registration | Package | Probes | Healthy / Unhealthy description |
|---|---|---|---|
| `AddRabbitMqHealthCheck<T>()` | RabbitMQ | the registered queue's connection (connects it if needed); while subscribed, also its consumer channel and consumer | `RabbitMQ connection is healthy.` / `RabbitMQ connection is not open.` / `RabbitMQ health check failed.` |
| `AddRabbitMqHealthCheck(connectionString, requireTls: null)` or `(connectionString, tls, requireTls: null)` | RabbitMQ | opens and closes a connection per probe (5 s connect timeout), same TLS rules as the queue; with a `RabbitMqTlsConfig` also its client certificate (mutual TLS), `CaCertificatePath`, server name and acceptable errors. One check instance per registration loads the certificates once and is disposed with the container; invalid TLS settings report Unhealthy and log event 6001 | as above |
| `AddAzureServiceBusHealthCheck<T>()` | Service Bus | the registered queue's client: Unhealthy while its processor is not running, else a peek (Listen only) | `Azure Service Bus entity is reachable.` / `Azure Service Bus processor is not running.` / `Azure Service Bus health check failed.` |
| `AddAzureServiceBusHealthCheck(connectionString, queueName, ...)` or `(fullyQualifiedNamespace, credential, queueName, ...)` | Service Bus | a peek through one client kept for every probe | as above |
| `AddAzureServiceBusMetricsHealthCheck(...)` | Service Bus | queue runtime properties; needs **Manage** rights (opt-in) | `Azure Service Bus queue metrics are available.` with data `activeMessageCount`, `deadLetterMessageCount`, `scheduledMessageCount` / `Azure Service Bus queue does not exist.` / `Azure Service Bus metrics health check failed.` |
| `AddMessageQueueHealthCheck(name, check)` | core | your own `Func<CancellationToken, Task<bool>>` | `Message queue '{name}' is accessible.` / `... is not accessible.` / `... health check failed.` |

Defaults: name `rabbitmq` / `azureservicebus` / `azureservicebus-metrics`, tags `messagequeue` plus `rabbitmq` or
`azureservicebus`, timeout 5 s (RabbitMQ) or 10 s (Service Bus). Results never contain a host, namespace, exception or
exception text (S5); a failed probe logs event 6001 with the exception.

Use the queue-bound checks (`<T>`) for readiness: they report the state of the connection or processor the
application actually uses. A failing readiness probe should stop traffic, not restart the process: RabbitMQ recovers
connections on its own and the Service Bus processor keeps retrying.

## Broker-side signals

The library cannot see its broker's queue depths. Watch them on the broker:

- **RabbitMQ:** messages ready and unacknowledged per queue, especially `*.deadletter`, and the consumer count of the
  main queue. With the `rabbitmq_prometheus` plugin (enabled in the official image) per-queue series need
  `/metrics/per-object` or `/metrics/detailed?family=queue_coarse_metrics`; the default `/metrics` endpoint aggregates
  all queues. [Alerts](alerts.md#broker-side-metrics) has the queries.
- **Azure Service Bus:** the Azure Monitor metrics `ActiveMessages`, `DeadletteredMessages`, `ScheduledMessages`,
  `ServerErrors`, `UserErrors` and `ThrottledRequests`, split by `EntityName`.
