# Failure modes

What happens when things go wrong, per provider, and what you see when they do. Event numbers refer to the
[log events](monitoring.md#log-events); procedures are in the [runbooks](runbooks.md). Design records:
[ADR 0001](../adr/0001-retry-semantics-and-default-policy.md) (retry), [0004](../adr/0004-dispatcher-and-transport-seam.md)
(shared pipeline), [0006](../adr/0006-rabbitmq-broker-side-backoff.md)-[0008](../adr/0008-rabbitmq-tls-and-consumer-shutdown.md)
(RabbitMQ), [0009](../adr/0009-azure-service-bus-backoff-and-attempt-counting.md)-[0012](../adr/0012-azure-service-bus-single-sender-and-batching.md)
(Service Bus).

## Guarantees

| | In-memory | RabbitMQ | Azure Service Bus queue | Service Bus topic subscription |
|---|---|---|---|---|
| Durable across a restart of your process | no | yes (persistent messages, durable quorum queues) | yes | yes |
| Delivery | at-least-once within the process | at-least-once | at-least-once | at-least-once |
| Order | not guaranteed with `MaxConcurrentCalls > 1` or after a retry | not guaranteed | not guaranteed | not guaranteed |
| Retry waits without holding a handler slot (`Capabilities.DelayedRedelivery`) | yes (timer) | yes (retry queue TTL) | yes (scheduled copy) | no: waits under the lock, or redelivers at once |
| Dead letters kept for inspection and replay | in process memory | `{QueueName}.deadletter` | the entity's dead-letter subqueue | the subscription's dead-letter subqueue (peek and purge; no replay) |

**At-least-once means duplicates.** A message is delivered again, after your handler may already have done its work,
when:

- the process crashes or is killed after the handler's side effects but before the acknowledgement;
- the connection (RabbitMQ) or the message lock (Service Bus) is lost while the handler runs;
- a retry copy was stored but its confirmation timed out (RabbitMQ `ConfirmTimeout`), or the entity refused the retry
  transaction and the copy was sent before the original was completed (Service Bus, after warning 3014);
- a dead-letter replay sent the message but failed before removing the dead letter;
- your publisher retried a `PublishAsync` whose first attempt actually reached the broker.

### Make handlers idempotent

`MessageContext.MessageId` is the idempotency key: it is the publisher's `PublishOptions.MessageId` (or a generated
version 7 GUID) and it stays the same on every retry, redelivery and dead-letter replay, on every provider (on Service
Bus retry and replay copies get a new broker id, but `MessageContext.MessageId` keeps the original).

- Record the ids you have processed in the same transaction as the work (a unique key on a `processed_messages` table,
  or an upsert keyed by the id), and acknowledge a message whose id is already recorded without doing the work again.
  The [processor in the guides](../getting-started/in-memory.md#3-a-processor) shows the shape.
- Set `PublishOptions.MessageId` from a business key (`order.OrderId`) when the publisher itself may retry: two
  `PublishAsync` calls without an id produce two different ids, and the consumer cannot tell they are the same event.
- Prefer naturally idempotent operations (set a status, upsert) over increments and appends.
- Keep the record of processed ids for longer than a message can live in the queue, the retry path and the dead-letter
  queue (Service Bus `MessageTimeToLive` defaults to 14 days).

## The delivery pipeline

Every provider runs every delivery through the same steps (`MessageDispatcher<T>`):

| Step | Outcome | Dead-letter reason | Counted in |
|---|---|---|---|
| Body larger than `MaxMessageSizeBytes` | dead-lettered at once, not deserialized | `MessageTooLarge` | `messages.dead_lettered`, event 1003 |
| Body does not deserialize (or deserializes to null) | dead-lettered at once, no retries | `DeserializationFailed` | event 1003 (detail: exception type) |
| Delivery metadata unreadable | dead-lettered at once | `DeliveryFailed` | event 1007 (Error) |
| Handler returns `Acknowledge` | removed | | `consumed.messages` without `error.type` |
| Handler returns `DeadLetter` | dead-lettered at once | `Rejected` | event 1003 |
| Handler returns `Retry` or throws | retried after the backoff, as attempt + 1 | | `messages.retried`; a throw also logs 1001 |
| ... once `Retry:MaxRetries` retries have failed | dead-lettered | `MaxRetriesExceeded` (detail: the last exception type) | event 1003 |
| Handler throws an exception `Retry:IsRetryable` rejects | dead-lettered at once | `NonRetryableError` | event 1003 |

With `EnableDeadLetterQueue = false`, every "dead-lettered" above becomes "acknowledged and dropped" (event 1004).
Dead-letter records hold the reason code and an exception **type**, never an exception message.

## Startup

| Situation | What happens |
|---|---|
| Invalid or incomplete configuration | Registration (`ConfigurationException`) or host start (`OptionsValidationException`, `InvalidOperationException`) fails; the process exits. See the [configuration reference](../configuration.md#startup-errors) |
| Broker unreachable or credentials refused, RabbitMQ | The hosted service subscribes during host start; connecting fails with `MessageQueueException` (event 2003) and **the host does not start**. A publish-only application learns it on the first `PublishAsync`. After a failure, connection attempts in the same process back off (1, 2, 4 ... up to 30 s); a call during the pause fails at once with `RabbitMQ for queue '...' was unreachable N time(s) in a row; the next connection retry is in N s` |
| Topology conflict, RabbitMQ | An existing queue or exchange with different arguments (another `UseQuorumQueues`, `QuorumInitialGroupSize`, dead-letter settings or `ExchangeType`) is refused by the broker: `PRECONDITION_FAILED - inequivalent arg ...` in event 2003, and the host does not start. See [rolling out configuration changes](runbooks.md#rolling-out-configuration-changes) |
| Missing permission, RabbitMQ | `ACCESS_REFUSED - ... refused for user ...` in event 2003; the host does not start |
| Namespace unreachable, identity without a role, entity missing, Service Bus | Creating the client does not connect, and starting the processor does not wait for a connection. The SDK processor reports the failures through its error handler: event 3009 (Error, throttled), metric `ese.messagequeue.transport.errors` with `error.type` (`ServiceUnavailable`, `MessagingEntityNotFound`, ... or `UnauthorizedAccessException`), and the health check is Unhealthy. The SDK keeps retrying, so the application recovers on its own once the cause is fixed. (This follows the SDK's processor model and the provider's unit specs; it has not yet been observed against a real namespace.) A publish fails with `MessageQueueException` (`Failed to publish to '...' (UnauthorizedAccessException)`, or `Sending to '...' timed out after ...` past `SendTimeout`) |
| No `IMessageProcessor<T>` registered | The hosted service logs event 5003 and consumes nothing |

## Broker unavailable while running

**RabbitMQ.** The client recovers the connection automatically every `NetworkRecoveryInterval` (10 s) and redeclares
the topology and the consumer. Meanwhile:

- deliveries in flight cannot be acknowledged; the broker requeues every unacknowledged delivery when the channel
  drops, and they are redelivered after recovery as the next attempt (the handler may run twice: idempotency);
- a failed settlement backs off per message (100 ms doubling to 30 s) before requeueing, logged as event 2001
  (throttled);
- `PublishAsync` throws `MessageQueueException` until the connection is back (or after `ConfirmTimeout` waiting for a
  confirm). The library does not buffer or retry publishes: retry in your code with the same `MessageId`, or use an
  outbox;
- the queue-bound health check is Unhealthy until the connection and consumer are back.

**Azure Service Bus.** The SDK retries its own operations (its default retry options) and the processor keeps
receiving once the service answers again. Settlements that fail back off per message and the message is abandoned
(event 3001), so it is redelivered with a higher `DeliveryCount`; when the lock expired meanwhile the broker redelivers
it on its own (warnings 3008/3010). Persistent processor errors appear as event 3009 and `transport.errors`. Publishes
fail with `MessageQueueException` after the SDK's retries, bounded by `SendTimeout`.

**In-memory** has no broker; it fails only with the process.

## Poison messages

A poison message is one that can never be processed. The pipeline stops it from cycling:

- **Unreadable or wrongly typed bodies** (`DeserializationFailed`, `DeliveryFailed`) and oversized bodies
  (`MessageTooLarge`) are dead-lettered on the first delivery, without retries.
- **A handler that keeps throwing** gets `Retry:MaxRetries + 1` deliveries (6 by default, about a minute), then the
  message is dead-lettered as `MaxRetriesExceeded`. Classify permanent errors early: return `MessageResult.DeadLetter`
  or configure `Retry:IsRetryable` (in code) so that, for example, a validation exception dead-letters at once as
  `NonRetryableError`.
- **A message that crashes the process** (stack overflow, out of memory, a native crash) never reaches the library's
  dead-letter step. The broker counts the deliveries instead:
  - RabbitMQ quorum queues count unsettled deliveries (`x-delivery-count`) and the library adds them to the attempt,
    so when a later delivery fails normally instead of crashing, the crashed deliveries have already used up their
    retries. If it crashes every time, the broker's quorum delivery limit (20 by default in RabbitMQ 4) dead-letters it
    into `{QueueName}.deadletter` with the broker's reason (`delivery_limit`). Classic queues have no such limit: a
    message that crashes every consumer is redelivered forever.
  - Service Bus dead-letters it after the entity's `MaxDeliveryCount` (`MaxDeliveryCountExceeded`).
  - In-memory loses it with the process.

Dead letters are inspected and replayed with `IDeadLetterQueue<T>` ([dead-letter operations](../dead-letter-operations.md),
[runbook](runbooks.md#dead-letter-backlog)).

## Oversized messages

`MaxMessageSizeBytes` (1 MiB by default) is enforced by the **consumer**: a larger body is dead-lettered as
`MessageTooLarge` without being deserialized. It is not checked on publish; what limits a publish is the broker:

- **RabbitMQ:** the broker's `max_message_size` (16 MiB by default in RabbitMQ 4) closes the publishing channel and
  `PublishAsync` throws `MessageQueueException`. The consumer's client accepts bodies up to the larger of
  `MaxMessageSizeBytes` and 16 MiB; if you raise the broker's `max_message_size` above that, raise
  `MaxMessageSizeBytes` with it, or a larger body closes the consumer's connection on every delivery.
- **Azure Service Bus:** the entity's maximum message size (256 KB on Standard, up to 100 MB on Premium) fails the send
  with `MessageQueueException`. A batch publish fails at the first message that does not fit an empty batch: `The
  message at index N is too large for an Azure Service Bus batch on '...' (at most N bytes); the messages before it
  were sent`.

To process a dead-lettered `MessageTooLarge` message, raise `MaxMessageSizeBytes`, redeploy, and replay naming the
reason explicitly (`ReasonCodes = [DeadLetterReasons.MessageTooLarge]`; it is excluded from a replay by default).

## Handler exceptions and slow handlers

- An exception from your processor counts as `MessageResult.Retry`. It is logged as event 1001 with the exception, set
  on the consume span (type only by default) and counted in `consumed.messages` with `error.type` = the exception type.
  The scope created for the message is disposed either way.
- **The library has no handler timeout.** The token a handler receives is cancelled only by shutdown. Bound your own
  work (`CancellationTokenSource.CancelAfter`, `HttpClient.Timeout`, database command timeouts) and let the timeout
  exception become a retry.
- A slow handler holds one of `MaxConcurrentCalls` slots. On RabbitMQ it also holds a prefetched delivery; the broker's
  `consumer_timeout` (30 minutes by default) closes the channel if a delivery stays unacknowledged that long, and the
  message is redelivered.
- **Service Bus lock loss:** the processor renews the message lock for up to `MaxAutoLockRenewalDuration` (5 minutes).
  A handler that runs longer loses the lock: the message becomes visible again and is redelivered, possibly to another
  instance **while the first handler still runs**, and the first handler's settlement fails (warning 3008). Keep
  handlers well under `MaxAutoLockRenewalDuration`, or raise it.

## Shutdown with handlers in flight

On host shutdown `MessageQueueHostedService<T>` unsubscribes: no new deliveries start, every running handler's
`CancellationToken` is cancelled, and the queue waits up to the provider's `ShutdownTimeout` (30 s) for them.

| | A handler that stops because of the token | Prefetched deliveries never handled | A handler still running after `ShutdownTimeout` |
|---|---|---|---|
| RabbitMQ | its delivery is requeued (event 2002); the redelivery counts as an attempt | returned without using an attempt | warning 2004; its channel stays open so it can still settle normally. When the queue is disposed, the connection is closed regardless after `ShutdownTimeout` (+1 s): its delivery is requeued and its late result lost |
| Service Bus | abandoned without using an attempt (event 3002) | abandoned without using an attempt | warning 3004; the processor is disposed when it finishes |
| In-memory | put back in the queue | stay queued | warning 4003; queued messages are lost with the process |

Configure the host to wait longer than the queue does: `HostOptions.ShutdownTimeout` defaults to 30 s, the same as the
providers' `ShutdownTimeout`, so set it higher (the guides use 45 s) and give your orchestrator's termination grace
period (Kubernetes `terminationGracePeriodSeconds`) more again. A process killed before the drain ends behaves like a
crash: unacknowledged messages are redelivered.

## Configuration that cannot do what it says

- A Service Bus topic subscription cannot delay retries: warning 1101 at startup, and retries wait under the lock or
  run at once. `RequireDelayedRedelivery = true` turns this into a startup failure.
- RabbitMQ `MaxConcurrentCalls` above `PrefetchCount`: only `PrefetchCount` handlers run (warning 2010).
- Service Bus subscription whose policy allows more deliveries than `MaxDeliveryCount`: the broker dead-letters first
  (`MaxDeliveryCountExceeded`; warning 3012 at startup for the default of 10).
