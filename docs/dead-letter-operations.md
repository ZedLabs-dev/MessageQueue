# Dead-letter operations: peek, replay, purge

How to inspect, replay and delete dead letters with `IDeadLetterQueue<T>` (ADR 0023). The API reference is in
[message-queue-api.md](message-queue-api.md#ideadletterqueuet-dead-letter-operations-adr-0023). The library offers the
operations; run them from an operator tool, an admin endpoint or a one-off job, not from the message handler.

## 1. Look before you act

```csharp
var deadLetters = services.GetRequiredService<IDeadLetterQueue<OrderCreated>>();
foreach (var letter in await deadLetters.PeekAsync(50))
    logger.LogInformation("{Id} {Reason} {Detail} attempt {Attempt} at {At}",
        letter.MessageId, letter.ReasonCode, letter.ReasonDetail, letter.DeliveryAttempt, letter.DeadLetteredAt);
```

`ReasonCode` tells you what to do next:

| Reason | Meaning | Usual action |
|---|---|---|
| `MaxRetriesExceeded`, `NonRetryableError` | the handler kept failing (`ReasonDetail` is the exception type) | fix the cause, then replay |
| `Rejected` | the handler returned `DeadLetter` | replay only if the handler's decision changed |
| `DeliveryFailed` | the delivery could not be read | inspect; replay or purge |
| `MessageTooLarge` | body over `MaxMessageSizeBytes` | raise the limit and replay (name the reason), or purge |
| `DeserializationFailed` | body does not match the message type | fix the type and replay (name the reason), or purge |
| anything else | the broker dead-lettered it (`delivery_limit`, `MaxDeliveryCountExceeded`, `TTLExpiredException`, ...) | inspect |

## 2. Count, then replay

```csharp
DeadLetterReplayOptions<OrderCreated> Selection(bool dryRun) => new()
{
    MaxCount = 500,
    ReasonCodes = [DeadLetterReasons.MaxRetriesExceeded],   // null: every reason except MessageTooLarge/DeserializationFailed
    Filter = m => m.Headers.GetValueOrDefault("tenant") == "acme",
    DryRun = dryRun
};
var preview = await deadLetters.ReplayAsync(Selection(dryRun: true));   // preview.Matched: what would be replayed
var result = await deadLetters.ReplayAsync(Selection(dryRun: false));   // .Replayed, .Failed, .Failures (id + exception type)
```

- A replayed message is a **first delivery** again: attempt 1 and the full retry budget; same message id, correlation id,
  headers and trace. If the cause is not fixed it is dead-lettered again after its retries.
- Delivery is **at-least-once**: handlers must be idempotent on `MessageContext.MessageId`. A failure or cancellation
  between the send and the removal leaves the message both replayed and dead-lettered (a duplicate), never lost.
- One call examines at most 10,000 dead letters; run it again for more. Failures stay dead letters and are listed in
  the result and the log (event 1203).

## 3. Purge

```csharp
var deleted = await deadLetters.PurgeAsync(new DeadLetterPurgeOptions<OrderCreated>
{
    MaxCount = 1000,                                          // required: there is no purge-all without a bound
    ReasonCodes = [DeadLetterReasons.DeserializationFailed]   // null: every reason
});
```

Deleting is permanent. Peek first; log what you delete if you need an audit trail.

## What a call does when things go wrong

- **Cancellation:** stops after the message in hand and throws `OperationCanceledException`; what was replayed or
  deleted stays done and is counted.
- **A lost connection or a failing broker call** ends the pass: the dead letters it held go back (the broker returns
  them), what was done is counted and logged (event 1206, "aborted after N"), and the call throws
  `MessageQueueException` naming only the failure's type (the exception itself is in the log).
- **A `Filter` that throws** ends the call the same way, but its own exception comes through unchanged.
- **Messages without a message id** are recognised by their body, correlation id and headers, so one call replays
  each at most once.

## Provider notes

- **RabbitMQ:** needs `EnableDeadLetterQueue = true`. Replays go through the default exchange to the queue itself, so
  other queues bound to a fanout or topic exchange do not get them again. While a call runs, the dead letters it read
  are unacknowledged on its channel and returned with `basic.nack` when it ends. A peek or dry run on a **quorum**
  dead-letter queue moves the messages it read to the end of the queue (order only; nothing is removed).
  A quorum dead-letter queue is declared with `x-delivery-limit = -1`: a dead letter returned by a lost connection
  counts as a failed delivery, and with the default limit (20) and no dead-letter exchange behind it the 21st return
  would drop it. A dead-letter queue declared by an earlier version keeps its arguments (RabbitMQ refuses to change
  them; the queue logs warning 2013 at startup). Lift the limit with a policy instead, for example:
  `rabbitmqctl set_policy dlq-unlimited '\.deadletter$' '{"delivery-limit": -1}' --apply-to quorum_queues --priority 10`
  (a queue takes one policy, the highest priority: merge this key into any policy that already matches those queues).
  The main and retry queues keep their limits: the main queue's broker dead-lettering ends in the dead-letter queue,
  and retry queues have no consumers.
- **Azure Service Bus:** peek needs Listen; replay needs Send and Listen; purge needs Listen. Replays work on queues
  only: on a topic subscription a resend would reach every subscription, so `ReplayAsync` throws `NotSupportedException`
  (peek and purge work). A replayed copy has a new broker message id (duplicate detection must not drop it);
  `MessageContext.MessageId` is the original. While a call runs, the dead letters it read are locked; the SDK settles
  by the whole received message, so a pass keeps their bodies and ends once it holds 1000 of them (call again).
  Transactions: a refusal is recognised by the exception type (`InvalidOperationException`, `NotSupportedException`,
  `TransactionException`); a disposed client or an aborted or in-doubt transaction is a failure of that message, not a
  refusal. A refusal the SDK reports as a `ServiceBusException` is not recognised (no failure reason names it).
- **In-memory:** the dead letters live in the process and are lost when it stops.

The sample worker shows the calls:
`dotnet run --project samples/ZedLabs.MessageQueue.Samples -- --Sample:DeadLetters=Replay`.
