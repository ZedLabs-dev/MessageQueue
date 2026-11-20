# 0023. Dead-letter operations: peek, replay, purge

- Status: Accepted (2026-10-22)
- Context: PRODUCTION-READINESS-PLAN "drain and replay dead-letter queues"; ADRs 0004, 0006, 0009, 0015, 0021
- Renamed 2026-11-20 (ADR 0016 amendment): the headers were `x-ese-*` when this was decided; they are now
  `x-zedlabs-*`.

## Decision

- **API:** `IDeadLetterQueue<T>` (peek, replay, purge), implemented by the queue, in DI only when it is. `Capabilities`
  gains an `init` flags property `DeadLetterOperations`. Snapshots are bounded; bodies over the size limit not read.
- **Shared logic** (as ADR 0004): public `DeadLetterQueue<T>` over the `IDeadLetterStore` SPI does checks, selection,
  limits, dry runs, counting, metrics and logs; a store returns each dead letter once per pass, holds it (lock, no ack;
  a tag, not the body, where it can) and hands back the rest. The loopback sample uses it.
- **Selection:** exact `ReasonCodes` plus `Filter`. Replay skips `MessageTooLarge`/`DeserializationFailed` unless named;
  purge takes every reason and needs `MaxCount`. Peek 1..1000; 10,000 examined per call; 100 failures listed.
- **Safety:** one operation per queue at a time; publish confirmed, then remove (a duplicate, never a loss). A call
  replays a key once (message id, else a hash of body, correlation id and headers). Cancellation, a failed pass
  (wrapped in `MessageQueueException`, type only) and a throwing filter (rethrown) all count and log what was done.
  Peeks and dry runs remove and alter nothing, but a RabbitMQ quorum queue moves what they read to its end.
- **In-memory:** `DeadLetters` is the store; a replay is queued (pending tracked) before it is removed.
- **RabbitMQ:** `basic.get` on a channel of its own; replays go through the **default exchange** to the queue,
  mandatory and confirmed (the configured exchange could reach other bound queues), with every `x-` header dropped so
  attempt 1 holds; acks never `multiple`. Held deliveries go back by explicit nack; a lost channel ends the pass. The
  quorum dead-letter queue gets `x-delivery-limit = -1`: a return by a lost channel counts as a failed delivery, and
  the default limit (20, no DLX) would drop it. An existing one cannot be redeclared: warning 2013 and a documented
  `delivery-limit: -1` policy. Main and retry queues keep their limits (the main one ends in the dead-letter queue,
  retry queues have no consumers). Queue depth is no bound (it lags after nacks). No dead-letter queue: `None`.
- **Service Bus:** a PeekLock receiver per pass on `$DeadLetterQueue`; peeks browse. A replay is a retry copy (ADR
  0009): new broker id (duplicate detection must not drop it), original in `x-zedlabs-message-id`, attempt 1, dead-letter
  properties stripped (any case); send and complete in one `RequiresNew` transaction, else send first. Only a refusal
  (`InvalidOperationException`, `NotSupportedException`, `TransactionException`, but not disposed, aborted or in-doubt)
  switches to send-first. Receives wait 1 s; a repeated sequence number or 1000 held messages (the SDK settles by
  message, body included) ends the pass. Subscriptions: peek and purge only (a resend reaches every subscription).
- Dead letters record `x-zedlabs-dead-lettered-at` (both brokers) and `x-zedlabs-dead-letter-attempt` (Service Bus).

## Consequences

- Unverified on a namespace (fakes only; gated integration spec; release blocker stands): the cross-subqueue
  transaction, receive timing, and refusals the SDK reports as `ServiceBusException` (no reason identifies one).
