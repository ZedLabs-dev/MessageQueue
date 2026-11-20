# 0003. In-memory provider guarantees

- Status: Accepted (2026-10-16)
- Context: OPTIMIZATION-PLAN D3, §3a.1, B12

## Context

The old `Testing.InMemoryMessageQueue<T>` called handlers synchronously from `ProcessAllAsync`, passed the published
object by reference, and re-queued failures inside the drain loop, so an always-failing handler looped forever (B12).
D3 makes in-memory a first-class provider for local development and simple deployments.

## Decision

`ZedLabs.MessageQueue.InMemory.InMemoryMessageQueue<T>`, selectable with `Provider = InMemory`:

- **Broker-like:** messages are serialized with the configured format on publish (serialization errors surface as
  `MessageQueueException`), and handlers receive a deserialized copy.
- **Background dispatch:** an unbounded `System.Threading.Channels` queue, read by one loop that runs at most
  `MaxConcurrentCalls` handlers at a time. One subscriber per queue.
- **Same pipeline:** deliveries go through the shared dispatcher (ADR 0004): size limit, retry policy, dead-letter.
- **Real backoff:** a retry waits on a timer (not in a handler slot), then re-enters the back of the queue with the
  next `DeliveryAttempt`. Retries end at `MaxRetries`, so B12 cannot recur.
- **Dead letters** are kept in `DeadLetters` with the reason code, body, last context and (when it deserializes)
  the message. With `EnableDeadLetterQueue = false` they are dropped with a warning.
- **Metadata:** version 7 GUID message ids (or `PublishOptions.MessageId`), correlation id, headers, publish time.
- **Test helpers:** `PublishedMessages`, `ProcessedMessages`, `DeadLetters`, `PendingCount`, `WaitForIdleAsync`
  (completes when nothing is queued, running or waiting to retry; throws `ObjectDisposedException` after dispose) and
  `Clear`. The synchronous `ProcessAllAsync`/`ProcessOneAsync` drains are removed: they bypassed the pipeline and
  caused B12. `QueueName` is required from configuration (code-first registration uses the message type's name).
- **Shutdown:** unsubscribe and dispose cancel handler tokens, requeue interrupted messages, and wait at most
  `ShutdownTimeout` (30 s; 25 s since ADR 0025) for handlers that ignore their token.

## Guarantees and limits

- **Single-process and non-durable.** Queued messages, pending retries and dead letters are lost when the process
  stops. `Capabilities` reports `Durable = false`, `DeadLetter = true`, `DelayedRedelivery = true`.
- At-least-once within the process: a handler cancelled by unsubscribe puts its message back unchanged.
- No ordering guarantee once `MaxConcurrentCalls > 1` or a retry happens.
