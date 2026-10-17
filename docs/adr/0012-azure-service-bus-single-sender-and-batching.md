# 0012. Azure Service Bus: one sender, byte-aware batches

- Status: Accepted (2026-10-17)
- Context: OPTIMIZATION-PLAN §4.3, B8, B1

## Context

The provider kept a round-robin pool of `ServiceBusSender`s (`SenderPoolSize`, default 2) behind semaphores
(`MaxConcurrentSends`). All senders of a `ServiceBusClient` share its single AMQP connection, and `ServiceBusSender`
is thread-safe; the pool added a link per sender and a second concurrency limit, not throughput. Batch publish
used `Chunk(MaxBatchSize)`, which ignores the entity's byte limit (256 KB Standard, up to 100 MB Premium).

## Decision

- **One sender per queue** (to the queue, or the topic in subscription mode), shared by publish and retry copies.
  `PublisherSender`, `SenderPoolSize` and `MaxConcurrentSends` are removed: binding them now fails at startup
  (unknown key). No benchmark showed a benefit, and §4's rule is no optimization without numbers; the pool's
  extra links on the same connection give the SDK no extra parallelism to exploit. Callers needing backpressure
  bound their own concurrency; the SDK's retry policy handles throttling. B1's `RoundRobin` has no remaining use here.
- **`SendTimeout`** (default 30 s) still bounds every send and batch; exceeding it throws `MessageQueueException`.
  Caller cancellation surfaces as `OperationCanceledException`. Errors name only the exception type.
- **Batches**: `CreateMessageBatchAsync` + `TryAddMessage`, closing a batch when it is full by bytes or reaches
  `MaxBatchSize` messages. A message that does not fit an empty batch throws `MessageQueueException` naming its
  index (never its content); earlier batches stay sent. `EnableBatchedOperations = false` sends one at a time.

## Consequences

- Breaking config removal (`SenderPoolSize`, `MaxConcurrentSends`), listed in the CHANGELOG.
- Batch publish is not atomic across batches; it never was, but the split is now explicit and documented.
- If a benchmark (Phase 6) ever shows a pool helping, it returns as a measured, documented option.
