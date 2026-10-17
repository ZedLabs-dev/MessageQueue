# 0007. RabbitMQ.Client 7 and lazy, async initialization

- Status: Accepted (2026-10-16)
- Context: OPTIMIZATION-PLAN Phase 5, B1, B2, B4, B15, §4.2, S10

## Context

RabbitMQ.Client 7 is async-only and tracks publisher confirms itself. The 6.x provider connected in its constructor,
and its `PublisherChannel` keyed confirms by `NextPublishSeqNo`, which recovery resets (B15). DI constructs synchronously.

## Decision

- **Lazy initialization.** The constructor only validates. The first `PublishAsync`, `SubscribeAsync` or
  `InitializeAsync` connects, opens the publisher channels and declares the topology, once, under a lock. Failures
  are not cached, but later attempts back off (1 s, doubling, at most 30 s; calls in between fail fast without
  dialling). `RabbitMqMessageQueue.CreateAsync<T>` constructs and connects in one call. DI registration is
  unchanged: `MessageQueueHostedService` subscribes at host start, so an unreachable broker still fails the host, and
  `AddRabbitMqHealthCheck<T>()` connects through its probe.
- **Errors**: connection failures throw `MessageQueueException`, whose message names only the exception type; the
  inner exception is kept and logged. Specs check that the whole `ToString()` has no password (S10).
- **Publishing**: `PublisherChannel` is deleted. `PublisherPool` opens channels with
  `publisherConfirmationsEnabled` + `publisherConfirmationTrackingEnabled`, picks one with the shared `RoundRobin`
  (B1), caps the wait at `ConfirmTimeout`, and maps a nack or return to `MessageQueueException`. `MaxOutstandingConfirms`
  is a per-channel `ConcurrencyLimiter` with an unbounded queue. The client's default `ThrottlingRateLimiter` rejects
  callers beyond its queue and sleeps under load.
- **Batches** (§4.2) publish everything, then await the confirms together, at most
  `MaxOutstandingConfirms * PublisherChannelPoolSize` in flight, and await every started publish even when cancelled.
- **Consuming**: one channel per subscription with `ConsumerDispatchConcurrency = MaxConcurrentCalls` and always
  `BasicQos(PrefetchCount)` (B4). Handlers of `AsyncEventingBasicConsumer` are awaited (B2). The body is used without
  a copy: the 7.2.2 `AsyncConsumerDispatcher` disposes the pooled buffer only after the awaited handler, and every
  read of it happens inside. `MaxConcurrentCalls > PrefetchCount` logs a startup warning (prefetch then bounds it).

## Consequences

- Breaking: a constructed queue no longer proves the broker is reachable; use `InitializeAsync`/`CreateAsync` to
  fail fast. `Dispose()` blocks on `DisposeAsync()`, as the client's own `Dispose` does.

## Later changes

- Phase 7 (S9): the connection factory caps inbound bodies at `max(MaxMessageSizeBytes, 16 MiB)` instead of the
  client's 64 MiB default. The floor is RabbitMQ's default `max_message_size`: a body above the client cap would
  close the connection and come back forever, so oversized bodies below it are dead-lettered by the pipeline.
