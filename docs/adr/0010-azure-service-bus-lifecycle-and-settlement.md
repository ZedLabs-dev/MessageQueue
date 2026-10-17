# 0010. Azure Service Bus lifecycle, settlement and shared consumer helpers

- Status: Accepted (2026-10-17)
- Context: OPTIMIZATION-PLAN B6, B7; ADR 0004 (dispatcher seam), ADR 0008 (RabbitMQ shutdown and settlement)

## Context

A second `SubscribeAsync` added a second handler (the SDK throws), errors were swallowed, `Dispose()` blocked on
async, shutdown was unbounded and deadlocked from inside a handler, and an unreadable message was never settled.
The RabbitMQ provider had solved the same problems with code private to it.

## Decision

- **Seams**: thin internal interfaces over the client factory, sender, processor, per-message settlement actions
  and a peek receiver. The SDK-backed classes only forward; unit specs drive the provider through fakes and
  `ServiceBusModelFactory` messages. Integration specs stay gated on `MQ_ASB_CONNECTION_STRING`.
- **One processor per subscription**, created on subscribe and disposed on unsubscribe, so re-subscribing works. A
  second subscriber throws `InvalidOperationException`. `ProcessErrorAsync` logs (LoggerMessage) the error source,
  entity path and exception type, throttled per distinct error; a lost lock is a warning.
- **Settlement**: each message is settled at most once (complete, dead-letter, retry or abandon), each call bounded by
  `SendTimeout`. A message that cannot be read (metadata or body) is dead-lettered as `DeliveryFailed` through the
  dispatcher. A failed settlement backs off per message (100 ms doubling, at most 30 s), then abandons once more;
  a lost lock is logged and left to the broker. Nothing escapes to the processor; a safety net abandons the rest.
- **Shutdown** (`UnsubscribeAsync`/`DisposeAsync`): cancel the handlers' token, stop the processor (which waits for
  them), dispose it. Cancelled handlers and messages arriving after shutdown began are abandoned without using an
  attempt. The call waits at most `ShutdownTimeout` (30 s); later handlers still settle normally and the processor
  is disposed once they finish. `DisposeAsync` is idempotent, gives all handlers one `ShutdownTimeout`, then closes
  the client with 1 s grace. `Dispose()` runs `DisposeAsync` on the thread pool (documented, no sync-context deadlock).
- **Re-entrancy**: a per-delivery `HandlerScope` (AsyncLocal, active only until its handler returns) makes
  unsubscribe or dispose from inside a handler finish in the background. A handler's own cancellation callback that
  throws is logged; shutdown still stops and disposes the processor (both providers, `Cancellation.CancelQuietlyAsync`).
  `SubscribeAsync` re-checks disposal after taking the subscription lock.
- **Shared helpers** in `Processing/` (RabbitMQ too): `HandlerScope`, `TransportBackoff`, `LogThrottle`,
  `BackgroundWork`, `Cancellation`, `MessageDispatcher.RejectUnreadableAsync`.

## Consequences

- At-least-once: a settlement failing after the broker applied it can redeliver; a handler that never returns holds
  its message until dispose closes the client. Abandons count towards MaxDeliveryCount, ending persistent failures.

## Later changes

- Phase 7: processor errors are also counted (`ITelemetryProvider.RecordTransportError`, metric
  `ese.messagequeue.transport.errors` with `error.type` = failure reason or exception type), throttled in the log or
  not. The shared helpers stay internal and reach the provider package through InternalsVisibleTo (ADR 0015).
