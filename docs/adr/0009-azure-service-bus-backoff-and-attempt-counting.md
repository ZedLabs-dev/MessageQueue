# 0009. Azure Service Bus backoff and attempt counting

- Status: Accepted (2026-10-17)
- Context: OPTIMIZATION-PLAN D2, B5, S6; ADR 0001 (retry semantics), ADR 0004 (dispatcher seam), ADR 0006 (RabbitMQ)
- Renamed 2026-11-20 (ADR 0016 amendment): the headers were `x-ese-*` when this was decided; they are now
  `x-zedlabs-*`.

## Context

Retries were abandoned for immediate redelivery, so the backoff never applied. A clone sent to a topic reaches every
subscription; one keeping its `MessageId` is dropped by duplicate detection, losing the message.

## Decision

- **Queue**: a retry sends a copy with `ScheduledEnqueueTime = now + delay` (new id; the original id in
  `x-zedlabs-message-id` stays `MessageContext.MessageId`), then completes the original, in one `TransactionScope` while
  `TransactionalRetries` (default true). Partitioned entities, the Basic tier and the emulator refuse it
  (`InvalidOperationException`, `NotSupportedException` or `TransactionException`; the rollback applied nothing): the
  queue then switches to send-then-complete for good, warning once. That order risks a duplicate, never a loss. Bounded
  by `SendTimeout`, not the stopping token. The commit runs synchronously in `TransactionScope.Dispose`, blocking a pool
  thread per retry for a round trip: a startup warning flags `MaxConcurrentCalls` above 100. `DelayedRedelivery = true`.
- **Subscription**: no copies. A retry waits under the lock when the delay fits `max(MaxAutoLockRenewalDuration -
  time since receipt, LockedUntil - now) - 2 s`, then abandons with the attempt properties; longer delays abandon at
  once (throttled warning); shutdown cuts the wait short. It holds a handler slot: `DelayedRedelivery = false`, startup
  warning kept, plus one when `PrefetchCount > 0` (prefetched locks run down meanwhile). A per-subscription retry
  queue was rejected: it needs provisioning, and forwarding to the topic fans out again.
- **Attempt** = `x-zedlabs-delivery-attempt` + (DeliveryCount - `x-zedlabs-delivery-count`) when both are positive and the
  marker is not ahead of DeliveryCount, else DeliveryCount. Values are capped at `int.MaxValue` and the sum clamped to
  [1, int.MaxValue]. Copies get marker 1, abandons DeliveryCount + 1 (no double count), dead letters `int.MaxValue`, so
  a message resubmitted from the DLQ starts at attempt 1. Shutdown abandons keep the attempt.
- **Forgery**: publishers cannot set `x-` names; markers ahead of the broker are ignored. Residual risk: an SDK sender
  can set both on its own message, changing only its attempt (fewer retries) or `MessageContext.MessageId`.
- **Dead letters**: reason code and exception type in reason, description and `x-zedlabs-dead-letter-*`; never text (S6).
- **MaxDeliveryCount**: copies reset DeliveryCount; on a subscription each retry uses a delivery, so the entity's value
  must exceed `MaxRetries + 1` (reading it needs Manage; a startup warning fires above the default 10).

## Consequences

- At-least-once. A copy's `EnqueuedTime` (`Timestamp`) and `SequenceNumber` are the copy's.
