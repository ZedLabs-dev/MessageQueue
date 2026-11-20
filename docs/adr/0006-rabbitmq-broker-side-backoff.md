# 0006. RabbitMQ broker-side backoff and attempt counting

- Status: Accepted (2026-10-16)
- Context: OPTIMIZATION-PLAN D2, B3, M28; ADR 0001 (retry semantics), ADR 0004 (dispatcher seam)

## Context

Retries waited out their backoff in process, holding the unacked message and a consumer slot. RabbitMQ has no native
delay, and a per-message TTL expires only at the head of a queue, so one retry queue with mixed TTLs makes short retries
wait behind long ones. The delayed-message-exchange plugin is not in the stock image; no plugin dependency.

## Decision

- **One retry queue per delay bucket**, `{queue}.retry.{ms}`: `x-message-ttl = ms`, dead-lettering (default exchange,
  routing key `{queue}`) back to the main queue, and the main queue's type (quorum: `at-least-once` + `reject-publish`).
- **Buckets follow the policy's un-jittered schedule**: retry n uses step s(n), or with jitter J the nearest of
  s(n) x (1 - J), s(n), s(n) x (1 + J), all capped at `MaxDelay`. Custom calculators are rounded to two significant
  digits instead. The default policy needs at most 14 queues (5 without jitter). A zero delay uses `{queue}.retry.0`.
- **Expiry, classic only**: `x-expires = max(1 h, 4 x TTL)`, and a cached bucket is redeclared after `expires / 4`
  (resetting the timer), so a copy leaves by `expires / 2` and its queue never expires holding it. Quorum retry queues
  get no `x-expires`: on 4.3.6 declaring a quorum queue does not reset its expiry, and a busy one was deleted holding
  messages. Their leak is bounded (at most 3 per schedule step); an operator policy (`expires`) can clean them up.
- **Flow**: a confirmed, `mandatory` copy (application headers only) goes to the bucket; then the original is acked.
  A refused copy (queue deleted) redeclares the queue, retry or dead-letter, and is published once more. If that
  fails too, or the confirm timed out (the copy may exist: at-least-once), the delivery is requeued after a per-message
  backoff (100 ms, doubling, at most 30 s, cut short by unsubscribe), and the error log is throttled.
  `DeadLetterAsync` works the same way towards the DLQ, adding `x-zedlabs-dead-letter-reason`/`-detail` (exception type).
- **Attempt** = `x-delivery-attempt` + (redelivered ? max(1, `x-delivery-count`) : 0). The library's header counts
  only when `x-first-death-queue` names one of this queue's retry queues, `x-delivery-count` only on redeliveries from
  quorum queues. Accepted risk: a forged `x-first-death-queue` changes only the forger's own message's attempt.
  On 4.3.6 a nack with requeue sets only the redelivered flag (+1); quorum `x-delivery-count` counts crashes and
  channel closes. Crash loops end in the DLQ; a handler interrupted by `UnsubscribeAsync` uses one attempt. Deliveries
  reaching the client after unsubscribing return via `{queue}.retry.0` with their attempt unchanged (else a nack, +1).
- `Capabilities.DelayedRedelivery = true`; the interim startup warning no longer applies to RabbitMQ.

## Consequences

- A waiting retry holds no slot or memory. At-least-once: a crash between confirm and ack duplicates, never loses.
  Classic queues count at most one crash per cycle; the quorum `delivery-limit` (20) dead-letters via the broker DLX.

## Later changes

- Phase 7: the attempt header is trusted only when the broker's `x-death` history records that the message
  **expired** out of one of this queue's retry queues (`x-first-death-queue` alone no longer counts). This does not
  stop forgery: an integration probe against RabbitMQ 4.3.6 showed that a client-published `x-death` header is
  delivered as sent, so a publisher can still set its own message's attempt. The accepted risk is unchanged: a
  forger can shorten the retries of its own messages only, and they still end in the dead-letter queue.
