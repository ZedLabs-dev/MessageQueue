# 0026. MaxMessageSizeBytes is enforced on publish

- Status: Accepted (2026-10-23)
- Context: OPTIMIZATION-PLAN S9; ADR 0004 (shared pipeline); ADR 0012 (Service Bus batching)

## Context

`MaxMessageSizeBytes` (1 MiB) was checked only by the consumer, which dead-letters a larger body as
`MessageTooLarge`. A publisher could therefore send a message every consumer of the queue would refuse: it travelled
to the broker, used a delivery, and landed in the dead-letter queue, while `PublishAsync` reported success.

## Decision

- Every provider checks the serialized body against `MaxMessageSizeBytes` in `PublishAsync`, after serializing and
  before telemetry or any send, with the consumer's rule (a body of exactly the limit passes). The check is the public
  SPI method `PublishValidation.ValidateBodySize`, so third-party providers apply the same rule and message.
- A violation throws `MessageQueueException` naming the size, the limit and the destination, never the content. It is
  a caller error like the other publish checks, so publish metrics and spans do not record it.
- **Batches are checked whole first:** the message's index is named and nothing of the batch is sent, so a refused
  batch never leaves a sent prefix behind.
- **Only the public publish paths:** retry copies, dead-letter copies and dead-letter replays carry bodies that were
  already accepted; they are not checked again (a replay of an oversized dead letter is dead-lettered again by the
  consumer, as before).
- The consumer keeps its check, for other publishers and for messages queued before the limit was lowered.

## Consequences

- A behavior change: `PublishAsync` of an oversized message now throws instead of succeeding and dead-lettering.
- The limit applies to the body only. Headers and broker framing still count against the broker's own limits
  (RabbitMQ `max_message_size`, the Service Bus entity's maximum message size), which can refuse a smaller body.
