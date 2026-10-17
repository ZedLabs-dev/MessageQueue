# 0013. Telemetry: trace propagation, span parenting and metrics

- Status: Accepted (2026-10-17). Context: OPTIMIZATION-PLAN B10, S6; ADRs 0004 (dispatcher), 0006 / 0009 (retries)

## Context

No provider published spans or propagated a trace; consume spans joined whatever span was current on the consumer
thread; `ExtractTraceContext` accepted any `a-b-c`; metrics had ad-hoc names, no durations and no `messaging.system`.

## Decision

- **Interface (breaking, pre-1.0):** calls take a `TelemetryTarget(System, Destination, MessageType)` (+ `Subscription`,
  `RoutingKey`, `PublishOperation`); publish spans get the count and a single message's id; consume spans the
  `MessageContext` and parsed parent; publish/consume records carry a duration and an `errorType` (null = success).
- **Destination joins both sides:** in-memory and ASB queue: the queue; ASB topic: the topic, plus
  `messaging.destination.subscription.name` on consume; RabbitMQ: `{exchange}:{routing key}` (the conventions'
  consume form `{exchange}:{routing key}:{queue}` collapses to it, the routing key being the queue) plus
  `messaging.rabbitmq.destination.routing_key`. Operation names: `publish`, `send` on ASB, `process`.
- **Headers:** W3C `traceparent` / `tracestate`, reserved (applications cannot set or see them), matched
  case-insensitively, strings or UTF-8 bytes; on ASB, `Diagnostic-Id` is the fallback. `TraceContext.Parse` is strict
  level 1 (lowercase hex, non-zero ids, no `ff`, 55 characters for 00), bounded (256 / 512 characters, 32 members),
  never throws, and keeps the sampled and random (level 2) flags, clearing unknown ones.
- **Spans:** one `Producer` span per publish call, a batch included (`messaging.batch.message_count`). The `Consumer`
  span's parent is the message's (remote) context; every outcome other than Acknowledge sets `error.type` (the
  metrics' value) and Error status, and dead letters and retries are tagged with their reason and next attempt.
- **Parent, not link:** the conventions default to a link plus the ambient span as parent, and allow parenting for
  single messages if documented. Every delivery here is a single message, and the ambient span on a consumer thread
  is accidental (the subscriber's, captured when the transport started its loop), so the dispatcher clears it and
  parents on the publisher. Retry copies keep the original headers, so every attempt is a child of the publish span.
- **Metrics:** `messaging.client.sent.messages`, `messaging.client.operation.duration` (s), `messaging.client.consumed
  .messages`, `messaging.process.duration` (s), `ese.messagequeue.messages.retried` / `.dead_lettered`; low-cardinality
  tags only (system, destination, operation, message type, subscription, error type, dead-letter reason).
- **Cost and safety:** nothing is parsed, formatted or tagged without a listener; without a provider publishes share
  one no-op. Every call is guarded and logged (events 1005, 1009); telemetry never changes a publish or delivery.
  DI passes the registered provider to every queue and disposes `OpenTelemetryProvider` (its source and meter).

## Consequences

- Custom `ITelemetryProvider` implementations must be updated; metric names changed (CHANGELOG).
