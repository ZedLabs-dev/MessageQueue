# 0021. Bounded values in brokers, telemetry and logs

- Status: Accepted (2026-10-22)
- Context: OPTIMIZATION-PLAN S6 (remainder); ADR 0009, ADR 0013

## Context

S6 removed exception text from brokers and spans, but values derived from callers, publishers and exceptions were
still written unbounded: dead-letter reasons and details (Service Bus refuses a dead-letter description over 4096
characters, so a long one would fail the settlement), exception type names (a generic type's `FullName` carries its
arguments' assembly-qualified names and can run to many kilobytes) in `error.type`, status descriptions, metric tags
and logs, and a consumed correlation id in spans and handler contexts.

## Decision

- **One helper, one limit:** `BoundedText` cuts free text to `PublishOptions.MaxHeaderValueLength` (4096), the limit
  already used for header values and recorded exception text, never inside a surrogate pair.
- **At the public boundary:** the `DeadLetterReason` constructor bounds `Code` and `Detail`, so every provider,
  third-party ones included, stores bounded values (RabbitMQ headers, Service Bus properties and description,
  in-memory dead letters). `OpenTelemetryProvider` bounds `error.type`, status descriptions, exception types and metric
  tags whoever calls it. Exception type names are bounded where they are taken (dispatcher, publish telemetry, health
  and Service Bus logs).
- **Ids:** the dispatcher gives a consumed `CorrelationId` the message id's treatment (C0, DEL and C1 control
  characters removed, at most 128 characters, never ending in half a surrogate pair; an absent one stays null). On
  publish a `CorrelationId` over 128 characters or with control characters is refused (`ArgumentException`), not
  cut: it is a key the application matches on, and 128 is Service Bus's limit (AMQP's is 255 bytes).
- Specs per provider assert the stored length with an 80-level generic exception type.

## Consequences

- A cut value is a prefix; consumers matching on full type names of exotic generic exceptions see the prefix.
- Publishing a correlation id over 128 characters now fails on every provider, not only on Service Bus.
