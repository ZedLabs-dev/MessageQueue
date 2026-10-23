# 0027. RabbitMQ transport-error metric and Debug publish logs

- Status: Accepted (2026-10-23)
- Context: ADR 0013 (telemetry), ADR 0021 (bounded values); PRODUCTION-READINESS-PLAN §4

## Context

`ese.messagequeue.transport.errors` was recorded by Service Bus only. On RabbitMQ a lost connection, a failing
reconnect, a consumer channel closed by `consumer_timeout` or a consumer cancelled because its queue was deleted
showed only in logs or the health check, so one alert rule could not cover both brokers. Separately, publishing wrote
no log line at all, and the readiness review left open whether it should.

## Decision

- **RabbitMQ transport errors** are counted from the client's events, with a fixed `error.type`: `ConnectionFailed`
  (an attempt to connect or declare the topology failed; calls refused during the reconnect backoff dial nothing and
  are not counted), `ConnectionShutdown` (connection closed by the broker or lost), `RecoveryFailed` (one automatic
  reconnect attempt failed), `ChannelShutdown` (the consumer channel closed while its connection stayed open),
  `ConsumerCancelled` (broker `basic.cancel`, seen through an override of `HandleBasicCancelAsync`).
- Closes the library initiates (`ShutdownInitiator.Application`: unsubscribe, dispose) are not counted, and a lost
  connection is counted once, not again for its channels. Never the broker's reply text (ADR 0021). A throwing
  telemetry provider is logged (2008) and never disturbs the client's event handling.
- **Publish logs:** no Information line per message; it would drown everything else, the outcome is already in
  metrics and spans, and failures throw to the caller. Each successful publish logs at **Debug** (1010 for one
  message with its id, control characters removed; 1011 for a batch with its count; both with the destination; never
  content or headers), so an operator can trace publishes by raising one category for a while.

## Consequences

- The `MessageQueueTransportErrors` alert now fires for RabbitMQ too; during a broker outage `RecoveryFailed` is
  expected about once per `NetworkRecoveryInterval` (the client's retry cadence; no spec runs a long outage).
- Publisher channels' own errors (other than a lost connection) are not counted: they fail the publish, which throws
  and is counted in `messaging.client.sent.messages` with `error.type`.
