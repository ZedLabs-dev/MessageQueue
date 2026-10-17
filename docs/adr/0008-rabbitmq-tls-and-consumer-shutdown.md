# 0008. RabbitMQ TLS enforcement, delivery settlement and consumer shutdown

- Status: Accepted (2026-10-16)
- Context: OPTIMIZATION-PLAN S7, S5; ADR 0002 (options validation), ADR 0004 (cancelled handlers)

## Context

`amqp://` was accepted silently for any host (S7), and the health check leaked details (S5). Shutdown left
deliveries unacked, did not honour its timeout, and deadlocked when called from inside a handler. A malformed delivery
could stay unacked forever, blocking the consumer's prefetch window.

## Decision

- **`RequireTls`** (`bool?`): when unset, TLS is required unless the host is loopback (`localhost`, `127.0.0.0/8`,
  `::1`). The rule is checked at startup and in the constructor (`OptionsValidationException`), at connect time
  (`ConfigurationException`) and by the connection-string health check. `RequireTls = false` opts out explicitly.
- **`Tls`**: `Enabled` (with `HostName`; `amqps://` turns TLS on by itself), `ServerName`, and
  `AcceptablePolicyErrors`; anything but `None` requires `AllowInsecureCertificates`. `HostName`/`Port`/`UserName`/
  `Password`/`VirtualHost` are an alternative to `ConnectionString`. Names must fit 255 bytes with the derived suffixes.
- **Health**: no `Data` and no exception text in the description. The queue-backed probe is unhealthy when the
  connection is closed or, while subscribed, the consumer channel is closed or its consumer was cancelled.
- **Every delivery is settled**: ack, dead-letter or requeue, at most once each (a second ack or nack closes the
  channel). If the delivery cannot be read, it is dead-lettered as `DeliveryFailed`. Failed transport actions requeue
  after a backoff (ADR 0006), and a safety net requeues anything the pipeline left unsettled.
- **Shutdown** (`UnsubscribeAsync`/`DisposeAsync`): cancel the handlers' token and the consumer. Cancelled handlers
  nack with requeue. Deliveries that arrive afterwards go back without using an attempt (ADR 0006). The call waits
  until nothing is in flight, or at most `ShutdownTimeout` (or until the caller's token cancels). Then it returns
  with the consumer cancelled and the channel open; a background task closes it once the remaining handlers finish,
  so they settle normally (no requeue racing a late ack or retry, no duplicate). `DisposeAsync` gives all handlers,
  draining ones included, one `ShutdownTimeout`, then closes the connection with at most 1 s grace.
- **Re-entrancy**: an AsyncLocal holds a per-delivery scope, active only until its handler returns (tasks the handler
  spawns inherit an inactive scope). A call made inside an active handler cancels, returns, and finishes in background.

## Consequences

- Breaking: remote hosts over plain `amqp://` fail at startup until they use `amqps://` or `RequireTls = false`.
- A handler that never returns keeps its subscription's channel open until the queue is disposed. On dispose its
  delivery is requeued by the closed connection (its late result is lost), and prefetched deliveries still queued
  behind it in the client come back with one attempt used.
