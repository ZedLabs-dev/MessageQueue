# 0025. Shutdown budget: provider drain below the host's wait

- Status: Accepted (2026-10-23)
- Context: ADR 0008, ADR 0010 (drain on unsubscribe); docs/operations/failure-modes.md

## Context

On host shutdown `MessageQueueHostedService<T>.StopAsync` unsubscribes, and the queue waits up to its
`ShutdownTimeout` for running handlers. The host itself waits at most `HostOptions.ShutdownTimeout` (30 s) for all
hosted services. Both defaulted to 30 s, so the host could stop waiting exactly when the drain was allowed to end:
a slow handler's result was lost and its message redelivered, although the queue had been told to wait for it.

## Decision

- **Lower defaults:** `ShutdownTimeout` is 25 s on `InMemoryConfig`, `RabbitMqConfig` and `AzureServiceBusConfig`,
  leaving 5 s of the host's default for the close (RabbitMQ and Service Bus add at most 1 s) and other services.
- **Startup warning 5005:** when a first-party queue's `ShutdownTimeout` is not below the host's
  `HostOptions.ShutdownTimeout`, the hosted service warns at start, before subscribing, naming both values and the
  queue's configuration key (`{section}:ShutdownTimeout`). No warning when the host waits forever (zero or less).
- **How the hosted service knows:** the three queues implement an internal `IShutdownBudget` (their
  `ShutdownTimeout`). `HostOptions` lives in `Microsoft.Extensions.Hosting`, which the core package does not reference
  (it needs only the abstractions; adding the full hosting package would pull configuration and logging providers
  into every consumer). The hosted service resolves `IOptions<HostOptions>` by type name through a scope of its
  `IServiceScopeFactory`; without a host, or if that fails (Debug event 5006), there is no warning and start goes on.
- **Not changed:** the guides keep `HostOptions.ShutdownTimeout = 45 s`, because the host stops hosted services one
  after another and with several queues needs the sum of their drains. The per-queue warning cannot see that sum;
  the docs say so.

## Consequences

- A behavior change for applications relying on the old default: handlers get 25 s instead of 30 s to finish after
  their token is cancelled. Set `ShutdownTimeout` explicitly to keep 30 s (and raise the host's timeout with it).
- Third-party providers do not implement the internal interface and get no warning.
- The reflection reads two public members of a stable type; a rename in a future `Microsoft.Extensions.Hosting` only
  turns the warning off, it cannot fail a start.
