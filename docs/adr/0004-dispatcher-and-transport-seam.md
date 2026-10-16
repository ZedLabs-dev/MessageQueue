# 0004. Shared consumer dispatcher and transport seam

Accepted 2026-10-16. Context: OPTIMIZATION-PLAN D2, §3a.2, B3, B5, B11, S6, S9.

## Context

Each provider had its own consume logic, each wrong differently (B3, B5, B11, S6, S9).

## Decision

One internal pipeline, `Processing.MessageDispatcher<T>`, used by every provider. Per delivery it takes the body
(`ReadOnlyMemory<byte>`), a `MessageContext` (with the 1-based `DeliveryAttempt`) and an `IDeliveryActions`:

```csharp
interface IDeliveryActions
{
    Task CompleteAsync(CancellationToken ct);
    Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken ct);
    Task DeadLetterAsync(DeadLetterReason reason, CancellationToken ct);  // Code + Detail
}
```

1. Body larger than `MaxMessageSizeBytes` → dead-letter `MessageTooLarge`, never deserialized.
2. Deserialization throws or yields null → dead-letter `DeserializationFailed`.
3. Handler → `Acknowledge` completes; `DeadLetter` dead-letters `Rejected`; `Retry` or a thrown exception asks the
   `RetryExecutor`: reschedule with the computed delay as `attempt + 1`, else dead-letter `MaxRetriesExceeded`
   (or `NonRetryableError` when `IsRetryable` refuses the exception).
4. Dead-lettering disabled → the delivery is completed (dropped) and a warning is logged.
5. A handler cancelled by the provider's stopping token rethrows without settling, so the broker redelivers.

At most one action per delivery; only a failing transport action propagates. Telemetry and logging failures are
swallowed, a throwing `IsRetryable` means non-retryable, a throwing delay calculator falls back to `InitialDelay`.
Only the exception **type** reaches `DeadLetterReason.Detail`; the exception goes to the app's `ILogger`.
Reason codes are a fixed set (`DeadLetterReasons`) because they tag metrics. Telemetry: a consume span plus
consumed/retried/dead-lettered counters; publish spans and `traceparent` come later (B10).

## Consequences

Providers implement three actions plus metadata mapping; retries are unit-tested once; no native delay → `DelayedRedelivery = false`.
