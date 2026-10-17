# 0001. Retry semantics and default retry policy

- Status: Accepted (2026-10-16)
- Context: OPTIMIZATION-PLAN D2, B3, B5, B13; PRODUCTION-READINESS-PLAN "5 retries over ~60 s"

## Context

`RetryPolicy.MaxAttempts` was ambiguous: `RetryExecutor.ShouldRetry` treated it as total attempts, the base config
had a separate `RetryAttempts`/`RetryDelay`, and ASB compared it against `DeliveryCount`. Delays were computed as
`long` ticks before the cap, so large attempt numbers overflowed to negative delays (B13), and `new Random()` was
shared across threads.

## Decision

- Rename to **`MaxRetries`**: the number of retries *after* the first attempt. A message is handled at most
  `MaxRetries + 1` times. `RetryPolicy.None` is `MaxRetries = 0`.
- `ShouldRetry(attempt)` takes the 1-based attempt that just failed and is true while `attempt <= MaxRetries`
  (and `IsRetryable`, if set, accepts the exception).
- `CalculateDelay(n)` is the wait before retry `n` (1-based): `Initial * Multiplier^(n-1)` for exponential.
  It is computed in `double`, clamped to `[0, MaxDelay]`, jittered with `Random.Shared`, and clamped again, so jitter
  never exceeds `MaxDelay` and nothing overflows. Custom calculators are clamped the same way.
- Default policy: exponential, `MaxRetries = 5`, `InitialDelay = 2 s`, multiplier 2, `MaxDelay = 30 s`, 10% jitter.
  Retries wait 2 + 4 + 8 + 16 + 30 = 60 s in total before the message is dead-lettered.
- The base config exposes the policy as `Retry` (section `MessageQueue:Retry`), replacing `RetryAttempts`/`RetryDelay`.
- Validation: `MaxRetries` 0..1000, `InitialDelay >= 0`, `InitialDelay <= MaxDelay <= 1 day` (inside `Task.Delay`'s
  and RabbitMQ's per-message TTL limits), multiplier 1..100, jitter 0..1, `Custom` needs a calculator.

## Consequences

- Breaking rename; config files using `RetryAttempts` must move to `Retry:MaxRetries`.
- The retry *count* means the same on every provider: the shared dispatcher (ADR 0004) alone decides retry vs
  dead-letter. The *delay* does not yet: only in-memory has `DelayedRedelivery`. Until the provider steps land
  broker-side delay, RabbitMQ waits out the backoff in process and Azure Service Bus redelivers immediately; both log
  a startup warning when the policy has a backoff. (RabbitMQ: superseded by ADR 0006, broker-side retry queues.) An `IsRetryable` that throws counts as non-retryable.
- A 60 s default makes failure paths slow in tests: tests set a millisecond policy explicitly.
