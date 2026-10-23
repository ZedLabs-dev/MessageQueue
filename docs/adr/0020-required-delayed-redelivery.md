# 0020. Capability-based startup failure: `RequireDelayedRedelivery`

- Status: Accepted (2026-10-22); amends ADR 0001
- Context: OPTIMIZATION-PLAN §3a.5; ADR 0009 (Service Bus backoff), ADR 0015 (provider SPI)

## Context

A provider or entity without broker-side delayed redelivery (`Capabilities.DelayedRedelivery = false`: today only an
Azure Service Bus topic subscription) cannot honor the retry backoff. Since Phase 4 the queue logs warning 1101 at
construction. Some applications rely on the delay (rate-limited downstreams) and want that to be a startup failure;
others accept immediate redelivery. Failing by default would break subscriptions that work today.

## Decision

- **`MessageQueueConfig.RequireDelayedRedelivery`** (bool, default false). False keeps the warning. True fails when the
  delivery path cannot delay and the policy has a backoff (`MaxRetries > 0` and `InitialDelay > 0` or a custom
  calculator; one predicate shared with the warning).
- **First-party providers fail `ValidateOnStart`:** the Service Bus validator applies the rule with the same
  `SupportsDelayedRedelivery` property the queue derives its `Capabilities` from, so the two cannot drift. RabbitMQ and
  the in-memory provider always delay.
- **Every provider, third-party included,** is checked when its queue is first resolved: `QueueRegistration` compares
  the created queue's `Capabilities.DelayedRedelivery` with the setting and throws `ConfigurationException` (after
  disposing the queue). A provider needs no knowledge of the setting.

## Consequences

- A subscription with the default policy and `RequireDelayedRedelivery = true` no longer starts; the message names the
  setting and the three ways out (a queue, `InitialDelay = 0`, or the setting off).
- Directly constructed queues are checked by their provider's validator (first-party) only.
