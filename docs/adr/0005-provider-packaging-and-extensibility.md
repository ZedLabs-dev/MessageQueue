# 0005. Provider packaging and extensibility

- Status: Accepted (2026-10-16); implementation scheduled for Phase 7 (before the v1 API lock), not now
- Context: OPTIMIZATION-PLAN §3a (AWS SNS/SQS and MQTT may follow), §6.2 PublicApiAnalyzers, Phase 7

## Context

`ESE.MessageQueue` ships one assembly that references RabbitMQ.Client and Azure.Messaging.ServiceBus. Every consumer
pulls both SDKs (and their vulnerability surface) even when it uses one broker or only the in-memory provider, and a
new provider can only be added by changing `AddMessageQueue<T>`'s `switch` and the `MessageQueueProvider` enum.

## Decision

Before the v1 API is locked, split into packages:

- **ESE.MessageQueue**: abstractions, `MessageDispatcher<T>` and the `IDeliveryActions` seam, configuration base and
  validation, retry, serialization, the in-memory provider, hosting and telemetry. No broker SDK.
- **ESE.MessageQueue.RabbitMQ** and **ESE.MessageQueue.AzureServiceBus**: one provider each, with its config,
  validator, health check and registration extension.
- A **public provider-registration extension point** in the core: a provider registers itself under a name
  (`Provider` stays a string in configuration) together with its config type, validator and queue factory. The
  core resolves the configured name against the registered providers and fails fast when none matches, so
  SNS/SQS or MQTT can ship as separate packages without core changes.
- The dispatcher seam becomes public (or exposed through a provider SDK surface) only as far as an external provider
  needs it; its shape is fixed by ADR 0004.

## Consequences

- `MessageQueueProvider` (the enum) is replaced by provider names; `AddMessageQueue<T>` keeps its signature.
- Apps add the package for the broker they use; the in-memory provider needs nothing extra.
- Until Phase 7 the single assembly stays, and new provider code keeps to the seam so the split is a move, not a rewrite.

## Later changes

- Implemented in Phase 7 by ADR 0015. `MessageQueueProvider` is gone and the split is done; the "until Phase 7"
  consequence no longer applies.
