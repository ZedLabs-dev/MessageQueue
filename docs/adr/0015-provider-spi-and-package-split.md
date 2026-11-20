# 0015. Provider SPI and the three-package split

- Status: Accepted (2026-10-17); implements ADR 0005
- Context: OPTIMIZATION-PLAN Phase 7, §3a; ADR 0004 (dispatcher seam), ADR 0005 (packaging)
- Renamed 2026-11-20 (ADR 0016 amendment): the packages, namespaces and the loopback sample were `ESE.MessageQueue*`
  when this was decided; they are now `ZedLabs.MessageQueue*`.

## Decision

- **Packages:** `src/ZedLabs.MessageQueue` (abstractions, pipeline, retry, serialization, hosting, telemetry, generic
  health check, options validation, in-memory provider), `src/ZedLabs.MessageQueue.RabbitMQ`,
  `src/ZedLabs.MessageQueue.AzureServiceBus`. Core references no broker SDK (`Packaging/Given_the_package_split`).
  Namespaces are unchanged (`…RabbitMQ` keeps the product's spelling; types use `RabbitMq`).
- **Registration:** `IMessageQueueProvider { Name; AddQueue<T>(MessageQueueRegistration<T>) }`, registered with
  `services.AddMessageQueueProvider(p)` (`AddRabbitMqProvider()`, `AddAzureServiceBusProvider()`); `InMemory` is built
  in. `AddMessageQueue<T>` resolves `Provider` (trimmed, case-insensitive) at registration, so providers are
  registered first; a missing or unknown name throws `ConfigurationException` listing the registered names. The same
  provider type twice is a no-op; another type claiming a taken name (or `InMemory`) throws.
  `MessageQueueRegistration<T>.AddQueue<TConfig, TValidator, TQueue>` does the binding in core for every provider:
  strict binding, enum-name checks, DataAnnotations, the base rules, the provider validator, `ValidateOnStart`,
  aliases. `MessageQueueProvider` (the enum) is gone; `MessageQueueConfig.Provider` is a string.
- **Public SPI** (namespace `ZedLabs.MessageQueue.Providers`, PublicAPI-locked): `MessageDispatcher<T>`,
  `IDeliveryActions`, `DeadLetterReason`, `DispatchOutcome`, `PublishTelemetry`, `PublishValidation`,
  `MessageSerializer`, `MessageQueueConfigValidator<TConfig>`, `MessageContextExtensions.WithTraceHeaders`,
  `QueueFactoryContext<TConfig>`; `Telemetry.TraceHeaders` names the trace headers. That is enough for a provider with
  the same retry, dead-letter, validation and tracing behavior as ours.
- **Internals stay internal** (`HandlerScope`, `TransportBackoff`, `LogThrottle`, `BackgroundWork`, `Cancellation`,
  `SafeLogger`, `RetryExecutor`, `MessagingSystems`, settlement helpers): shutdown and backoff conveniences, not a
  contract. First-party providers get them through `InternalsVisibleTo`, and because internals carry no compatibility
  promise, the provider packages depend on the **exact** core version (`[1.0.0-rc.1]`, a target in
  `src/Directory.Build.targets` rewrites the ProjectReference range at pack time).
- **Proof:** `samples/ZedLabs.MessageQueue.Samples.LoopbackProvider` is built against the public SPI only (no IVT); the
  unit specs in `Providers/Given_a_third_party_provider` select it by name and check retry, dead-lettering, tracing,
  strict binding and shared validation through it.
- **Tests:** one project (`tests/ZedLabs.MessageQueue.Tests`) for all three packages: fixtures, the sample smoke test and
  the coverage merge are shared; Unit/Integration traits unchanged. Coverage: 87% across the three, plus a floor each.

## Consequences

- Breaking for RabbitMQ / Service Bus apps: add the package and call its `Add…Provider()` before `AddMessageQueue<T>`.
- SPI additions are API changes (PublicAPI files, CHANGELOG); first-party providers ship in lockstep with core.
