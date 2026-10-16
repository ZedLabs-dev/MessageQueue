# 0002. Provider-agnostic configuration and options validation

- Status: Accepted (2026-10-16)
- Context: OPTIMIZATION-PLAN D4, §3a.3, B14, B16; refactoring instructions §5

## Context

The base `MessageQueueConfig` mixed portable and broker settings (`ConnectionString`, `DeadLetterQueueName`), ASB had
four settings nothing read, and `AddMessageQueue<T>` bound a missing `Provider` to enum value 0, silently selecting
RabbitMQ. Validation was hand-written `if`/`throw` at registration time, and only one queue per container worked well.

## Decision

- Base config holds only portable settings: `QueueName`, `Retry`, `SerializationFormat`, `EnableDeadLetterQueue`,
  `MaxMessageSizeBytes` (default 1 MB) and `MaxConcurrentCalls` (default 10), plus `const SectionName = "MessageQueue"`.
- `ConnectionString` moves to `RabbitMqConfig` and `AzureServiceBusConfig`; `InMemoryConfig` has none.
  `DeadLetterQueueName` moves to `RabbitMqConfig` (ASB uses its fixed `$deadletterqueue`).
  `EnablePartitioning`, `RequiresDuplicateDetection`, `DuplicateDetectionHistoryTimeWindow` and `BatchFlushInterval`
  are removed: entity shape belongs to infrastructure-as-code, and idempotency is `PublishOptions.MessageId`.
- Config records follow the house pattern: no XML comments, `[Required]`/`[Range]`, one `IValidateOptions<T>` per
  provider config (`RabbitMqConfigValidator`, `AzureServiceBusConfigValidator`, `InMemoryConfigValidator`) for rules
  attributes cannot express (retry sanity, timeouts > 0, ASB `SubscriptionName` required with `TopicName`).
- Registration: `AddOptions<TConfig>(name).Bind(section).ValidateDataAnnotations().ValidateOnStart()`. Options are
  **named by message type** (`typeof(T).FullName`), so `AddMessageQueue<Order>(cfg, "Orders")` and
  `AddMessageQueue<Payment>(cfg, "Payments")` coexist, each with its own provider.
- `Provider` is read at registration (it decides which config type to bind). It must be one of the enum's names
  (trimmed, case-insensitive); missing values, numbers and combinations throw `ConfigurationException`. There is no
  default broker. `SerializationFormat` and `Retry:Strategy` get the same name-only check at startup.
- Binding uses `ErrorOnUnknownConfiguration`, so removed or misspelled keys (`RetryAttempts`) fail at startup.
  Registering a second queue for the same `T` throws. Public provider constructors run the same validation.
  Config `ToString()` redacts `ConnectionString`.
- Queues are singletons created by the container and disposed by it; `IMessagePublisher<T>` and
  `IMessageSubscriber<T>` resolve to the same instance. Every provider's dispose is idempotent.

## Consequences

- Invalid settings now fail at host start (`OptionsValidationException`) instead of at `AddMessageQueue`.
- Apps that relied on RabbitMQ being the default must set `Provider` explicitly.
