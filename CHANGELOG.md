# Changelog

All notable changes to ESE.MessageQueue are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Breaking:** retargeted from `net9.0` to `net10.0`. Consumers must target .NET 10.
- **Behavior:** MessagePack deserialization now uses `MessagePackSecurity.UntrustedData`.
  Dictionaries and hash sets are rebuilt with hash-collision-resistant comparers, and object graphs
  deeper than 500 levels are rejected. Message types whose dictionary/hash-set keys are custom types
  without a supported collision-resistant hash may now fail to deserialize with
  `MessagePackSerializationException`.
- **Source-breaking:** `InMemoryMessageQueue<T>.SubscribeAsync` parameter renamed from `messageHandler`
  to `handler` to match `IMessageQueue<T>`. Only callers that pass the argument by name are affected.
- MessagePack 3.1.4 → 3.1.12 (fixes known high/moderate advisories).
- Azure.Messaging.ServiceBus 7.20.1 → 7.21.0.
- Microsoft.Extensions.Diagnostics.HealthChecks 9.0.4 → 10.0.13.
- Local development compose file: RabbitMQ 4.3, real Azure Service Bus emulator (replaces Azurite),
  credentials from `MessageQueue/.env`.

### Added
- Explicit references to Microsoft.Extensions.Configuration.Binder, DependencyInjection.Abstractions,
  Hosting.Abstractions, Logging.Abstractions and Options (10.0.13). These were previously only transitive.

### Removed
- Unused package references that leaked into consumers: Microsoft.AspNetCore.OpenApi, OpenTelemetry,
  OpenTelemetry.Api (vulnerable), OpenTelemetry.Extensions.Hosting,
  OpenTelemetry.Exporter.OpenTelemetryProtocol, OpenTelemetry.Exporter.Prometheus.AspNetCore.
  Telemetry still uses `System.Diagnostics` `ActivitySource`/`Meter`; hosts choose their own exporters.
  Apps that relied on these packages arriving transitively must reference them directly.
- Stale `MessageQueue/scripts/*.sh` (referenced docker-compose v1, RabbitMQ 3 and a non-existent test project).
