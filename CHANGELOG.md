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

### Development
Test infrastructure only; no change to the library or its public API.
- Tests migrated to xunit.v3 4.0.2 (run through VSTest with xunit.runner.visualstudio 4.0.1) and from
  FluentAssertions 8 to AwesomeAssertions 9.6.0 (Apache-2.0).
- No more vacuous passes: the no-op `Skip.cs` and the `Helper.IsRabbitMqAvailable` early returns are gone. RabbitMQ
  integration tests run against a `rabbitmq:4.3.6-management` container started by Testcontainers and report
  **skipped** when Docker is unavailable (`MQ_REQUIRE_DOCKER=true` makes that a failure). Azure Service Bus integration
  tests run only when `MQ_ASB_CONNECTION_STRING` is set (replaces `AZURE_SERVICEBUS_CONNECTION_STRING`) and are
  skipped otherwise.
- Every test class carries `[Trait("Category","Unit")]` or `[Trait("Category","Integration")]`;
  `dotnet test --filter Category=Unit` is the fast loop.
- Tests reorganized into one folder per area and one behavior-named class per file (`Given_a_...`), and
  `TelemetryTests.cs` split by class under test. The cleanup method that ran as a `[Fact]` is now `IAsyncLifetime`
  cleanup. Hardcoded broker credentials removed from the tests.
- Coverage gate: `scripts/coverage.sh` (coverlet + ReportGenerator local tool) fails CI below 59% line coverage of
  `ESE.MessageQueue`. CI now runs unit and integration tests as separate steps.
