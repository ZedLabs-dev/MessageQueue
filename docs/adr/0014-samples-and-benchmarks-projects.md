# 0014. Samples and benchmarks live outside the library

- Status: Accepted (2026-10-17)
- Context: OPTIMIZATION-PLAN §4, §4.7, Phase 6; ADR 0013 (telemetry)

## Context

`MessageQueue/Examples/` shipped sample classes inside the library assembly, and §4's rule ("no optimization without
numbers") had no benchmark project to measure with. The plan forbids the library from choosing exporters.

## Decision

- **`samples/ESE.MessageQueue.Samples`**: a generic-host worker in the solution. It runs `AddMessageQueue<T>` from
  `appsettings.json` (in-memory by default; `--environment RabbitMq` or `AzureServiceBus` switches the broker), a
  processor showing Acknowledge / Retry / DeadLetter, and an OpenTelemetry console exporter. Only the sample references
  `OpenTelemetry.Extensions.Hosting` and `OpenTelemetry.Exporter.Console` (CPM); the library still uses only
  `System.Diagnostics`. No credentials are committed. A Unit spec boots `SampleHost.CreateBuilder` and waits for one
  processed message, so a broken sample fails `dotnet test`. CI builds it with the solution.
- **`benchmarks/ESE.MessageQueue.Benchmarks`**: BenchmarkDotNet with `[MemoryDiagnoser]`. It is not a test project:
  `dotnet test` never runs it, and CI only builds it. It measures serializer round trips (JSON and MessagePack),
  dispatcher overhead per message (no telemetry, an unobserved provider, recorded spans), in-memory publish-to-consume
  throughput, and RabbitMQ publisher confirms (one by one vs batch). The broker comes from `MQ_BENCH_RABBITMQ` or a
  Testcontainers container started by `Program` and shared with BenchmarkDotNet's child processes. Without Docker
  those benchmarks are left out with a message. Results and machine info go in `docs/benchmarks.md`.
- **No RabbitMQ.Client 6 comparison:** the 6.x provider was replaced in Phase 5. Rebuilding it next to 7.x would need a
  second assembly with a conflicting package version, so only the current numbers are recorded.

## Consequences

- The library package no longer contains sample types (`ESE.MessageQueue.Examples` removed).
- The test project references the sample (and so its OpenTelemetry packages) for the smoke test only.
- Benchmark numbers come from short runs (`--job short`): they are indicative, not release-grade.
