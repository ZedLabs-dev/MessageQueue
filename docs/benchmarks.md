# Benchmarks

`benchmarks/ESE.MessageQueue.Benchmarks` (BenchmarkDotNet, `[MemoryDiagnoser]`; ADR 0014). It is not a test project,
so `dotnet test` never runs it; CI only builds it. Run it in Release:

```bash
dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*' --job short
dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*Serializer*'   # one class
MQ_BENCH_RABBITMQ=amqp://user:pass@host:5672/ dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*RabbitMq*' --job short
```

Without `MQ_BENCH_RABBITMQ`, `Program` starts a `rabbitmq:4.3.6-management` container with Testcontainers and removes
it at the end. Without Docker, the RabbitMQ benchmarks are left out and the others still run.

## Results (2026-10-17, after the Phase 6 review fixes)

ShortRun job (3 warm-up, 3 measured iterations, 1 launch): indicative numbers, with wide error bars.

- **Machine:** Apple M4 Pro, 12 cores, macOS Tahoe 26.7.2.
- **Runtime:** .NET SDK 10.0.300, runtime 10.0.8 (Arm64 RyuJIT), BenchmarkDotNet 0.15.8.
- **Broker:** RabbitMQ 4.3.6 (classic queue) in Docker Desktop 29.8.2 on the same machine. The loopback round trip
  makes the one-by-one case optimistic compared with a network broker.

| Benchmark | Case | Mean | Allocated |
|---|---|---:|---:|
| Serializer `Serialize` (order, ~400 B JSON) | JSON | 335 ns | 616 B |
| Serializer `Deserialize` | JSON | 802 ns | 1,432 B |
| Serializer `Serialize` | MessagePack | 233 ns | 240 B |
| Serializer `Deserialize` | MessagePack | 405 ns | 464 B |
| Dispatcher, per message (baseline: deserialize + handler, ~750 ns / 1.4 KB) | no telemetry | 784 ns (+~25 ns) | 1.43 KB |
| | `OpenTelemetryProvider`, nothing listening | 811 ns (+~65 ns) | 1.46 KB |
| | spans and metrics recorded (ActivityListener + MeterListener) | 1,109 ns (+~0.37 µs) | 2.74 KB |
| In-memory publish → consume → ack, per message (10,000 per op) | `MaxConcurrentCalls = 1` | 3.0 µs | 2.31 KB |
| | `MaxConcurrentCalls = 10` | 3.0 µs | 2.31 KB |
| RabbitMQ publish with confirms, per message (500 per op) | one by one (`await` each) | 370 µs | 2.95 KB |
| | batch `PublishAsync(IEnumerable<T>)` | 10.4 µs | 3.08 KB |

### Before and after the Phase 6 review fixes

These are the hot-path changes. A provider with no listener now parses, formats and tags nothing. Without a provider,
publishes share one no-op. Guards use static lambdas with explicit state, and trace headers travel in a two-field
carrier instead of a dictionary. The "recorded" row now also has a `MeterListener`, so it measures spans **and**
metrics, where before it measured spans only.

| Path | Before | After |
|---|---:|---:|
| Dispatcher, no telemetry | 821 ns / 1.81 KB | 784 ns / 1.43 KB |
| Dispatcher, provider nothing listens to | 1,002 ns / 2.36 KB | 811 ns / 1.46 KB |
| Dispatcher, recorded (before: spans only; after: spans + metrics) | 1,186 ns / 3.09 KB | 1,109 ns / 2.74 KB |
| In-memory publish → ack (`MaxConcurrentCalls = 1` / `10`) | 3.3 / 3.7 µs, 2.77 KB | 3.0 / 3.0 µs, 2.31 KB |
| RabbitMQ one by one / batch | 340 / 11.0 µs | 370 / 10.4 µs (same within short-run error; loopback broker) |

## Reading the numbers

- **§4.1 serializer:** JSON serializes straight to UTF-8 bytes and deserializes from a span, so the only allocations
  are the output array and the object graph. MessagePack is about 2x faster and allocates about a third as much.
- **Dispatcher (ADR 0004):** the shared pipeline adds about 25 ns and 30 bytes per message without telemetry, and
  about 65 ns when a provider is registered but nothing listens. Recording spans and metrics costs about 0.37 µs and
  1.3 KB per message, mostly the `Activity` and its tags. That is small next to any broker round trip.
- **In-memory:** about 330,000 messages/s through the full pipeline (publish, background dispatch, acknowledge). More concurrency does not help a
  handler that does no work; it helps handlers that wait on I/O.
- **§4.2 RabbitMQ batch confirms:** publishing a batch and awaiting the confirms together is about 31x faster per
  message than awaiting each confirm in turn (about 95,000 vs 2,700 messages/s on loopback).
- **RabbitMQ.Client 6.x comparison: not run.** The 6.x provider was replaced in Phase 5 (ADR 0007). Measuring it would
  mean rebuilding the old provider in a separate assembly against a conflicting RabbitMQ.Client version. These are
  the 7.x numbers only. The Phase 5 exit criterion "benchmark at least matches 6.x" is therefore unverified;
  the batch path is no longer bound by one confirm round trip per message, which was 6.x's limit.
