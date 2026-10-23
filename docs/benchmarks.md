# Benchmarks

`benchmarks/ESE.MessageQueue.Benchmarks` (BenchmarkDotNet, `[MemoryDiagnoser]`; ADR 0014). It is not a test project,
so `dotnet test` never runs it; CI only builds it. Run it in Release:

```bash
dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*'                 # release-grade (default job, ~5 min)
dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*' --job short     # quick look
dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*Serializer*'      # one class
MQ_BENCH_RABBITMQ=amqp://user:pass@host:5672/ dotnet run -c Release --project benchmarks/ESE.MessageQueue.Benchmarks -- --filter '*RabbitMq*'
```

Without `MQ_BENCH_RABBITMQ`, `Program` starts the RabbitMQ image pinned in `docker/test-broker/Dockerfile` (tag and
digest, the same as the integration tests) with Testcontainers and removes it at the end. Without Docker, the RabbitMQ
benchmarks are left out and the others still run.

## Results (2026-10-22, default job)

BenchmarkDotNet's default job: pilot and warm-up stages, then as many measured iterations as it needs for a stable
mean (up to 100). The whole run (14 benchmarks) took 4 min 21 s. These are the release-grade numbers.

- **Machine:** Apple M4 Pro, 12 cores, macOS Tahoe 26.7.2 (25G330).
- **Runtime:** .NET SDK 10.0.300, runtime 10.0.8 (Arm64 RyuJIT armv8.0-a), BenchmarkDotNet 0.15.8, Release build of
  commit `11e3b29` (the library code is unchanged since).
- **Broker:** RabbitMQ 4.3.6 (classic queue, no dead-letter queue, publisher confirms) in Docker Desktop 29.8.2 on the
  same machine. The loopback round trip makes the one-by-one case optimistic compared with a broker over a network;
  a quorum queue (the provider's default) adds replication latency to every confirm.

| Benchmark | Case | Mean | Error (99.9%) | Allocated |
|---|---|---:|---:|---:|
| Serializer `Serialize` (order, ~400 B JSON) | JSON | 324.5 ns | ±0.8 ns | 616 B |
| Serializer `Deserialize` | JSON | 791.8 ns | ±1.8 ns | 1,432 B |
| Serializer `Serialize` | MessagePack | 233.9 ns | ±2.1 ns | 240 B |
| Serializer `Deserialize` | MessagePack | 396.6 ns | ±1.3 ns | 464 B |
| Dispatcher, per message (baseline: deserialize + handler, 751-774 ns / 1.4 KB) | no telemetry | 804.6 ns (+54 ns) | ±2.6 ns | 1.43 KB |
| | `OpenTelemetryProvider`, nothing listening | 822.8 ns (+49 ns) | ±2.3 ns | 1.46 KB |
| | spans and metrics recorded (ActivityListener + MeterListener) | 1,147.0 ns (+0.38 µs) | ±3.1 ns | 2.74 KB |
| In-memory publish → consume → ack, per message (10,000 per op) | `MaxConcurrentCalls = 1` | 2.00 µs (median 1.31 µs) | ±0.34 µs | 2.31 KB |
| | `MaxConcurrentCalls = 10` | 2.08 µs (median 1.30 µs) | ±0.38 µs | 2.31 KB |
| RabbitMQ publish with confirms, per message (500 per op) | one by one (`await` each) | 372.8 µs | ±5.4 µs | 2.95 KB |
| | batch `PublishAsync(IEnumerable<T>)` | 10.86 µs | ±0.08 µs | 3.09 KB |

The dispatcher's "+" is the difference to the baseline measured in the same job. The in-memory benchmark runs one
invocation per iteration (`InvocationCount = 1`) and its distribution is multimodal (background dispatch and the
thread pool), so its mean sits well above its median; read it as "1.3 to 2 µs".

### RabbitMQ consumption (2026-10-23, default job)

`RabbitMqConsumeBenchmarks`: 1,000 messages wait in the queue (published in `[IterationSetup]`, not measured); one
operation subscribes (a channel and `basic.consume`, spread over the 1,000) and consumes them all through the full
pipeline (deserialize, handler, ack), so the mean is per message. `PrefetchCount = 64`. `HandlerDelayMs = 1` stands for a handler waiting on I/O. Same machine, runtime and
broker as above (classic queue, no dead-letter queue); run of 1 min 14 s for the four cases, on the working tree after
commit `2a3b033`.

| `MaxConcurrentCalls` | Handler | Mean per message | Error (99.9%) | Allocated | Throughput |
|---:|---|---:|---:|---:|---:|
| 1 | no work | 19.56 µs | ±0.91 µs | 2.96 KB | about 51,000/s |
| 16 | no work | 19.38 µs | ±0.66 µs | 3.31 KB | about 52,000/s |
| 1 | 1 ms `Task.Delay` | 1,308.95 µs | ±7.00 µs | 4.21 KB | about 760/s |
| 16 | 1 ms `Task.Delay` | 85.52 µs | ±1.45 µs | 4.09 KB | about 11,700/s |

### Default job compared with the earlier ShortRun

| Benchmark | ShortRun (2026-10-17) | Default job (2026-10-22) |
|---|---:|---:|
| JSON serialize / deserialize | 335 / 802 ns | 325 / 792 ns |
| MessagePack serialize / deserialize | 233 / 405 ns | 234 / 397 ns |
| Dispatcher: none / unobserved / recorded | 784 / 811 / 1,109 ns | 805 / 823 / 1,147 ns |
| In-memory, `MaxConcurrentCalls` 1 / 10 | 3.0 / 3.0 µs | 2.0 / 2.1 µs (median 1.3) |
| RabbitMQ one by one / batch | 370 / 10.4 µs | 373 / 10.9 µs |

Within the ShortRun's error bars everywhere except the in-memory path, where three iterations could not average out
the multimodal distribution. Allocations are identical.

## Earlier results (2026-10-17, after the Phase 6 review fixes)

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
  are the output array and the object graph. MessagePack is 1.4x (serialize) to 2x (deserialize) faster and allocates
  about a third as much.
- **Dispatcher (ADR 0004):** the shared pipeline adds about 50 ns and 30 bytes per message without telemetry, and
  about the same when a provider is registered but nothing listens. Recording spans and metrics costs about 0.38 µs
  and 1.3 KB per message, mostly the `Activity` and its tags. That is small next to any broker round trip.
- **In-memory:** about 500,000 to 750,000 messages/s through the full pipeline (publish, background dispatch,
  acknowledge; mean 2.0 µs, median 1.3 µs). More concurrency does not help a handler that does no work; it helps
  handlers that wait on I/O.
- **§4.2 RabbitMQ batch confirms:** publishing a batch and awaiting the confirms together is about 34x faster per
  message than awaiting each confirm in turn (about 92,000 vs 2,700 messages/s on loopback).
- **RabbitMQ consumption:** about 51,000 messages/s per consumer when the handler does no work, whatever
  `MaxConcurrentCalls` is: the pipeline and the client's dispatch are the limit, not handler slots. With a handler
  that waits 1 ms, 16 concurrent handlers consume 15x faster than one (85.5 vs 1,309 µs per message); the ideal 16x is
  lost to dispatch and timer granularity. Raise `MaxConcurrentCalls` (and `PrefetchCount` with it) for I/O-bound
  handlers, not for CPU-bound ones.
- **Not measured:** Azure Service Bus (no namespace is available to the project) and a broker over a real network.
- **RabbitMQ.Client 6.x comparison: not run.** The 6.x provider was replaced in Phase 5 (ADR 0007). Measuring it would
  mean rebuilding the old provider in a separate assembly against a conflicting RabbitMQ.Client version. These are
  the 7.x numbers only. The Phase 5 exit criterion "benchmark at least matches 6.x" is therefore unverified;
  the batch path is no longer bound by one confirm round trip per message, which was 6.x's limit.
