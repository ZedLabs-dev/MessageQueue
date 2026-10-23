# ESE.MessageQueue

Provider-agnostic event messaging for .NET 10. Applications code against `IMessageQueue<T>`
(`IMessagePublisher<T>` + `IMessageSubscriber<T>`); RabbitMQ, Azure Service Bus and an in-memory queue are
interchangeable providers chosen in configuration. Every provider runs deliveries through one pipeline: size limit,
deserialization, your handler, then retry with real backoff and dead-lettering.

> Status: `1.0.0-rc.1`. The Azure Service Bus integration specs have not yet run against a real namespace, which
> blocks 1.0.0 (see the [CHANGELOG](CHANGELOG.md)).

## Packages

| Package | Use it for |
|---|---|
| `ESE.MessageQueue` | Abstractions, pipeline, hosting, telemetry, the in-memory provider, and the provider SPI |
| `ESE.MessageQueue.RabbitMQ` | RabbitMQ (RabbitMQ.Client 7) |
| `ESE.MessageQueue.AzureServiceBus` | Azure Service Bus (Entra ID or connection string) |

All three share one version; upgrade them together.

## Quick start

```csharp
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;

builder.Services.AddRabbitMqProvider();                                   // before AddMessageQueue<T>; InMemory is built in
builder.Services.AddMessageQueue<OrderCreated>(builder.Configuration);    // section "MessageQueue"
builder.Services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
builder.Services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
builder.Services.AddMessageQueueTelemetry("MyApp.Messaging");             // optional: spans and metrics
```

```json
{
  "MessageQueue": {
    "Provider": "RabbitMq",
    "ConnectionString": "amqps://broker.example.com",
    "QueueName": "order-created",
    "Retry": { "MaxRetries": 5, "InitialDelay": "00:00:02", "MaxDelay": "00:00:30" }
  }
}
```

Publish with `IMessagePublisher<OrderCreated>.PublishAsync(order)`; the processor returns `Acknowledge`, `Retry` or
`DeadLetter`. A runnable worker is in [samples/ESE.MessageQueue.Samples](samples/ESE.MessageQueue.Samples); the
[getting-started guides](docs/getting-started/in-memory.md) walk through it per provider.

## Documentation

Everything is indexed in [docs/](docs/README.md).

- Getting started: [in-memory](docs/getting-started/in-memory.md), [RabbitMQ](docs/getting-started/rabbitmq.md),
  [Azure Service Bus](docs/getting-started/azure-service-bus.md)
- [Configuration reference](docs/configuration.md): every setting, default and startup error
- Operations: [failure modes](docs/operations/failure-modes.md), [monitoring](docs/operations/monitoring.md),
  [alerts](docs/operations/alerts.md), [runbooks](docs/operations/runbooks.md),
  [dead-letter operations](docs/dead-letter-operations.md)
- [API](docs/message-queue-api.md), [Azure Service Bus](docs/azure-service-bus-api.md),
  [internals](docs/message-queue-internal.md), [benchmarks](docs/benchmarks.md)
- [Release checklist](docs/release-checklist.md), [production readiness status](docs/production-readiness-status.md)
- [Architecture decision records](docs/adr/), including the [provider SPI](docs/adr/0015-provider-spi-and-package-split.md)
  for writing a provider for another broker
- [CHANGELOG](CHANGELOG.md), [roadmap](OPTIMIZATION-PLAN.md), contributor rules in [CLAUDE.md](CLAUDE.md)

## Building

```bash
dotnet build ESE.MessageQueue.sln -warnaserror
dotnet test ESE.MessageQueue.sln --filter Category=Unit     # no Docker needed
scripts/coverage.sh                                         # full suite; RabbitMQ via Testcontainers (Docker)
```

MIT licensed; see [LICENSE](LICENSE).
