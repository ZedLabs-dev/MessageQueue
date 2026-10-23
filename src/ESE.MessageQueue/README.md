# ESE.MessageQueue

Provider-agnostic event messaging for .NET 10. Applications code against `IMessageQueue<T>`
(`IMessagePublisher<T>` + `IMessageSubscriber<T>`); the broker is chosen in configuration.

This package contains the abstractions, the shared consumer pipeline (size limit, deserialization, retry with
backoff, dead-lettering), JSON and MessagePack serialization, OpenTelemetry-compatible spans and metrics
(`System.Diagnostics` only, no exporter), a hosted consumer service with a DI scope per message, and the
**in-memory provider** (single process, not durable). Brokers come as separate packages:

- `ESE.MessageQueue.RabbitMQ`
- `ESE.MessageQueue.AzureServiceBus`

```csharp
services.AddRabbitMqProvider();                      // from ESE.MessageQueue.RabbitMQ; InMemory is built in
services.AddMessageQueue<OrderCreated>(configuration); // section "MessageQueue", Provider = "RabbitMq" | "InMemory" | ...
services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
services.AddMessageQueueTelemetry("MyApp.Messaging");  // optional
```

```json
{ "MessageQueue": { "Provider": "InMemory", "QueueName": "orders", "Retry": { "MaxRetries": 5 } } }
```

Dead letters can be inspected, replayed and purged through `IDeadLetterQueue<T>` (resolved next to `IMessageQueue<T>`;
see `Capabilities.DeadLetterOperations`).

Writing a provider for another broker: implement `IMessageQueueProvider` and hand every delivery to
`MessageDispatcher<T>` with an `IDeliveryActions` (namespace `ESE.MessageQueue.Providers`). See
[ADR 0015](https://github.com/ericsewing/MessageQueue/blob/main/docs/adr/0015-provider-spi-and-package-split.md) and
the [loopback sample provider](https://github.com/ericsewing/MessageQueue/tree/main/samples/ESE.MessageQueue.Samples.LoopbackProvider).

Documentation, decision records and the changelog: https://github.com/ericsewing/MessageQueue
