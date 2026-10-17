# ESE.MessageQueue.RabbitMQ

RabbitMQ provider for [ESE.MessageQueue](https://www.nuget.org/packages/ESE.MessageQueue), on RabbitMQ.Client 7.

- Publisher confirms (batches confirmed together), quorum or classic queues, prefetch always applied.
- Real backoff on the broker: retries wait in per-delay retry queues, then return; a dead-letter queue after the last
  retry.
- TLS required by default for non-loopback hosts (`RequireTls`, `Tls:*`); credentials never appear in exceptions.
- `AddRabbitMqHealthCheck<T>()` reuses the queue's connection.

```csharp
services.AddRabbitMqProvider();
services.AddMessageQueue<OrderCreated>(configuration);
services.AddHealthChecks().AddRabbitMqHealthCheck<OrderCreated>();
```

```json
{ "MessageQueue": { "Provider": "RabbitMq", "ConnectionString": "amqps://broker.example.com", "QueueName": "orders" } }
```

Settings, behavior and decision records: https://github.com/ericsewing/MessageQueue
