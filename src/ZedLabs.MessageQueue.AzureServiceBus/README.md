# ZedLabs.MessageQueue.AzureServiceBus

Azure Service Bus provider for [ZedLabs.MessageQueue](https://www.nuget.org/packages/ZedLabs.MessageQueue).

- Queues, or topic subscriptions (`TopicName` + `SubscriptionName`).
- Entra ID by default: `FullyQualifiedNamespace` with a registered `TokenCredential` or `DefaultAzureCredential`
  (Data Sender / Data Receiver roles); SAS connection strings are also accepted.
- Retries on queues are scheduled copies (broker-side backoff); byte-aware batch publishing.
- `AddAzureServiceBusHealthCheck<T>()` peeks with the queue's own client (Listen rights only).

```csharp
services.AddAzureServiceBusProvider();
services.AddMessageQueue<OrderCreated>(configuration);
services.AddHealthChecks().AddAzureServiceBusHealthCheck<OrderCreated>();
```

```json
{ "MessageQueue": { "Provider": "AzureServiceBus", "FullyQualifiedNamespace": "contoso.servicebus.windows.net", "QueueName": "orders" } }
```

This is a release candidate: the integration specs have not yet run against a real Service Bus namespace.
Settings, behavior and decision records: https://github.com/ZedLabs/MessageQueue
