# Message Queue Internal Documentation

## Architecture Overview

The MessageQueue library implements a provider-agnostic message queue abstraction with type safety, automatic retry logic, and dead letter queue support. The architecture follows several key design patterns to ensure maintainability, testability, and extensibility.

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│  IMessageQueue<T>  │  IMessageProcessor<T>  │  Extensions   │
├─────────────────────────────────────────────────────────────┤
│                   Provider Layer                            │
├─────────────────────────────────────────────────────────────┤
│  RabbitMQMessageQueue<T>  │  AzureServiceBusQueue<T>        │
├─────────────────────────────────────────────────────────────┤
│                   Infrastructure Layer                      │
├─────────────────────────────────────────────────────────────┤
│     RabbitMQ Client     │     Azure Service Bus SDK        │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Details

### Generic Type System

The library uses C# generics extensively to provide compile-time type safety:

```csharp
public interface IMessageQueue<T> where T : class
{
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
    Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default);
}
```

**Benefits:**
- Compile-time type checking prevents runtime serialization errors
- IntelliSense support for message properties
- Eliminates need for manual type casting
- Enables generic repository pattern for different message types

**Constraints:**
- `T` must be a reference type (`class` constraint)
- Messages must be JSON serializable
- Generic type information preserved at runtime for serialization

### Provider Abstraction Pattern

The library implements the Strategy pattern for message queue providers:

```csharp
public static IServiceCollection AddMessageQueue<T>(
    this IServiceCollection services,
    Action<MessageQueueConfig> configureOptions) where T : class
{
    var config = new MessageQueueConfig();
    configureOptions(config);
    
    return config.Provider.ToLower() switch
    {
        "rabbitmq" => services.AddSingleton<IMessageQueue<T>>(provider => 
            new RabbitMQMessageQueue<T>(config)),
        "azureservicebus" => services.AddSingleton<IMessageQueue<T>>(provider => 
            new AzureServiceBusMessageQueue<T>(config)),
        _ => throw new NotSupportedException($"Provider '{config.Provider}' is not supported")
    };
}
```

**Design Decisions:**
- Factory method pattern for provider instantiation
- Configuration-driven provider selection
- Singleton lifetime for connection pooling
- Extensible for additional providers

### RabbitMQ Implementation Deep Dive

RabbitMQ.Client 7 (async-only). Design records: ADR 0004 (dispatcher seam), 0006 (broker-side backoff),
0007 (client 7 and lazy initialization), 0008 (TLS and consumer shutdown).

| Type (`RabbitMQ/`) | Role |
|---|---|
| `RabbitMqMessageQueue<T>` | Provider: lazy connection, topology, publish, subscribe, `IDeliveryActions` |
| `RabbitMqMessageQueue.CreateAsync<T>` | Construct and connect in one call |
| `PublisherPool` | Publisher channels with the client's confirm tracking, round-robin, `ConfirmTimeout`, nack/return mapping |
| `RetryQueues` | Delay buckets (policy schedule steps, x3 for jitter; custom: two significant digits), retry queue names, arguments, `x-expires` |
| `BatchPublisher` | Batch publish window; awaits every started publish |
| `RabbitMqNames` | 255-byte name limits including derived queue names |
| `RabbitMqConnections` | Endpoint/credential/TLS rules (`RequireTls`, `Tls`) and the `ConnectionFactory` built from them |
| `RabbitMqConfig`, `RabbitMqTlsConfig` | Settings; validated by `RabbitMqConfigValidator` |

#### Connection and topology

The constructor validates only. `ConnectAsync` (behind `InitializeAsync`, the first publish or subscribe, and the
health probe) creates under a lock: one connection (automatic and topology recovery on), the publisher pool, and
a confirmed pool for retry and dead-letter copies (the same pool when `EnablePublisherConfirms` is on). It also
declares the exchange, the main queue, its binding and the dead-letter queue on a short-lived channel. Failures
are not cached. They throw `MessageQueueException` with the exception type only (no client text, which can echo
endpoints).

#### Publishing

`BasicPublishAsync` on a channel opened with `publisherConfirmationTrackingEnabled` completes when the broker
confirms and throws `PublishException` on a nack or a mandatory return. `PublisherPool` picks channels with
`RoundRobin.Next`, caps the wait at `ConfirmTimeout` and turns failures into `MessageQueueException`. Caller
cancellation stays an `OperationCanceledException`. Each channel's `MaxOutstandingConfirms` is a
`ConcurrencyLimiter` with an unbounded queue. Batches start every publish (bounded by the limiters) and await the
confirms together.

#### Consuming

A subscription owns a consumer channel created with `ConsumerDispatchConcurrency = MaxConcurrentCalls` and
`BasicQos(PrefetchCount)` on every queue type. `AsyncEventingBasicConsumer.ReceivedAsync` hands each delivery to
the shared `MessageDispatcher<T>` with `ea.Body` in place. The client returns its pooled buffer only after the
awaited handler completes, and every action that reads the body (retry and dead-letter copies) runs inside it.
`ToContext` maps the message id, correlation id and timestamp. It also maps the attempt: `x-delivery-attempt` +
(`x-delivery-count` or 1 if redelivered) — plus application headers only.

The delivery actions:

- `CompleteAsync`: ack.
- `RescheduleAsync(delay, next)`: a copy goes to `{queue}.retry.{bucket}` (TTL = bucket, dead-letters back to the
  queue), through the default exchange, mandatory and confirmed, with `x-delivery-attempt = next`. Then ack.
- `DeadLetterAsync(reason)`: a copy goes to the dead-letter queue with `x-ese-dead-letter-reason` / `-detail`. Then
  ack.

Every delivery is settled exactly once (`Settlement`; a safety net requeues anything left unsettled). An unreadable
delivery is dead-lettered as `DeliveryFailed`. If the stopping token cancels the handler it is nacked with requeue. A
failed transport action requeues after a backoff (100 ms, doubling, at most 30 s) with a throttled error log.

#### Shutdown

`UnsubscribeAsync`/`DisposeAsync`: cancel the subscription token, then `BasicCancelAsync`, then wait until nothing is
in flight (with a 100 ms quiet period), at most `ShutdownTimeout`. Cancelled handlers nack with requeue. Deliveries
arriving after the stop return through `{queue}.retry.0` with their attempt unchanged. On timeout the call returns,
and a background task closes the channel once the late handlers have settled. An AsyncLocal marks the running
handler, so a call from inside one runs the shutdown in the background. A new `SubscribeAsync` opens a new channel.

### Message Processing Pipeline

The hosted service implements the message processing pipeline:

```csharp
protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
    using var scope = _serviceProvider.CreateScope();
    var messageQueue = scope.ServiceProvider.GetRequiredService<IMessageQueue<T>>();
    var processor = scope.ServiceProvider.GetService<IMessageProcessor<T>>();
    
    if (processor != null)
    {
        await messageQueue.SubscribeAsync(async message =>
        {
            var context = new MessageContext
            {
                MessageId = Guid.NewGuid().ToString(),
                Timestamp = DateTime.UtcNow,
                DeliveryCount = 1
            };
            
            var result = await processor.ProcessAsync(message, context);
            return result == MessageResult.Acknowledge;
        }, stoppingToken);
    }
}
```

**Pipeline Features:**
- Dependency injection scope per message
- Message context creation
- Result mapping from enum to boolean
- Graceful shutdown support

### Azure Service Bus Implementation Deep Dive

ADRs 0009–0012 hold the decisions; `azure-service-bus-api.md` the user-facing behavior.

#### Seams

`AzureServiceBus/ServiceBusTransport.cs` defines thin internal interfaces over the SDK: `IServiceBusTransportFactory`
(builds the client from a connection string, or `FullyQualifiedNamespace` + `TokenCredential`), `IServiceBusTransport`
(the client), `IServiceBusSender`, `IServiceBusProcessor` (one per subscription), `IServiceBusDelivery` (the settlement
actions of `ProcessMessageEventArgs`) and `IServiceBusPeeker` (health). The SDK-backed classes only forward. Unit
specs drive the provider through `Support/FakeServiceBus.cs` and `ServiceBusModelFactory` messages, so no namespace
is needed; integration specs need `MQ_ASB_CONNECTION_STRING`.

#### Publishing

One `ServiceBusSender` per queue (the queue, or the topic in subscription mode) on the client's single AMQP
connection; every send is bounded by `SendTimeout`. Batches use `CreateMessageBatchAsync` + `TryAddMessage` and close
when full by bytes or at `MaxBatchSize` messages.

#### Consuming

`SubscribeAsync` creates and starts a processor (`AutoCompleteMessages = false`, peek-lock, `MaxConcurrentCalls`,
`PrefetchCount`, `MaxAutoLockRenewalDuration`). Each message: map metadata (`ServiceBusMessages.ToContext`, which
also computes the attempt) and read the body (failure → `MessageDispatcher.RejectUnreadableAsync`, dead-letter
`DeliveryFailed`), then `MessageDispatcher.DispatchAsync` with `DeliveryActions`:

- complete / dead-letter (reason code and exception type, never text) through `ServiceBusSettlement` (at most once);
- reschedule: queue mode sends `ServiceBusMessages.RetryCopy` with `ScheduledEnqueueTime` and completes the original
  in a `TransactionScope`; subscription mode waits under the lock if the delay fits, then abandons with the attempt
  properties.

A failing transport action backs off per message (`Processing/TransportBackoff`) and abandons; a lost lock is logged
and left to the broker; a safety net abandons anything unsettled. Shutdown cancels the handlers' token, stops the
processor (which waits for handlers) within `ShutdownTimeout`, and disposes it; calls from inside a handler finish
in the background (`Processing/HandlerScope`).

## Performance Optimizations

### Connection Pooling

- Single `ServiceBusClient` per message queue instance
- **Sender pool** for concurrent publishing with backpressure
- Channel/sender reuse for publishing operations
- Connection factory configuration for optimal settings

### Memory Management

```csharp
public void Dispose()
{
    _consumer?.Model?.Close();
    _channel?.Close();
    _connection?.Close();
    
    _channel?.Dispose();
    _connection?.Dispose();
}
```

**Resource Management:**
- Explicit disposal of RabbitMQ resources (the RabbitMQ provider is async: `DisposeAsync` closes the consumer
  channel, publisher channels and connection; `Dispose` blocks on it)
- Finalizer pattern for cleanup
- Channel and connection lifecycle management

### Serialization Performance

- System.Text.Json for high-performance serialization
- UTF-8 encoding without BOM
- Minimal allocations in hot path

### Quorum Queue Optimizations

```csharp
if (_config.UseQuorumQueues)
{
    properties.DeliveryMode = 2; // Persistent
    _channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);
    _channel.BasicReject(ea.DeliveryTag, false); // Instead of BasicNack
}
```

**Quorum Queue Benefits:**
- Better throughput for persistent messages
- Reduced memory usage with prefetch limits
- Optimized reject handling

## Error Handling Strategy

### Exception Hierarchy

```csharp
public class MessageQueueException : Exception
{
    public MessageQueueException(string message) : base(message) { }
    public MessageQueueException(string message, Exception innerException) : base(message, innerException) { }
}

public class TransientMessageException : MessageQueueException
{
    // Indicates retryable failures
}
```

**Exception Design:**
- Base exception for all message queue errors
- Transient exception for retry scenarios
- Inner exception preservation for debugging

### Failure Scenarios

1. **Connection Failures**: Automatic reconnection by RabbitMQ client; RabbitMQ connects lazily and retries the
   initial connection on the next call
2. **Serialization Failures**: Immediate dead letter routing
3. **Processing Failures**: Retry with exponential backoff
4. **Queue Declaration Failures**: Startup exception with clear message

## Testing Strategy

### Unit Testing

```csharp
public class MockMessageQueue<T> : IMessageQueue<T> where T : class
{
    public List<T> PublishedMessages { get; } = new();
    
    public Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        PublishedMessages.Add(message);
        return Task.CompletedTask;
    }
}
```

**Mock Implementation:**
- In-memory message storage
- Synchronous operations for testing
- State inspection capabilities

### Integration Testing

```csharp
[Collection("RabbitMQ Integration")]
public class RabbitMQIntegrationTests : IDisposable
{
    private readonly RabbitMQMessageQueue<TestMessage> _queue;
    
    public RabbitMQIntegrationTests()
    {
        var config = new MessageQueueConfig
        {
            ConnectionString = "amqp://admin:password123@localhost:5672/",
            QueueName = $"integration-test-{Guid.NewGuid():N}"
        };
        
        _queue = new RabbitMQMessageQueue<TestMessage>(config);
    }
}
```

**Integration Test Strategy:**
- Unique queue names per test
- Docker container for RabbitMQ
- Test collection for resource sharing
- Graceful skipping when infrastructure unavailable

## Future Enhancements

### Planned Features

1. **Azure Service Bus Provider**: Complete implementation
2. **Message Routing**: Topic-based message routing
3. **Batch Processing**: Efficient batch message handling
4. **Metrics Integration**: Prometheus and Application Insights
5. **Schema Evolution**: Message versioning support

### Extensibility Points

- `IMessageQueue<T>` interface for new providers
- `IMessageProcessor<T>` for custom processing logic
- Configuration system for provider-specific options
- Middleware pipeline for message processing

### Performance Improvements

- Connection pooling across multiple queues
- Async enumerable for batch processing
- Memory-mapped files for large messages
- Compression for message payloads

## Troubleshooting Guide

### Common Issues

1. **Connection Refused**: Check RabbitMQ service status and credentials
2. **Queue Not Found**: Verify queue declaration and permissions
3. **Serialization Errors**: Ensure message types are JSON serializable
4. **Memory Leaks**: Verify proper disposal of message queue instances

### Debugging Tips

- Enable RabbitMQ management plugin for queue inspection
- Use structured logging for message tracing
- Monitor dead letter queues for failed messages
- Check delivery count headers for retry analysis

### Performance Monitoring

```csharp
_logger.LogInformation("Published {MessageType} to {QueueName} in {Duration}ms",
    typeof(T).Name, _queueName, stopwatch.ElapsedMilliseconds);
```

**Key Metrics:**
- Message publish/consume rates
- Queue depth and growth trends
- Processing latency percentiles
- Error rates and retry counts