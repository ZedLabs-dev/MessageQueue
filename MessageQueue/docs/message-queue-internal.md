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

#### Connection Management

```csharp
public RabbitMQMessageQueue(MessageQueueConfig config)
{
    _config = config;
    var factory = new ConnectionFactory { Uri = new Uri(config.ConnectionString) };
    _connection = factory.CreateConnection();

    // Create publisher channel pool for concurrent publishing
    var poolSize = Math.Max(1, config.PublisherChannelPoolSize);
    _publisherChannels = new PublisherChannel[poolSize];
    for (var i = 0; i < poolSize; i++)
    {
        _publisherChannels[i] = new PublisherChannel(
            _connection,
            config.EnablePublisherConfirms,
            config.ConfirmTimeout,
            config.MaxOutstandingConfirms / poolSize);
    }

    // Separate consumer channel for subscriptions
    _consumerChannel = _connection.CreateModel();

    DeclareExchangeAndQueues();
}
```

**Connection Strategy:**
- Single persistent connection per queue instance
- **Publisher channel pool** for concurrent publishing with backpressure
- Separate consumer channel for message consumption
- Automatic reconnection handled by RabbitMQ client
- Connection disposal in finalizer

#### PublisherChannel Class (Internal)

The `PublisherChannel` class manages async publisher confirms with backpressure control:

```csharp
internal sealed class PublisherChannel : IDisposable
{
    readonly IModel channel;
    readonly TimeSpan confirmTimeout;
    readonly SemaphoreSlim outstandingConfirmsSemaphore;
    readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> pendingConfirms;
    readonly object publishLock;
    readonly bool enableConfirms;
}
```

**Purpose:**
- Wraps a RabbitMQ channel with async publisher confirm tracking
- Provides backpressure via `SemaphoreSlim` to limit outstanding confirms
- Handles `BasicAcks`/`BasicNacks` events including the `Multiple` flag
- Supports both confirmed and fire-and-forget publishing modes

**Key Design Decisions:**
- `ConcurrentDictionary` for thread-safe tracking of pending confirms by sequence number
- `TaskCompletionSource` with `RunContinuationsAsynchronously` to avoid deadlocks
- Lock around publish to ensure sequence number and dictionary update are atomic
- Timeout handling with linked cancellation tokens
- Clean shutdown that fails all pending confirms

**Backpressure Mechanism:**
```csharp
public async Task PublishAsync(...)
{
    // Wait for available slot (backpressure)
    await outstandingConfirmsSemaphore.WaitAsync(cancellationToken);

    try
    {
        // Publish and track
        lock (publishLock)
        {
            seqNo = channel.NextPublishSeqNo;
            pendingConfirms[seqNo] = tcs;
            channel.BasicPublish(...);
        }

        // Wait for confirm with timeout
        var confirmed = await tcs.Task.WaitAsync(timeoutCts.Token);
    }
    finally
    {
        outstandingConfirmsSemaphore.Release();
    }
}
```

**Confirm Handling:**
```csharp
void HandleConfirmation(ulong deliveryTag, bool multiple, bool confirmed)
{
    if (multiple)
    {
        // Ack/Nack all messages up to and including deliveryTag
        var keysToRemove = pendingConfirms.Keys.Where(k => k <= deliveryTag);
        foreach (var key in keysToRemove)
        {
            if (pendingConfirms.TryRemove(key, out var tcs))
                tcs.TrySetResult(confirmed);
        }
    }
    else
    {
        if (pendingConfirms.TryRemove(deliveryTag, out var tcs))
            tcs.TrySetResult(confirmed);
    }
}
```

#### Exchange-Based Publishing

The RabbitMQ implementation now uses exchange-based publishing for flexibility:

```csharp
private void DeclareExchangeAndQueues()
{
    // Declare exchange (default: direct exchange named after queue)
    var exchangeName = config.ExchangeName ?? config.QueueName;
    var exchangeType = config.ExchangeType ?? "direct";

    _channel.ExchangeDeclare(exchangeName, exchangeType, durable: true);

    // Declare queue and bind to exchange
    _channel.QueueDeclare(_queueName, durable: true, ...);
    _channel.QueueBind(_queueName, exchangeName, routingKey: _queueName);
}
```

**Benefits:**
- Decouples publishers from queue names
- Enables topic/fanout routing patterns
- Supports multiple queues bound to same exchange
- Allows routing key-based message filtering

#### Queue Declaration Logic

The queue declaration logic handles both classic and quorum queues:

```csharp
private void DeclareQueues()
{
    var queueArgs = new Dictionary<string, object>();
    
    if (_config.UseQuorumQueues)
    {
        queueArgs.Add("x-queue-type", "quorum");
        queueArgs.Add("x-quorum-initial-group-size", _config.QuorumInitialGroupSize);
        queueArgs.Add("x-overflow", "reject-publish");
    }
    
    if (_config.EnableDeadLetterQueue)
    {
        // Declare dead letter queue first
        _channel.QueueDeclare(_deadLetterQueueName, durable: true, ...);
        
        // Configure main queue to route to dead letter
        queueArgs.Add("x-dead-letter-exchange", "");
        queueArgs.Add("x-dead-letter-routing-key", _deadLetterQueueName);
    }
    
    _channel.QueueDeclare(_queueName, durable: true, exclusive: false, 
        autoDelete: false, arguments: queueArgs);
}
```

**Queue Configuration Strategy:**
- Durable queues survive broker restarts
- Non-exclusive allows multiple consumers
- Auto-delete disabled for persistence
- Dead letter routing configured at queue level

#### Message Serialization

```csharp
public Task PublishAsync(T message, CancellationToken cancellationToken = default)
{
    var json = JsonSerializer.Serialize(message);
    var body = Encoding.UTF8.GetBytes(json);
    
    var properties = _channel.CreateBasicProperties();
    properties.Persistent = true;
    properties.MessageId = Guid.NewGuid().ToString();
    properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
    
    _channel.BasicPublish("", _queueName, properties, body);
}
```

**Serialization Decisions:**
- System.Text.Json for performance and .NET integration
- UTF-8 encoding for international character support
- Message properties include ID and timestamp for tracing
- Persistent delivery mode for durability

#### Consumer Implementation

```csharp
public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default)
{
    _consumer = new EventingBasicConsumer(_channel);
    
    if (_config.UseQuorumQueues)
    {
        _channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);
    }
    
    _consumer.Received += async (model, ea) =>
    {
        var body = ea.Body.ToArray();
        var json = Encoding.UTF8.GetString(body);
        var deliveryCount = GetDeliveryCount(ea.BasicProperties);
        
        try
        {
            var message = JsonSerializer.Deserialize<T>(json);
            var success = await handler(message);
            
            if (success)
            {
                _channel.BasicAck(ea.DeliveryTag, false);
            }
            else
            {
                HandleRetry(ea, deliveryCount);
            }
        }
        catch (Exception)
        {
            HandleRetry(ea, deliveryCount);
        }
    };
    
    _channel.BasicConsume(_queueName, autoAck: false, consumer: _consumer);
}
```

**Consumer Design:**
- Event-driven message processing
- Manual acknowledgment for reliability
- Prefetch count optimization for quorum queues
- Exception handling with retry logic

### Retry Mechanism

The retry mechanism implements exponential backoff with dead letter routing:

```csharp
private void HandleRetry(BasicDeliverEventArgs ea, int deliveryCount)
{
    if (deliveryCount >= _config.RetryAttempts)
    {
        // Send to dead letter queue
        if (_config.UseQuorumQueues)
        {
            _channel.BasicReject(ea.DeliveryTag, false);
        }
        else
        {
            _channel.BasicNack(ea.DeliveryTag, false, false);
        }
    }
    else
    {
        // Requeue for retry
        _channel.BasicNack(ea.DeliveryTag, false, true);
    }
}
```

**Retry Strategy:**
- Delivery count tracked in message headers
- Reject vs Nack optimization for quorum queues
- Dead letter routing after max attempts
- No explicit delay (relies on queue redelivery)

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

#### Sender Pool Architecture

The Azure Service Bus implementation uses a sender pool pattern similar to RabbitMQ:

```csharp
public AzureServiceBusMessageQueue(AzureServiceBusConfig config)
{
    client = new ServiceBusClient(config.ConnectionString);

    // Create sender pool for concurrent publishing
    var poolSize = Math.Max(1, config.SenderPoolSize);
    var maxConcurrentPerSender = Math.Max(1, config.MaxConcurrentSends / poolSize);
    senderPool = new PublisherSender[poolSize];

    for (var i = 0; i < poolSize; i++)
    {
        senderPool[i] = new PublisherSender(
            client,
            publishDestination,  // Queue or Topic name
            maxConcurrentPerSender,
            config.SendTimeout);
    }

    // Create processor for receiving messages
    processor = useTopicPublishing && !string.IsNullOrEmpty(config.SubscriptionName)
        ? client.CreateProcessor(config.TopicName, config.SubscriptionName, processorOptions)
        : client.CreateProcessor(queueName, processorOptions);
}
```

#### PublisherSender Class (Internal)

The `PublisherSender` class wraps `ServiceBusSender` with backpressure control:

```csharp
internal sealed class PublisherSender : IAsyncDisposable
{
    readonly ServiceBusSender sender;
    readonly SemaphoreSlim concurrencySemaphore;
    readonly TimeSpan sendTimeout;
}
```

**Purpose:**
- Wraps Azure Service Bus sender with concurrency limiting
- Provides backpressure via `SemaphoreSlim` to limit concurrent sends
- Handles timeout for send operations
- Supports both single message and batch sends

**Key Design Decisions:**
- Simpler than RabbitMQ's `PublisherChannel` because Azure Service Bus handles confirms internally
- `SendMessageAsync` returns when the message is accepted by the broker
- Backpressure prevents overwhelming the service with too many concurrent requests
- Timeout handling prevents indefinite waits on network issues

**Backpressure Mechanism:**
```csharp
public async Task SendAsync(ServiceBusMessage message, CancellationToken cancellationToken)
{
    // Wait for available slot (backpressure)
    await concurrencySemaphore.WaitAsync(cancellationToken);

    try
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(sendTimeout);

        await sender.SendMessageAsync(message, timeoutCts.Token);
    }
    finally
    {
        concurrencySemaphore.Release();
    }
}
```

#### Topic-Based Publishing

Azure Service Bus supports Topics/Subscriptions for pub/sub patterns:

```csharp
// Configuration
var config = new AzureServiceBusConfig
{
    TopicName = "order-events",           // Publish to topic
    SubscriptionName = "order-processor", // Receive from subscription
    QueueName = "fallback-queue"          // Used if TopicName not set
};

// Publishing destination selection
useTopicPublishing = !string.IsNullOrEmpty(config.TopicName);
publishDestination = useTopicPublishing ? config.TopicName : queueName;

// Processor creation for receiving
processor = useTopicPublishing && !string.IsNullOrEmpty(config.SubscriptionName)
    ? client.CreateProcessor(config.TopicName, config.SubscriptionName, options)
    : client.CreateProcessor(queueName, options);
```

**Benefits:**
- Multiple subscribers can receive the same message
- Subscription filters for message routing
- Decouples publishers from consumers
- Supports fan-out messaging patterns

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
- Explicit disposal of RabbitMQ resources
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

1. **Connection Failures**: Automatic reconnection by RabbitMQ client
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