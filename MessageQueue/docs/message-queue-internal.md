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
    _channel = _connection.CreateModel();
    
    DeclareQueues();
}
```

**Connection Strategy:**
- Single persistent connection per queue instance
- Automatic reconnection handled by RabbitMQ client
- Channel per queue for thread safety
- Connection disposal in finalizer

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

## Performance Optimizations

### Connection Pooling

- Single connection per message queue instance
- Channel reuse for publishing operations
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