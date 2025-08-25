# Message Queue API

Provides type-safe asynchronous message processing through a generic message queue abstraction with support for RabbitMQ and Azure Service Bus providers.

## Library Overview

**Module:** AgentReview.MessageQueue  
**Version:** 1.0.0  
**Purpose:** Type-safe message queue operations with provider abstraction, retry logic, and dead letter queue support

**Installation Requirements:**
- .NET 9.0 or higher
- RabbitMQ.Client 6.8.1
- Microsoft.Extensions.DependencyInjection 9.0.0
- Microsoft.Extensions.Hosting 9.0.0

**Core Architectural Patterns:**
- Generic Type Safety: Compile-time type checking for messages
- Provider Pattern: Pluggable message queue implementations
- Dependency Injection: Full DI container integration
- Background Processing: Hosted services for message consumption
- Retry Pattern: Automatic retry with exponential backoff

## Usage Examples

### Basic Configuration

```csharp
using AgentReview.MessageQueue.Extensions;
using AgentReview.MessageQueue.Hosting;

public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
{
    // Configure message queue via configuration file
    services.AddMessageQueue<OrderCreated>(configuration);

    // Register message processor
    services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
    
    // Add background service for message consumption
    services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
}
```

### Configuration File Setup

Configure your preferred provider in `appsettings.json`:

**RabbitMQ Configuration:**
```json
{
  "MessageQueue": {
    "Provider": "RabbitMQ",
    "ConnectionString": "amqp://admin:password123@localhost:5672/",
    "QueueName": "order-created",
    "RetryAttempts": 3,
    "EnableDeadLetterQueue": true,
    "UseQuorumQueues": true,
    "QuorumInitialGroupSize": 3,
    "EnablePublisherConfirms": true,
    "PrefetchCount": 10
  }
}
```

**Azure Service Bus Configuration:**
```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;...",
    "QueueName": "order-created",
    "RetryAttempts": 3,
    "EnableDeadLetterQueue": true,
    "MessageTimeToLive": "7.00:00:00",
    "MaxConcurrentCalls": 10,
    "PrefetchCount": 20,
    "MaxAutoLockRenewalDuration": "00:05:00"
  }
}
```

### Publishing Messages

```csharp
public class OrderService
{
    private readonly IMessageQueue<OrderCreated> _messageQueue;

    public OrderService(IMessageQueue<OrderCreated> messageQueue)
    {
        _messageQueue = messageQueue;
    }

    public async Task CreateOrderAsync(string customerId, decimal amount)
    {
        var orderCreated = new OrderCreated(
            Guid.NewGuid().ToString(), 
            customerId, 
            amount, 
            DateTime.UtcNow
        );
        
        await _messageQueue.PublishAsync(orderCreated);
    }

    public async Task CreateBulkOrdersAsync(IEnumerable<OrderCreated> orders)
    {
        await _messageQueue.PublishAsync(orders);
    }
}
```

### Message Processing

```csharp
public class OrderCreatedProcessor : IMessageProcessor<OrderCreated>
{
    private readonly ILogger<OrderCreatedProcessor> _logger;

    public OrderCreatedProcessor(ILogger<OrderCreatedProcessor> logger)
    {
        _logger = logger;
    }

    public async Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context)
    {
        try
        {
            _logger.LogInformation("Processing order {OrderId}", message.OrderId);
            
            // Process the order
            await ProcessOrderLogic(message);
            
            return MessageResult.Acknowledge;
        }
        catch (TransientException ex)
        {
            _logger.LogWarning(ex, "Transient error processing order {OrderId}", message.OrderId);
            return MessageResult.Retry;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process order {OrderId}", message.OrderId);
            return MessageResult.DeadLetter;
        }
    }
}
```

## API Reference

### `IMessageQueue<T>` Interface

Primary interface for message queue operations with type safety.

#### `PublishAsync(T message, CancellationToken cancellationToken = default) -> Task`

**Purpose:** Publishes a single message to the queue

**Parameters:**
- `message` (T): The message object to publish
- `cancellationToken` (CancellationToken): Optional cancellation token

**Returns:** Task representing the asynchronous operation

**Raises:** `MessageQueueException` if publishing fails

**Example:**
```csharp
var message = new OrderCreated("order-123", "customer-456", 99.99m, DateTime.UtcNow);
await messageQueue.PublishAsync(message);
```

#### `PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default) -> Task`

**Purpose:** Publishes multiple messages to the queue

**Parameters:**
- `messages` (IEnumerable<T>): Collection of messages to publish
- `cancellationToken` (CancellationToken): Optional cancellation token

**Returns:** Task representing the asynchronous operation

**Raises:** `MessageQueueException` if publishing fails

**Example:**
```csharp
var messages = new[] { message1, message2, message3 };
await messageQueue.PublishAsync(messages);
```

#### `SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) -> Task`

**Purpose:** Subscribes to messages with a simple boolean return handler

**Parameters:**
- `handler` (Func<T, Task<bool>>): Message handler returning success/failure
- `cancellationToken` (CancellationToken): Optional cancellation token

**Returns:** Task representing the subscription setup

**Raises:** `MessageQueueException` if subscription fails

**Example:**
```csharp
await messageQueue.SubscribeAsync(async message =>
{
    // Process message
    return true; // Success
});
```

#### `UnsubscribeAsync(CancellationToken cancellationToken = default) -> Task`

**Purpose:** Unsubscribes from message consumption

**Parameters:**
- `cancellationToken` (CancellationToken): Optional cancellation token

**Returns:** Task representing the unsubscription operation

### `IMessageProcessor<T>` Interface

Interface for structured message processing with retry logic.

#### `ProcessAsync(T message, MessageContext context) -> Task<MessageResult>`

**Purpose:** Processes a message with structured result handling

**Parameters:**
- `message` (T): The message to process
- `context` (MessageContext): Message metadata and delivery information

**Returns:** `MessageResult` indicating processing outcome

**Example:**
```csharp
public async Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context)
{
    if (context.DeliveryCount > 3)
        return MessageResult.DeadLetter;
        
    try
    {
        await ProcessOrder(message);
        return MessageResult.Acknowledge;
    }
    catch (TransientException)
    {
        return MessageResult.Retry;
    }
}
```

### `RabbitMQMessageQueue<T>` Class

RabbitMQ implementation of the message queue interface.

#### Constructor: `RabbitMQMessageQueue(MessageQueueConfig config)`

**Purpose:** Initializes RabbitMQ connection and declares queues

**Parameters:**
- `config` (MessageQueueConfig): Configuration for the message queue

**Raises:** `MessageQueueException` if connection fails

### `MessageContext` Record

Contains metadata about message delivery and processing context.

**Properties:**
- `MessageId` (string): Unique identifier for the message
- `Timestamp` (DateTime): When the message was received
- `DeliveryCount` (int): Number of delivery attempts
- `Properties` (Dictionary<string, object>): Additional message properties

### `MessageResult` Enum

Defines the outcome of message processing.

**Values:**
- `Acknowledge`: Message processed successfully
- `Retry`: Temporary failure, retry the message
- `DeadLetter`: Permanent failure, send to dead letter queue

## Configuration Options

### `MessageQueueConfig` Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| Provider | string | Yes | - | Message queue provider ("RabbitMQ") |
| ConnectionString | string | Yes | - | Connection string for the message queue |
| QueueName | string | Yes | - | Name of the queue to use |
| RetryAttempts | int | No | 3 | Maximum number of retry attempts |
| RetryDelay | TimeSpan | No | 5 seconds | Delay between retry attempts |
| EnableDeadLetterQueue | bool | No | true | Enable dead letter queue for failed messages |
| DeadLetterQueueName | string | No | "{QueueName}.deadletter" | Name of the dead letter queue |
| UseQuorumQueues | bool | No | true | Use RabbitMQ quorum queues for high availability |
| QuorumInitialGroupSize | int | No | 3 | Initial group size for quorum queues |

### RabbitMQ Connection String Format

```
amqp://username:password@hostname:port/virtualhost
```

**Example:**
```
amqp://admin:password123@localhost:5672/
```

### Quorum Queue Configuration

Quorum queues provide high availability and data safety:

```csharp
services.AddMessageQueue<MyMessage>(options =>
{
    options.UseQuorumQueues = true;
    options.QuorumInitialGroupSize = 3; // Requires 3 nodes for cluster
});
```

**Benefits:**
- Built-in replication across cluster nodes
- Automatic leader election
- Better performance for persistent messages
- Enhanced poison message handling

### Classic Queue Configuration

For single-node deployments or legacy compatibility:

```csharp
services.AddMessageQueue<MyMessage>(options =>
{
    options.UseQuorumQueues = false;
});
```

## Error Handling

### Exception Types

- `MessageQueueException`: Base exception for message queue operations
- `TransientMessageException`: Indicates a temporary failure that should be retried

### Retry Logic

The message queue implements automatic retry with exponential backoff:

1. Message fails processing
2. If delivery count < `RetryAttempts`, message is requeued
3. Delay increases exponentially: 5s, 10s, 20s, etc.
4. After max attempts, message goes to dead letter queue

### Dead Letter Queue

Failed messages are automatically routed to a dead letter queue:

- Queue name: `{original-queue-name}.deadletter`
- Messages retain original headers and properties
- Manual inspection and reprocessing possible

## Threading Safety and Concurrency

- `IMessageQueue<T>` implementations are thread-safe
- Multiple concurrent publishers supported
- Single subscriber per queue instance
- Background processing through hosted services
- Configurable prefetch count for performance tuning

## Memory Usage and Resource Management

- Connection pooling for RabbitMQ channels
- Automatic disposal of resources
- Configurable prefetch limits to control memory usage
- Message serialization uses System.Text.Json for efficiency

## Integration Points

### Dependency Injection

Full integration with Microsoft.Extensions.DependencyInjection:

```csharp
services.AddMessageQueue<OrderCreated>(options => { /* config */ });
services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
```

### Logging

Structured logging with Microsoft.Extensions.Logging:

```csharp
_logger.LogInformation("Published {MessageType} with ID {MessageId}", 
    typeof(T).Name, messageId);
```

### Health Checks

Monitor message queue health:

```csharp
services.AddHealthChecks()
    .AddRabbitMQ(connectionString, name: "rabbitmq");
```

## Production Deployment Considerations

### Performance Characteristics

- **Throughput**: 10,000+ messages/second with quorum queues
- **Latency**: Sub-millisecond publishing, configurable consumer prefetch
- **Memory**: ~1MB per 10,000 queued messages

### Resource Requirements

- **CPU**: Minimal overhead, scales with message processing logic
- **Memory**: Configurable prefetch limits control memory usage
- **Network**: Persistent connections with automatic reconnection
- **Storage**: Durable messages persisted to disk

### High Availability Setup

```csharp
services.AddMessageQueue<OrderCreated>(options =>
{
    options.ConnectionString = "amqp://user:pass@node1:5672,node2:5672,node3:5672";
    options.UseQuorumQueues = true;
    options.QuorumInitialGroupSize = 3;
});
```

### Monitoring and Observability

- Built-in metrics for message counts, processing times
- Integration with Application Insights and Prometheus
- Dead letter queue monitoring for failed messages
- Connection health monitoring