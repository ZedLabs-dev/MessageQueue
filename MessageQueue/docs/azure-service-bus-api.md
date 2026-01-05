# Azure Service Bus Message Queue API

Provides type-safe asynchronous message processing through Azure Service Bus with automatic retry logic, dead letter queue support, and enterprise-grade reliability features.

## Library Overview

**Module:** AgentReview.MessageQueue.AzureServiceBus  
**Version:** 1.0.0  
**Purpose:** Azure Service Bus implementation of the message queue abstraction with enterprise messaging features

**Installation Requirements:**
- .NET 9.0 or higher
- Azure.Messaging.ServiceBus 7.17.0
- Microsoft.Extensions.DependencyInjection 9.0.0
- Microsoft.Extensions.Hosting 9.0.0

**Core Features:**
- Enterprise-grade message delivery guarantees
- Built-in dead letter queue support
- Automatic message lock renewal
- Batch message publishing
- Concurrent message processing
- Exponential backoff retry logic

## Usage Examples

### Basic Configuration

```csharp
using AgentReview.MessageQueue.Extensions;
using AgentReview.MessageQueue.Hosting;

public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
{
    // Configure Azure Service Bus message queue via configuration
    services.AddMessageQueue<OrderCreated>(configuration);

    // Register message processor
    services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
    
    // Add background service for message consumption
    services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
}
```

### Configuration File Setup

Configure Azure Service Bus in `appsettings.json`:

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key",
    "QueueName": "order-created",
    "RetryAttempts": 3,
    "EnableDeadLetterQueue": true,
    "MessageTimeToLive": "7.00:00:00",
    "MaxConcurrentCalls": 10,
    "PrefetchCount": 20,
    "MaxAutoLockRenewalDuration": "00:05:00",
    "EnableBatchedOperations": true,
    "MaxBatchSize": 100,
    "BatchFlushInterval": "00:00:01"
  }
}
```

### Alternative: Programmatic Configuration

```csharp
public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
{
    // Configure Azure Service Bus message queue programmatically
    services.AddMessageQueue<OrderCreated>(options =>
    {
        options.Provider = MessageQueueProvider.AzureServiceBus;
        options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/;...";
        options.QueueName = "order-created";
        options.RetryAttempts = 3;
        options.EnableDeadLetterQueue = true;
        
        // Azure Service Bus specific settings
        if (options is AzureServiceBusConfig asbConfig)
        {
            asbConfig.MessageTimeToLive = TimeSpan.FromDays(7);
            asbConfig.MaxConcurrentCalls = 10;
            asbConfig.PrefetchCount = 20;
            asbConfig.MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5);
        }
    });
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
        // Azure Service Bus supports efficient batch publishing
        await _messageQueue.PublishAsync(orders);
    }
}
```

### Message Processing with Azure Service Bus Features

```csharp
public class OrderCreatedProcessor : IMessageProcessor<OrderCreated>
{
    private readonly ILogger<OrderCreatedProcessor> _logger;
    private readonly IOrderService _orderService;

    public OrderCreatedProcessor(
        ILogger<OrderCreatedProcessor> logger,
        IOrderService orderService)
    {
        _logger = logger;
        _orderService = orderService;
    }

    public async Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context)
    {
        try
        {
            _logger.LogInformation("Processing order {OrderId} (Delivery #{DeliveryCount})", 
                message.OrderId, context.DeliveryCount);
            
            // Long-running processing is supported with automatic lock renewal
            await _orderService.ProcessOrderAsync(message);
            
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

## Azure Service Bus Specific Features

### Connection String Format

Azure Service Bus uses connection strings with the following format:

```
Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key
```

**Authentication Options:**
- Shared Access Key (shown above)
- Managed Identity: `Endpoint=sb://your-namespace.servicebus.windows.net/`
- Connection String with Entity Path: `...;EntityPath=your-queue-name`

### Message Properties

Azure Service Bus messages include rich metadata:

```csharp
// Automatically set properties
serviceBusMessage.MessageId = Guid.NewGuid().ToString();
serviceBusMessage.ContentType = "application/json";
serviceBusMessage.TimeToLive = TimeSpan.FromDays(14);
serviceBusMessage.Subject = typeof(T).Name;

// Custom application properties
serviceBusMessage.ApplicationProperties.Add("MessageType", typeof(T).Name);
serviceBusMessage.ApplicationProperties.Add("PublishedAt", DateTimeOffset.UtcNow);
```

### Dead Letter Queue Handling

Azure Service Bus provides built-in dead letter queue functionality:

- Messages automatically moved to dead letter queue after max delivery attempts
- Dead letter reason and description automatically populated
- Dead letter queue name: `{queue-name}/$deadletterqueue`
- Messages retain all original properties and custom metadata

### Lock Renewal

Long-running message processing is supported through automatic lock renewal:

```csharp
var processorOptions = new ServiceBusProcessorOptions
{
    MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5) // Configurable
};
```

**Benefits:**
- Prevents message timeout during long processing
- Automatic renewal without manual intervention
- Configurable renewal duration
- Graceful handling of renewal failures

## Configuration Options

### Azure Service Bus Specific Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| MessageTimeToLive | TimeSpan | 14 days | Maximum time message can remain in queue |
| MaxConcurrentCalls | int | 10 | Maximum concurrent message processing |
| PrefetchCount | int | 10 | Number of messages to prefetch for performance |
| MaxAutoLockRenewalDuration | TimeSpan | 5 minutes | Maximum duration for automatic lock renewal |

### Sender Pool and Topic Publishing Settings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| TopicName | string | null | Topic name for topic-based publishing. When set, publishes to topic instead of queue. |
| SubscriptionName | string | null | Subscription name for receiving from a topic. Required when TopicName is set. |
| SenderPoolSize | int | 2 | Number of sender instances in the pool for concurrent publishing |
| MaxConcurrentSends | int | 1000 | Maximum concurrent send operations across all senders (backpressure) |
| SendTimeout | TimeSpan | 30 seconds | Timeout for send operations |

### Topic-Based Publishing

Configure topic-based pub/sub messaging:

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;...",
    "QueueName": "fallback-queue",
    "TopicName": "order-events",
    "SubscriptionName": "order-processor",
    "SenderPoolSize": 4,
    "MaxConcurrentSends": 2000,
    "SendTimeout": "00:00:30"
  }
}
```

**Topic Publishing Benefits:**
- Multiple subscribers can receive the same message
- Subscription filters for message routing
- Decouples publishers from consumers
- Supports fan-out messaging patterns

**Usage Example:**
```csharp
// Publisher sends to topic
await messageQueue.PublishAsync(new OrderCreated { OrderId = "123" });

// Multiple services can subscribe via different subscriptions:
// - order-processor subscription: processes orders
// - analytics subscription: tracks order metrics
// - notification subscription: sends customer notifications
```

### Performance Tuning

Configure high-throughput settings in `appsettings.json`:

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;...",
    "QueueName": "high-volume-queue",
    "MaxConcurrentCalls": 50,
    "PrefetchCount": 100,
    "MaxAutoLockRenewalDuration": "00:10:00",
    "EnableBatchedOperations": true,
    "MaxBatchSize": 200,
    "BatchFlushInterval": "00:00:00.500",
    "SenderPoolSize": 4,
    "MaxConcurrentSends": 4000,
    "SendTimeout": "00:00:30"
  }
}
```

**Performance Considerations:**
- Higher `MaxConcurrentCalls` increases throughput but uses more resources
- Higher `PrefetchCount` reduces latency but increases memory usage
- Longer `MaxAutoLockRenewalDuration` supports longer processing times
- Higher `SenderPoolSize` increases concurrent publishing capacity
- `MaxConcurrentSends` provides backpressure to prevent overwhelming the service
- Each sender can have `MaxConcurrentSends / SenderPoolSize` concurrent operations

## Error Handling and Retry Logic

### Retry Strategy

Azure Service Bus implementation uses exponential backoff:

```csharp
private async Task HandleRetry(ProcessMessageEventArgs args, int deliveryCount, 
    CancellationToken cancellationToken, Exception? exception = null)
{
    if (deliveryCount >= _config.RetryAttempts)
    {
        // Move to dead letter queue with detailed information
        await args.DeadLetterMessageAsync(args.Message, 
            exception?.GetType().Name ?? "Processing failed",
            exception?.Message ?? "Message processing failed after maximum retry attempts");
    }
    else
    {
        // Abandon message for retry with exponential backoff
        var delay = TimeSpan.FromSeconds(Math.Pow(2, deliveryCount - 1) * _config.RetryDelay.TotalSeconds);
        
        var properties = new Dictionary<string, object>
        {
            ["RetryCount"] = deliveryCount,
            ["NextRetryAt"] = DateTimeOffset.UtcNow.Add(delay),
            ["OriginalException"] = exception?.Message ?? "Processing failed"
        };
        
        await args.AbandonMessageAsync(args.Message, properties);
    }
}
```

### Exception Handling

- **Transient Exceptions**: Automatically retried with exponential backoff
- **Permanent Exceptions**: Immediately sent to dead letter queue
- **Timeout Exceptions**: Handled through automatic lock renewal
- **Connection Exceptions**: Automatic reconnection by Azure Service Bus client

## Monitoring and Observability

### Built-in Metrics

Azure Service Bus provides comprehensive metrics through Azure Monitor:

- Message count (active, dead letter, scheduled)
- Incoming/outgoing message rates
- Message size statistics
- Connection count and errors
- Throttling metrics

### Custom Logging

```csharp
_logger.LogInformation("Published {MessageType} to {QueueName} with MessageId {MessageId}",
    typeof(T).Name, _queueName, serviceBusMessage.MessageId);

_logger.LogInformation("Processing message {MessageId} (Delivery #{DeliveryCount})",
    args.Message.MessageId, args.Message.DeliveryCount);
```

### Health Checks

```csharp
services.AddHealthChecks()
    .AddAzureServiceBusQueue(connectionString, queueName, name: "servicebus-queue");
```

## Production Deployment

### Namespace Configuration

```json
{
  "ConnectionStrings": {
    "AzureServiceBus": "Endpoint=sb://prod-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-production-key"
  },
  "MessageQueue": {
    "OrderCreated": {
      "QueueName": "order-created",
      "MaxConcurrentCalls": 20,
      "PrefetchCount": 50,
      "MessageTimeToLive": "7.00:00:00"
    }
  }
}
```

### Security Best Practices

1. **Use Managed Identity** when running in Azure:
```csharp
options.ConnectionString = "Endpoint=sb://your-namespace.servicebus.windows.net/";
```

2. **Least Privilege Access**: Create specific shared access policies
3. **Network Security**: Use Virtual Network integration
4. **Encryption**: Enable encryption at rest and in transit

### Scaling Considerations

- **Standard Tier**: Up to 1,000 concurrent connections
- **Premium Tier**: Dedicated resources, higher throughput
- **Partitioned Queues**: Higher throughput with multiple message brokers
- **Auto-scaling**: Configure based on queue depth metrics

## Comparison with RabbitMQ

| Feature | Azure Service Bus | RabbitMQ |
|---------|------------------|----------|
| **Hosting** | Fully managed | Self-hosted or managed |
| **Reliability** | Enterprise SLA | Depends on setup |
| **Dead Letter** | Built-in | Manual configuration |
| **Lock Renewal** | Automatic | Manual implementation |
| **Batch Publishing** | Native support | Sequential |
| **Monitoring** | Azure Monitor | External tools |
| **Cost** | Pay-per-use | Infrastructure costs |
| **Latency** | Higher (cloud) | Lower (on-premises) |

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify connection string format
   - Check network connectivity to Azure
   - Validate shared access key permissions

2. **Message Lock Timeout**
   - Increase `MaxAutoLockRenewalDuration`
   - Optimize message processing logic
   - Consider breaking large operations into smaller chunks

3. **Dead Letter Queue Growth**
   - Monitor dead letter queue metrics
   - Implement dead letter message reprocessing
   - Analyze failure patterns and root causes

4. **Performance Issues**
   - Adjust `MaxConcurrentCalls` and `PrefetchCount`
   - Monitor CPU and memory usage
   - Consider upgrading to Premium tier

### Debugging Tips

```csharp
// Enable detailed logging
services.AddLogging(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Debug);
});

// Monitor message properties
_logger.LogDebug("Message properties: {@Properties}", 
    args.Message.ApplicationProperties);
```

## Migration from RabbitMQ

To migrate from RabbitMQ to Azure Service Bus:

1. **Update Configuration**:
```csharp
// Change from
options.Provider = "RabbitMQ";
options.ConnectionString = "amqp://localhost:5672";

// To
options.Provider = "AzureServiceBus";
options.ConnectionString = "Endpoint=sb://namespace.servicebus.windows.net/;...";
```

2. **Remove RabbitMQ-specific Settings**:
```csharp
// Remove these properties
// options.UseQuorumQueues = true;
// options.QuorumInitialGroupSize = 3;
```

3. **Add Azure Service Bus Settings**:
```csharp
options.MessageTimeToLive = TimeSpan.FromDays(7);
options.MaxConcurrentCalls = 10;
options.PrefetchCount = 20;
```

4. **Update Dependencies**:
```xml
<!-- Remove -->
<PackageReference Include="RabbitMQ.Client" Version="6.8.1" />

<!-- Add -->
<PackageReference Include="Azure.Messaging.ServiceBus" Version="7.17.0" />
```

The message processing logic and abstractions remain unchanged, ensuring a smooth migration path.