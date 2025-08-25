using System.Text;
using System.Text.Json;
using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.Exceptions;
using Azure.Messaging.ServiceBus;

namespace AgentReview.MessageQueue.AzureServiceBus;

public class AzureServiceBusMessageQueue<T> : IMessageQueue<T>, IDisposable where T : class
{
    private readonly AzureServiceBusConfig _config;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusSender _sender;
    private readonly ServiceBusProcessor _processor;
    private readonly string _queueName;
    private readonly string _deadLetterQueueName;

    public AzureServiceBusMessageQueue(AzureServiceBusConfig config)
    {
        _config = config;
        _queueName = config.QueueName;
        _deadLetterQueueName = string.IsNullOrEmpty(config.DeadLetterQueueName) 
            ? $"{config.QueueName}.deadletter" 
            : config.DeadLetterQueueName;
        
        try
        {
            _client = new ServiceBusClient(config.ConnectionString);
            _sender = _client.CreateSender(_queueName);
            
            var processorOptions = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = _config.MaxConcurrentCalls,
                PrefetchCount = _config.PrefetchCount,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                MaxAutoLockRenewalDuration = _config.MaxAutoLockRenewalDuration
            };
            
            _processor = _client.CreateProcessor(_queueName, processorOptions);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to initialize Azure Service Bus connection: {ex.Message}", ex);
        }
    }

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        try
        {
            var json = JsonSerializer.Serialize(message);
            var body = Encoding.UTF8.GetBytes(json);
            
            var serviceBusMessage = new ServiceBusMessage(body)
            {
                MessageId = Guid.NewGuid().ToString(),
                ContentType = "application/json",
                TimeToLive = _config.MessageTimeToLive,
                Subject = typeof(T).Name
            };
            
            serviceBusMessage.ApplicationProperties.Add("MessageType", typeof(T).Name);
            serviceBusMessage.ApplicationProperties.Add("PublishedAt", DateTimeOffset.UtcNow.ToString("O"));
            
            await _sender.SendMessageAsync(serviceBusMessage, cancellationToken);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to publish message: {ex.Message}", ex);
        }
    }

    public async Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        try
        {
            var messageList = messages.ToList();
            
            if (_config.EnableBatchedOperations && messageList.Count > 1)
            {
                var batches = messageList
                    .Select(CreateServiceBusMessage)
                    .Chunk(_config.MaxBatchSize);

                foreach (var batch in batches)
                {
                    await _sender.SendMessagesAsync(batch, cancellationToken);
                }
            }
            else
            {
                foreach (var message in messageList)
                {
                    await PublishAsync(message, cancellationToken);
                }
            }
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to publish messages: {ex.Message}", ex);
        }
    }

    private ServiceBusMessage CreateServiceBusMessage(T message)
    {
        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);
        
        var serviceBusMessage = new ServiceBusMessage(body)
        {
            MessageId = Guid.NewGuid().ToString(),
            ContentType = "application/json",
            TimeToLive = _config.MessageTimeToLive,
            Subject = typeof(T).Name
        };
        
        serviceBusMessage.ApplicationProperties.Add("MessageType", typeof(T).Name);
        serviceBusMessage.ApplicationProperties.Add("PublishedAt", DateTimeOffset.UtcNow.ToString("O"));
        
        return serviceBusMessage;
    }

    public async Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default)
    {
        try
        {
            _processor.ProcessMessageAsync += async args =>
            {
                var body = args.Message.Body.ToArray();
                var json = Encoding.UTF8.GetString(body);
                var deliveryCount = args.Message.DeliveryCount;
                
                try
                {
                    var message = JsonSerializer.Deserialize<T>(json);
                    if (message != null)
                    {
                        var success = await handler(message);
                        if (success)
                        {
                            await args.CompleteMessageAsync(args.Message, cancellationToken);
                        }
                        else
                        {
                            await HandleRetry(args, deliveryCount, cancellationToken);
                        }
                    }
                    else
                    {
                        await args.DeadLetterMessageAsync(args.Message, 
                            "Deserialization failed", "Message could not be deserialized", 
                            cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    await HandleRetry(args, deliveryCount, cancellationToken, ex);
                }
            };
            
            _processor.ProcessErrorAsync += args =>
            {
                return Task.CompletedTask;
            };
            
            await _processor.StartProcessingAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to subscribe to queue: {ex.Message}", ex);
        }
    }

    private async Task HandleRetry(ProcessMessageEventArgs args, int deliveryCount, 
        CancellationToken cancellationToken, Exception? exception = null)
    {
        if (deliveryCount >= _config.RetryAttempts)
        {
            var reason = exception?.GetType().Name ?? "Processing failed";
            var description = exception?.Message ?? "Message processing failed after maximum retry attempts";
            
            await args.DeadLetterMessageAsync(args.Message, reason, description, cancellationToken);
        }
        else
        {
            var delay = TimeSpan.FromSeconds(Math.Pow(2, deliveryCount - 1) * _config.RetryDelay.TotalSeconds);
            
            var properties = new Dictionary<string, object>
            {
                ["RetryCount"] = deliveryCount,
                ["NextRetryAt"] = DateTimeOffset.UtcNow.Add(delay).ToString("O"),
                ["OriginalException"] = exception?.Message ?? "Processing failed"
            };
            
            await args.AbandonMessageAsync(args.Message, properties, cancellationToken);
        }
    }

    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        if (_processor.IsProcessing)
        {
            await _processor.StopProcessingAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_processor.IsProcessing)
        {
            await _processor.StopProcessingAsync();
        }
        
        await _processor.DisposeAsync();
        await _sender.DisposeAsync();
        await _client.DisposeAsync();
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}