using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Serialization;

namespace ESE.MessageQueue.AzureServiceBus;

public class AzureServiceBusMessageQueue<T> : IMessageQueue<T>, IDisposable where T : class
{
    readonly AzureServiceBusConfig config;
    readonly ServiceBusClient client;
    readonly PublisherSender[] senderPool;
    readonly ServiceBusProcessor processor;
    readonly string queueName;
    readonly string deadLetterQueueName;
    readonly string publishDestination;
    readonly bool useTopicPublishing;
    int senderIndex;

    public AzureServiceBusMessageQueue(AzureServiceBusConfig config)
    {
        this.config = config;
        queueName = config.QueueName;
        deadLetterQueueName = string.IsNullOrEmpty(config.DeadLetterQueueName)
            ? $"{config.QueueName}.deadletter"
            : config.DeadLetterQueueName;

        useTopicPublishing = !string.IsNullOrEmpty(config.TopicName);
        publishDestination = useTopicPublishing ? config.TopicName! : queueName;

        try
        {
            client = new ServiceBusClient(config.ConnectionString);

            // Create sender pool
            var poolSize = Math.Max(1, config.SenderPoolSize);
            var maxConcurrentPerSender = Math.Max(1, config.MaxConcurrentSends / poolSize);
            senderPool = new PublisherSender[poolSize];

            for (var i = 0; i < poolSize; i++)
            {
                senderPool[i] = new PublisherSender(
                    client,
                    publishDestination,
                    maxConcurrentPerSender,
                    config.SendTimeout);
            }

            // Create processor for receiving messages
            var processorOptions = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = this.config.MaxConcurrentCalls,
                PrefetchCount = this.config.PrefetchCount,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                MaxAutoLockRenewalDuration = this.config.MaxAutoLockRenewalDuration
            };

            processor = useTopicPublishing && !string.IsNullOrEmpty(config.SubscriptionName)
                ? client.CreateProcessor(config.TopicName, config.SubscriptionName, processorOptions)
                : client.CreateProcessor(queueName, processorOptions);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to initialize Azure Service Bus connection: {ex.Message}", ex);
        }
    }

    PublisherSender GetNextSender()
    {
        var index = Interlocked.Increment(ref senderIndex);
        return senderPool[index % senderPool.Length];
    }

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        try
        {
            var serviceBusMessage = CreateServiceBusMessage(message);
            var sender = GetNextSender();
            await sender.SendAsync(serviceBusMessage, cancellationToken);
        }
        catch (MessageQueueException)
        {
            throw;
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

            if (config.EnableBatchedOperations && messageList.Count > 1)
            {
                var batches = messageList
                    .Select(CreateServiceBusMessage)
                    .Chunk(config.MaxBatchSize);

                foreach (var batch in batches)
                {
                    var sender = GetNextSender();
                    await sender.SendBatchAsync(batch, cancellationToken);
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
        catch (MessageQueueException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to publish messages: {ex.Message}", ex);
        }
    }

    ServiceBusMessage CreateServiceBusMessage(T message)
    {
        var body = MessageSerializer.Serialize(message, config.SerializationFormat);

        var serviceBusMessage = new ServiceBusMessage(body)
        {
            MessageId = Guid.NewGuid().ToString(),
            ContentType = MessageSerializer.GetContentType(config.SerializationFormat),
            TimeToLive = config.MessageTimeToLive,
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
            processor.ProcessMessageAsync += async args =>
            {
                var body = args.Message.Body.ToArray();
                var deliveryCount = args.Message.DeliveryCount;

                try
                {
                    var message = MessageSerializer.Deserialize<T>(body, config.SerializationFormat);
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

            processor.ProcessErrorAsync += args =>
            {
                return Task.CompletedTask;
            };

            await processor.StartProcessingAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to subscribe to queue: {ex.Message}", ex);
        }
    }

    async Task HandleRetry(ProcessMessageEventArgs args, int deliveryCount, 
        CancellationToken cancellationToken, Exception? exception = null)
    {
        if (deliveryCount >= config.RetryAttempts)
        {
            var reason = exception?.GetType().Name ?? "Processing failed";
            var description = exception?.Message ?? "Message processing failed after maximum retry attempts";
            
            await args.DeadLetterMessageAsync(args.Message, reason, description, cancellationToken);
        }
        else
        {
            var delay = TimeSpan.FromSeconds(Math.Pow(2, deliveryCount - 1) * config.RetryDelay.TotalSeconds);
            
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
        if (processor.IsProcessing)
        {
            await processor.StopProcessingAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (processor.IsProcessing)
        {
            await processor.StopProcessingAsync();
        }

        await processor.DisposeAsync();

        foreach (var sender in senderPool)
        {
            await sender.DisposeAsync();
        }

        await client.DisposeAsync();
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}