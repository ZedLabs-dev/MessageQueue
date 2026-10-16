using System.Globalization;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.AzureServiceBus;

public class AzureServiceBusMessageQueue<T> : IMessageQueue<T>, IDisposable, IAsyncDisposable where T : class
{
    // Retries are abandoned for immediate redelivery for now (no broker-side backoff yet).
    static readonly MessageQueueCapabilities AzureServiceBusCapabilities = new(Durable: true, DeadLetter: true, DelayedRedelivery: false);

    readonly AzureServiceBusConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly ServiceBusClient client;
    readonly PublisherSender[] senderPool;
    readonly ServiceBusProcessor processor;
    int senderIndex;
    int disposed;

    public AzureServiceBusMessageQueue(AzureServiceBusConfig config, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        ConfigValidation.ThrowIfInvalid(config, new AzureServiceBusConfigValidator());
        this.config = config;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = logger ?? NullLogger.Instance;
        CapabilityChecks.WarnIfBackoffUnsupported(AzureServiceBusCapabilities, config, this.logger);

        var useTopicPublishing = !string.IsNullOrEmpty(config.TopicName);
        var publishDestination = useTopicPublishing ? config.TopicName! : config.QueueName;

        try
        {
            client = new ServiceBusClient(config.ConnectionString);

            var maxConcurrentPerSender = Math.Max(1, config.MaxConcurrentSends / config.SenderPoolSize);
            senderPool = new PublisherSender[config.SenderPoolSize];

            for (var i = 0; i < senderPool.Length; i++)
            {
                senderPool[i] = new PublisherSender(
                    client,
                    publishDestination,
                    maxConcurrentPerSender,
                    config.SendTimeout);
            }

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
                : client.CreateProcessor(config.QueueName, processorOptions);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to initialize Azure Service Bus connection: {ex.Message}", ex);
        }
    }

    public MessageQueueCapabilities Capabilities => AzureServiceBusCapabilities;

    PublisherSender GetNextSender() => senderPool[RoundRobin.Next(ref senderIndex, senderPool.Length)];

    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    public async Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        try
        {
            var serviceBusMessage = CreateServiceBusMessage(message, options);
            await GetNextSender().SendAsync(serviceBusMessage, cancellationToken);
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
        PublishValidation.Validate(messages);
        try
        {
            var messageList = messages.ToList();

            if (config.EnableBatchedOperations && messageList.Count > 1)
            {
                var batches = messageList
                    .Select(m => CreateServiceBusMessage(m, new PublishOptions()))
                    .Chunk(config.MaxBatchSize);

                foreach (var batch in batches)
                {
                    await GetNextSender().SendBatchAsync(batch, cancellationToken);
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

    ServiceBusMessage CreateServiceBusMessage(T message, PublishOptions options)
    {
        var serviceBusMessage = new ServiceBusMessage(MessageSerializer.Serialize(message, config.SerializationFormat))
        {
            MessageId = options.MessageId ?? Guid.CreateVersion7().ToString(),
            CorrelationId = options.CorrelationId,
            ContentType = MessageSerializer.GetContentType(config.SerializationFormat),
            TimeToLive = config.MessageTimeToLive,
            Subject = typeof(T).Name
        };

        if (options.Headers is not null)
        {
            foreach (var (key, value) in options.Headers)
            {
                serviceBusMessage.ApplicationProperties[key] = value;
            }
        }

        return serviceBusMessage;
    }

    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(MessageHandlers.FromPredicate(handler), cancellationToken);

    public async Task SubscribeAsync(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var dispatcher = new MessageDispatcher<T>(config, handler, telemetry, logger);

            processor.ProcessMessageAsync += args => dispatcher.DispatchAsync(
                args.Message.Body.ToMemory(),
                ToContext(args.Message),
                new ServiceBusDeliveryActions(args),
                args.CancellationToken);

            processor.ProcessErrorAsync += _ => Task.CompletedTask;

            await processor.StartProcessingAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to subscribe to queue: {ex.Message}", ex);
        }
    }

    internal static MessageContext ToContext(ServiceBusReceivedMessage message)
    {
        var headers = new Dictionary<string, string>();
        foreach (var (key, value) in message.ApplicationProperties)
        {
            var text = value switch
            {
                string s => s,
                IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
                null => null,
                _ => value.ToString()
            };
            if (text is not null)
                headers[key] = text;
        }

        return new MessageContext
        {
            MessageId = message.MessageId,
            CorrelationId = message.CorrelationId,
            Timestamp = message.EnqueuedTime,
            DeliveryAttempt = Math.Max(1, message.DeliveryCount),
            Headers = headers
        };
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
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

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
        GC.SuppressFinalize(this);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    sealed class ServiceBusDeliveryActions : IDeliveryActions
    {
        readonly ProcessMessageEventArgs args;

        public ServiceBusDeliveryActions(ProcessMessageEventArgs args)
        {
            this.args = args;
        }

        public Task CompleteAsync(CancellationToken cancellationToken) =>
            args.CompleteMessageAsync(args.Message, cancellationToken);

        // Interim: abandon for immediate redelivery; Service Bus increments DeliveryCount, which is the attempt number.
        public Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken) =>
            args.AbandonMessageAsync(args.Message, cancellationToken: cancellationToken);

        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken) =>
            args.DeadLetterMessageAsync(args.Message, reason.Code, reason.Detail ?? string.Empty, cancellationToken);
    }
}
