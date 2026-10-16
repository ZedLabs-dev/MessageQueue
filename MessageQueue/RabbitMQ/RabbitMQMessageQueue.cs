using System.Globalization;
using System.Text;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.RabbitMQ;

public class RabbitMqMessageQueue<T> : IMessageQueue<T>, IDisposable, IAsyncDisposable where T : class
{
    // Carries the 1-based delivery attempt on republished retries.
    internal const string DeliveryAttemptHeader = "x-delivery-attempt";

    // Retries wait in this process before being republished (not yet broker-side, so no DelayedRedelivery).
    static readonly MessageQueueCapabilities RabbitMqCapabilities = new(Durable: true, DeadLetter: true, DelayedRedelivery: false);

    readonly RabbitMqConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly IConnection connection;
    readonly IModel consumerChannel;
    readonly object consumerChannelLock = new();
    readonly PublisherChannel[] publisherChannels;
    readonly string queueName;
    readonly string exchangeName;
    readonly string deadLetterQueueName;
    readonly CancellationTokenSource disposing = new();
    int nextPublisherIndex;
    EventingBasicConsumer? consumer;
    string? consumerTag;
    CancellationTokenSource? subscription;
    int disposed;

    public RabbitMqMessageQueue(RabbitMqConfig config, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        ConfigValidation.ThrowIfInvalid(config, new RabbitMqConfigValidator());
        this.config = config;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = logger ?? NullLogger.Instance;
        CapabilityChecks.WarnIfBackoffUnsupported(RabbitMqCapabilities, config, this.logger);
        queueName = config.QueueName;
        exchangeName = string.IsNullOrWhiteSpace(config.ExchangeName)
            ? config.QueueName
            : config.ExchangeName;
        deadLetterQueueName = config.EffectiveDeadLetterQueueName;

        try
        {
            var factory = new ConnectionFactory
            {
                Uri = new Uri(config.ConnectionString),
                RequestedHeartbeat = this.config.HeartbeatInterval,
                NetworkRecoveryInterval = this.config.NetworkRecoveryInterval,
                AutomaticRecoveryEnabled = true
            };

            connection = factory.CreateConnection();
            consumerChannel = connection.CreateModel();

            publisherChannels = new PublisherChannel[config.PublisherChannelPoolSize];
            for (var i = 0; i < publisherChannels.Length; i++)
            {
                publisherChannels[i] = new PublisherChannel(
                    connection,
                    config.EnablePublisherConfirms,
                    config.ConfirmTimeout,
                    config.MaxOutstandingConfirms);
            }

            DeclareExchangeAndQueues();
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to initialize RabbitMQ connection: {ex.Message}", ex);
        }
    }

    public MessageQueueCapabilities Capabilities => RabbitMqCapabilities;

    void DeclareExchangeAndQueues()
    {
        consumerChannel.ExchangeDeclare(
            exchange: exchangeName,
            type: config.ExchangeType,
            durable: true,
            autoDelete: false,
            arguments: null);

        var queueArgs = new Dictionary<string, object>();

        if (config.UseQuorumQueues)
        {
            queueArgs.Add("x-queue-type", "quorum");
            queueArgs.Add("x-quorum-initial-group-size", config.QuorumInitialGroupSize);
        }

        if (config.EnableDeadLetterQueue)
        {
            var deadLetterQueueArgs = new Dictionary<string, object>();

            if (config.UseQuorumQueues)
            {
                deadLetterQueueArgs.Add("x-queue-type", "quorum");
                deadLetterQueueArgs.Add("x-quorum-initial-group-size", config.QuorumInitialGroupSize);
            }

            consumerChannel.QueueDeclare(
                queue: deadLetterQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: deadLetterQueueArgs);

            queueArgs["x-dead-letter-exchange"] = "";
            queueArgs["x-dead-letter-routing-key"] = deadLetterQueueName;

            if (config.UseQuorumQueues)
            {
                queueArgs["x-overflow"] = "reject-publish";
            }
        }

        consumerChannel.QueueDeclare(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs);

        consumerChannel.QueueBind(
            queue: queueName,
            exchange: exchangeName,
            routingKey: queueName);
    }

    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    public async Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        try
        {
            var body = MessageSerializer.Serialize(message, config.SerializationFormat);
            var publisherChannel = NextPublisherChannel();

            var properties = publisherChannel.CreateBasicProperties();
            properties.Persistent = true;
            properties.MessageId = options.MessageId ?? Guid.CreateVersion7().ToString();
            properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            properties.ContentType = MessageSerializer.GetContentType(config.SerializationFormat);
            if (options.CorrelationId is not null)
            {
                properties.CorrelationId = options.CorrelationId;
            }
            if (options.Headers is { Count: > 0 })
            {
                properties.Headers = options.Headers.ToDictionary(h => h.Key, h => (object)h.Value);
            }

            await publisherChannel.PublishAsync(body, properties, exchangeName, queueName, cancellationToken);
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
        foreach (var message in messages)
        {
            await PublishAsync(message, cancellationToken);
        }
    }

    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(MessageHandlers.FromPredicate(handler), cancellationToken);

    public Task SubscribeAsync(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        CancellationToken cancellationToken = default)
    {
        if (consumer is not null)
            throw new InvalidOperationException("This queue already has a subscriber. Call UnsubscribeAsync first.");

        try
        {
            var dispatcher = new MessageDispatcher<T>(config, handler, telemetry, logger);
            subscription?.Dispose();
            var stopping = (subscription = CancellationTokenSource.CreateLinkedTokenSource(disposing.Token)).Token;
            var subscribed = consumer = new EventingBasicConsumer(consumerChannel);

            if (config.UseQuorumQueues)
            {
                lock (consumerChannelLock)
                {
                    consumerChannel.BasicQos(prefetchSize: 0, prefetchCount: (ushort)config.PrefetchCount, global: false);
                }
            }

            // EventingBasicConsumer handlers are async void in RabbitMQ.Client 6.x: nothing may escape. The body is a
            // pooled buffer that is only valid until the handler first yields, so it is copied once here.
            subscribed.Received += async (_, delivery) =>
            {
                try
                {
                    var body = delivery.Body.ToArray();
                    var actions = new RabbitMqDeliveryActions(this, delivery, body);
                    await dispatcher.DispatchAsync(body, ToContext(delivery), actions, stopping);
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    RabbitMqLog.DeliveryAbandoned(logger, queueName);
                }
                catch (Exception ex)
                {
                    RabbitMqLog.DeliveryFailed(logger, queueName, ex);
                }
            };

            lock (consumerChannelLock)
            {
                consumerTag = consumerChannel.BasicConsume(queue: queueName, autoAck: false, consumer: subscribed);
            }

            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            consumer = null;
            throw new MessageQueueException($"Failed to subscribe to queue: {ex.Message}", ex);
        }
    }

    // Stops consuming and cancels the token of handlers still running.
    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        if (subscription is not null)
        {
            await subscription.CancelAsync();
        }

        if (consumer is not null && consumerTag is not null)
        {
            lock (consumerChannelLock)
            {
                consumerChannel.BasicCancel(consumerTag);
            }
        }
        consumer = null;
        consumerTag = null;
    }

    PublisherChannel NextPublisherChannel() => publisherChannels[RoundRobin.Next(ref nextPublisherIndex, publisherChannels.Length)];

    static MessageContext ToContext(BasicDeliverEventArgs delivery)
    {
        var properties = delivery.BasicProperties;
        var headers = new Dictionary<string, string>();
        var attempt = 1;

        if (properties.Headers is not null)
        {
            foreach (var (key, value) in properties.Headers)
            {
                var text = value switch
                {
                    byte[] bytes => Encoding.UTF8.GetString(bytes),
                    string s => s,
                    IConvertible convertible => convertible.ToString(CultureInfo.InvariantCulture),
                    _ => null
                };
                if (text is null)
                    continue;

                if (key == DeliveryAttemptHeader)
                    attempt = int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0 ? parsed : 1;
                else
                    headers[key] = text;
            }
        }

        return new MessageContext
        {
            MessageId = properties.MessageId ?? string.Empty,
            CorrelationId = properties.CorrelationId,
            Timestamp = DateTimeOffset.FromUnixTimeSeconds(properties.Timestamp.UnixTime),
            DeliveryAttempt = attempt,
            Headers = headers
        };
    }

    // RabbitMQ.Client 6.x channels are not thread-safe: acks and nacks from concurrent deliveries are serialized.
    void Ack(ulong deliveryTag)
    {
        lock (consumerChannelLock)
        {
            consumerChannel.BasicAck(deliveryTag, multiple: false);
        }
    }

    void Reject(ulong deliveryTag)
    {
        lock (consumerChannelLock)
        {
            consumerChannel.BasicNack(deliveryTag, multiple: false, requeue: false);
        }
    }

    async Task RepublishAsync(BasicDeliverEventArgs delivery, byte[] body, int nextAttempt, CancellationToken cancellationToken)
    {
        var publisherChannel = NextPublisherChannel();
        var original = delivery.BasicProperties;
        var properties = publisherChannel.CreateBasicProperties();
        properties.Persistent = true;
        properties.MessageId = original.MessageId;
        properties.CorrelationId = original.CorrelationId;
        properties.Timestamp = original.Timestamp;
        properties.ContentType = original.ContentType;
        properties.Headers = original.Headers is null
            ? new Dictionary<string, object>()
            : new Dictionary<string, object>(original.Headers);
        properties.Headers[DeliveryAttemptHeader] = nextAttempt.ToString(CultureInfo.InvariantCulture);

        // Default exchange, routed straight to this queue: other queues bound to a custom exchange must not get retries.
        await publisherChannel.PublishAsync(body, properties, string.Empty, queueName, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        Dispose();
        await Task.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        disposing.Cancel();
        consumer = null;

        foreach (var publisherChannel in publisherChannels)
        {
            publisherChannel.Dispose();
        }

        consumerChannel.Dispose();
        connection.Dispose();
        subscription?.Dispose();
        disposing.Dispose();
        GC.SuppressFinalize(this);
    }

    sealed class RabbitMqDeliveryActions : IDeliveryActions
    {
        readonly RabbitMqMessageQueue<T> queue;
        readonly BasicDeliverEventArgs delivery;
        readonly byte[] body;

        public RabbitMqDeliveryActions(RabbitMqMessageQueue<T> queue, BasicDeliverEventArgs delivery, byte[] body)
        {
            this.queue = queue;
            this.delivery = delivery;
            this.body = body;
        }

        public Task CompleteAsync(CancellationToken cancellationToken)
        {
            queue.Ack(delivery.DeliveryTag);
            return Task.CompletedTask;
        }

        // Interim backoff: wait in process, republish with the next attempt number, then ack the original.
        // If the process stops during the wait, the unacked original is redelivered by the broker.
        public async Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            await queue.RepublishAsync(delivery, body, nextAttempt, cancellationToken);
            queue.Ack(delivery.DeliveryTag);
        }

        // Nack without requeue: the queue's dead-letter exchange routes the message to the dead-letter queue.
        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken)
        {
            queue.Reject(delivery.DeliveryTag);
            return Task.CompletedTask;
        }
    }
}

static partial class RabbitMqLog
{
    [LoggerMessage(EventId = 2001, Level = LogLevel.Error, Message = "Delivery from RabbitMQ queue {QueueName} failed and was left unacknowledged")]
    public static partial void DeliveryFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Debug, Message = "Delivery from RabbitMQ queue {QueueName} was interrupted by shutdown and left for redelivery")]
    public static partial void DeliveryAbandoned(ILogger logger, string queueName);
}
