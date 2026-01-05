using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Serialization;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.RabbitMQ;

public class RabbitMqMessageQueue<T> : IMessageQueue<T>, IDisposable where T : class
{
    readonly RabbitMqConfig config;
    readonly IConnection connection;
    readonly IModel consumerChannel;
    readonly PublisherChannel[] publisherChannels;
    readonly string queueName;
    readonly string exchangeName;
    readonly string deadLetterQueueName;
    int nextPublisherIndex;
    EventingBasicConsumer? consumer;

    public RabbitMqMessageQueue(RabbitMqConfig config)
    {
        this.config = config;
        queueName = config.QueueName;
        exchangeName = string.IsNullOrWhiteSpace(config.ExchangeName)
            ? config.QueueName
            : config.ExchangeName;
        deadLetterQueueName = string.IsNullOrEmpty(config.DeadLetterQueueName)
            ? $"{config.QueueName}.deadletter"
            : config.DeadLetterQueueName;

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

            // Consumer/admin channel for queue declarations and consuming
            consumerChannel = connection.CreateModel();

            // Publisher channel pool
            var poolSize = Math.Max(1, config.PublisherChannelPoolSize);
            publisherChannels = new PublisherChannel[poolSize];
            for (var i = 0; i < poolSize; i++)
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

    void DeclareExchangeAndQueues()
    {
        // Declare the exchange
        consumerChannel.ExchangeDeclare(
            exchange: exchangeName,
            type: config.ExchangeType,
            durable: true,
            autoDelete: false,
            arguments: null);

        var deadLetterArgs = new Dictionary<string, object>();
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

            deadLetterArgs.Add("x-dead-letter-exchange", "");
            deadLetterArgs.Add("x-dead-letter-routing-key", deadLetterQueueName);

            if (config.UseQuorumQueues)
            {
                deadLetterArgs.Add("x-overflow", "reject-publish");
            }
        }

        foreach (var (key, value) in deadLetterArgs)
        {
            queueArgs[key] = value;
        }

        consumerChannel.QueueDeclare(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs);

        // Bind queue to exchange with routing key = queue name
        consumerChannel.QueueBind(
            queue: queueName,
            exchange: exchangeName,
            routingKey: queueName);
    }

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        try
        {
            var body = MessageSerializer.Serialize(message, config.SerializationFormat);

            // Round-robin selection of publisher channel
            var index = Interlocked.Increment(ref nextPublisherIndex) % publisherChannels.Length;
            var publisherChannel = publisherChannels[index];

            var properties = publisherChannel.CreateBasicProperties();
            properties.Persistent = true;
            properties.MessageId = Guid.NewGuid().ToString();
            properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            properties.ContentType = MessageSerializer.GetContentType(config.SerializationFormat);

            if (config.UseQuorumQueues)
            {
                properties.DeliveryMode = 2;
            }

            await publisherChannel.PublishAsync(
                body,
                properties,
                exchangeName,
                queueName,
                cancellationToken);
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
        foreach (var message in messages)
        {
            await PublishAsync(message, cancellationToken);
        }
    }

    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default)
    {
        try
        {
            consumer = new EventingBasicConsumer(consumerChannel);

            if (config.UseQuorumQueues)
            {
                consumerChannel.BasicQos(prefetchSize: 0, prefetchCount: (ushort)config.PrefetchCount, global: false);
            }

            consumer.Received += async (model, ea) =>
            {
                var body = ea.Body.ToArray();

                try
                {
                    var message = MessageSerializer.Deserialize<T>(body, config.SerializationFormat);
                    if (message != null)
                    {
                        var success = await handler(message);
                        if (success)
                        {
                            consumerChannel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            consumerChannel.BasicNack(ea.DeliveryTag, false, false);
                        }
                    }
                    else
                    {
                        consumerChannel.BasicNack(ea.DeliveryTag, false, false);
                    }
                }
                catch (Exception)
                {
                    consumerChannel.BasicNack(ea.DeliveryTag, false, false);
                }
            };

            consumerChannel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to subscribe to queue: {ex.Message}", ex);
        }
    }

    public Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        if (consumer != null)
        {
            consumerChannel.BasicCancel(consumer.ConsumerTags.FirstOrDefault() ?? "");
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        consumer = null;

        foreach (var publisherChannel in publisherChannels)
        {
            publisherChannel.Dispose();
        }

        consumerChannel?.Dispose();
        connection?.Dispose();
    }
}
