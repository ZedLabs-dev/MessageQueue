using System.Text;
using System.Text.Json;
using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.Exceptions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AgentReview.MessageQueue.RabbitMQ;

public class RabbitMqMessageQueue<T> : IMessageQueue<T>, IDisposable where T : class
{
    readonly RabbitMqConfig config;
    readonly IConnection connection;
    readonly IModel channel;
    readonly string queueName;
    readonly string deadLetterQueueName;
    EventingBasicConsumer? consumer;

    public RabbitMqMessageQueue(RabbitMqConfig config)
    {
        this.config = config;
        queueName = config.QueueName;
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
            channel = connection.CreateModel();
            
            if (this.config.EnablePublisherConfirms)
            {
                channel.ConfirmSelect();
            }
            
            DeclareQueues();
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to initialize RabbitMQ connection: {ex.Message}", ex);
        }
    }

    void DeclareQueues()
    {
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
            
            channel.QueueDeclare(
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

        channel.QueueDeclare(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs);
    }

    public Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        try
        {
            var json = JsonSerializer.Serialize(message);
            var body = Encoding.UTF8.GetBytes(json);
            
            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.MessageId = Guid.NewGuid().ToString();
            properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            
            if (config.UseQuorumQueues)
            {
                properties.DeliveryMode = 2;
            }
            
            channel.BasicPublish(
                exchange: "",
                routingKey: queueName,
                basicProperties: properties,
                body: body);
            
            if (config.EnablePublisherConfirms)
            {
                channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
            }
            
            return Task.CompletedTask;
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
            consumer = new EventingBasicConsumer(channel);
            
            if (config.UseQuorumQueues)
            {
                channel.BasicQos(prefetchSize: 0, prefetchCount: (ushort)config.PrefetchCount, global: false);
            }

            consumer.Received += async (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var json = Encoding.UTF8.GetString(body);
                
                try
                {
                    var message = JsonSerializer.Deserialize<T>(json);
                    if (message != null)
                    {
                        var success = await handler(message);
                        if (success)
                        {
                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            channel.BasicNack(ea.DeliveryTag, false, false);
                        }
                    }
                    else
                    {
                        channel.BasicNack(ea.DeliveryTag, false, false);
                    }
                }
                catch (Exception)
                {
                    channel.BasicNack(ea.DeliveryTag, false, false);
                }
            };

            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
            
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
            channel.BasicCancel(consumer.ConsumerTags.FirstOrDefault() ?? "");
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        consumer = null;
        channel?.Dispose();
        connection?.Dispose();
    }
}
