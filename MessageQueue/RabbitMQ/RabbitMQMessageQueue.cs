using System.Text;
using System.Text.Json;
using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.Exceptions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace AgentReview.MessageQueue.RabbitMQ;

public class RabbitMQMessageQueue<T> : IMessageQueue<T>, IDisposable where T : class
{
    private readonly RabbitMQConfig _config;
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _queueName;
    private readonly string _deadLetterQueueName;
    private EventingBasicConsumer? _consumer;

    public RabbitMQMessageQueue(RabbitMQConfig config)
    {
        _config = config;
        _queueName = config.QueueName;
        _deadLetterQueueName = string.IsNullOrEmpty(config.DeadLetterQueueName) 
            ? $"{config.QueueName}.deadletter" 
            : config.DeadLetterQueueName;
        
        try
        {
            var factory = new ConnectionFactory 
            { 
                Uri = new Uri(config.ConnectionString),
                RequestedHeartbeat = _config.HeartbeatInterval,
                NetworkRecoveryInterval = _config.NetworkRecoveryInterval,
                AutomaticRecoveryEnabled = true
            };
            
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            
            if (_config.EnablePublisherConfirms)
            {
                _channel.ConfirmSelect();
            }
            
            DeclareQueues();
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to initialize RabbitMQ connection: {ex.Message}", ex);
        }
    }

    private void DeclareQueues()
    {
        var deadLetterArgs = new Dictionary<string, object>();
        var queueArgs = new Dictionary<string, object>();
        
        if (_config.UseQuorumQueues)
        {
            queueArgs.Add("x-queue-type", "quorum");
            queueArgs.Add("x-quorum-initial-group-size", _config.QuorumInitialGroupSize);
        }
        
        if (_config.EnableDeadLetterQueue)
        {
            var deadLetterQueueArgs = new Dictionary<string, object>();
            
            if (_config.UseQuorumQueues)
            {
                deadLetterQueueArgs.Add("x-queue-type", "quorum");
                deadLetterQueueArgs.Add("x-quorum-initial-group-size", _config.QuorumInitialGroupSize);
            }
            
            _channel.QueueDeclare(
                queue: _deadLetterQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: deadLetterQueueArgs);
            
            deadLetterArgs.Add("x-dead-letter-exchange", "");
            deadLetterArgs.Add("x-dead-letter-routing-key", _deadLetterQueueName);
            
            if (_config.UseQuorumQueues)
            {
                deadLetterArgs.Add("x-overflow", "reject-publish");
            }
        }

        foreach (var (key, value) in deadLetterArgs)
        {
            queueArgs[key] = value;
        }

        _channel.QueueDeclare(
            queue: _queueName,
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
            
            var properties = _channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.MessageId = Guid.NewGuid().ToString();
            properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            
            if (_config.UseQuorumQueues)
            {
                properties.DeliveryMode = 2;
            }
            
            _channel.BasicPublish(
                exchange: "",
                routingKey: _queueName,
                basicProperties: properties,
                body: body);
            
            if (_config.EnablePublisherConfirms)
            {
                _channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
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
            _consumer = new EventingBasicConsumer(_channel);
            
            if (_config.UseQuorumQueues)
            {
                _channel.BasicQos(prefetchSize: 0, prefetchCount: (ushort)_config.PrefetchCount, global: false);
            }

            _consumer.Received += async (model, ea) =>
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
                            _channel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            _channel.BasicNack(ea.DeliveryTag, false, false);
                        }
                    }
                    else
                    {
                        _channel.BasicNack(ea.DeliveryTag, false, false);
                    }
                }
                catch (Exception)
                {
                    _channel.BasicNack(ea.DeliveryTag, false, false);
                }
            };

            _channel.BasicConsume(queue: _queueName, autoAck: false, consumer: _consumer);
            
            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to subscribe to queue: {ex.Message}", ex);
        }
    }

    public Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        if (_consumer != null)
        {
            _channel.BasicCancel(_consumer.ConsumerTags.FirstOrDefault() ?? "");
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _consumer = null;
        _channel?.Dispose();
        _connection?.Dispose();
    }
}
