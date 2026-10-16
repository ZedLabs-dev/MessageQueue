using ESE.MessageQueue.RabbitMQ;

namespace ESE.MessageQueue.Tests;

// Creates queues against the fixture's broker and removes every queue, dead-letter queue and exchange
// they declared when the test class is disposed.
sealed class RabbitMqTestQueues : IDisposable
{
    readonly RabbitMqBrokerFixture broker;
    readonly List<RabbitMqMessageQueue<TestMessage>> created = [];
    readonly HashSet<string> queues = [];
    readonly HashSet<string> exchanges = [];

    public RabbitMqTestQueues(RabbitMqBrokerFixture broker)
    {
        this.broker = broker;
    }

    public RabbitMqConfig NewConfig(string prefix) => new()
    {
        ConnectionString = broker.ConnectionString,
        QueueName = $"{prefix}-{Guid.NewGuid():N}",
        RetryAttempts = 3,
        EnableDeadLetterQueue = true,
        UseQuorumQueues = true,
        QuorumInitialGroupSize = 1
    };

    public RabbitMqMessageQueue<TestMessage> Create(RabbitMqConfig config)
    {
        queues.Add(config.QueueName);
        queues.Add(string.IsNullOrEmpty(config.DeadLetterQueueName) ? $"{config.QueueName}.deadletter" : config.DeadLetterQueueName);
        exchanges.Add(string.IsNullOrWhiteSpace(config.ExchangeName) ? config.QueueName : config.ExchangeName);

        var queue = new RabbitMqMessageQueue<TestMessage>(config);
        created.Add(queue);
        return queue;
    }

    // Declares a plain queue bound to `exchange`, to observe what is routed through that exchange.
    public string BindProbeQueue(string exchange, string routingKey)
    {
        var probe = $"probe-{Guid.NewGuid():N}";
        queues.Add(probe);
        broker.DeclareBoundQueue(probe, exchange, routingKey);
        return probe;
    }

    public void Dispose()
    {
        foreach (var queue in created)
        {
            queue.Dispose();
        }

        if (queues.Count > 0)
        {
            broker.DeleteQueuesAndExchanges(queues, exchanges);
        }
    }
}
