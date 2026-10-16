using ESE.MessageQueue.RabbitMQ;
using ESE.MessageQueue.Retry;

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

    // Millisecond backoff so retry specs finish quickly.
    public static readonly RetryPolicy FastRetry = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(20),
        MaxDelay = TimeSpan.FromMilliseconds(100),
        UseJitter = false
    };

    public RabbitMqConfig NewConfig(string prefix) => new()
    {
        ConnectionString = broker.ConnectionString,
        QueueName = $"{prefix}-{Guid.NewGuid():N}",
        Retry = FastRetry,
        EnableDeadLetterQueue = true,
        UseQuorumQueues = true,
        QuorumInitialGroupSize = 1
    };

    public RabbitMqMessageQueue<TestMessage> Create(RabbitMqConfig config)
    {
        queues.Add(config.QueueName);
        queues.Add(config.EffectiveDeadLetterQueueName);
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
