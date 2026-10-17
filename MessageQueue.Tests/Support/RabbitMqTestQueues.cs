using ESE.MessageQueue.RabbitMQ;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests;

// Creates queues against the fixture's broker and removes every queue, retry queue, dead-letter queue and exchange
// they declared when the test class is disposed.
sealed class RabbitMqTestQueues : IAsyncDisposable
{
    readonly RabbitMqBrokerFixture broker;
    readonly List<RabbitMqMessageQueue<TestMessage>> created = [];
    readonly HashSet<string> queues = [];
    readonly HashSet<string> exchanges = [];
    readonly List<string> mainQueues = [];

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
        // Testcontainers' host is not always localhost, and the test broker has no TLS listener.
        RequireTls = false,
        QueueName = $"{prefix}-{Guid.NewGuid():N}",
        Retry = FastRetry,
        EnableDeadLetterQueue = true,
        UseQuorumQueues = true,
        QuorumInitialGroupSize = 1
    };

    // Removes the queues and exchange `config` declares on dispose, for queues created outside this helper (DI).
    public void Track(RabbitMqConfig config)
    {
        queues.Add(config.QueueName);
        mainQueues.Add(config.QueueName);
        queues.Add(config.EffectiveDeadLetterQueueName);
        exchanges.Add(string.IsNullOrWhiteSpace(config.ExchangeName) ? config.QueueName : config.ExchangeName);
    }

    public RabbitMqMessageQueue<TestMessage> Create(RabbitMqConfig config)
    {
        Track(config);

        var queue = new RabbitMqMessageQueue<TestMessage>(config);
        created.Add(queue);
        return queue;
    }

    // Declares a plain queue bound to `exchange`, to observe what is routed through that exchange.
    public async Task<string> BindProbeQueueAsync(string exchange, string routingKey)
    {
        var probe = $"probe-{Guid.NewGuid():N}";
        queues.Add(probe);
        await broker.DeclareBoundQueueAsync(probe, exchange, routingKey);
        return probe;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var queue in created)
        {
            await queue.DisposeAsync();
        }

        // Retry queues are declared lazily by the library, so they are found by name.
        foreach (var main in mainQueues)
        {
            queues.UnionWith(await broker.ListQueuesAsync($"{main}.retry."));
        }

        if (queues.Count > 0)
        {
            await broker.DeleteQueuesAndExchangesAsync(queues, exchanges);
        }
    }
}
