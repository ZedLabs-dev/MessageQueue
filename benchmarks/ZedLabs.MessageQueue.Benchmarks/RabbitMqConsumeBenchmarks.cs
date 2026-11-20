using BenchmarkDotNet.Attributes;
using RabbitMQ.Client;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Benchmarks;

// Consume throughput against a real broker: the queue holds Messages messages (published in IterationSetup, not
// measured); one operation subscribes and consumes them all, each through the full pipeline (deserialize, handler,
// ack). HandlerDelayMs simulates I/O in the handler, where MaxConcurrentCalls pays off. One operation = one message.
// Needs MQ_BENCH_RABBITMQ (set by Program).
[MemoryDiagnoser]
public class RabbitMqConsumeBenchmarks : IAsyncDisposable
{
    const int Messages = 1_000;

    readonly Order[] orders = [.. Enumerable.Range(0, Messages).Select(Order.Sample)];
    RabbitMqMessageQueue<Order> queue = null!;
    string connectionString = null!;
    string queueName = null!;
    TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    int consumed;

    [Params(1, 16)]
    public int MaxConcurrentCalls { get; set; }

    [Params(0, 1)]
    public int HandlerDelayMs { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        connectionString = Environment.GetEnvironmentVariable(BenchmarkBroker.Variable)
            ?? throw new InvalidOperationException($"{BenchmarkBroker.Variable} is not set");
        queueName = $"bench-consume-{Guid.NewGuid():N}";
        queue = new RabbitMqMessageQueue<Order>(new RabbitMqConfig
        {
            ConnectionString = connectionString,
            RequireTls = false,
            QueueName = queueName,
            UseQuorumQueues = false,
            EnableDeadLetterQueue = false,
            MaxConcurrentCalls = MaxConcurrentCalls,
            PrefetchCount = 64
        });
        await queue.InitializeAsync();
    }

    [IterationSetup]
    public void Fill()
    {
        consumed = 0;
        drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        queue.PublishAsync(orders).GetAwaiter().GetResult();
    }

    [IterationCleanup]
    public void Stop() => queue.UnsubscribeAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public async ValueTask DisposeAsync()
    {
        await queue.DisposeAsync();
        // The queue and its exchange are durable: remove them, so a broker given in MQ_BENCH_RABBITMQ is left clean.
        await using var connection = await new ConnectionFactory { Uri = new Uri(connectionString) }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeleteAsync(queueName);
        await channel.ExchangeDeleteAsync(queueName);
        GC.SuppressFinalize(this);
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task Consume()
    {
        await queue.SubscribeAsync(HandleAsync);
        await drained.Task;
    }

    async Task<MessageResult> HandleAsync(Order order, MessageContext context, CancellationToken cancellationToken)
    {
        if (HandlerDelayMs > 0)
            await Task.Delay(HandlerDelayMs, cancellationToken);
        if (Interlocked.Increment(ref consumed) == Messages)
            drained.TrySetResult();
        return MessageResult.Acknowledge;
    }
}
