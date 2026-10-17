using BenchmarkDotNet.Attributes;
using ESE.MessageQueue.RabbitMQ;

namespace ESE.MessageQueue.Benchmarks;

// Publisher-confirm throughput against a real broker (§4.2): awaiting each confirm in turn vs publishing a batch and
// awaiting the confirms together. One operation = one confirmed message. Needs MQ_BENCH_RABBITMQ (set by Program).
[MemoryDiagnoser]
public class RabbitMqPublishBenchmarks : IAsyncDisposable
{
    const int Messages = 500;

    readonly Order[] orders = [.. Enumerable.Range(0, Messages).Select(Order.Sample)];
    RabbitMqMessageQueue<Order> queue = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable(BenchmarkBroker.Variable)
            ?? throw new InvalidOperationException($"{BenchmarkBroker.Variable} is not set");
        queue = new RabbitMqMessageQueue<Order>(new RabbitMqConfig
        {
            ConnectionString = connectionString,
            RequireTls = false,
            QueueName = $"bench-{Guid.NewGuid():N}",
            UseQuorumQueues = false,
            EnableDeadLetterQueue = false
        });
        await queue.InitializeAsync();
    }

    [GlobalCleanup]
    public async ValueTask DisposeAsync()
    {
        await queue.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Messages)]
    public async Task OneByOne()
    {
        foreach (var order in orders)
            await queue.PublishAsync(order);
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public Task Batch() => queue.PublishAsync(orders);
}
