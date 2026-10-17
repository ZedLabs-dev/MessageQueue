using BenchmarkDotNet.Attributes;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.InMemory;

namespace ESE.MessageQueue.Benchmarks;

// In-memory publish -> background dispatch -> acknowledge, end to end. One operation = one message.
[MemoryDiagnoser]
public class InMemoryThroughputBenchmarks : IAsyncDisposable
{
    const int Messages = 10_000;

    readonly Order[] orders = [.. Enumerable.Range(0, Messages).Select(Order.Sample)];
    InMemoryMessageQueue<Order> queue = null!;

    [Params(1, 10)]
    public int MaxConcurrentCalls { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        queue = new InMemoryMessageQueue<Order>(new InMemoryConfig { QueueName = "orders", MaxConcurrentCalls = MaxConcurrentCalls });
        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge));
    }

    [GlobalCleanup]
    public async ValueTask DisposeAsync()
    {
        await queue.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    // The queue records every message for its test helpers; clear them so memory stays flat across iterations.
    [IterationCleanup]
    public void Clear() => queue.Clear();

    [Benchmark(OperationsPerInvoke = Messages)]
    public async Task PublishAndConsume()
    {
        foreach (var order in orders)
            await queue.PublishAsync(order);
        await queue.WaitForIdleAsync();
    }
}
