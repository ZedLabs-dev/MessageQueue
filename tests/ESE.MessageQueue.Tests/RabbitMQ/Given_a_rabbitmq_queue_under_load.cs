using System.Diagnostics;
using AwesomeAssertions;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Sustained load at high concurrency against a real broker (readiness §5): 5,000 messages, 32 concurrent handlers,
// prefetch 64, a random few milliseconds of work and a transient failure on 5% of first attempts. Every message must
// be handled successfully exactly once, the failed ones retried through the retry queue once, concurrency stay within
// MaxConcurrentCalls, and nothing be dead-lettered.
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_under_load : IAsyncLifetime
{
    const int MessageCount = 5_000;
    const int MaxConcurrentCalls = 32;

    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_under_load(RabbitMqBrokerFixture broker)
    {
        this.broker = broker;
        queues = new RabbitMqTestQueues(broker);
    }

    public ValueTask InitializeAsync()
    {
        broker.SkipUnlessAvailable();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => queues.DisposeAsync();

    [Fact(Timeout = 120_000)]
    public async Task When_Thousands_Of_Messages_Are_Handled_Concurrently_With_Transient_Failures()
    {
        var config = queues.NewConfig("load-test") with { MaxConcurrentCalls = MaxConcurrentCalls, PrefetchCount = 64 };
        var queue = queues.Create(config);
        var probe = new LoadProbe();
        var messages = LoadProbe.Messages(MessageCount);
        var expectedFailures = messages.Select(m => m.Id).Where(LoadProbe.FailsFirstAttempt).ToList();
        await queue.SubscribeAsync(probe.HandleAsync, TestContext.Current.CancellationToken);
        var elapsed = Stopwatch.StartNew();

        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => probe.Succeeded, MessageCount, TimeSpan.FromSeconds(90));
        elapsed.Stop();

        // Ordered and compared with Equal: BeEquivalentTo is quadratic on thousands of items.
        probe.Successes.Keys.Order(StringComparer.Ordinal).Should().Equal(messages.Select(m => m.Id), "it should handle every message successfully");
        probe.Successes.Values.Should().AllSatisfy(n => n.Should().Be(1, "it should handle each message successfully exactly once"));
        probe.Failures.Keys.Order(StringComparer.Ordinal).Should().Equal(expectedFailures, "it should retry exactly the messages that failed transiently");
        probe.Failures.Values.Should().AllSatisfy(n => n.Should().Be(1, "it should deliver a failed message again only once it was retried"));
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(0u,
            "it should dead-letter nothing when every failure is transient");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should leave nothing behind in the queue");
        probe.Peak.Should().BeLessThanOrEqualTo(MaxConcurrentCalls, "it should never run more than MaxConcurrentCalls handlers at once")
            .And.BeGreaterThan(1, "it should run handlers concurrently");
        elapsed.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(90), "it should keep up with the load");
    }
}
