using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests.InMemory;

// Sustained load at high concurrency (readiness §5): every message handled successfully exactly once, transient
// failures retried, concurrency bounded by MaxConcurrentCalls, nothing dead-lettered.
[Trait("Category", "Unit")]
public sealed class Given_an_in_memory_queue_under_load
{
    const int MessageCount = 2_000;
    const int MaxConcurrentCalls = 16;

    [Fact(Timeout = 60_000)]
    public async Task When_Thousands_Of_Messages_Are_Handled_Concurrently_With_Transient_Failures()
    {
        await using var queue = new InMemoryMessageQueue<TestMessage>(new InMemoryConfig
        {
            QueueName = "load",
            MaxConcurrentCalls = MaxConcurrentCalls,
            Retry = new RetryPolicy { MaxRetries = 2, InitialDelay = TimeSpan.FromMilliseconds(5), MaxDelay = TimeSpan.FromMilliseconds(20), UseJitter = false }
        });
        var probe = new LoadProbe();
        var messages = LoadProbe.Messages(MessageCount);
        var expectedFailures = messages.Select(m => m.Id).Where(LoadProbe.FailsFirstAttempt).ToList();
        await queue.SubscribeAsync(probe.HandleAsync, TestContext.Current.CancellationToken);
        var elapsed = Stopwatch.StartNew();

        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
        elapsed.Stop();

        // Ordered and compared with Equal: BeEquivalentTo is quadratic on thousands of items.
        probe.Successes.Keys.Order(StringComparer.Ordinal).Should().Equal(messages.Select(m => m.Id), "it should handle every message successfully");
        probe.Successes.Values.Should().AllSatisfy(n => n.Should().Be(1, "it should handle each message successfully exactly once"));
        probe.Failures.Keys.Order(StringComparer.Ordinal).Should().Equal(expectedFailures, "it should retry exactly the messages that failed transiently");
        probe.Failures.Values.Should().AllSatisfy(n => n.Should().Be(1, "it should deliver a failed message again only once it was retried"));
        queue.DeadLetters.Should().BeEmpty("it should dead-letter nothing when every failure is transient");
        probe.Peak.Should().BeLessThanOrEqualTo(MaxConcurrentCalls, "it should never run more than MaxConcurrentCalls handlers at once")
            .And.BeGreaterThan(1, "it should run handlers concurrently");
        elapsed.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30), "it should keep up with the load");
    }
}
