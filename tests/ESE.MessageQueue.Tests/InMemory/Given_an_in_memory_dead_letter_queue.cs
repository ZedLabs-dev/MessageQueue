using System.Collections.Concurrent;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Tests.Telemetry;

namespace ESE.MessageQueue.Tests.InMemory;

// ADR 0023 on the in-memory provider: its DeadLetters are the store. A replayed message is a fresh first delivery of
// the same message (id, correlation id, headers, trace) with the full retry budget.
[Trait("Category", "Unit")]
public sealed class Given_an_in_memory_dead_letter_queue : IAsyncDisposable
{
    static readonly RetryPolicy FastRetry = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(5),
        MaxDelay = TimeSpan.FromMilliseconds(20),
        UseJitter = false
    };

    readonly TestTelemetryProvider telemetry = new();
    readonly InMemoryMessageQueue<TestMessage> queue;
    readonly ConcurrentQueue<(TestMessage Message, MessageContext Context)> deliveries = new();
    volatile bool accept;

    public Given_an_in_memory_dead_letter_queue()
    {
        queue = new InMemoryMessageQueue<TestMessage>(new InMemoryConfig { QueueName = "orders", Retry = FastRetry }, telemetry);
    }

    public ValueTask DisposeAsync() => queue.DisposeAsync();

    // Fails every delivery (retries, then dead-letters) until `accept` is set.
    async Task SubscribeAsync(MessageResult failure = MessageResult.Retry) =>
        await queue.SubscribeAsync((message, context, _) =>
        {
            deliveries.Enqueue((message, context));
            return Task.FromResult(accept ? MessageResult.Acknowledge : failure);
        }, TestContext.Current.CancellationToken);

    async Task DeadLetterAsync(string id)
    {
        await queue.PublishAsync(new TestMessage(id, "content"), new PublishOptions
        {
            MessageId = id,
            CorrelationId = $"corr-{id}",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void When_Asking_What_It_Supports() =>
        queue.Capabilities.DeadLetterOperations.Should().Be(DeadLetterOperations.All, "it should support peek, replay and purge");

    [Fact(Timeout = 10_000)]
    public async Task When_Peeking()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        var before = DateTimeOffset.UtcNow;
        await DeadLetterAsync("order-1");

        var letter = (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Single();

        letter.MessageId.Should().Be("order-1", "it should report the message id");
        letter.CorrelationId.Should().Be("corr-order-1", "it should report the correlation id");
        letter.ReasonCode.Should().Be(DeadLetterReasons.Rejected, "it should report the reason");
        letter.DeliveryAttempt.Should().Be(1, "it should report the attempt it died on");
        letter.DeadLetteredAt.Should().BeOnOrAfter(before, "it should record when it was dead-lettered")
            .And.BeOnOrBefore(DateTimeOffset.UtcNow);
        letter.EnqueuedAt.Should().BeCloseTo(before, TimeSpan.FromSeconds(10), "it should report when it was published");
        letter.Headers.Should().Contain("tenant", "acme", "it should report the headers");
        letter.Message.Should().Be(new TestMessage("order-1", "content"), "it should report the message");
        queue.DeadLetters.Should().ContainSingle("it should not remove anything");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying_After_The_Handler_Is_Fixed()
    {
        await SubscribeAsync();
        await DeadLetterAsync("order-1");
        var original = deliveries.First().Context;
        accept = true;

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(1, "it should replay the dead letter");
        queue.DeadLetters.Should().BeEmpty("it should remove the replayed dead letter");
        queue.ProcessedMessages.Should().ContainSingle("it should deliver the replayed message to the subscriber")
            .Which.Should().Be(new TestMessage("order-1", "content"));
        var replayed = deliveries.Last().Context;
        replayed.DeliveryAttempt.Should().Be(1, "it should deliver the replay as a first attempt");
        replayed.MessageId.Should().Be("order-1", "it should keep the message id, the idempotency key");
        replayed.CorrelationId.Should().Be("corr-order-1", "it should keep the correlation id");
        replayed.Headers.Should().Equal(original.Headers, "it should keep the application headers");
        replayed.TraceParent.Should().Be(original.TraceParent, "it should keep the publisher's trace context").And.NotBeNull();
        telemetry.DeadLettersReplayed.Should().ContainSingle("it should count the replay").Which.Count.Should().Be(1);
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Replayed_Message_Fails_Again()
    {
        await SubscribeAsync();
        await DeadLetterAsync("order-1");
        var firstLife = deliveries.Select(d => d.Context.DeliveryAttempt).ToList();

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        firstLife.Should().Equal([1, 2, 3], "it should have used the whole retry budget before dead-lettering");
        deliveries.Skip(3).Select(d => d.Context.DeliveryAttempt).Should().Equal([1, 2, 3], "it should give the replay the full retry budget again");
        queue.DeadLetters.Should().ContainSingle("it should dead-letter it again once the retries are used up")
            .Which.Reason.Should().Be(DeadLetterReasons.MaxRetriesExceeded);
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying_Without_A_Subscriber()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("order-1");
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        queue.PendingCount.Should().Be(1, "it should queue the replayed message until a subscriber takes it");
        queue.DeadLetters.Should().BeEmpty("it should remove it from the dead letters");
        accept = true;
        await SubscribeAsync();
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
        queue.ProcessedMessages.Should().ContainSingle("it should deliver it once subscribed");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying_As_A_Dry_Run()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("order-1");

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10, DryRun = true }, TestContext.Current.CancellationToken);

        result.Matched.Should().Be(1, "it should count what it would replay");
        queue.DeadLetters.Should().ContainSingle("it should change nothing");
        queue.PendingCount.Should().Be(0, "it should queue nothing");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Purging()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("keep");
        await DeadLetterAsync("drop");

        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10, Filter = m => m.MessageId == "drop" },
            TestContext.Current.CancellationToken);

        purged.Should().Be(1, "it should delete what the filter selects");
        queue.DeadLetters.Select(d => d.Context.MessageId).Should().Equal(["keep"], "it should keep the rest");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Two_Dead_Letters_Share_A_Message_Id()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("twin");
        await queue.PublishAsync(new TestMessage("twin", "content"), new PublishOptions
        {
            MessageId = "twin",
            CorrelationId = "corr-twin",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 2 }, TestContext.Current.CancellationToken);

        purged.Should().Be(2, "it should delete each dead letter once, even when two share a message id");
        queue.DeadLetters.Should().BeEmpty("it should delete both");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Dead_Letters_Are_Cleared_During_A_Replay()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("order-1");
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = _ =>
            {
                queue.Clear();
                return true;
            }
        }, TestContext.Current.CancellationToken);

        result.Failed.Should().Be(1, "it should fail a dead letter that is no longer there");
        result.Replayed.Should().Be(0, "it should not replay it");
        queue.PendingCount.Should().Be(0, "it should not queue a dead letter Clear() removed");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Dead_Letters_Are_Cleared_During_A_Purge()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("order-1");

        var purge = () => queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = _ =>
            {
                queue.Clear();
                return true;
            }
        }, TestContext.Current.CancellationToken);

        await purge.Should().ThrowAsync<ESE.MessageQueue.Exceptions.MessageQueueException>(
            "it should not report a dead letter Clear() removed as purged");
        telemetry.DeadLettersPurged.Should().BeEmpty("it should count nothing as purged");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Queue_Is_Disposed()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("order-1");
        await queue.DisposeAsync();

        var replay = () => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);

        await replay.Should().ThrowAsync<ObjectDisposedException>("it should refuse dead-letter operations after dispose");
        queue.DeadLetters.Should().ContainSingle("it should keep the dead letter");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Queue_Is_Disposed_During_A_Replay()
    {
        await SubscribeAsync(MessageResult.DeadLetter);
        await DeadLetterAsync("first");
        await DeadLetterAsync("second");
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = m =>
            {
                if (m.MessageId == "second")
                    queue.Dispose();
                return true;
            }
        }, TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(1, "it should replay the first");
        result.Failed.Should().Be(1, "it should fail the one it can no longer queue");
        queue.DeadLetters.Select(d => d.Context.MessageId).Should().Equal(["second"], "it should keep a dead letter it could not queue");
    }
}
