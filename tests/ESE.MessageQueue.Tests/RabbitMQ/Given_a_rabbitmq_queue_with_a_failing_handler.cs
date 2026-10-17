using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Retry, backoff and dead-letter behavior on a real broker. The shared dispatcher decides; these specs check that
// the RabbitMQ transport carries the decisions out (ADR 0004, ADR 0006).
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_with_a_failing_handler : IAsyncLifetime
{
    const string ReasonHeader = "x-ese-dead-letter-reason";
    const string DetailHeader = "x-ese-dead-letter-detail";

    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_with_a_failing_handler(RabbitMqBrokerFixture broker)
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

    async Task<IReadOnlyDictionary<string, string>> TakeDeadLetterAsync(string deadLetterQueue)
    {
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(deadLetterQueue), 1, Wait.DeliveryTimeout);
        var headers = await broker.TakeMessageHeadersAsync(deadLetterQueue);
        headers.Should().NotBeNull("it should move the message to the dead-letter queue");
        return headers!;
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Handler_Always_Fails()
    {
        var config = queues.NewConfig("retry-test") with
        {
            Retry = new RetryPolicy
            {
                MaxRetries = 2,
                InitialDelay = TimeSpan.FromMilliseconds(200),
                BackoffMultiplier = 2,
                MaxDelay = TimeSpan.FromSeconds(1),
                UseJitter = false
            }
        };
        var queue = queues.Create(config);
        var attempts = new ConcurrentQueue<(int Attempt, TimeSpan At)>();
        var clock = Stopwatch.StartNew();

        await queue.SubscribeAsync((_, context, _) =>
        {
            attempts.Enqueue((context.DeliveryAttempt, clock.Elapsed));
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("poison", "always fails"), TestContext.Current.CancellationToken);
        var headers = await TakeDeadLetterAsync(config.EffectiveDeadLetterQueueName);

        attempts.Select(a => a.Attempt).Should().Equal([1, 2, 3],
            "it should make the first attempt plus MaxRetries retries, counting attempts across redeliveries");
        var at = attempts.Select(a => a.At).ToArray();
        (at[1] - at[0]).Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(180), "it should not retry before the first backoff");
        (at[1] - at[0]).Should().BeLessThan(TimeSpan.FromMilliseconds(1200), "it should retry soon after the first backoff (200 ms)");
        (at[2] - at[1]).Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(360), "it should not retry before the second backoff");
        (at[2] - at[1]).Should().BeLessThan(TimeSpan.FromMilliseconds(1400), "it should retry soon after the second backoff (400 ms)");
        headers.Should().Contain(ReasonHeader, DeadLetterReasons.MaxRetriesExceeded, "it should record why the message was dead-lettered");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should leave nothing behind on the main queue");
    }

    // Highest ready count seen while watching `queue` for `window`: a requeue after a channel closes can take a
    // moment to show up, so a single read straight after closing is not enough.
    async Task<uint> PeakDepthAsync(string queue, TimeSpan window)
    {
        var peak = 0u;
        var watch = Stopwatch.StartNew();
        do
        {
            peak = Math.Max(peak, await broker.GetMessageCountAsync(queue));
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
        while (watch.Elapsed < window);
        return peak;
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_The_Handler_Fails_Once(bool quorum)
    {
        var config = queues.NewConfig("retry-once-test") with { UseQuorumQueues = quorum };
        var queue = queues.Create(config);
        var attempts = new ConcurrentQueue<int>();

        await queue.SubscribeAsync((_, context, _) =>
        {
            attempts.Enqueue(context.DeliveryAttempt);
            return Task.FromResult(context.DeliveryAttempt == 1 ? MessageResult.Retry : MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("flaky", "fails once"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => attempts.Count, 2, Wait.DeliveryTimeout);
        await queue.DisposeAsync();

        attempts.Should().Equal([1, 2], "it should handle the message once more after the retry, and no more");
        (await PeakDepthAsync(config.QueueName, TimeSpan.FromSeconds(1.5))).Should().Be(0u,
            "it should have acknowledged the original delivery, so closing the channel brings nothing back");
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(0u, "it should not dead-letter a retried message");
        foreach (var retryQueue in await broker.ListQueuesAsync($"{config.QueueName}.retry."))
        {
            (await broker.GetMessageCountAsync(retryQueue)).Should().Be(0u, "it should leave no copy waiting in a retry queue");
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Retry_Is_Waiting_Out_Its_Backoff()
    {
        var config = queues.NewConfig("backoff-slot-test") with
        {
            PrefetchCount = 1,
            MaxConcurrentCalls = 1,
            Retry = new RetryPolicy { MaxRetries = 1, InitialDelay = TimeSpan.FromSeconds(3), MaxDelay = TimeSpan.FromSeconds(3), UseJitter = false }
        };
        var queue = queues.Create(config);
        var handled = new ConcurrentDictionary<string, TimeSpan>();
        var clock = Stopwatch.StartNew();

        await queue.SubscribeAsync((message, context, _) =>
        {
            handled.TryAdd($"{message.Id}#{context.DeliveryAttempt}", clock.Elapsed);
            return Task.FromResult(message.Id == "slow" ? MessageResult.Retry : MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("slow", "fails first"), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("next", "behind it"), TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => handled.ContainsKey("next#1"), Wait.DeliveryTimeout);

        handled.Should().ContainKey("next#1", "it should deliver the next message");
        (handled["next#1"] - handled["slow#1"]).Should().BeLessThan(TimeSpan.FromSeconds(1.5),
            "it should not hold the consumer's only prefetch slot while a retry waits out its 3 s backoff");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Handler_Rejects_The_Message()
    {
        var config = queues.NewConfig("reject-test");
        var queue = queues.Create(config);

        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("bad", "rejected"), TestContext.Current.CancellationToken);
        var headers = await TakeDeadLetterAsync(config.EffectiveDeadLetterQueueName);

        headers.Should().Contain(ReasonHeader, DeadLetterReasons.Rejected, "it should record that the handler rejected the message");
        headers.Should().NotContainKey(DetailHeader, "it should add a detail only when there is one");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Handler_Throws_On_The_Last_Attempt()
    {
        var config = queues.NewConfig("throw-test") with { Retry = RetryPolicy.None };
        var queue = queues.Create(config);

        await queue.SubscribeAsync((_, _, _) => throw new InvalidOperationException("card 4111-1111 declined"),
            TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("boom", "throws"), TestContext.Current.CancellationToken);
        var headers = await TakeDeadLetterAsync(config.EffectiveDeadLetterQueueName);

        headers.Should().Contain(ReasonHeader, DeadLetterReasons.MaxRetriesExceeded, "it should record the reason code");
        headers.Should().Contain(DetailHeader, typeof(InvalidOperationException).FullName!, "it should record the exception type");
        headers.Values.Should().NotContain(v => v.Contains("4111", StringComparison.Ordinal),
            "it should never copy exception text into the broker");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Message_Is_Too_Large()
    {
        var config = queues.NewConfig("too-large-test") with { MaxMessageSizeBytes = 16 };
        var queue = queues.Create(config);
        var handled = 0;

        await queue.SubscribeAsync(_ => Task.FromResult(Interlocked.Increment(ref handled) > 0), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("big", new string('x', 1024)), TestContext.Current.CancellationToken);
        var headers = await TakeDeadLetterAsync(config.EffectiveDeadLetterQueueName);

        headers.Should().Contain(ReasonHeader, DeadLetterReasons.MessageTooLarge, "it should dead-letter an oversized body");
        handled.Should().Be(0, "it should never hand an oversized message to the handler");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Dead_Lettering_Is_Disabled()
    {
        var config = queues.NewConfig("no-dlq-test") with { EnableDeadLetterQueue = false };
        var queue = queues.Create(config);
        var handled = 0;

        await queue.SubscribeAsync((_, _, _) =>
        {
            Interlocked.Increment(ref handled);
            return Task.FromResult(MessageResult.DeadLetter);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("drop", "me"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => Volatile.Read(ref handled), 1, Wait.DeliveryTimeout);
        await queue.DisposeAsync();

        handled.Should().Be(1, "it should handle the message once");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should drop the message instead of keeping it");
        (await broker.ListQueuesAsync(config.EffectiveDeadLetterQueueName)).Should().BeEmpty("it should not declare a dead-letter queue");
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_A_Consumer_Crashed_Mid_Handler(bool quorum)
    {
        var config = queues.NewConfig("crash-test") with { UseQuorumQueues = quorum };
        var queue = queues.Create(config);
        var attempts = new ConcurrentQueue<int>();

        await queue.PublishAsync(new TestMessage("crash", "process died"), TestContext.Current.CancellationToken);
        await broker.TakeWithoutAckAndDisconnectAsync(config.QueueName);
        await queue.SubscribeAsync((_, context, _) =>
        {
            attempts.Enqueue(context.DeliveryAttempt);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => attempts.Count, 1, Wait.DeliveryTimeout);

        attempts.Should().Equal([2], "it should count a delivery that died mid-handler as a used attempt, so crash loops end");
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_A_Foreign_Publisher_Sets_Attempt_Headers(bool quorum)
    {
        var config = queues.NewConfig("forged-attempt-test") with { UseQuorumQueues = quorum };
        var queue = queues.Create(config);
        var attempts = new ConcurrentDictionary<string, int>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            attempts.TryAdd(message.Id, context.DeliveryAttempt);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        foreach (var (id, header) in new[] { ("count", "x-delivery-count"), ("attempt", "x-delivery-attempt") })
        {
            await broker.PublishRawAsync(config.QueueName,
                ESE.MessageQueue.Providers.MessageSerializer.Serialize(new TestMessage(id, "c"), ESE.MessageQueue.Serialization.SerializationFormat.Json),
                new Dictionary<string, string> { [header] = "100" });
        }
        await Wait.ForCountAsync(() => attempts.Count, 2, Wait.DeliveryTimeout);

        attempts.Should().BeEquivalentTo(new Dictionary<string, int> { ["count"] = 1, ["attempt"] = 1 },
            "it should trust attempt headers only from the broker's redelivery and the library's own retry queues");
    }
}
