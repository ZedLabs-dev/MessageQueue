using System.Collections.Concurrent;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Tests.RabbitMQ;

[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_with_a_subscriber : IAsyncLifetime
{
    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_with_a_subscriber(RabbitMqBrokerFixture broker)
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

    static Task<bool> Collect(ConcurrentQueue<TestMessage> received, TestMessage message)
    {
        received.Enqueue(message);
        return Task.FromResult(true);
    }

    [Fact]
    public async Task When_Publishing_And_Subscribing()
    {
        var queue = queues.Create(queues.NewConfig("integration-test"));
        var received = new ConcurrentQueue<TestMessage>();
        var message = new TestMessage("integration-test", "test-content");

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should receive the published message")
            .Which.Should().Be(message, "it should receive the correct message id and content");
    }

    [Fact]
    public async Task When_Publishing_Multiple_Messages()
    {
        var queue = queues.Create(queues.NewConfig("integration-test"));
        var received = new ConcurrentQueue<TestMessage>();
        var messages = new[]
        {
            new TestMessage("msg-1", "content-1"),
            new TestMessage("msg-2", "content-2"),
            new TestMessage("msg-3", "content-3")
        };

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 3, Wait.DeliveryTimeout);

        received.Should().BeEquivalentTo(messages, "it should receive all published messages");
    }

    [Fact]
    public async Task When_Using_Quorum_Queue()
    {
        var queue = queues.Create(queues.NewConfig("integration-test") with { UseQuorumQueues = true });
        var received = new ConcurrentQueue<TestMessage>();
        var message = new TestMessage("quorum-test", "test-content");

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should receive the message via quorum queue")
            .Which.Id.Should().Be(message.Id, "it should receive the correct message");
    }

    [Fact]
    public async Task When_Publishing_Concurrently_With_Publisher_Confirms()
    {
        var queue = queues.Create(queues.NewConfig("integration-test"));
        var received = new ConcurrentQueue<TestMessage>();
        const int messageCount = 100;

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await Task.WhenAll(Enumerable.Range(0, messageCount)
            .Select(i => queue.PublishAsync(new TestMessage($"concurrent-{i}", $"content-{i}"), TestContext.Current.CancellationToken)));
        await Wait.ForCountAsync(() => received.Count, messageCount, Wait.DeliveryTimeout);

        received.Should().HaveCount(messageCount, "it should receive all concurrently published messages");
    }

    [Fact]
    public async Task When_Publishing_With_Custom_Exchange()
    {
        var config = queues.NewConfig("exchange-test") with
        {
            ExchangeName = $"test-exchange-{Guid.NewGuid():N}",
            ExchangeType = "direct",
            EnableDeadLetterQueue = false,
            EnablePublisherConfirms = true
        };
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);
        var probeQueue = await queues.BindProbeQueueAsync(config.ExchangeName!, routingKey: config.QueueName);
        var received = new ConcurrentQueue<TestMessage>();
        var message = new TestMessage("exchange-test", "test-content");

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(probeQueue), 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should receive message via custom exchange")
            .Which.Id.Should().Be(message.Id, "it should route the message through the custom exchange to the queue");
        (await broker.GetMessageCountAsync(probeQueue)).Should().Be(1u,
            "it should publish to the custom exchange, so another queue bound to it gets a copy");
    }

    [Fact]
    public async Task When_Publishing_With_Channel_Pool()
    {
        var config = queues.NewConfig("pool-test") with
        {
            EnableDeadLetterQueue = false,
            EnablePublisherConfirms = true,
            PublisherChannelPoolSize = 4,
            MaxOutstandingConfirms = 500
        };
        var queue = queues.Create(config);
        var received = new ConcurrentQueue<TestMessage>();
        const int messageCount = 200;

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await Task.WhenAll(Enumerable.Range(0, messageCount)
            .Select(i => queue.PublishAsync(new TestMessage($"pool-{i}", $"content-{i}"), TestContext.Current.CancellationToken)));
        await Wait.ForCountAsync(() => received.Count, messageCount, Wait.DeliveryTimeout);

        received.Should().HaveCount(messageCount, "it should receive all messages published via channel pool");
    }

    [Fact]
    public async Task When_Publishing_Without_Confirms()
    {
        var config = queues.NewConfig("no-confirm-test") with
        {
            EnableDeadLetterQueue = false,
            UseQuorumQueues = false,
            EnablePublisherConfirms = false,
            PublisherChannelPoolSize = 2
        };
        var queue = queues.Create(config);
        const int messageCount = 100;

        var action = () => Task.WhenAll(Enumerable.Range(0, messageCount)
            .Select(i => queue.PublishAsync(new TestMessage($"fast-{i}", $"content-{i}"), TestContext.Current.CancellationToken)));

        await action.Should().NotThrowAsync("it should publish without waiting for broker confirms");
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(config.QueueName), messageCount, Wait.DeliveryTimeout);
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be((uint)messageCount, "it should still deliver every message to the queue");
    }

    [Fact]
    public async Task When_Publishing_With_Options()
    {
        var queue = queues.Create(queues.NewConfig("metadata-test"));
        var contexts = new ConcurrentQueue<MessageContext>();

        await queue.SubscribeAsync((_, context, _) =>
        {
            contexts.Enqueue(context);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("1", "with options"), new PublishOptions
        {
            MessageId = "order-1",
            CorrelationId = "checkout-1",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("2", "without options"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => contexts.Count, 2, Wait.DeliveryTimeout);

        var withOptions = contexts.Single(c => c.MessageId == "order-1");
        withOptions.CorrelationId.Should().Be("checkout-1", "it should carry the correlation id through the broker");
        withOptions.Headers.Should().Contain("tenant", "acme", "it should carry application headers through the broker");
        withOptions.DeliveryAttempt.Should().Be(1, "it should report the first delivery as attempt 1");
        var generated = contexts.Single(c => c.MessageId != "order-1");
        Guid.Parse(generated.MessageId).Version.Should().Be(7, "it should generate a version 7 message id");
    }

    [Fact]
    public void When_Reading_Capabilities()
    {
        var queue = queues.Create(queues.NewConfig("capabilities-test"));

        queue.Capabilities.Durable.Should().BeTrue("it should report that RabbitMQ keeps messages across restarts");
        queue.Capabilities.DelayedRedelivery.Should().BeTrue("it should wait out retry backoff on the broker, not in a consumer slot");
    }

    [Fact]
    public async Task When_Subscribing_Twice()
    {
        var queue = queues.Create(queues.NewConfig("resubscribe-test"));
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        var act = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>("it should allow one subscriber per queue");
    }

    [Fact]
    public async Task When_Unsubscribing_While_A_Handler_Runs()
    {
        var queue = queues.Create(queues.NewConfig("unsubscribe-test"));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await queue.SubscribeAsync(async (_, _, ct) =>
        {
            using var registration = ct.Register(() => cancelled.TrySetResult());
            started.TrySetResult();
            await cancelled.Task;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("1", "long running"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        var act = () => cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should cancel the handler's token when the subscription stops");
    }

    [Fact]
    public async Task When_Publishing_A_Reserved_Header()
    {
        var config = queues.NewConfig("reserved-header-test");
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);

        var act = () => queue.PublishAsync(new TestMessage("1", "forged"),
            new PublishOptions { Headers = new Dictionary<string, string> { ["x-delivery-attempt"] = "99" } },
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ArgumentException>("it should not let a publisher forge the retry counter");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should publish nothing");
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_Unsubscribing_Mid_Handler_Then_Resubscribing(bool quorum)
    {
        var queue = queues.Create(queues.NewConfig("redeliver-test") with { UseQuorumQueues = quorum });
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new ConcurrentQueue<(TestMessage Message, MessageContext Context)>();

        await queue.SubscribeAsync(async (_, _, ct) =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("interrupted", "redeliver me"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        await queue.SubscribeAsync((message, context, _) =>
        {
            received.Enqueue((message, context));
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should give an interrupted delivery back to the broker for redelivery")
            .Which.Message.Id.Should().Be("interrupted", "it should redeliver the message whose handler was cancelled");
        received.Single().Context.DeliveryAttempt.Should().Be(2,
            "it should count the interrupted delivery as a used attempt (ADR 0006), so a handler that never finishes cannot loop forever");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Resubscribing_After_Unsubscribing()
    {
        var queue = queues.Create(queues.NewConfig("resubscribe-after-test"));
        var received = new ConcurrentQueue<TestMessage>();
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("after", "resubscribed"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should deliver to a subscriber added after an unsubscribe");
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_Handlers_Run_Concurrently(bool quorum)
    {
        var config = queues.NewConfig("concurrency-test") with { UseQuorumQueues = quorum, PrefetchCount = 20, MaxConcurrentCalls = 3 };
        var peak = await MeasurePeakConcurrencyAsync(config, messageCount: 12, TestContext.Current.CancellationToken);

        peak.Should().BeLessThanOrEqualTo(3, "it should run at most MaxConcurrentCalls handlers at once");
        peak.Should().Be(3, "it should use the concurrency it is allowed when messages are waiting");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Prefetch_Is_Lower_Than_The_Concurrency_Limit_On_A_Classic_Queue()
    {
        var config = queues.NewConfig("prefetch-test") with { UseQuorumQueues = false, PrefetchCount = 3, MaxConcurrentCalls = 10 };
        var peak = await MeasurePeakConcurrencyAsync(config, messageCount: 12, TestContext.Current.CancellationToken);

        peak.Should().BeLessThanOrEqualTo(3, "it should apply PrefetchCount to classic queues too, so unacked deliveries are bounded");
        peak.Should().Be(3, "it should still use the whole prefetch window");
    }

    async Task<int> MeasurePeakConcurrencyAsync(ESE.MessageQueue.RabbitMQ.RabbitMqConfig config, int messageCount, CancellationToken cancellationToken)
    {
        var queue = queues.Create(config);
        var running = 0;
        var peak = 0;
        var done = 0;

        await queue.PublishAsync(Enumerable.Range(0, messageCount).Select(i => new TestMessage($"c-{i}", "slow")), cancellationToken);
        await queue.SubscribeAsync(async (_, _, ct) =>
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref peak, now);
            await Task.Delay(300, ct);
            Interlocked.Decrement(ref running);
            Interlocked.Increment(ref done);
            return MessageResult.Acknowledge;
        }, cancellationToken);
        await Wait.ForCountAsync(() => Volatile.Read(ref done), messageCount, TimeSpan.FromSeconds(30));

        done.Should().Be(messageCount, "it should handle every message");
        return peak;
    }

    static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Publishing_A_Large_Batch()
    {
        var queue = queues.Create(queues.NewConfig("batch-test"));
        var received = new ConcurrentQueue<TestMessage>();
        var messages = Enumerable.Range(0, 500).Select(i => new TestMessage($"batch-{i}", $"content-{i}")).ToList();

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, messages.Count, Wait.DeliveryTimeout);

        received.Should().BeEquivalentTo(messages, "it should deliver every message of a batch exactly once, in any order");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Publishing_More_Than_The_Outstanding_Confirm_Limit()
    {
        var config = queues.NewConfig("confirm-limit-test") with { MaxOutstandingConfirms = 2, PublisherChannelPoolSize = 1 };
        var queue = queues.Create(config);
        var received = new ConcurrentQueue<TestMessage>();

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        var publish = () => Task.WhenAll(
            queue.PublishAsync(Enumerable.Range(0, 50).Select(i => new TestMessage($"batch-{i}", "b")), TestContext.Current.CancellationToken),
            Task.WhenAll(Enumerable.Range(0, 50).Select(i =>
                queue.PublishAsync(new TestMessage($"single-{i}", "s"), TestContext.Current.CancellationToken))));

        await publish.Should().NotThrowAsync("it should wait for confirm capacity instead of failing the publish");
        await Wait.ForCountAsync(() => received.Count, 100, Wait.DeliveryTimeout);
        received.Should().HaveCount(100, "it should deliver every message published under the confirm limit");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Message_Carries_Broker_And_Foreign_Headers()
    {
        var config = queues.NewConfig("headers-test");
        var queue = queues.Create(config);
        var contexts = new ConcurrentQueue<MessageContext>();

        await queue.SubscribeAsync((_, context, _) =>
        {
            contexts.Enqueue(context);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await broker.PublishRawAsync(config.QueueName, System.Text.Encoding.UTF8.GetBytes("""{"Id":"raw","Content":"c"}"""),
            new Dictionary<string, string> { ["tenant"] = "acme", ["x-foreign-internal"] = "1", ["x-death"] = "forged" });
        await queue.PublishAsync(new TestMessage("lib", "c"), new PublishOptions
        {
            Headers = new Dictionary<string, string> { ["tenant"] = "globex" }
        }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => contexts.Count, 2, Wait.DeliveryTimeout);

        contexts.Should().AllSatisfy(c => c.Headers.Keys.Should().NotContain(k => k.StartsWith("x-", StringComparison.OrdinalIgnoreCase),
            "it should keep broker and library headers out of the application headers"));
        contexts.Select(c => c.Headers["tenant"]).Should().BeEquivalentTo(["acme", "globex"], "it should keep application headers");
    }
}
