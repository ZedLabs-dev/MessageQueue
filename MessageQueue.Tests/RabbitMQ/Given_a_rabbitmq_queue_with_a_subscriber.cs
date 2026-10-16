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

    public ValueTask DisposeAsync()
    {
        queues.Dispose();
        return ValueTask.CompletedTask;
    }

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
        var probeQueue = queues.BindProbeQueue(config.ExchangeName!, routingKey: config.QueueName);
        var received = new ConcurrentQueue<TestMessage>();
        var message = new TestMessage("exchange-test", "test-content");

        await queue.SubscribeAsync(msg => Collect(received, msg), TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);
        await Wait.ForCountAsync(() => (int)broker.GetMessageCount(probeQueue), 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should receive message via custom exchange")
            .Which.Id.Should().Be(message.Id, "it should route the message through the custom exchange to the queue");
        broker.GetMessageCount(probeQueue).Should().Be(1u,
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
        await Wait.ForCountAsync(() => (int)broker.GetMessageCount(config.QueueName), messageCount, Wait.DeliveryTimeout);
        broker.GetMessageCount(config.QueueName).Should().Be((uint)messageCount, "it should still deliver every message to the queue");
    }

    [Fact]
    public async Task When_The_Handler_Always_Fails()
    {
        var config = queues.NewConfig("retry-test");
        var queue = queues.Create(config);
        var attempts = new ConcurrentQueue<int>();

        await queue.SubscribeAsync((_, context, _) =>
        {
            attempts.Enqueue(context.DeliveryAttempt);
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("poison", "always fails"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => (int)broker.GetMessageCount(config.EffectiveDeadLetterQueueName), 1, Wait.DeliveryTimeout);

        attempts.Should().Equal([1, 2, 3], "it should make the first attempt plus MaxRetries retries, counting attempts across redeliveries");
        broker.GetMessageCount(config.EffectiveDeadLetterQueueName).Should().Be(1u, "it should move the message to the dead-letter queue");
        broker.GetMessageCount(config.QueueName).Should().Be(0u, "it should leave nothing behind on the main queue");
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
        queue.Capabilities.DelayedRedelivery.Should().BeFalse("it should admit that retries still wait in process");
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

        var act = () => queue.PublishAsync(new TestMessage("1", "forged"),
            new PublishOptions { Headers = new Dictionary<string, string> { ["x-delivery-attempt"] = "99" } },
            TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ArgumentException>("it should not let a publisher forge the retry counter");
        broker.GetMessageCount(config.QueueName).Should().Be(0u, "it should publish nothing");
    }
}
