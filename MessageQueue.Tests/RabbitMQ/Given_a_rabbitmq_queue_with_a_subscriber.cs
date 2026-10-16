using System.Collections.Concurrent;
using AwesomeAssertions;

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
}
