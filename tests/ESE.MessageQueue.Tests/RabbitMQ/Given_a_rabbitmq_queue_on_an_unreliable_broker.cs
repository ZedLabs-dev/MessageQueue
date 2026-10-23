using System.Collections.Concurrent;
using System.Text;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Tests.Telemetry;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// What a real broker does to a long-running consumer: connections dropped under it, bodies no serializer can read,
// and messages at the size limit (OPTIMIZATION-PLAN section 5, integration row).
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_on_an_unreliable_broker : IAsyncLifetime
{
    const string ReasonHeader = "x-ese-dead-letter-reason";

    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_on_an_unreliable_broker(RabbitMqBrokerFixture broker)
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

    [Fact(Timeout = 90_000)]
    public async Task When_The_Broker_Closes_The_Connection()
    {
        var config = queues.NewConfig("recovery-test") with { NetworkRecoveryInterval = TimeSpan.FromMilliseconds(500) };
        var queue = queues.Create(config);
        var received = new ConcurrentQueue<string>();
        await queue.SubscribeAsync((message, _, _) =>
        {
            received.Enqueue(message.Id);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("before", "x"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        var closed = await broker.CloseConnectionsAsync($"ESE.MessageQueue:{config.QueueName}");
        await PublishUntilAcceptedAsync(queue, new TestMessage("after", "x"));
        await Wait.UntilAsync(() => received.Contains("after"), TimeSpan.FromSeconds(30));

        closed.Should().BePositive("it should have had its connection closed by the broker");
        received.Should().Contain("after", "it should recover the connection and both publish and consume again");
    }

    [Fact(Timeout = 90_000)]
    public async Task When_The_Broker_Closes_The_Connection_It_Counts_A_Transport_Error()
    {
        var telemetry = new TestTelemetryProvider();
        var config = queues.NewConfig("transport-error-test") with { NetworkRecoveryInterval = TimeSpan.FromMilliseconds(500) };
        var queue = queues.Create(config, telemetry);
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await broker.CloseConnectionsAsync($"ESE.MessageQueue:{config.QueueName}");
        await Wait.UntilAsync(() => !telemetry.TransportErrors.IsEmpty, Wait.DeliveryTimeout);
        await PublishUntilAcceptedAsync(queue, new TestMessage("after", "x"));
        await queue.DisposeAsync();

        telemetry.TransportErrors.Select(e => e.ErrorType).Should().Equal(["ConnectionShutdown"],
            "it should count the closed connection once (not its channels, nor its own close on dispose)");
        telemetry.TransportErrors.Single().Target.Destination.Should().Be($"{config.QueueName}:{config.QueueName}",
            "it should name the queue's destination");
    }

    // While the client reconnects a publish may fail; the caller retries, as an application would.
    static async Task PublishUntilAcceptedAsync(ESE.MessageQueue.RabbitMQ.RabbitMqMessageQueue<TestMessage> queue, TestMessage message)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            try
            {
                await queue.PublishAsync(message, TestContext.Current.CancellationToken);
                return;
            }
            catch (Exception) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(250, TestContext.Current.CancellationToken);
            }
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Body_Cannot_Be_Deserialized()
    {
        var config = queues.NewConfig("undeserializable-test");
        var queue = queues.Create(config);
        var handled = 0;
        await queue.SubscribeAsync(_ => Task.FromResult(Interlocked.Increment(ref handled) > 0), TestContext.Current.CancellationToken);

        await broker.PublishRawAsync(config.QueueName, Encoding.UTF8.GetBytes("{ this is not json"), new Dictionary<string, string>());
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName), 1, Wait.DeliveryTimeout);
        var headers = await broker.TakeMessageHeadersAsync(config.EffectiveDeadLetterQueueName);

        headers.Should().Contain(ReasonHeader, DeadLetterReasons.DeserializationFailed, "it should dead-letter a body no serializer can read");
        handled.Should().Be(0, "it should never hand an unreadable body to the handler");
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_A_Message_Is_Near_The_Size_Limit(bool fits)
    {
        const int limit = 256 * 1024;
        var config = queues.NewConfig("size-limit-test") with { MaxMessageSizeBytes = limit };
        var queue = queues.Create(config);
        // {"id":"big","content":"xxx..."}: 25 bytes of JSON around the content.
        var content = new string('x', limit - 25 + (fits ? 0 : 1));
        var received = new ConcurrentQueue<TestMessage>();
        await queue.SubscribeAsync((message, _, _) =>
        {
            received.Enqueue(message);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        var message = new TestMessage("big", content);

        if (fits)
        {
            await queue.PublishAsync(message, TestContext.Current.CancellationToken);
            await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);
            received.Single().Content.Should().HaveLength(content.Length, "it should deliver a body exactly at the limit intact");
        }
        else
        {
            var publish = () => queue.PublishAsync(message, TestContext.Current.CancellationToken);
            (await publish.Should().ThrowAsync<MessageQueueException>("it should refuse a body one byte over the limit on publish"))
                .WithMessage($"*{limit + 1} bytes*MaxMessageSizeBytes ({limit})*");

            // Another publisher's oversized body is still dead-lettered by the consumer.
            await broker.PublishRawAsync(config.QueueName, MessageSerializer.Serialize(message, SerializationFormat.Json), new Dictionary<string, string>());
            await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName), 1, Wait.DeliveryTimeout);
            var headers = await broker.TakeMessageHeadersAsync(config.EffectiveDeadLetterQueueName);
            headers.Should().Contain(ReasonHeader, DeadLetterReasons.MessageTooLarge, "it should dead-letter a body one byte over the limit");
            received.Should().BeEmpty("it should not hand an oversized body to the handler");
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Publishing_A_Batch_With_An_Oversized_Message()
    {
        var small = new TestMessage("small", "x");
        var large = new TestMessage("large", new string('x', 4096));
        var config = queues.NewConfig("size-batch-test") with { MaxMessageSizeBytes = 1024 };
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);

        var publish = () => queue.PublishAsync([small, large, small], TestContext.Current.CancellationToken);

        (await publish.Should().ThrowAsync<MessageQueueException>("it should refuse a batch holding an oversized message"))
            .WithMessage("*index 1*");
        await Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should send none of the batch to the broker");
    }
}
