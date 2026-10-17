using System.Collections.Concurrent;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Serialization;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Foreign publishers can put anything in AMQP properties. A malformed delivery must be settled one way or another,
// never left unacknowledged where it blocks the consumer's prefetch window.
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_with_malformed_messages : IAsyncLifetime
{
    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_with_malformed_messages(RabbitMqBrokerFixture broker)
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

    static byte[] Body(string id) => MessageSerializer.Serialize(new TestMessage(id, "x"), SerializationFormat.Json);

    public static TheoryData<bool, string> Garbage => new()
    {
        { true, "timestamp" }, { false, "timestamp" },
        { true, "headers" }, { false, "headers" }
    };

    static void Spoil(BasicProperties properties, string kind)
    {
        if (kind == "timestamp")
        {
            properties.Timestamp = new AmqpTimestamp(long.MaxValue);
            return;
        }

        properties.MessageId = null;
        properties.ContentType = "\u0001garbage";
        properties.Headers = new Dictionary<string, object?>
        {
            ["x-delivery-attempt"] = "99999999999999999999",
            ["x-delivery-count"] = "not a number",
            ["nested"] = new Dictionary<string, object?> { ["inner"] = 1 },
            ["list"] = new List<object?> { 1, "two", null },
            ["empty"] = null,
            ["when"] = new AmqpTimestamp(long.MinValue)
        };
    }

    [Theory(Timeout = 60_000)]
    [MemberData(nameof(Garbage))]
    public async Task When_Messages_Carry_Malformed_Properties(bool quorum, string kind)
    {
        var config = queues.NewConfig("malformed-test") with { UseQuorumQueues = quorum, PrefetchCount = 2, MaxConcurrentCalls = 2 };
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);
        for (var i = 0; i < 2; i++)
        {
            await broker.PublishRawWithAsync(config.QueueName, Body($"poison-{i}"), p => Spoil(p, kind));
        }
        await broker.PublishRawWithAsync(config.QueueName, Body("good"), _ => { });
        var handled = new ConcurrentQueue<string>();

        await queue.SubscribeAsync((message, _, _) =>
        {
            handled.Enqueue(message.Id);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => handled.Contains("good"), Wait.DeliveryTimeout);
        await queue.DisposeAsync();

        handled.Should().Contain("good", "it should not let malformed deliveries block the messages behind them");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u,
            "it should settle every malformed delivery instead of leaving it to come back");
    }
}
