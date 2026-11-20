using AwesomeAssertions;
using ZedLabs.MessageQueue.Exceptions;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// B8: batches are built with CreateMessageBatchAsync + TryAddMessage and split by byte size, not message count alone.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_batch_publish : IAsyncDisposable
{
    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();

    static TestMessage[] Messages(int count, int contentLength = 20) =>
        Enumerable.Range(0, count).Select(i => new TestMessage($"{i}", new string('c', contentLength))).ToArray();

    [Fact]
    public async Task When_The_Messages_Exceed_One_Batch()
    {
        bus.BatchLimitBytes = 200;
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());

        await queue.PublishAsync(Messages(10), TestContext.Current.CancellationToken);

        bus.Sender.Batches.Should().HaveCountGreaterThan(1, "it should split messages that do not fit one batch");
        bus.Sender.Batches.Should().AllSatisfy(batch => batch.Sum(m => m.Body.ToMemory().Length).Should().BeLessThanOrEqualTo(200,
            "it should keep every batch within the broker's byte limit"));
        bus.Sender.Batches.SelectMany(b => b).Select(m => m.Body.ToObjectFromJson<TestMessage>(System.Text.Json.JsonSerializerOptions.Web)!.Id)
            .Should().Equal(Enumerable.Range(0, 10).Select(i => $"{i}"), "it should send every message once, in order");
    }

    [Fact]
    public async Task When_MaxBatchSize_Is_Reached_First()
    {
        bus.BatchLimitBytes = 1_000_000;
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MaxBatchSize = 3 });

        await queue.PublishAsync(Messages(7), TestContext.Current.CancellationToken);

        bus.Sender.Batches.Select(b => b.Count).Should().Equal([3, 3, 1], "it should cap a batch at MaxBatchSize messages");
    }

    [Fact]
    public async Task When_A_Message_Is_Too_Large_For_Any_Batch()
    {
        bus.BatchLimitBytes = 200;
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var messages = Messages(4);
        messages[2] = new TestMessage("2", "SENSITIVE-" + new string('z', 500));

        var act = () => queue.PublishAsync(messages, TestContext.Current.CancellationToken);

        var error = (await act.Should().ThrowAsync<MessageQueueException>("it should refuse a message no batch can hold")).Which;
        error.Message.Should().Contain("index 2", "it should name the message by its index").And.NotContain("SENSITIVE", "it should not echo the content");
        bus.Sender.Batches.SelectMany(b => b).Should().HaveCount(2, "it should have sent only the messages before it");
    }

    [Fact]
    public async Task When_Batching_Is_Disabled()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { EnableBatchedOperations = false });

        await queue.PublishAsync(Messages(3), TestContext.Current.CancellationToken);

        bus.Sender.Sent.Should().HaveCount(3, "it should send each message on its own");
        bus.Sender.Batches.Should().BeEmpty("it should not build batches");
    }

    [Fact]
    public async Task When_There_Are_No_Messages()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());

        await queue.PublishAsync([], TestContext.Current.CancellationToken);

        bus.Operations.Should().BeEmpty("it should not contact the broker for an empty batch");
    }
}
