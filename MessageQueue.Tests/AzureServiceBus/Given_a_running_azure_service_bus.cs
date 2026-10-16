using System.Collections.Concurrent;
using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// Runs only when MQ_ASB_CONNECTION_STRING is set; otherwise every test is reported as skipped.
// All tests share one predeclared queue (the emulator cannot create entities), so each test tags its
// messages with a unique run id and ignores messages left behind by other tests.
[Trait("Category", "Integration")]
public sealed class Given_a_running_azure_service_bus : IAsyncLifetime
{
    readonly string runId = Guid.NewGuid().ToString("N");
    readonly List<AzureServiceBusMessageQueue<TestMessage>> queues = [];
    AzureServiceBusConfig config = null!;

    public ValueTask InitializeAsync()
    {
        config = new AzureServiceBusConfig
        {
            ConnectionString = AzureServiceBusEnvironment.RequireConnectionString(),
            QueueName = AzureServiceBusEnvironment.QueueName,
            RetryAttempts = 3,
            EnableDeadLetterQueue = true,
            MessageTimeToLive = TimeSpan.FromMinutes(30),
            MaxConcurrentCalls = 2,
            PrefetchCount = 5
        };
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var queue in queues)
        {
            await queue.DisposeAsync();
        }
    }

    AzureServiceBusMessageQueue<TestMessage> CreateQueue()
    {
        var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        queues.Add(queue);
        return queue;
    }

    TestMessage NewMessage(string name) => new($"{runId}-{name}", "test-content");

    bool IsMine(TestMessage message) => message.Id.StartsWith(runId, StringComparison.Ordinal);

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        var queue = CreateQueue();

        var action = () => queue.PublishAsync(NewMessage("single"), TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should publish message without error");
    }

    [Fact]
    public async Task When_Publishing_Multiple_Messages()
    {
        var queue = CreateQueue();
        var messages = new[] { NewMessage("multi-1"), NewMessage("multi-2") };

        var action = () => queue.PublishAsync(messages, TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should publish multiple messages without error");
    }

    [Fact]
    public async Task When_Subscribing_With_Handler()
    {
        var queue = CreateQueue();

        var action = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should subscribe without error");
    }

    [Fact]
    public async Task When_Publishing_And_Subscribing()
    {
        var queue = CreateQueue();
        var received = new ConcurrentQueue<TestMessage>();
        var message = NewMessage("round-trip");

        await queue.SubscribeAsync(msg =>
        {
            if (IsMine(msg))
                received.Enqueue(msg);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should receive the published message")
            .Which.Id.Should().Be(message.Id, "it should receive the correct message");
    }

    [Fact]
    public async Task When_Publishing_Batch()
    {
        var queue = CreateQueue();
        var received = new ConcurrentQueue<TestMessage>();
        var messages = new[] { NewMessage("batch-1"), NewMessage("batch-2"), NewMessage("batch-3") };

        await queue.SubscribeAsync(msg =>
        {
            if (IsMine(msg))
                received.Enqueue(msg);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 3, Wait.DeliveryTimeout);

        received.Select(m => m.Id).Should().BeEquivalentTo(messages.Select(m => m.Id), "it should receive all batch messages");
    }

    [Fact]
    public async Task When_Message_Processing_Fails()
    {
        var queue = CreateQueue();
        var attempts = 0;
        var message = NewMessage("retry");

        await queue.SubscribeAsync(msg =>
        {
            if (msg.Id == message.Id)
                Interlocked.Increment(ref attempts);
            return Task.FromResult(msg.Id != message.Id);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => Volatile.Read(ref attempts) >= config.RetryAttempts, Wait.DeliveryTimeout);

        Volatile.Read(ref attempts).Should().BeGreaterThanOrEqualTo(config.RetryAttempts, "it should retry failed messages");
    }
}
