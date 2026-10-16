using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.InMemory;

[Trait("Category", "Unit")]
public class Given_an_in_memory_queue
{
    sealed record TestMessage(string Id, string Content);

    [Fact]
    public async Task When_Publishing_A_Single_Message()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var message = new TestMessage("1", "Hello");

        await queue.PublishAsync(message, TestContext.Current.CancellationToken);

        queue.Count.Should().Be(1, "it should add the message to the queue");
        queue.PublishedMessages.Should().ContainSingle("it should record exactly one published message")
            .Which.Should().Be(message, "it should store the published message");
    }

    [Fact]
    public async Task When_Publishing_A_Batch_Of_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var messages = new[]
        {
            new TestMessage("1", "First"),
            new TestMessage("2", "Second"),
            new TestMessage("3", "Third")
        };

        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);

        queue.Count.Should().Be(3, "it should add all messages to the queue");
        queue.PublishedMessages.Should().HaveCount(3, "it should store all published messages");
    }

    [Fact]
    public async Task When_Subscribing_To_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        queue.IsSubscribed.Should().BeTrue("it should register the handler");
    }

    [Fact]
    public async Task When_Processing_All_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var processed = new List<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "First"), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("2", "Second"), TestContext.Current.CancellationToken);

        await queue.SubscribeAsync(msg =>
        {
            processed.Add(msg);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);

        await queue.ProcessAllAsync(TestContext.Current.CancellationToken);

        processed.Should().HaveCount(2, "it should process all messages");
        queue.ProcessedMessages.Should().HaveCount(2, "it should track processed messages");
        queue.Count.Should().Be(0, "it should remove processed messages from the queue");
    }

    [Fact]
    public async Task When_Processing_One_Message()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "First"), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("2", "Second"), TestContext.Current.CancellationToken);

        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        var result = await queue.ProcessOneAsync(TestContext.Current.CancellationToken);

        result.Should().BeTrue("it should return true on successful processing");
        queue.ProcessedMessages.Should().HaveCount(1, "it should process only one message");
        queue.Count.Should().Be(1, "it should leave remaining messages in the queue");
    }

    [Fact]
    public async Task When_Handler_Returns_False()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "Will fail"), TestContext.Current.CancellationToken);

        await queue.SubscribeAsync(_ => Task.FromResult(false), TestContext.Current.CancellationToken);

        var result = await queue.ProcessOneAsync(TestContext.Current.CancellationToken);

        result.Should().BeFalse("it should return false when handler fails");
        queue.ProcessedMessages.Should().BeEmpty("it should not mark failed messages as processed");
    }

    [Fact]
    public async Task When_Handler_Fails_During_ProcessAll()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var message = new TestMessage("1", "Fails once");
        var attempts = 0;

        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await queue.SubscribeAsync(_ => Task.FromResult(++attempts > 1), TestContext.Current.CancellationToken);
        await queue.ProcessAllAsync(TestContext.Current.CancellationToken);

        attempts.Should().Be(2, "it should re-queue a failed message and deliver it again");
        queue.ProcessedMessages.Should().ContainSingle("it should process the message once the handler succeeds")
            .Which.Should().Be(message, "it should re-deliver the same message");
        queue.Count.Should().Be(0, "it should leave nothing pending after the retry succeeds");
    }

    [Fact]
    public async Task When_Clearing_The_Queue()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        await queue.PublishAsync(new TestMessage("1", "Test"), TestContext.Current.CancellationToken);

        queue.Clear();

        queue.Count.Should().Be(0, "it should remove all messages");
        queue.PublishedMessages.Should().BeEmpty("it should clear published messages");
        queue.ProcessedMessages.Should().BeEmpty("it should clear processed messages");
    }

    [Fact]
    public void When_Registering_Via_DI()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue<TestMessage>();

        var provider = services.BuildServiceProvider();

        var queue = provider.GetService<IMessageQueue<TestMessage>>();
        var inMemory = provider.GetService<InMemoryMessageQueue<TestMessage>>();

        queue.Should().NotBeNull("it should register IMessageQueue");
        inMemory.Should().NotBeNull("it should register InMemoryMessageQueue");
        queue.Should().BeSameAs(inMemory, "it should use the same instance for both registrations");
    }
}
