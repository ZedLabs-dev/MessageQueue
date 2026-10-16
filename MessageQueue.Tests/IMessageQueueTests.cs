using FluentAssertions;

namespace ESE.MessageQueue.Tests;

public class MessageQueueTests
{
    readonly TestMessage testMessage = new("test-id", "test-content");

    readonly List<TestMessage> testMessages =
    [
        new("id-1", "content-1"),
        new("id-2", "content-2")
    ];

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        var queue = new MockMessageQueue<TestMessage>();

        await queue.PublishAsync(testMessage);

        queue.PublishedMessages.Should().ContainSingle("it should store exactly one message");
        queue.PublishedMessages.First().Should().Be(testMessage, "it should store the published message");
    }

    [Fact]
    public async Task When_Publishing_Multiple_Messages()
    {
        var queue = new MockMessageQueue<TestMessage>();

        await queue.PublishAsync(testMessages);

        queue.PublishedMessages.Should().HaveCount(2, "it should store all published messages");
    }

    [Fact]
    public async Task When_Subscribed_And_Message_Received()
    {
        var queue = new MockMessageQueue<TestMessage>();
        var processedMessages = new List<TestMessage>();

        await queue.SubscribeAsync(message =>
        {
            processedMessages.Add(message);
            return Task.FromResult(true);
        });
        await queue.SimulateMessageReceived(testMessage);

        processedMessages.Should().ContainSingle("it should process the received message");
        processedMessages.First().Should().Be(testMessage, "it should process the correct message");
    }

    [Fact]
    public async Task When_Unsubscribed()
    {
        var queue = new MockMessageQueue<TestMessage>();
        var processedMessages = new List<TestMessage>();

        await queue.SubscribeAsync(message =>
        {
            processedMessages.Add(message);
            return Task.FromResult(true);
        });
        await queue.UnsubscribeAsync();
        await queue.SimulateMessageReceived(testMessage);

        processedMessages.Should().BeEmpty("it should not process messages after unsubscribing");
    }
}
