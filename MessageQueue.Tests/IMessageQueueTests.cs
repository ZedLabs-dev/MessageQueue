namespace ESE.MessageQueue.Tests;

public class MessageQueueTests
{
    readonly TestMessage testMessage = new("test-id", "test-content");

    readonly List<TestMessage> testMessages = new()
    {
        new("id-1", "content-1"),
        new("id-2", "content-2")
    };

    [Fact]
    public async Task PublishAsync_SingleMessage_ShouldSucceed()
    {
        var queue = new MockMessageQueue<TestMessage>();
        
        await queue.PublishAsync(testMessage);
        
        Assert.Single(queue.PublishedMessages);
        Assert.Equal(testMessage, queue.PublishedMessages.First());
    }

    [Fact]
    public async Task PublishAsync_MultipleMessages_ShouldSucceed()
    {
        var queue = new MockMessageQueue<TestMessage>();
        
        await queue.PublishAsync(testMessages);
        
        Assert.Equal(2, queue.PublishedMessages.Count);
    }

    [Fact]
    public async Task SubscribeAsync_WithHandler_ShouldProcessMessages()
    {
        var queue = new MockMessageQueue<TestMessage>();
        var processedMessages = new List<TestMessage>();
        
        await queue.SubscribeAsync(message =>
        {
            processedMessages.Add(message);
            return Task.FromResult(true);
        });
        
        await queue.SimulateMessageReceived(testMessage);
        
        Assert.Single(processedMessages);
        Assert.Equal(testMessage, processedMessages.First());
    }

    [Fact]
    public async Task UnsubscribeAsync_ShouldStopProcessing()
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
        
        Assert.Empty(processedMessages);
    }
}