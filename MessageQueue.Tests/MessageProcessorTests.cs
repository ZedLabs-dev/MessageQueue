using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;

namespace ESE.MessageQueue.Tests;

public class MessageProcessorTests
{
    [Fact]
    public async Task ProcessAsync_SuccessfulProcessing_ReturnsAcknowledge()
    {
        var processor = new TestMessageProcessor(shouldSucceed: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 1 };
        
        var result = await processor.ProcessAsync(message, context);
        
        Assert.Equal(MessageResult.Acknowledge, result);
    }

    [Fact]
    public async Task ProcessAsync_TransientError_ReturnsRetry()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 1 };
        
        var result = await processor.ProcessAsync(message, context);
        
        Assert.Equal(MessageResult.Retry, result);
    }

    [Fact]
    public async Task ProcessAsync_FatalError_ReturnsDeadLetter()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: false);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 1 };
        
        var result = await processor.ProcessAsync(message, context);
        
        Assert.Equal(MessageResult.DeadLetter, result);
    }

    [Fact]
    public async Task ProcessAsync_MaxRetriesExceeded_ReturnsDeadLetter()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 4 };
        
        var result = await processor.ProcessAsync(message, context);
        
        Assert.Equal(MessageResult.DeadLetter, result);
    }
}

public class TestMessageProcessor : IMessageProcessor<TestMessage>
{
    readonly bool shouldSucceed;
    readonly bool isTransient;

    public TestMessageProcessor(bool shouldSucceed, bool isTransient = false)
    {
        this.shouldSucceed = shouldSucceed;
        this.isTransient = isTransient;
    }

    public Task<MessageResult> ProcessAsync(TestMessage message, MessageContext context)
    {
        if (context.DeliveryCount > 3)
            return Task.FromResult(MessageResult.DeadLetter);
            
        if (shouldSucceed)
            return Task.FromResult(MessageResult.Acknowledge);
        
        if (isTransient)
            return Task.FromResult(MessageResult.Retry);
        
        return Task.FromResult(MessageResult.DeadLetter);
    }
}
