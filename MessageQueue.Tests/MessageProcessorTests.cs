using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using FluentAssertions;

namespace ESE.MessageQueue.Tests;

public class MessageProcessorTests
{
    [Fact]
    public async Task When_Processing_Succeeds()
    {
        var processor = new TestMessageProcessor(shouldSucceed: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 1 };

        var result = await processor.ProcessAsync(message, context);

        result.Should().Be(MessageResult.Acknowledge, "it should acknowledge successfully processed messages");
    }

    [Fact]
    public async Task When_Transient_Error_Occurs()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 1 };

        var result = await processor.ProcessAsync(message, context);

        result.Should().Be(MessageResult.Retry, "it should retry on transient errors");
    }

    [Fact]
    public async Task When_Fatal_Error_Occurs()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: false);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 1 };

        var result = await processor.ProcessAsync(message, context);

        result.Should().Be(MessageResult.DeadLetter, "it should dead-letter on fatal errors");
    }

    [Fact]
    public async Task When_Max_Retries_Exceeded()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryCount = 4 };

        var result = await processor.ProcessAsync(message, context);

        result.Should().Be(MessageResult.DeadLetter, "it should dead-letter after exceeding max retries");
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
