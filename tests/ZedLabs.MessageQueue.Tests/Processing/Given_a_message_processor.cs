using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Processing;

namespace ZedLabs.MessageQueue.Tests.Processing;

[Trait("Category", "Unit")]
public class Given_a_message_processor
{
    [Fact]
    public async Task When_Processing_Succeeds()
    {
        var processor = new TestMessageProcessor(shouldSucceed: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryAttempt = 1 };

        var result = await processor.ProcessAsync(message, context, TestContext.Current.CancellationToken);

        result.Should().Be(MessageResult.Acknowledge, "it should acknowledge successfully processed messages");
    }

    [Fact]
    public async Task When_Transient_Error_Occurs()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryAttempt = 1 };

        var result = await processor.ProcessAsync(message, context, TestContext.Current.CancellationToken);

        result.Should().Be(MessageResult.Retry, "it should retry on transient errors");
    }

    [Fact]
    public async Task When_Fatal_Error_Occurs()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: false);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryAttempt = 1 };

        var result = await processor.ProcessAsync(message, context, TestContext.Current.CancellationToken);

        result.Should().Be(MessageResult.DeadLetter, "it should dead-letter on fatal errors");
    }

    [Fact]
    public async Task When_Max_Retries_Exceeded()
    {
        var processor = new TestMessageProcessor(shouldSucceed: false, isTransient: true);
        var message = new TestMessage("test-id", "test-content");
        var context = new MessageContext { MessageId = "msg-1", DeliveryAttempt = 4 };

        var result = await processor.ProcessAsync(message, context, TestContext.Current.CancellationToken);

        result.Should().Be(MessageResult.DeadLetter, "it should dead-letter after exceeding max retries");
    }
}
