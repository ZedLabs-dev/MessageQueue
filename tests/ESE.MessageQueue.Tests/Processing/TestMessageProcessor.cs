using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;

namespace ESE.MessageQueue.Tests.Processing;

public class TestMessageProcessor : IMessageProcessor<TestMessage>
{
    readonly bool shouldSucceed;
    readonly bool isTransient;

    public TestMessageProcessor(bool shouldSucceed, bool isTransient = false)
    {
        this.shouldSucceed = shouldSucceed;
        this.isTransient = isTransient;
    }

    public Task<MessageResult> ProcessAsync(TestMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.DeliveryAttempt > 3)
            return Task.FromResult(MessageResult.DeadLetter);

        if (shouldSucceed)
            return Task.FromResult(MessageResult.Acknowledge);

        if (isTransient)
            return Task.FromResult(MessageResult.Retry);

        return Task.FromResult(MessageResult.DeadLetter);
    }
}
