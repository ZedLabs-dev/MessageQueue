using AgentReview.MessageQueue.Abstractions;

namespace AgentReview.MessageQueue.Processing;

public interface IMessageProcessor<T> where T : class
{
    Task<MessageResult> ProcessAsync(T message, MessageContext context);
}
