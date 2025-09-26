using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Processing;

public interface IMessageProcessor<T> where T : class
{
    Task<MessageResult> ProcessAsync(T message, MessageContext context);
}
