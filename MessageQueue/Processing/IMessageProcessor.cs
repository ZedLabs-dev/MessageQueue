using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Processing;

/// <summary>
/// Handles messages of type <typeparamref name="T"/> for <see cref="Hosting.MessageQueueHostedService{T}"/>.
/// Registered as scoped: a new scope, and so a new processor, is created for every message.
/// </summary>
public interface IMessageProcessor<T> where T : class
{
    /// <summary>
    /// Processes one message. A thrown exception counts as <see cref="MessageResult.Retry"/>.
    /// </summary>
    Task<MessageResult> ProcessAsync(T message, MessageContext context, CancellationToken cancellationToken);
}
