namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>
/// Consumes messages of type <typeparamref name="T"/> from a queue. Every provider runs deliveries through the same
/// pipeline: size limit, deserialization, the handler, then the retry policy and dead-lettering.
/// </summary>
public interface IMessageSubscriber<T> where T : class
{
    /// <summary>
    /// Starts delivering messages to <paramref name="handler"/>. The handler gets the message, its broker metadata and
    /// a token that is cancelled when the subscription stops. A thrown exception counts as <see cref="MessageResult.Retry"/>.
    /// </summary>
    Task SubscribeAsync(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts delivering messages to <paramref name="handler"/>: <c>true</c> acknowledges the message,
    /// <c>false</c> retries it (<see cref="MessageResult.Retry"/>).
    /// </summary>
    Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default);

    /// <summary>Stops delivering messages. Messages not yet delivered stay in the queue.</summary>
    Task UnsubscribeAsync(CancellationToken cancellationToken = default);
}
