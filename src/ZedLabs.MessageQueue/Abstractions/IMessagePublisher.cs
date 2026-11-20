namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>
/// Publishes messages of type <typeparamref name="T"/> to a queue.
/// </summary>
public interface IMessagePublisher<T> where T : class
{
    /// <summary>Publishes one message with a generated message id.</summary>
    Task PublishAsync(T message, CancellationToken cancellationToken = default);

    /// <summary>Publishes one message with the given id, correlation id and headers.</summary>
    Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default);

    /// <summary>Publishes several messages, each with a generated message id.</summary>
    Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default);
}
