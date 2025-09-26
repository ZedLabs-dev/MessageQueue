namespace ESE.MessageQueue.Abstractions;

public interface IMessageQueue<T> where T : class
{
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
    Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default);
    Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default);
    Task UnsubscribeAsync(CancellationToken cancellationToken = default);
}
