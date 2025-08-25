namespace AgentReview.MessageQueue.Abstractions;

public interface IMessagePublisher<T> where T : class
{
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
    Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default);
}