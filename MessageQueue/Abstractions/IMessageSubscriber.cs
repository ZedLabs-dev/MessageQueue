namespace ESE.MessageQueue.Abstractions;

public interface IMessageSubscriber<T> where T : class
{
    Task SubscribeAsync(Func<T, MessageContext, Task<MessageResult>> handler, CancellationToken cancellationToken = default);
    Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default);
    Task UnsubscribeAsync(CancellationToken cancellationToken = default);
}

public record MessageContext
{
    public string MessageId { get; init; } = string.Empty;
    public DateTime Timestamp { get; init; }
    public int DeliveryCount { get; init; }
    public Dictionary<string, object> Properties { get; init; } = new();
}

public enum MessageResult
{
    Acknowledge,
    Retry,
    DeadLetter
}
