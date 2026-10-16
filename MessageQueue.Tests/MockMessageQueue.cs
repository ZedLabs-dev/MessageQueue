using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Tests;

public class MockMessageQueue<T> : IMessageQueue<T> where T : class
{
    public List<T> PublishedMessages { get; } = new();
    Func<T, Task<bool>>? handler;
    bool isSubscribed;

    public Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        PublishedMessages.Add(message);
        return Task.CompletedTask;
    }

    public Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        PublishedMessages.AddRange(messages);
        return Task.CompletedTask;
    }

    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default)
    {
        this.handler = handler;
        isSubscribed = true;
        return Task.CompletedTask;
    }

    public Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        handler = null;
        isSubscribed = false;
        return Task.CompletedTask;
    }

    public async Task SimulateMessageReceived(T message)
    {
        if (handler != null && isSubscribed)
        {
            await handler(message);
        }
    }
}
