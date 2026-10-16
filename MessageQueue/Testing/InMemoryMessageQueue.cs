using System.Collections.Concurrent;
using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Testing;

/// <summary>
/// In-memory message queue implementation for unit testing.
/// </summary>
public class InMemoryMessageQueue<T> : IMessageQueue<T> where T : class
{
    readonly ConcurrentQueue<T> messages = new();
    readonly List<T> publishedMessages = new();
    readonly List<T> processedMessages = new();
    readonly object lockObj = new();
    Func<T, Task<bool>>? handler;
    bool isSubscribed;

    /// <summary>
    /// All messages that have been published.
    /// </summary>
    public IReadOnlyList<T> PublishedMessages
    {
        get
        {
            lock (lockObj)
            {
                return publishedMessages.ToList();
            }
        }
    }

    /// <summary>
    /// All messages that have been successfully processed.
    /// </summary>
    public IReadOnlyList<T> ProcessedMessages
    {
        get
        {
            lock (lockObj)
            {
                return processedMessages.ToList();
            }
        }
    }

    /// <summary>
    /// Messages currently in the queue waiting to be processed.
    /// </summary>
    public IReadOnlyList<T> PendingMessages => messages.ToList();

    /// <summary>
    /// Number of messages in the queue.
    /// </summary>
    public int Count => messages.Count;

    /// <summary>
    /// Whether a subscriber is currently active.
    /// </summary>
    public bool IsSubscribed => isSubscribed;

    public Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        lock (lockObj)
        {
            publishedMessages.Add(message);
        }
        messages.Enqueue(message);
        return Task.CompletedTask;
    }

    public async Task PublishAsync(IEnumerable<T> messageBatch, CancellationToken cancellationToken = default)
    {
        foreach (var message in messageBatch)
        {
            await PublishAsync(message, cancellationToken);
        }
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

    /// <summary>
    /// Processes all pending messages using the registered handler.
    /// </summary>
    public async Task ProcessAllAsync(CancellationToken cancellationToken = default)
    {
        if (handler == null)
            throw new InvalidOperationException("No handler registered. Call SubscribeAsync first.");

        while (messages.TryDequeue(out var message))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var success = await handler(message);
            if (success)
            {
                lock (lockObj)
                {
                    processedMessages.Add(message);
                }
            }
            else
            {
                // Re-queue failed messages
                messages.Enqueue(message);
            }
        }
    }

    /// <summary>
    /// Processes a single message from the queue.
    /// </summary>
    public async Task<bool> ProcessOneAsync(CancellationToken cancellationToken = default)
    {
        if (handler == null)
            throw new InvalidOperationException("No handler registered. Call SubscribeAsync first.");

        if (!messages.TryDequeue(out var message))
            return false;

        var success = await handler(message);
        if (success)
        {
            lock (lockObj)
            {
                processedMessages.Add(message);
            }
        }
        return success;
    }

    /// <summary>
    /// Clears all messages and resets state.
    /// </summary>
    public void Clear()
    {
        while (messages.TryDequeue(out _)) { }
        lock (lockObj)
        {
            publishedMessages.Clear();
            processedMessages.Clear();
        }
    }
}

