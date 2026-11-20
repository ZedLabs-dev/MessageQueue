using System.Collections.Concurrent;

namespace ZedLabs.MessageQueue.Samples.Docs;

// The application's own store, standing in for a database table keyed by message id.
public interface IOrderLedger
{
    // Records the order unless a message with this id was already recorded; returns false for a duplicate.
    Task<bool> RecordOnceAsync(string messageId, OrderPlaced order, CancellationToken cancellationToken);
}

public sealed class InMemoryOrderLedger : IOrderLedger
{
    readonly ConcurrentDictionary<string, OrderPlaced> orders = new(StringComparer.Ordinal);

    public IReadOnlyDictionary<string, OrderPlaced> Orders => orders;

    public Task<bool> RecordOnceAsync(string messageId, OrderPlaced order, CancellationToken cancellationToken) =>
        Task.FromResult(orders.TryAdd(messageId, order));
}
