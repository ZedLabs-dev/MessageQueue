using ZedLabs.MessageQueue.Exceptions;

namespace ZedLabs.MessageQueue.RabbitMQ;

// Batch publishing (§4.2): start publishes up to `window` at a time and await their confirms together, instead of
// one confirm round trip per message. Every publish that was started is awaited before returning, also on
// cancellation, so none fails unobserved after its caller is gone.
static class BatchPublisher
{
    public static async Task PublishAllAsync<TItem>(
        IReadOnlyList<TItem> items,
        int window,
        Func<TItem, CancellationToken, Task> publish,
        CancellationToken cancellationToken)
    {
        using var inFlight = new SemaphoreSlim(window, window);
        var publishes = new List<Task>(items.Count);
        var cancelled = false;
        try
        {
            foreach (var item in items)
            {
                await inFlight.WaitAsync(cancellationToken);
                publishes.Add(PublishReleasingAsync(item, publish, inFlight, cancellationToken));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            cancelled = true;
        }

        try
        {
            await Task.WhenAll(publishes);
        }
        catch (Exception)
        {
            // Inspected below, per publish.
        }

        if (cancelled)
            cancellationToken.ThrowIfCancellationRequested();

        var failures = publishes.Where(p => p.IsFaulted || p.IsCanceled).ToList();
        if (failures.Count == 0)
            return;

        var errors = failures.Where(p => p.IsFaulted).Select(p => p.Exception!.InnerException!).ToList();
        if (errors.Count == 0 || (cancellationToken.IsCancellationRequested && errors.All(e => e is OperationCanceledException)))
            cancellationToken.ThrowIfCancellationRequested();

        var message = $"{failures.Count} of {items.Count} messages in the batch were not confirmed by the broker";
        var first = errors.FirstOrDefault(e => e is not OperationCanceledException) ?? errors.FirstOrDefault();
        throw first is null ? new MessageQueueException(message) : new MessageQueueException(message, first);
    }

    static async Task PublishReleasingAsync<TItem>(
        TItem item,
        Func<TItem, CancellationToken, Task> publish,
        SemaphoreSlim inFlight,
        CancellationToken cancellationToken)
    {
        try
        {
            await publish(item, cancellationToken);
        }
        finally
        {
            inFlight.Release();
        }
    }
}
