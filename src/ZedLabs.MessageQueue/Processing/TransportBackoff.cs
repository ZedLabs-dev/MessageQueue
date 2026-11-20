using System.Collections.Concurrent;

namespace ZedLabs.MessageQueue.Processing;

// Transport failures (settling a delivery, publishing a retry copy) repeat while the broker or the entity is broken.
// Each message gets a growing pause before it is handed back (100 ms doubling, at most 30 s), whatever happens to
// other messages, so a poison or unsettleable delivery cannot come straight back in a hot loop.
sealed class TransportBackoff
{
    const int MaxTracked = 10_000;
    static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);
    static readonly TimeSpan Max = TimeSpan.FromSeconds(30);

    readonly ConcurrentDictionary<string, int> failures = new(StringComparer.Ordinal);

    // Returns how many failures in a row this message has had, including this one.
    public int RecordFailure(string key)
    {
        if (failures.Count >= MaxTracked)
            failures.Clear();
        return failures.AddOrUpdate(key, 1, (_, count) => count + 1);
    }

    public void Reset(string key) => failures.TryRemove(key, out _);

    public static TimeSpan Delay(int consecutiveFailures) =>
        TimeSpan.FromMilliseconds(Math.Min(Max.TotalMilliseconds, Step.TotalMilliseconds * Math.Pow(2, Math.Min(consecutiveFailures - 1, 16))));

    // Waits out the backoff; stopping (unsubscribe, dispose) cuts it short.
    public static async Task WaitAsync(int consecutiveFailures, CancellationToken stopping)
    {
        try
        {
            await Task.Delay(Delay(consecutiveFailures), stopping);
        }
        catch (OperationCanceledException)
        {
            // Stopping: hand the delivery back right away.
        }
    }
}
