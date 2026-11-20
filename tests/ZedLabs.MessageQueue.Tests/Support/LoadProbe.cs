using System.Collections.Concurrent;
using ZedLabs.MessageQueue.Abstractions;

namespace ZedLabs.MessageQueue.Tests;

// A handler for load specs: a random 0-3 ms of work, a transient failure on the first attempt of every 20th message
// (deterministic, so a spec can name exactly which messages retry and none can exhaust its retries by chance), and
// counters for successes and failures per message id and for the peak number of handlers running at once.
sealed class LoadProbe
{
    int running;
    int peak;
    int succeeded;

    public ConcurrentDictionary<string, int> Successes { get; } = new();
    public ConcurrentDictionary<string, int> Failures { get; } = new();
    public int Peak => Volatile.Read(ref peak);
    public int Succeeded => Volatile.Read(ref succeeded);

    public static TestMessage[] Messages(int count) =>
        Enumerable.Range(0, count).Select(i => new TestMessage($"m-{i:D5}", "load")).ToArray();

    public static bool FailsFirstAttempt(string id) => int.Parse(id.AsSpan(2), System.Globalization.CultureInfo.InvariantCulture) % 20 == 0;

    public async Task<MessageResult> HandleAsync(TestMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        var now = Interlocked.Increment(ref running);
        try
        {
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) && Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            await Task.Delay(Random.Shared.Next(0, 4), cancellationToken);
            if (context.DeliveryAttempt == 1 && FailsFirstAttempt(message.Id))
            {
                Failures.AddOrUpdate(message.Id, 1, (_, n) => n + 1);
                throw new TimeoutException("transient dependency failure");
            }

            Successes.AddOrUpdate(message.Id, 1, (_, n) => n + 1);
            Interlocked.Increment(ref succeeded);
            return MessageResult.Acknowledge;
        }
        finally
        {
            Interlocked.Decrement(ref running);
        }
    }
}
