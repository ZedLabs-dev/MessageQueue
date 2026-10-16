using System.Diagnostics;

namespace ESE.MessageQueue.Tests;

static class Wait
{
    public static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan SettleTime = TimeSpan.FromMilliseconds(500);

    // Polls until the condition holds or the timeout elapses. The caller asserts afterwards,
    // so a condition that never holds still fails the test with a readable message.
    public static async Task UntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var elapsed = Stopwatch.StartNew();
        while (!condition() && elapsed.Elapsed < timeout)
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
    }

    // For exact-count assertions: waits until at least `expected` items arrived, then keeps watching for
    // SettleTime (restarting whenever the count changes) so late duplicates are seen by the assertion.
    public static async Task ForCountAsync(Func<int> count, int expected, TimeSpan timeout)
    {
        await UntilAsync(() => count() >= expected, timeout);

        var settled = Stopwatch.StartNew();
        var last = count();
        while (settled.Elapsed < SettleTime)
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
            var current = count();
            if (current != last)
            {
                last = current;
                settled.Restart();
            }
        }
    }
}
