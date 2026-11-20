namespace ZedLabs.MessageQueue.Processing;

static class RoundRobin
{
    // Picks the next index in [0, length) from a shared counter, thread-safely.
    // Unsigned modulo keeps the index in range after the counter wraps past int.MaxValue (B1).
    public static int Next(ref int counter, int length) => (int)((uint)Interlocked.Increment(ref counter) % (uint)length);
}
