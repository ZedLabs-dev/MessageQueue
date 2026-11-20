using System.Collections.Concurrent;

namespace ZedLabs.MessageQueue.Processing;

// Shutdown work that outlives the call that started it (a drain started from inside a handler, or one that ran past
// ShutdownTimeout), so dispose can still wait for it.
sealed class BackgroundWork
{
    readonly ConcurrentDictionary<Task, bool> running = new();

    public void Track(Task work)
    {
        running[work] = true;
        work.ContinueWith(done => running.TryRemove(done, out _), TaskScheduler.Default);
    }

    public Task WhenAll() => Task.WhenAll(running.Keys);
}
