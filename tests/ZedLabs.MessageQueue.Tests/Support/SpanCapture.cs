using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;

namespace ZedLabs.MessageQueue.Tests;

// Listens to one ActivitySource (use a source name unique to the test, since listeners are process-wide) and keeps
// every span it finishes.
sealed class SpanCapture : IDisposable
{
    readonly ActivityListener listener;
    readonly ConcurrentQueue<Activity> stopped = new();

    public SpanCapture(string? sourceName = null)
    {
        SourceName = sourceName ?? $"ZedLabs.MessageQueue.Tests.{Guid.NewGuid():N}";
        listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = stopped.Enqueue
        };
        ActivitySource.AddActivityListener(listener);
    }

    public string SourceName { get; }

    public IReadOnlyList<Activity> Spans => [.. stopped];

    public IReadOnlyList<Activity> OfKind(ActivityKind kind) => [.. stopped.Where(a => a.Kind == kind)];

    public Activity Single(ActivityKind kind) =>
        OfKind(kind).Should().ContainSingle($"it should have produced one {kind} span").Subject;

    // Spans stop after the code under test returns (consume spans end after settling), so wait for them.
    public async Task<IReadOnlyList<Activity>> WaitForAsync(ActivityKind kind, int count)
    {
        await Wait.UntilAsync(() => OfKind(kind).Count >= count, Wait.DeliveryTimeout);
        return OfKind(kind);
    }

    public void Dispose() => listener.Dispose();
}
