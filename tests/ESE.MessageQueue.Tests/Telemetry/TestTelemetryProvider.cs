using System.Collections.Concurrent;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.Tests.Telemetry;

// Records every telemetry call, so specs can assert on what the library reported without an exporter.
public class TestTelemetryProvider : ITelemetryProvider
{
    int published;
    int consumedSuccess;
    int consumedFailure;
    int retried;
    int deadLettered;

    public int PublishedCount => Volatile.Read(ref published);
    public int ConsumedSuccessCount => Volatile.Read(ref consumedSuccess);
    public int ConsumedFailureCount => Volatile.Read(ref consumedFailure);
    public int RetriedCount => Volatile.Read(ref retried);
    public int DeadLetteredCount => Volatile.Read(ref deadLettered);
    public ConcurrentQueue<string> DeadLetterReasons { get; } = new();
    public ConcurrentQueue<int> RetriedAttempts { get; } = new();
    public ConcurrentQueue<RecordedActivity> Activities { get; } = new();
    public ConcurrentQueue<TelemetryTarget> Targets { get; } = new();
    public ConcurrentQueue<(TimeSpan Duration, string? ErrorType)> Consumed { get; } = new();
    public ConcurrentQueue<(int Count, TimeSpan Duration, string? ErrorType)> Published { get; } = new();

    // What InjectTraceContext writes; the default is a valid version 00 traceparent.
    public string InjectedTraceParent { get; init; } = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    public TelemetryActivity? StartPublishActivity(TelemetryTarget target, int messageCount, string? messageId)
    {
        Targets.Enqueue(target);
        var activity = new RecordedActivity("publish", target, null) { MessageCount = messageCount, MessageId = messageId };
        Activities.Enqueue(activity);
        return activity;
    }

    public TelemetryActivity? StartConsumeActivity(TelemetryTarget target, MessageContext context, TraceContext? parent)
    {
        Targets.Enqueue(target);
        var activity = new RecordedActivity("consume", target, parent) { MessageId = context.MessageId, Context = context };
        Activities.Enqueue(activity);
        return activity;
    }

    public void RecordMessagePublished(TelemetryTarget target, int messageCount, TimeSpan duration, string? errorType)
    {
        Published.Enqueue((messageCount, duration, errorType));
        Interlocked.Add(ref published, messageCount);
    }

    public void RecordMessageConsumed(TelemetryTarget target, TimeSpan duration, string? errorType)
    {
        Consumed.Enqueue((duration, errorType));
        if (errorType is null)
            Interlocked.Increment(ref consumedSuccess);
        else
            Interlocked.Increment(ref consumedFailure);
    }

    public void RecordMessageRetried(TelemetryTarget target, int nextAttempt)
    {
        RetriedAttempts.Enqueue(nextAttempt);
        Interlocked.Increment(ref retried);
    }

    public void RecordMessageDeadLettered(TelemetryTarget target, string reason)
    {
        DeadLetterReasons.Enqueue(reason);
        Interlocked.Increment(ref deadLettered);
    }

    public TraceContext? ExtractTraceContext(IReadOnlyDictionary<string, string> headers) =>
        TraceContext.Parse(headers.GetValueOrDefault("traceparent"), headers.GetValueOrDefault("tracestate"));

    public void InjectTraceContext(IDictionary<string, string> headers) => headers["traceparent"] = InjectedTraceParent;

    public ConcurrentQueue<(TelemetryTarget Target, string ErrorType)> TransportErrors { get; } = new();

    public void RecordTransportError(TelemetryTarget target, string errorType) => TransportErrors.Enqueue((target, errorType));

    public IReadOnlyList<RecordedActivity> Consumes => [.. Activities.Where(a => a.OperationName == "consume")];

    public IReadOnlyList<RecordedActivity> Publishes => [.. Activities.Where(a => a.OperationName == "publish")];
}

public sealed class RecordedActivity : TelemetryActivity
{
    readonly ConcurrentDictionary<string, object?> tags = new();

    public RecordedActivity(string operationName, TelemetryTarget target, TraceContext? parent)
        : base(operationName, target.Destination, target.MessageType)
    {
        Target = target;
        Parent = parent;
    }

    public TelemetryTarget Target { get; }
    public TraceContext? Parent { get; }
    public MessageContext? Context { get; init; }
    public int MessageCount { get; init; }
    public bool Disposed { get; private set; }
    public IReadOnlyDictionary<string, object?> Tags => tags;

    public override void SetTag(string name, object? value) => tags[name] = value;

    protected override void Dispose(bool disposing)
    {
        Disposed = true;
        base.Dispose(disposing);
    }
}
