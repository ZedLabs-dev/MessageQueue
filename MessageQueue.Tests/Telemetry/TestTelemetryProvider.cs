using System.Collections.Concurrent;
using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.Tests.Telemetry;

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
    public ConcurrentQueue<TelemetryActivity> Activities { get; } = new();

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType)
    {
        var activity = new TelemetryActivity("publish", queueName, messageType);
        Activities.Enqueue(activity);
        return activity;
    }

    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId)
    {
        var activity = new TelemetryActivity("consume", queueName, messageType) { MessageId = messageId };
        Activities.Enqueue(activity);
        return activity;
    }

    public void RecordMessagePublished(string queueName, string messageType) => Interlocked.Increment(ref published);

    public void RecordMessageConsumed(string queueName, string messageType, bool success)
    {
        if (success)
            Interlocked.Increment(ref consumedSuccess);
        else
            Interlocked.Increment(ref consumedFailure);
    }

    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber) => Interlocked.Increment(ref retried);

    public void RecordMessageDeadLettered(string queueName, string messageType, string reason)
    {
        DeadLetterReasons.Enqueue(reason);
        Interlocked.Increment(ref deadLettered);
    }

    public TraceContext? ExtractTraceContext(IDictionary<string, object> headers)
    {
        if (headers.TryGetValue("traceparent", out var value) && value is string traceparent)
        {
            var parts = traceparent.Split('-');
            if (parts.Length >= 3)
                return new TraceContext(parts[1], parts[2]);
        }
        return null;
    }

    public void InjectTraceContext(IDictionary<string, object> headers)
    {
        headers["traceparent"] = "00-generated-trace-id-generated-span-id-01";
    }
}
