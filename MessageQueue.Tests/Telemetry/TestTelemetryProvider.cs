using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.Tests.Telemetry;

public class TestTelemetryProvider : ITelemetryProvider
{
    public int PublishedCount { get; private set; }
    public int ConsumedSuccessCount { get; private set; }
    public int ConsumedFailureCount { get; private set; }
    public int RetriedCount { get; private set; }
    public int DeadLetteredCount { get; private set; }

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType)
    {
        return new TelemetryActivity("publish", queueName, messageType);
    }

    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId)
    {
        return new TelemetryActivity("consume", queueName, messageType) { MessageId = messageId };
    }

    public void RecordMessagePublished(string queueName, string messageType)
    {
        PublishedCount++;
    }

    public void RecordMessageConsumed(string queueName, string messageType, bool success)
    {
        if (success)
            ConsumedSuccessCount++;
        else
            ConsumedFailureCount++;
    }

    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber)
    {
        RetriedCount++;
    }

    public void RecordMessageDeadLettered(string queueName, string messageType, string reason)
    {
        DeadLetteredCount++;
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
