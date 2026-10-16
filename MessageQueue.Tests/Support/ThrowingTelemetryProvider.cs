using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.Tests;

// A telemetry provider whose every method throws, standing in for a broken exporter.
public sealed class ThrowingTelemetryProvider : ITelemetryProvider
{
    int retriedCalls;

    public int RetriedCalls => Volatile.Read(ref retriedCalls);

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType) => throw Broken();
    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId) => throw Broken();
    public void RecordMessagePublished(string queueName, string messageType) => throw Broken();
    public void RecordMessageConsumed(string queueName, string messageType, bool success) => throw Broken();

    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber)
    {
        Interlocked.Increment(ref retriedCalls);
        throw Broken();
    }

    public void RecordMessageDeadLettered(string queueName, string messageType, string reason) => throw Broken();
    public TraceContext? ExtractTraceContext(IDictionary<string, object> headers) => throw Broken();
    public void InjectTraceContext(IDictionary<string, object> headers) => throw Broken();

    static InvalidOperationException Broken() => new("telemetry exporter failed");
}
