using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.Tests;

// A telemetry provider whose every method throws, standing in for a broken exporter.
public sealed class ThrowingTelemetryProvider : ITelemetryProvider
{
    int retriedCalls;
    int injectCalls;

    public int RetriedCalls => Volatile.Read(ref retriedCalls);
    public int InjectCalls => Volatile.Read(ref injectCalls);

    public TelemetryActivity? StartPublishActivity(TelemetryTarget target, int messageCount, string? messageId) => throw Broken();
    public TelemetryActivity? StartConsumeActivity(TelemetryTarget target, MessageContext context, TraceContext? parent) => throw Broken();
    public void RecordMessagePublished(TelemetryTarget target, int messageCount, TimeSpan duration, string? errorType) => throw Broken();
    public void RecordMessageConsumed(TelemetryTarget target, TimeSpan duration, string? errorType) => throw Broken();

    public void RecordMessageRetried(TelemetryTarget target, int nextAttempt)
    {
        Interlocked.Increment(ref retriedCalls);
        throw Broken();
    }

    public void RecordMessageDeadLettered(TelemetryTarget target, string reason) => throw Broken();
    public TraceContext? ExtractTraceContext(IReadOnlyDictionary<string, string> headers) => throw Broken();

    public void InjectTraceContext(IDictionary<string, string> headers)
    {
        Interlocked.Increment(ref injectCalls);
        throw Broken();
    }

    static InvalidOperationException Broken() => new("telemetry exporter failed");
}
