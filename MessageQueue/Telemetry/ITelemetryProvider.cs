namespace ESE.MessageQueue.Telemetry;

public interface ITelemetryProvider
{
    TelemetryActivity? StartPublishActivity(string queueName, string messageType);
    TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId);
    void RecordMessagePublished(string queueName, string messageType);
    void RecordMessageConsumed(string queueName, string messageType, bool success);
    void RecordMessageRetried(string queueName, string messageType, int attemptNumber);
    void RecordMessageDeadLettered(string queueName, string messageType, string reason);
    TraceContext? ExtractTraceContext(IDictionary<string, object> headers);
    void InjectTraceContext(IDictionary<string, object> headers);
}

public class TelemetryActivity : IDisposable
{
    public string OperationName { get; }
    public string QueueName { get; }
    public string MessageType { get; }
    public string? MessageId { get; init; }

    public TelemetryActivity(string operationName, string queueName, string messageType)
    {
        OperationName = operationName;
        QueueName = queueName;
        MessageType = messageType;
    }

    public void SetError(Exception exception)
    {
        Error = exception;
    }

    public Exception? Error { get; private set; }

    public virtual void Dispose() { }
}

public record TraceContext(string TraceId, string SpanId);

public class NullTelemetryProvider : ITelemetryProvider
{
    public static readonly NullTelemetryProvider Instance = new();

    NullTelemetryProvider() { }

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType) => null;
    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId) => null;
    public void RecordMessagePublished(string queueName, string messageType) { }
    public void RecordMessageConsumed(string queueName, string messageType, bool success) { }
    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber) { }
    public void RecordMessageDeadLettered(string queueName, string messageType, string reason) { }
    public TraceContext? ExtractTraceContext(IDictionary<string, object> headers) => null;
    public void InjectTraceContext(IDictionary<string, object> headers) { }
}

