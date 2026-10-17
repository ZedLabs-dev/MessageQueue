using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Telemetry;

/// <summary>
/// Spans, metrics and trace propagation for every provider. The library calls it from the publish path and from the
/// shared consumer pipeline; a failing implementation never changes what happens to a message.
/// </summary>
public interface ITelemetryProvider
{
    /// <summary>
    /// Starts a producer span for one publish call (<paramref name="messageCount"/> &gt; 1 for a batch);
    /// <paramref name="messageId"/> is set for a single message.
    /// </summary>
    TelemetryActivity? StartPublishActivity(TelemetryTarget target, int messageCount, string? messageId);

    /// <summary>Starts a consumer span for one delivery, parented on the publisher's trace context when it has one.</summary>
    TelemetryActivity? StartConsumeActivity(TelemetryTarget target, MessageContext context, TraceContext? parent);

    /// <summary>Counts published messages and records the publish duration. <paramref name="errorType"/> is null on success.</summary>
    void RecordMessagePublished(TelemetryTarget target, int messageCount, TimeSpan duration, string? errorType);

    /// <summary>Counts one delivery and records its processing duration. <paramref name="errorType"/> is null when acknowledged.</summary>
    void RecordMessageConsumed(TelemetryTarget target, TimeSpan duration, string? errorType);

    /// <summary>Counts a delivery rescheduled as <paramref name="nextAttempt"/>.</summary>
    void RecordMessageRetried(TelemetryTarget target, int nextAttempt);

    /// <summary>Counts a dead-lettered delivery; <paramref name="reason"/> is a <c>DeadLetterReasons</c> code.</summary>
    void RecordMessageDeadLettered(TelemetryTarget target, string reason);

    /// <summary>Reads the W3C trace context from message headers. Malformed values yield null; never throws.</summary>
    TraceContext? ExtractTraceContext(IReadOnlyDictionary<string, string> headers);

    /// <summary>Writes the current W3C trace context (<c>traceparent</c>, <c>tracestate</c>) into message headers.</summary>
    void InjectTraceContext(IDictionary<string, string> headers);
}

/// <summary>
/// What a span or metric is about: the messaging system, the destination (the same value on publish and consume, so
/// the two join) and the message type.
/// </summary>
public sealed record TelemetryTarget(string System, string Destination, string MessageType)
{
    /// <summary>The subscription consumed from (Azure Service Bus topics).</summary>
    public string? Subscription { get; init; }

    /// <summary>The routing key messages are published with (RabbitMQ).</summary>
    public string? RoutingKey { get; init; }

    /// <summary>The system's name for a publish (<c>publish</c>; <c>send</c> on Azure Service Bus).</summary>
    public string PublishOperation { get; init; } = "publish";
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

    public virtual void SetError(Exception exception)
    {
        Error = exception;
    }

    public Exception? Error { get; private set; }

    /// <summary>Why the operation did not succeed, as reported to metrics (an exception type or a reason code).</summary>
    public string? ErrorType { get; private set; }

    /// <summary>Marks the span as failed with a low-cardinality error type, without an exception.</summary>
    public virtual void SetErrorType(string errorType)
    {
        ErrorType = errorType;
    }

    /// <summary>Adds an attribute to the span, such as the dead-letter reason or the next retry attempt.</summary>
    public virtual void SetTag(string name, object? value) { }

    public virtual void Dispose() { }
}

public class NullTelemetryProvider : ITelemetryProvider
{
    public static readonly NullTelemetryProvider Instance = new();

    NullTelemetryProvider() { }

    public TelemetryActivity? StartPublishActivity(TelemetryTarget target, int messageCount, string? messageId) => null;
    public TelemetryActivity? StartConsumeActivity(TelemetryTarget target, MessageContext context, TraceContext? parent) => null;
    public void RecordMessagePublished(TelemetryTarget target, int messageCount, TimeSpan duration, string? errorType) { }
    public void RecordMessageConsumed(TelemetryTarget target, TimeSpan duration, string? errorType) { }
    public void RecordMessageRetried(TelemetryTarget target, int nextAttempt) { }
    public void RecordMessageDeadLettered(TelemetryTarget target, string reason) { }
    public TraceContext? ExtractTraceContext(IReadOnlyDictionary<string, string> headers) => null;
    public void InjectTraceContext(IDictionary<string, string> headers) { }
}
