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

    /// <summary>
    /// Counts a transport error a provider handled outside any delivery, such as a Service Bus processor error.
    /// <paramref name="errorType"/> is low-cardinality: a broker failure-reason code or an exception type name. The
    /// default implementation records nothing, so existing providers keep working.
    /// </summary>
    void RecordTransportError(TelemetryTarget target, string errorType) { }

    /// <summary>
    /// Counts dead letters moved back to the queue by one replay call (ADR 0023). The default implementation records
    /// nothing, so existing providers keep working.
    /// </summary>
    void RecordDeadLettersReplayed(TelemetryTarget target, int count) { }

    /// <summary>
    /// Counts dead letters deleted by one purge call (ADR 0023). The default implementation records nothing, so
    /// existing providers keep working.
    /// </summary>
    void RecordDeadLettersPurged(TelemetryTarget target, int count) { }
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

/// <summary>
/// A span the library started through an <see cref="ITelemetryProvider"/>: the library sets errors and tags on it and
/// disposes it when the operation ends. Custom telemetry providers derive from it and override the virtual members;
/// the base records only <see cref="Error"/> and <see cref="ErrorType"/>. Used by one operation at a time.
/// </summary>
public class TelemetryActivity : IDisposable
{
    /// <summary>The operation: <c>publish</c> or <c>consume</c>.</summary>
    public string OperationName { get; }
    /// <summary>The destination (queue, topic or exchange) the operation is about.</summary>
    public string QueueName { get; }
    /// <summary>The message type's name.</summary>
    public string MessageType { get; }
    /// <summary>The message id, for a single-message operation.</summary>
    public string? MessageId { get; init; }

    /// <summary>Creates the activity for one operation.</summary>
    public TelemetryActivity(string operationName, string queueName, string messageType)
    {
        OperationName = operationName;
        QueueName = queueName;
        MessageType = messageType;
    }

    /// <summary>Marks the operation as failed with <paramref name="exception"/>; the base records it in <see cref="Error"/>.</summary>
    public virtual void SetError(Exception exception)
    {
        Error = exception;
    }

    /// <summary>The exception passed to <see cref="SetError"/>, if any.</summary>
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

    /// <summary>Ends the span. Safe to call more than once.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Ends the span; override to release what the activity holds.</summary>
    /// <param name="disposing">True when called from <see cref="Dispose()"/>.</param>
    protected virtual void Dispose(bool disposing) { }
}

/// <summary>
/// The telemetry provider used when none is registered: no spans, no metrics, no trace propagation. Stateless and
/// thread-safe; publishing with it allocates nothing for telemetry.
/// </summary>
public sealed class NullTelemetryProvider : ITelemetryProvider
{
    /// <summary>The shared instance.</summary>
    public static readonly NullTelemetryProvider Instance = new();

    NullTelemetryProvider() { }

    /// <inheritdoc />
    public TelemetryActivity? StartPublishActivity(TelemetryTarget target, int messageCount, string? messageId) => null;
    /// <inheritdoc />
    public TelemetryActivity? StartConsumeActivity(TelemetryTarget target, MessageContext context, TraceContext? parent) => null;
    /// <inheritdoc />
    public void RecordMessagePublished(TelemetryTarget target, int messageCount, TimeSpan duration, string? errorType) { }
    /// <inheritdoc />
    public void RecordMessageConsumed(TelemetryTarget target, TimeSpan duration, string? errorType) { }
    /// <inheritdoc />
    public void RecordMessageRetried(TelemetryTarget target, int nextAttempt) { }
    /// <inheritdoc />
    public void RecordMessageDeadLettered(TelemetryTarget target, string reason) { }
    /// <inheritdoc />
    public TraceContext? ExtractTraceContext(IReadOnlyDictionary<string, string> headers) => null;
    /// <inheritdoc />
    public void InjectTraceContext(IDictionary<string, string> headers) { }
}
