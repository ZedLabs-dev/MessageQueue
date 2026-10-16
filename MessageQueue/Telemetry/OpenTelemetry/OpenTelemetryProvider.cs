using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ESE.MessageQueue.Telemetry.OpenTelemetry;

public class OpenTelemetryProvider : ITelemetryProvider
{
    readonly ActivitySource activitySource;
    readonly Meter meter;
    readonly Counter<long> messagesPublished;
    readonly Counter<long> messagesConsumed;
    readonly Counter<long> messagesRetried;
    readonly Counter<long> messagesDeadLettered;
    readonly bool recordExceptionDetails;

    // recordExceptionDetails: also put exception messages and stack traces on spans. Off by default (S6):
    // exception text can carry personal or secret data, and spans often leave the trust boundary.
    public OpenTelemetryProvider(string sourceName, bool recordExceptionDetails = false)
    {
        this.recordExceptionDetails = recordExceptionDetails;
        activitySource = new ActivitySource(sourceName);
        meter = new Meter(sourceName);

        messagesPublished = meter.CreateCounter<long>(
            "messagequeue.messages.published",
            unit: "messages",
            description: "Number of messages published");

        messagesConsumed = meter.CreateCounter<long>(
            "messagequeue.messages.consumed",
            unit: "messages",
            description: "Number of messages consumed");

        messagesRetried = meter.CreateCounter<long>(
            "messagequeue.messages.retried",
            unit: "messages",
            description: "Number of messages retried");

        messagesDeadLettered = meter.CreateCounter<long>(
            "messagequeue.messages.deadlettered",
            unit: "messages",
            description: "Number of messages sent to dead letter queue");
    }

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType)
    {
        var activity = activitySource.StartActivity($"{queueName} publish", ActivityKind.Producer);
        if (activity == null)
            return null;

        activity.SetTag("messaging.system", "messagequeue");
        activity.SetTag("messaging.destination.name", queueName);
        activity.SetTag("messaging.message.type", messageType);
        activity.SetTag("messaging.operation.type", "publish");

        return new OpenTelemetryActivity(activity, "publish", queueName, messageType, recordExceptionDetails);
    }

    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId)
    {
        var activity = activitySource.StartActivity($"{queueName} consume", ActivityKind.Consumer);
        if (activity == null)
            return null;

        activity.SetTag("messaging.system", "messagequeue");
        activity.SetTag("messaging.destination.name", queueName);
        activity.SetTag("messaging.message.type", messageType);
        activity.SetTag("messaging.operation.type", "consume");
        if (messageId != null)
            activity.SetTag("messaging.message.id", messageId);

        return new OpenTelemetryActivity(activity, "consume", queueName, messageType, recordExceptionDetails) { MessageId = messageId };
    }

    public void RecordMessagePublished(string queueName, string messageType)
    {
        messagesPublished.Add(1,
            new KeyValuePair<string, object?>("queue", queueName),
            new KeyValuePair<string, object?>("message_type", messageType));
    }

    public void RecordMessageConsumed(string queueName, string messageType, bool success)
    {
        messagesConsumed.Add(1,
            new KeyValuePair<string, object?>("queue", queueName),
            new KeyValuePair<string, object?>("message_type", messageType),
            new KeyValuePair<string, object?>("success", success));
    }

    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber)
    {
        messagesRetried.Add(1,
            new KeyValuePair<string, object?>("queue", queueName),
            new KeyValuePair<string, object?>("message_type", messageType),
            new KeyValuePair<string, object?>("attempt", attemptNumber));
    }

    public void RecordMessageDeadLettered(string queueName, string messageType, string reason)
    {
        messagesDeadLettered.Add(1,
            new KeyValuePair<string, object?>("queue", queueName),
            new KeyValuePair<string, object?>("message_type", messageType),
            new KeyValuePair<string, object?>("reason", reason));
    }

    public TraceContext? ExtractTraceContext(IDictionary<string, object> headers)
    {
        if (!headers.TryGetValue("traceparent", out var value) || value is not string traceparent)
            return null;

        var parts = traceparent.Split('-');
        if (parts.Length < 3)
            return null;

        return new TraceContext(parts[1], parts[2]);
    }

    public void InjectTraceContext(IDictionary<string, object> headers)
    {
        var activity = Activity.Current;
        if (activity == null)
            return;

        var traceparent = $"00-{activity.TraceId}-{activity.SpanId}-{(activity.Recorded ? "01" : "00")}";
        headers["traceparent"] = traceparent;

        if (!string.IsNullOrEmpty(activity.TraceStateString))
            headers["tracestate"] = activity.TraceStateString;
    }
}

public class OpenTelemetryActivity : TelemetryActivity
{
    public const int MaxRecordedLength = 4096;

    readonly Activity activity;
    readonly bool recordExceptionDetails;

    public OpenTelemetryActivity(
        Activity activity,
        string operationName,
        string queueName,
        string messageType,
        bool recordExceptionDetails = false)
        : base(operationName, queueName, messageType)
    {
        this.activity = activity;
        this.recordExceptionDetails = recordExceptionDetails;
    }

    public override void SetError(Exception exception)
    {
        base.SetError(exception);

        var exceptionType = exception.GetType().FullName;
        var tags = new ActivityTagsCollection { { "exception.type", exceptionType } };
        if (recordExceptionDetails)
        {
            tags.Add("exception.message", Truncate(exception.Message));
            tags.Add("exception.stacktrace", Truncate(exception.ToString()));
        }

        activity.SetStatus(ActivityStatusCode.Error, Truncate(recordExceptionDetails ? exception.Message : exceptionType));
        activity.AddEvent(new ActivityEvent("exception", tags: tags));
    }

    // Exporters and backends cap attribute sizes; long values are cut here so the span is still exported.
    static string? Truncate(string? value) =>
        value is { Length: > MaxRecordedLength } ? value[..MaxRecordedLength] : value;

    public override void Dispose()
    {
        activity.Dispose();
        base.Dispose();
    }
}
