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

    public OpenTelemetryProvider(string sourceName)
    {
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

        return new OpenTelemetryActivity(activity, "publish", queueName, messageType);
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

        return new OpenTelemetryActivity(activity, "consume", queueName, messageType) { MessageId = messageId };
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
    readonly Activity activity;

    public OpenTelemetryActivity(Activity activity, string operationName, string queueName, string messageType)
        : base(operationName, queueName, messageType)
    {
        this.activity = activity;
    }

    public new void SetError(Exception exception)
    {
        base.SetError(exception);
        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            { "exception.type", exception.GetType().FullName },
            { "exception.message", exception.Message },
            { "exception.stacktrace", exception.StackTrace }
        }));
    }

    public override void Dispose()
    {
        activity.Dispose();
        base.Dispose();
    }
}

