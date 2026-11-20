using System.Diagnostics;
using System.Diagnostics.Metrics;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.Telemetry.OpenTelemetry;

/// <summary>
/// <see cref="ITelemetryProvider"/> on <see cref="ActivitySource"/> and <see cref="Meter"/> (both named
/// <c>sourceName</c>), following the OpenTelemetry messaging semantic conventions where they define something.
/// The library references no OpenTelemetry package or exporter; the host subscribes with
/// <c>AddSource(sourceName)</c> / <c>AddMeter(sourceName)</c>.
/// </summary>
public sealed class OpenTelemetryProvider : ITelemetryProvider, IDisposable
{
    // Semantic conventions' advice for messaging duration histograms, in seconds.
    static readonly double[] DurationBuckets = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10];

    readonly ActivitySource activitySource;
    readonly Meter meter;
    readonly Counter<long> messagesSent;
    readonly Histogram<double> operationDuration;
    readonly Counter<long> messagesConsumed;
    readonly Histogram<double> processDuration;
    readonly Counter<long> messagesRetried;
    readonly Counter<long> messagesDeadLettered;
    readonly Counter<long> transportErrors;
    readonly Counter<long> deadLettersReplayed;
    readonly Counter<long> deadLettersPurged;
    readonly bool recordExceptionDetails;

    /// <summary>
    /// Creates the provider with an <c>ActivitySource</c> and a <c>Meter</c> named <paramref name="sourceName"/>.
    /// <paramref name="recordExceptionDetails"/> also puts exception messages and stack traces (truncated to 4096
    /// characters) on spans; it is off by default (S6) because exception text can carry personal or secret data and
    /// spans often leave the trust boundary.
    /// </summary>
    public OpenTelemetryProvider(string sourceName, bool recordExceptionDetails = false)
    {
        this.recordExceptionDetails = recordExceptionDetails;
        activitySource = new ActivitySource(sourceName);
        meter = new Meter(sourceName);
        var durationAdvice = new InstrumentAdvice<double> { HistogramBucketBoundaries = DurationBuckets };

        messagesSent = meter.CreateCounter<long>(TelemetryNames.SentMessages, "{message}", "Number of messages published");
        operationDuration = meter.CreateHistogram(TelemetryNames.OperationDuration, "s",
            "Duration of publish operations", advice: durationAdvice);
        messagesConsumed = meter.CreateCounter<long>(TelemetryNames.ConsumedMessages, "{message}", "Number of deliveries handed to the pipeline");
        processDuration = meter.CreateHistogram(TelemetryNames.ProcessDuration, "s",
            "Duration of processing one delivery, from receipt to settlement", advice: durationAdvice);
        messagesRetried = meter.CreateCounter<long>(TelemetryNames.RetriedMessages, "{message}", "Number of deliveries rescheduled for a retry");
        messagesDeadLettered = meter.CreateCounter<long>(TelemetryNames.DeadLetteredMessages, "{message}", "Number of deliveries dead-lettered");
        transportErrors = meter.CreateCounter<long>(TelemetryNames.TransportErrors, "{error}", "Number of transport errors handled outside a delivery");
        deadLettersReplayed = meter.CreateCounter<long>(TelemetryNames.ReplayedDeadLetters, "{message}", "Number of dead letters moved back to their queue");
        deadLettersPurged = meter.CreateCounter<long>(TelemetryNames.PurgedDeadLetters, "{message}", "Number of dead letters deleted");
    }

    /// <inheritdoc />
    public TelemetryActivity? StartPublishActivity(TelemetryTarget target, int messageCount, string? messageId)
    {
        if (!activitySource.HasListeners())
            return null;

        var activity = activitySource.StartActivity($"{target.PublishOperation} {target.Destination}", ActivityKind.Producer);
        if (activity == null)
            return null;

        SetCommonTags(activity, target, target.PublishOperation, "send");
        if (messageCount > 1)
            activity.SetTag(TelemetryNames.BatchMessageCount, messageCount);
        else if (!string.IsNullOrEmpty(messageId))
            activity.SetTag(TelemetryNames.MessageId, messageId);

        return new OpenTelemetryActivity(activity, "publish", target.Destination, target.MessageType, recordExceptionDetails);
    }

    // The publisher's context is the parent (it arrived with the message, so it is remote). Without one the span
    // falls back to the ambient Activity.Current; the dispatcher clears that, so a message without trace headers
    // starts a new trace instead of joining whatever span was current on the consumer thread.
    /// <inheritdoc />
    public TelemetryActivity? StartConsumeActivity(TelemetryTarget target, MessageContext context, TraceContext? parent)
    {
        // Nothing records the span: skip formatting and parsing the parent.
        if (!activitySource.HasListeners())
            return null;

        var parentContext = default(ActivityContext);
        if (parent is not null)
            ActivityContext.TryParse(parent.ToTraceParent(), parent.TraceState, isRemote: true, out parentContext);

        var activity = activitySource.StartActivity($"process {target.Destination}", ActivityKind.Consumer, parentContext);
        if (activity == null)
            return null;

        SetCommonTags(activity, target, "process", "process");
        if (target.Subscription is not null)
            activity.SetTag(TelemetryNames.SubscriptionName, target.Subscription);
        if (!string.IsNullOrEmpty(context.MessageId))
            activity.SetTag(TelemetryNames.MessageId, context.MessageId);
        if (context.CorrelationId is not null)
            activity.SetTag(TelemetryNames.ConversationId, context.CorrelationId);
        activity.SetTag(TelemetryNames.DeliveryAttempt, context.DeliveryAttempt);

        return new OpenTelemetryActivity(activity, "consume", target.Destination, target.MessageType, recordExceptionDetails)
        {
            MessageId = context.MessageId
        };
    }

    // Instruments nobody listens to are skipped before any tags are built.
    /// <inheritdoc />
    public void RecordMessagePublished(TelemetryTarget target, int messageCount, TimeSpan duration, string? errorType)
    {
        if (!messagesSent.Enabled && !operationDuration.Enabled)
            return;

        var tags = Tags(target, target.PublishOperation, errorType, consumer: false);
        messagesSent.Add(messageCount, tags);
        operationDuration.Record(duration.TotalSeconds, tags);
    }

    /// <inheritdoc />
    public void RecordMessageConsumed(TelemetryTarget target, TimeSpan duration, string? errorType)
    {
        if (!messagesConsumed.Enabled && !processDuration.Enabled)
            return;

        var tags = Tags(target, "process", errorType, consumer: true);
        messagesConsumed.Add(1, tags);
        processDuration.Record(duration.TotalSeconds, tags);
    }

    /// <inheritdoc />
    public void RecordMessageRetried(TelemetryTarget target, int nextAttempt)
    {
        if (messagesRetried.Enabled)
            messagesRetried.Add(1, Tags(target, "process", null, consumer: true));
    }

    /// <inheritdoc />
    public void RecordMessageDeadLettered(TelemetryTarget target, string reason)
    {
        if (!messagesDeadLettered.Enabled)
            return;

        var tags = Tags(target, "process", null, consumer: true);
        tags.Add(TelemetryNames.DeadLetterReason, BoundedText.Truncate(reason));
        messagesDeadLettered.Add(1, tags);
    }

    /// <inheritdoc />
    public void RecordTransportError(TelemetryTarget target, string errorType)
    {
        if (!transportErrors.Enabled)
            return;

        transportErrors.Add(1, Tags(target, "process", errorType, consumer: true));
    }

    /// <inheritdoc />
    public void RecordDeadLettersReplayed(TelemetryTarget target, int count)
    {
        if (deadLettersReplayed.Enabled)
            deadLettersReplayed.Add(count, Tags(target, "replay", null, consumer: true));
    }

    /// <inheritdoc />
    public void RecordDeadLettersPurged(TelemetryTarget target, int count)
    {
        if (deadLettersPurged.Enabled)
            deadLettersPurged.Add(count, Tags(target, "purge", null, consumer: true));
    }

    // Only consume spans use the parent, so nothing is parsed while no span would be recorded.
    /// <inheritdoc />
    public TraceContext? ExtractTraceContext(IReadOnlyDictionary<string, string> headers) =>
        activitySource.HasListeners()
            ? TraceContext.Parse(headers.GetValueOrDefault(TraceHeaders.TraceParent), headers.GetValueOrDefault(TraceHeaders.TraceState))
            : null;

    /// <inheritdoc />
    public void InjectTraceContext(IDictionary<string, string> headers)
    {
        var activity = Activity.Current;
        if (activity == null || activity.IdFormat != ActivityIdFormat.W3C)
            return;

        headers[TraceHeaders.TraceParent] = $"00-{activity.TraceId}-{activity.SpanId}-{(activity.Recorded ? "01" : "00")}";
        if (!string.IsNullOrEmpty(activity.TraceStateString))
            headers[TraceHeaders.TraceState] = activity.TraceStateString;
    }

    /// <summary>Disposes the <c>ActivitySource</c> and <c>Meter</c>; spans and metrics stop.</summary>
    public void Dispose()
    {
        activitySource.Dispose();
        meter.Dispose();
        GC.SuppressFinalize(this);
    }

    static void SetCommonTags(Activity activity, TelemetryTarget target, string operationName, string operationType)
    {
        activity.SetTag(TelemetryNames.System, target.System);
        activity.SetTag(TelemetryNames.DestinationName, target.Destination);
        activity.SetTag(TelemetryNames.OperationName, operationName);
        activity.SetTag(TelemetryNames.OperationType, operationType);
        activity.SetTag(TelemetryNames.MessageType, target.MessageType);
        if (target.RoutingKey is not null)
            activity.SetTag(TelemetryNames.RoutingKey, target.RoutingKey);
    }

    static TagList Tags(TelemetryTarget target, string operationName, string? errorType, bool consumer)
    {
        var tags = new TagList
        {
            { TelemetryNames.System, target.System },
            { TelemetryNames.DestinationName, target.Destination },
            { TelemetryNames.OperationName, operationName },
            { TelemetryNames.MessageType, target.MessageType }
        };
        if (consumer && target.Subscription is not null)
            tags.Add(TelemetryNames.SubscriptionName, target.Subscription);
        if (errorType is not null)
            tags.Add(TelemetryNames.ErrorType, BoundedText.Truncate(errorType));
        return tags;
    }
}

// The span handed out by OpenTelemetryProvider; callers see it as a TelemetryActivity.
sealed class OpenTelemetryActivity : TelemetryActivity
{
    public const int MaxRecordedLength = BoundedText.MaxLength;

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

        var exceptionType = BoundedText.TypeName(exception);
        var tags = new ActivityTagsCollection { { "exception.type", exceptionType } };
        if (recordExceptionDetails)
        {
            tags.Add("exception.message", Truncate(exception.Message));
            tags.Add("exception.stacktrace", Truncate(exception.ToString()));
        }

        activity.SetTag(TelemetryNames.ErrorType, exceptionType);
        activity.SetStatus(ActivityStatusCode.Error, recordExceptionDetails ? Truncate(exception.Message) : exceptionType);
        activity.AddEvent(new ActivityEvent("exception", tags: tags));
    }

    // An outcome that is not a success (a retry, a dead letter): error.type and Error status, as on the metrics. A span
    // already failed by SetError keeps its status description.
    public override void SetErrorType(string errorType)
    {
        errorType = BoundedText.Truncate(errorType);
        base.SetErrorType(errorType);
        activity.SetTag(TelemetryNames.ErrorType, errorType);
        if (activity.Status != ActivityStatusCode.Error)
            activity.SetStatus(ActivityStatusCode.Error, errorType);
    }

    public override void SetTag(string name, object? value) => activity.SetTag(name, value);

    // Exporters and backends cap attribute sizes; long values are cut here so the span is still exported.
    static string Truncate(string value) => BoundedText.Truncate(value);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            activity.Dispose();
        base.Dispose(disposing);
    }
}
