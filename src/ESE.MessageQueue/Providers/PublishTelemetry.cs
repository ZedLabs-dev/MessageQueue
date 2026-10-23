using System.Diagnostics;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// Publish-side telemetry shared by every provider (B10, ADR 0013): a producer span around the send, the W3C trace
/// headers to put on the message(s), and the sent-messages count and publish duration, recorded on
/// <see cref="Dispose"/>. Telemetry never fails or changes a publish: every call into the telemetry provider is guarded
/// and logged.
/// </summary>
/// <remarks>
/// Use one instance per publish call, with <c>using</c>, and start it synchronously in the publishing method (not in a
/// helper async method): the producer span becomes <c>Activity.Current</c> there, so the trace headers name it as the
/// consumer's parent. Not thread-safe.
/// </remarks>
public sealed class PublishTelemetry : IDisposable
{
    static readonly IReadOnlyDictionary<string, string> NoHeaders = new Dictionary<string, string>();

    // Without a telemetry provider every publish shares this instance: nothing is allocated, started or recorded.
    static readonly PublishTelemetry Disabled = new();

    readonly ITelemetryProvider telemetry = NullTelemetryProvider.Instance;
    readonly TelemetryTarget? target;
    readonly int messageCount;
    readonly ILogger? logger;
    readonly TelemetryActivity? activity;
    readonly long started;
    readonly bool enabled;
    string? errorType;

    PublishTelemetry()
    {
        TraceHeaders = NoHeaders;
    }

    PublishTelemetry(ITelemetryProvider telemetry, TelemetryTarget target, int messageCount, string? messageId, ILogger logger)
    {
        this.telemetry = telemetry;
        this.target = target;
        this.messageCount = messageCount;
        this.logger = logger;
        enabled = true;
        started = Stopwatch.GetTimestamp();

        try
        {
            activity = telemetry.StartPublishActivity(target, messageCount, messageId);
        }
        catch (Exception ex)
        {
            DispatcherLog.PublishTelemetryFailed(logger, target.Destination, ex);
        }

        TraceHeaders = Inject();
    }

    /// <summary>
    /// The headers to add to every message of this publish: <c>traceparent</c> and, when present, <c>tracestate</c>
    /// (see <see cref="Telemetry.TraceHeaders"/>). Only values that parse; empty without telemetry.
    /// </summary>
    public IReadOnlyDictionary<string, string> TraceHeaders { get; }

    /// <summary>
    /// Starts telemetry for publishing <paramref name="messageCount"/> messages to <paramref name="target"/>
    /// (<paramref name="messageId"/> for a single message). With <see cref="NullTelemetryProvider"/> it returns a shared
    /// no-op instance and allocates nothing.
    /// </summary>
    public static PublishTelemetry Start(ITelemetryProvider telemetry, TelemetryTarget target, int messageCount, ILogger logger,
        string? messageId = null) =>
        telemetry is NullTelemetryProvider ? Disabled : new(telemetry, target, messageCount, messageId, logger);

    /// <summary>Marks the publish as failed: the span gets Error status and the metric the exception type as <c>error.type</c>.</summary>
    public void Failed(Exception exception)
    {
        if (!enabled)
            return;

        errorType = Processing.BoundedText.TypeName(exception);
        if (activity is not null)
            Guard((activity, exception), static s => s.activity.SetError(s.exception));
    }

    /// <summary>Records the sent-messages count and the publish duration, and ends the span.</summary>
    public void Dispose()
    {
        if (!enabled)
            return;

        Guard((telemetry, target!, messageCount, Stopwatch.GetElapsedTime(started), errorType),
            static s => s.telemetry.RecordMessagePublished(s.Item2, s.messageCount, s.Item4, s.errorType));
        if (activity is not null)
            Guard(activity, static a => a.Dispose());
    }

    IReadOnlyDictionary<string, string> Inject()
    {
        var carrier = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            telemetry.InjectTraceContext(carrier);
        }
        catch (Exception ex)
        {
            DispatcherLog.PublishTelemetryFailed(logger!, target!.Destination, ex);
            return NoHeaders;
        }

        var traceParent = carrier.GetValueOrDefault(Telemetry.TraceHeaders.TraceParent);
        var traceState = carrier.GetValueOrDefault(Telemetry.TraceHeaders.TraceState);
        if (TraceContext.Parse(traceParent, traceState) is not { } parsed)
            return NoHeaders;

        var headers = new Dictionary<string, string>(2, StringComparer.Ordinal) { [Telemetry.TraceHeaders.TraceParent] = traceParent! };
        if (parsed.TraceState is { } state)
            headers[Telemetry.TraceHeaders.TraceState] = state;
        return headers;
    }

    void Guard<TState>(TState state, Action<TState> telemetryCall)
    {
        try
        {
            telemetryCall(state);
        }
        catch (Exception ex)
        {
            DispatcherLog.PublishTelemetryFailed(logger!, target!.Destination, ex);
        }
    }
}
