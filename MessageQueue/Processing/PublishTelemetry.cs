using System.Diagnostics;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Processing;

// Publish-side telemetry shared by every provider (B10): a producer span around the send, the W3C trace headers to
// put on the message(s), and the sent-messages count and publish duration on dispose. Telemetry never fails or
// changes a publish: every call into the provider is guarded and logged.
//
// Start it synchronously in the publishing method (not inside a helper async method): the producer span becomes
// Activity.Current there, so the trace headers name it as the parent of the consumer's span.
sealed class PublishTelemetry : IDisposable
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

    // Only the two trace headers, and only values that parse: a provider cannot smuggle other headers onto the message.
    public IReadOnlyDictionary<string, string> TraceHeaders { get; }

    public static PublishTelemetry Start(ITelemetryProvider telemetry, TelemetryTarget target, int messageCount, ILogger logger,
        string? messageId = null) =>
        telemetry is NullTelemetryProvider ? Disabled : new(telemetry, target, messageCount, messageId, logger);

    public void Failed(Exception exception)
    {
        if (!enabled)
            return;

        errorType = exception.GetType().FullName ?? exception.GetType().Name;
        if (activity is not null)
            Guard((activity, exception), static s => s.activity.SetError(s.exception));
    }

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
