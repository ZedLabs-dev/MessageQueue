using ZedLabs.MessageQueue.Abstractions;

namespace ZedLabs.MessageQueue.Providers;

/// <summary>Helpers for providers that build a <see cref="MessageContext"/> from a broker delivery.</summary>
public static class MessageContextExtensions
{
    /// <summary>
    /// Returns a copy of <paramref name="context"/> carrying the W3C trace headers the delivery arrived with
    /// (<c>traceparent</c>, <c>tracestate</c>; see <see cref="Telemetry.TraceHeaders"/>). <see cref="MessageDispatcher{T}"/>
    /// parents the consume span on them; malformed or oversized values are ignored there, so pass them as received.
    /// Handlers do not see these values.
    /// </summary>
    public static MessageContext WithTraceHeaders(this MessageContext context, string? traceParent, string? traceState)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context with { TraceParent = traceParent, TraceState = traceState };
    }
}
