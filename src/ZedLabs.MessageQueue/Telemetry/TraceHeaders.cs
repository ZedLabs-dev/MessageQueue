using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.Telemetry;

/// <summary>
/// The message header names that carry W3C trace context. The library writes them on publish and reads them on
/// consume; they are reserved, so application headers can neither set nor see them.
/// </summary>
public static class TraceHeaders
{
    /// <summary>The W3C <c>traceparent</c> header.</summary>
    public const string TraceParent = "traceparent";
    /// <summary>The W3C <c>tracestate</c> header.</summary>
    public const string TraceState = "tracestate";
}
