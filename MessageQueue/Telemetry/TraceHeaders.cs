namespace ESE.MessageQueue.Telemetry;

// The message headers that carry W3C trace context. The library writes them on publish and reads them on consume;
// PublishValidation reserves them, so application headers can neither set nor see them.
static class TraceHeaders
{
    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";
}
