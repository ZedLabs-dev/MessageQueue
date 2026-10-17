using System.Globalization;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.AzureServiceBus;

// Maps Service Bus messages to and from the portable model, and builds the library's retry copies (ADR 0009).
static class ServiceBusMessages
{
    // Library-owned application properties. Publishers cannot set them: PublishValidation reserves every "x-" name.
    public const string AttemptProperty = "x-ese-delivery-attempt";
    public const string DeliveryCountProperty = "x-ese-delivery-count";
    public const string OriginalMessageIdProperty = "x-ese-message-id";
    public const string DeadLetterReasonProperty = "x-ese-dead-letter-reason";
    public const string DeadLetterDetailProperty = "x-ese-dead-letter-detail";

    // Added by the SDK's tracing on send; not an application header.
    const string DiagnosticIdProperty = "Diagnostic-Id";

    public static MessageContext ToContext(ServiceBusReceivedMessage message)
    {
        var headers = new Dictionary<string, string>();
        foreach (var (key, value) in message.ApplicationProperties)
        {
            if (IsInternal(key) || Text(value) is not { } text)
                continue;
            headers[key] = text;
        }

        var library = LibraryState(message);
        return new MessageContext
        {
            // W3C traceparent, else the SDK's own Diagnostic-Id (also W3C when the SDK's tracing wrote it).
            TraceParent = TraceText(Property(message, TraceHeaders.TraceParent), TraceContext.MaxTraceParentLength)
                ?? TraceText(Property(message, DiagnosticIdProperty), TraceContext.MaxTraceParentLength),
            TraceState = TraceText(Property(message, TraceHeaders.TraceState), TraceContext.MaxTraceStateLength),
            MessageId = library is not null && Text(message.ApplicationProperties.GetValueOrDefault(OriginalMessageIdProperty)) is { Length: > 0 } original
                ? original
                : message.MessageId ?? string.Empty,
            CorrelationId = message.CorrelationId,
            Timestamp = message.EnqueuedTime,
            DeliveryAttempt = Attempt(message, library),
            Headers = headers
        };
    }

    // The attempt is the library's attempt property plus every broker delivery since the library wrote it, but only
    // when the library's delivery-count marker is consistent with the broker's DeliveryCount (it can never be ahead).
    // Otherwise DeliveryCount alone is the attempt. ADR 0009 describes the residual forgery risk.
    // Saturating: both properties are capped at int.MaxValue when read, so the sum fits a long, and the result is
    // clamped to [1, int.MaxValue] whatever a forger wrote.
    static int Attempt(ServiceBusReceivedMessage message, (long Attempt, long ExpectedDeliveryCount)? library)
    {
        var deliveryCount = Math.Max(1, message.DeliveryCount);
        if (library is not { } state)
            return deliveryCount;
        return (int)Math.Clamp(state.Attempt + (deliveryCount - state.ExpectedDeliveryCount), 1, int.MaxValue);
    }

    static (long Attempt, long ExpectedDeliveryCount)? LibraryState(ServiceBusReceivedMessage message)
    {
        var properties = message.ApplicationProperties;
        if (Number(properties.GetValueOrDefault(AttemptProperty)) is not { } attempt
            || Number(properties.GetValueOrDefault(DeliveryCountProperty)) is not { } expected
            || expected > Math.Max(1, message.DeliveryCount))
            return null;
        return (attempt, expected);
    }

    // A queue-mode retry: a copy for the same queue, with its own message id (so duplicate detection cannot drop it as
    // a repeat of the original), the original id, the next attempt and the delivery count its first delivery will have.
    // Application and W3C trace properties survive; SDK tracing, foreign "x-" and earlier library properties do not.
    public static ServiceBusMessage RetryCopy(ServiceBusReceivedMessage original, MessageContext context, int nextAttempt)
    {
        var copy = new ServiceBusMessage(original)
        {
            MessageId = Guid.CreateVersion7().ToString()
        };
        // The trace headers stay: every attempt continues the original publisher's trace.
        foreach (var key in copy.ApplicationProperties.Keys.Where(key => IsInternal(key) && !IsTraceHeader(key)).ToList())
            copy.ApplicationProperties.Remove(key);

        copy.ApplicationProperties[OriginalMessageIdProperty] = context.MessageId;
        copy.ApplicationProperties[AttemptProperty] = nextAttempt;
        copy.ApplicationProperties[DeliveryCountProperty] = 1;
        return copy;
    }

    // Properties for an abandon after which the next delivery should count as `attempt` (DeliveryCount rises by one).
    public static Dictionary<string, object> AttemptProperties(ServiceBusReceivedMessage message, int attempt) => new()
    {
        [AttemptProperty] = attempt,
        [DeliveryCountProperty] = message.DeliveryCount >= int.MaxValue - 1 ? int.MaxValue : Math.Max(1, message.DeliveryCount) + 1
    };

    // S6: a reason code and an exception type, never exception text. The delivery-count marker is set out of reach
    // (it is then always ahead of the broker's DeliveryCount), so a message an operator resubmits from the dead-letter
    // queue with its properties starts again from attempt 1 instead of inheriting the exhausted attempt.
    public static Dictionary<string, object> DeadLetterProperties(DeadLetterReason reason)
    {
        var properties = new Dictionary<string, object>
        {
            [DeadLetterReasonProperty] = reason.Code,
            [DeliveryCountProperty] = int.MaxValue
        };
        if (reason.Detail is not null)
            properties[DeadLetterDetailProperty] = reason.Detail;
        return properties;
    }

    static bool IsInternal(string key) =>
        PublishValidation.IsReserved(key) || string.Equals(key, DiagnosticIdProperty, StringComparison.OrdinalIgnoreCase);

    static bool IsTraceHeader(string key) =>
        key.Equals(TraceHeaders.TraceParent, StringComparison.OrdinalIgnoreCase) || key.Equals(TraceHeaders.TraceState, StringComparison.OrdinalIgnoreCase);

    // Header names are matched case-insensitively: other instrumentations do not agree on case.
    static object? Property(ServiceBusReceivedMessage message, string name)
    {
        foreach (var (key, value) in message.ApplicationProperties)
        {
            if (key.Equals(name, StringComparison.OrdinalIgnoreCase))
                return value;
        }
        return null;
    }

    // Trace headers are strings or UTF-8 bytes, read only up to the parser's bound (they come from any publisher).
    static string? TraceText(object? value, int maxLength) => value switch
    {
        string { Length: > 0 } text when text.Length <= maxLength => text,
        byte[] { Length: > 0 } bytes when bytes.Length <= maxLength => System.Text.Encoding.UTF8.GetString(bytes),
        _ => null
    };

    // A positive whole number, capped at int.MaxValue (larger values, forged or not, saturate).
    static long? Number(object? value) =>
        Text(value) is { } text && long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) && number > 0
            ? Math.Min(number, int.MaxValue)
            : null;

    static string? Text(object? value) => value switch
    {
        string s => s,
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        null => null,
        _ => value.ToString()
    };
}
