using System.Diagnostics.CodeAnalysis;
using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Processing;

// S6: free text the library writes into a broker property, a span, a metric tag or a log argument, when a caller, a
// publisher or an exception supplied it, is cut to one limit (the publish-side header value limit). Brokers and
// exporters cap sizes (Service Bus refuses a dead-letter reason or description over 4096 characters), so a long value
// must not stop a message from being settled or a span from being exported. Ids have their own, smaller limit
// (DeliveryText).
static class BoundedText
{
    public const int MaxLength = PublishOptions.MaxHeaderValueLength;

    [return: NotNullIfNotNull(nameof(value))]
    public static string? Truncate(string? value)
    {
        if (value is null || value.Length <= MaxLength)
            return value;

        // Never end on the first half of a surrogate pair.
        var length = char.IsHighSurrogate(value[MaxLength - 1]) ? MaxLength - 1 : MaxLength;
        return value[..length];
    }

    // An exception's type name: generic types carry their arguments' assembly-qualified names and can be very long.
    public static string TypeName(Exception exception) =>
        Truncate(exception.GetType().FullName ?? exception.GetType().Name);
}
