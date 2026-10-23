using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Processing;

// Broker metadata is written by whoever published the message, not necessarily this library. Ids (message and
// correlation) are logged and tagged on spans, so they
// lose control characters (no forged log lines) and are cut to the publish-side limit; headers get the publish-side
// caps (PublishOptions) on consume too.
static class DeliveryText
{
    public static string MessageId(string? raw)
    {
        if (string.IsNullOrEmpty(raw))
            return string.Empty;
        // C0, DEL and C1 (U+0080-U+009F): exactly char.IsControl, checked without allocating.
        if (raw.Length <= PublishOptions.MaxMessageIdLength && !raw.AsSpan().ContainsAnyInRange('\u0000', '\u001f')
            && !raw.AsSpan().ContainsAnyInRange('\u007f', '\u009f'))
            return raw;

        var clean = new string(raw.Where(c => !char.IsControl(c)).Take(PublishOptions.MaxMessageIdLength).ToArray());
        // Never end on the first half of a surrogate pair.
        return clean.Length == PublishOptions.MaxMessageIdLength && char.IsHighSurrogate(clean[^1]) ? clean[..^1] : clean;
    }

    // A correlation id is logged and tagged on spans like the message id, so it gets the same treatment; null stays null.
    public static string? CorrelationId(string? raw) => raw is null ? null : MessageId(raw);

    // Adds an application header if it is within the publish-side limits; false when it was dropped.
    public static bool TryAddHeader(Dictionary<string, string> headers, string name, string value)
    {
        if (headers.Count >= PublishOptions.MaxHeaders
            || string.IsNullOrWhiteSpace(name) || name.Length > PublishOptions.MaxHeaderNameLength
            || value.Length > PublishOptions.MaxHeaderValueLength)
            return false;
        headers[name] = value;
        return true;
    }
}
