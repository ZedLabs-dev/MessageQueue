using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Processing;

// Broker metadata is written by whoever published the message, not necessarily this library. Ids are logged, so they
// lose control characters (no forged log lines) and are cut to the publish-side limit; headers get the publish-side
// caps (PublishOptions) on consume too.
static class DeliveryText
{
    public static string MessageId(string? raw)
    {
        if (string.IsNullOrEmpty(raw))
            return string.Empty;
        if (raw.Length <= PublishOptions.MaxMessageIdLength && !raw.AsSpan().ContainsAnyInRange('\u0000', '\u001f') && !raw.Contains('\u007f', StringComparison.Ordinal))
            return raw;

        var clean = new string(raw.Where(c => !char.IsControl(c)).Take(PublishOptions.MaxMessageIdLength).ToArray());
        return clean;
    }

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
