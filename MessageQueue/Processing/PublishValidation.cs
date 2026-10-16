using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Processing;

// Publish-time checks shared by every provider, run before anything is serialized or sent.
static class PublishValidation
{
    // Header names the library, brokers and telemetry own. "x-" covers x-delivery-attempt, x-death,
    // x-first-death-*, x-match and other broker-defined headers.
    const string ReservedPrefix = "x-";
    static readonly string[] ReservedNames = ["traceparent", "tracestate"];

    public static void Validate<T>(T message, PublishOptions options) where T : class
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(options);

        if (options.MessageId is { } id && (string.IsNullOrWhiteSpace(id) || id.Length > PublishOptions.MaxMessageIdLength))
            throw new ArgumentException(
                $"MessageId must be non-blank and at most {PublishOptions.MaxMessageIdLength} characters", nameof(options));

        if (options.Headers is not { } headers)
            return;

        if (headers.Count > PublishOptions.MaxHeaders)
            throw new ArgumentException($"At most {PublishOptions.MaxHeaders} headers are allowed", nameof(options));

        foreach (var (name, value) in headers)
        {
            if (string.IsNullOrWhiteSpace(name) || name.Length > PublishOptions.MaxHeaderNameLength)
                throw new ArgumentException(
                    $"Header names must be non-blank and at most {PublishOptions.MaxHeaderNameLength} characters", nameof(options));

            if (IsReserved(name))
                throw new ArgumentException(
                    $"Header '{name}' is reserved: names starting with '{ReservedPrefix}' and {string.Join(", ", ReservedNames)} belong to the broker, the library or telemetry",
                    nameof(options));

            if (value is null || value.Length > PublishOptions.MaxHeaderValueLength)
                throw new ArgumentException(
                    $"Header '{name}' must have a value of at most {PublishOptions.MaxHeaderValueLength} characters", nameof(options));
        }
    }

    public static void Validate<T>(IEnumerable<T> messages) where T : class
    {
        ArgumentNullException.ThrowIfNull(messages);
    }

    static bool IsReserved(string name) =>
        name.StartsWith(ReservedPrefix, StringComparison.OrdinalIgnoreCase)
        || ReservedNames.Contains(name.Trim(), StringComparer.OrdinalIgnoreCase);
}
