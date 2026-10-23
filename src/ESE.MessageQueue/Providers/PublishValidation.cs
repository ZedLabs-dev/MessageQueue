using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// Publish-time checks every provider runs before anything is serialized or sent, so publishing fails the same way on
/// every broker. Stateless and thread-safe.
/// </summary>
public static class PublishValidation
{
    // Header names the library, brokers and telemetry own. "x-" covers x-delivery-attempt, x-death,
    // x-first-death-*, x-match and other broker-defined headers.
    const string ReservedPrefix = "x-";
    static readonly string[] ReservedNames = ["traceparent", "tracestate"];

    /// <summary>
    /// Checks one message and its <see cref="PublishOptions"/>: non-null, a non-blank message id and a correlation id
    /// (without control characters) within <see cref="PublishOptions.MaxMessageIdLength"/>, and headers within the limits on <see cref="PublishOptions"/>
    /// that do not use a reserved name (see <see cref="IsReserved"/>).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> or <paramref name="options"/> is null.</exception>
    /// <exception cref="ArgumentException">The options break a limit or use a reserved header name.</exception>
    public static void Validate<T>(T message, PublishOptions options) where T : class
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(options);

        if (options.MessageId is { } id && (string.IsNullOrWhiteSpace(id) || id.Length > PublishOptions.MaxMessageIdLength))
            throw new ArgumentException(
                $"MessageId must be non-blank and at most {PublishOptions.MaxMessageIdLength} characters", nameof(options));

        // A correlation id is a key the application matches on: one that does not fit every broker is refused, not cut.
        if (options.CorrelationId is { } correlationId
            && (correlationId.Length > PublishOptions.MaxMessageIdLength || correlationId.Any(char.IsControl)))
            throw new ArgumentException(
                $"CorrelationId must be at most {PublishOptions.MaxMessageIdLength} characters, without control characters", nameof(options));

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

    /// <summary>Checks a batch argument; validate each message with <see cref="Validate{T}(T, PublishOptions)"/> as it is enumerated.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="messages"/> is null.</exception>
    public static void Validate<T>(IEnumerable<T> messages) where T : class
    {
        ArgumentNullException.ThrowIfNull(messages);
    }

    /// <summary>
    /// Checks a serialized body against the queue's <c>MaxMessageSizeBytes</c> before it is sent, so a message every
    /// consumer would dead-letter as <c>MessageTooLarge</c> never reaches the broker. Uses the consumer's rule: a body
    /// of exactly <paramref name="maxMessageSizeBytes"/> bytes passes. For a batch, check every body before sending any
    /// and pass the message's <paramref name="batchIndex"/>. The error names the size, the limit and the destination,
    /// never the content.
    /// </summary>
    /// <exception cref="MessageQueueException">The body is larger than <paramref name="maxMessageSizeBytes"/>.</exception>
    public static void ValidateBodySize(int bodyLength, int maxMessageSizeBytes, string destination, int? batchIndex = null)
    {
        if (bodyLength <= maxMessageSizeBytes)
            return;

        var which = batchIndex is { } index ? $"The message at index {index} of the batch" : "The message";
        var outcome = batchIndex is null ? "it was not published" : "no message of the batch was published";
        throw new MessageQueueException(
            $"{which} is {bodyLength} bytes serialized, above MaxMessageSizeBytes ({maxMessageSizeBytes}) for '{destination}'; {outcome}");
    }

    /// <summary>
    /// True for header names owned by the broker, the library or telemetry: names starting with <c>x-</c>
    /// (case-insensitive) and <c>traceparent</c> / <c>tracestate</c>. Providers also use it on consume to keep those
    /// headers out of <c>MessageContext.Headers</c>.
    /// </summary>
    public static bool IsReserved(string name) =>
        name.StartsWith(ReservedPrefix, StringComparison.OrdinalIgnoreCase)
        || ReservedNames.Contains(name.Trim(), StringComparer.OrdinalIgnoreCase);
}
