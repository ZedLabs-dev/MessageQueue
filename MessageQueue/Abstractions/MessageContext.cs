using System.Collections.Frozen;

namespace ESE.MessageQueue.Abstractions;

/// <summary>
/// Broker metadata for one delivery of a message.
/// </summary>
public sealed record MessageContext
{
    readonly IReadOnlyDictionary<string, string> headers = FrozenDictionary<string, string>.Empty;

    /// <summary>The message id: the publisher's <see cref="PublishOptions.MessageId"/>, or a generated version 7 GUID.</summary>
    public required string MessageId { get; init; }

    /// <summary>The correlation id supplied by the publisher, if any.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>When the message was published (or enqueued by the broker, where the broker records that instead).</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Which delivery this is, 1-based: 1 for the first attempt, 2 for the first retry, and so on.</summary>
    public int DeliveryAttempt { get; init; } = 1;

    /// <summary>
    /// Application headers supplied by the publisher through <see cref="PublishOptions.Headers"/>. Immutable: the
    /// given dictionary is copied, so a handler cannot change what a retry or another handler sees.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers
    {
        get => headers;
        init => headers = value is FrozenDictionary<string, string> frozen ? frozen : value.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
