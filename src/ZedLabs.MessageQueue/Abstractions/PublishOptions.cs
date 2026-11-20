namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>
/// Optional per-message metadata for <see cref="IMessagePublisher{T}.PublishAsync(T, PublishOptions, CancellationToken)"/>.
/// </summary>
public sealed record PublishOptions
{
    /// <summary>Longest accepted <see cref="MessageId"/> and <see cref="CorrelationId"/> (Azure Service Bus's limit).</summary>
    public const int MaxMessageIdLength = 128;

    /// <summary>Most headers accepted on one message.</summary>
    public const int MaxHeaders = 64;

    /// <summary>Longest accepted header name.</summary>
    public const int MaxHeaderNameLength = 128;

    /// <summary>Longest accepted header value.</summary>
    public const int MaxHeaderValueLength = 4096;

    /// <summary>
    /// The message id, and the portable idempotency key: providers map it to their native de-duplication where they
    /// have one, and consumers can de-duplicate on <see cref="MessageContext.MessageId"/>. A version 7 GUID when not set.
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>Correlates this message with a request, workflow or conversation.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Application headers, delivered in <see cref="MessageContext.Headers"/>.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
