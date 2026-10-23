using System.Collections.Frozen;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// What a provider read from one dead letter (<see cref="IDeadLetterEntry.Metadata"/>). The values come from the broker,
/// so <see cref="DeadLetterQueue{T}"/> sanitizes and bounds them before an application sees them.
/// </summary>
public sealed record DeadLetterMetadata
{
    readonly IReadOnlyDictionary<string, string> headers = FrozenDictionary<string, string>.Empty;

    /// <summary>The message id the subscriber would see; empty when the message has none.</summary>
    public required string MessageId { get; init; }

    /// <summary>The correlation id, if any.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The library's dead-letter reason code, else the broker's own reason, else null.</summary>
    public string? ReasonCode { get; init; }

    /// <summary>The library's dead-letter detail (an exception type name), else the broker's, else null.</summary>
    public string? ReasonDetail { get; init; }

    /// <summary>When the message was originally published or enqueued, when known.</summary>
    public DateTimeOffset? EnqueuedAt { get; init; }

    /// <summary>When it was dead-lettered, when known.</summary>
    public DateTimeOffset? DeadLetteredAt { get; init; }

    /// <summary>The delivery attempt on which it was dead-lettered, when known.</summary>
    public int? DeliveryAttempt { get; init; }

    /// <summary>The application headers (no broker, library or trace headers). Copied and immutable.</summary>
    public IReadOnlyDictionary<string, string> Headers
    {
        get => headers;
        init => headers = value is FrozenDictionary<string, string> frozen ? frozen : value.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
