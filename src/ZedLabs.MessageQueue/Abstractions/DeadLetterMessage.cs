using System.Collections.Frozen;

namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>
/// A read-only snapshot of one dead letter. Values read from the broker are bounded like consumed metadata: ids lose
/// control characters and are cut to <see cref="PublishOptions.MaxMessageIdLength"/>, reason texts to
/// <see cref="PublishOptions.MaxHeaderValueLength"/>, and headers follow the publish-side limits.
/// </summary>
/// <remarks>
/// A plain sealed class rather than a record, so that every public member carries its nullable annotations (the
/// compiler's synthesized members of a generic record are nullable-oblivious).
/// </remarks>
/// <typeparam name="T">The message type.</typeparam>
public sealed class DeadLetterMessage<T> where T : class
{
    readonly IReadOnlyDictionary<string, string> headers = FrozenDictionary<string, string>.Empty;

    /// <summary>The message id the subscriber saw (<see cref="MessageContext.MessageId"/>); empty when the message had none.</summary>
    public required string MessageId { get; init; }

    /// <summary>The correlation id, if any.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Why it was dead-lettered: one of <see cref="Processing.DeadLetterReasons"/> when this library dead-lettered it,
    /// else the broker's own reason (such as Service Bus's <c>MaxDeliveryCountExceeded</c>), or null when none was recorded.
    /// </summary>
    public string? ReasonCode { get; init; }

    /// <summary>Diagnostic detail such as an exception type name; never exception text.</summary>
    public string? ReasonDetail { get; init; }

    /// <summary>When the message was originally published or enqueued, when known.</summary>
    public DateTimeOffset? EnqueuedAt { get; init; }

    /// <summary>When it was dead-lettered, when known.</summary>
    public DateTimeOffset? DeadLetteredAt { get; init; }

    /// <summary>The delivery attempt on which it was dead-lettered, when known.</summary>
    public int? DeliveryAttempt { get; init; }

    /// <summary>The application headers it was published with. Immutable.</summary>
    public IReadOnlyDictionary<string, string> Headers
    {
        get => headers;
        init => headers = value is FrozenDictionary<string, string> frozen ? frozen : value.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <summary>
    /// The deserialized message, or null when the body could not be deserialized or is larger than the queue's
    /// <c>MaxMessageSizeBytes</c> (it is then not read at all).
    /// </summary>
    public T? Message { get; init; }

    /// <summary>The length of the serialized body in bytes.</summary>
    public int BodyLength { get; init; }
}
