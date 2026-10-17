namespace ESE.MessageQueue.Processing;

/// <summary>
/// The fixed reason codes the shared pipeline dead-letters with. Bounded on purpose: they are used as metric tags.
/// </summary>
public static class DeadLetterReasons
{
    /// <summary>The body was larger than <c>MaxMessageSizeBytes</c>; it was not deserialized.</summary>
    public const string MessageTooLarge = "MessageTooLarge";

    /// <summary>The body could not be deserialized into the message type.</summary>
    public const string DeserializationFailed = "DeserializationFailed";

    /// <summary>The handler kept failing and the retry policy ran out of retries.</summary>
    public const string MaxRetriesExceeded = "MaxRetriesExceeded";

    /// <summary>The handler threw an exception the retry policy's <c>IsRetryable</c> rejects.</summary>
    public const string NonRetryableError = "NonRetryableError";

    /// <summary>The delivery could not be read (broker metadata or body), or the pipeline failed before settling it.</summary>
    public const string DeliveryFailed = "DeliveryFailed";

    /// <summary>The handler returned <see cref="Abstractions.MessageResult.DeadLetter"/>.</summary>
    public const string Rejected = "Rejected";
}
