namespace ESE.MessageQueue.Abstractions;

/// <summary>
/// What a handler wants done with a message.
/// </summary>
public enum MessageResult
{
    /// <summary>Processed; remove the message from the queue.</summary>
    Acknowledge,

    /// <summary>Failed transiently; redeliver after the retry policy's backoff, or dead-letter once retries are exhausted.</summary>
    Retry,

    /// <summary>Failed permanently; dead-letter now without retrying.</summary>
    DeadLetter
}
