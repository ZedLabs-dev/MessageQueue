namespace ESE.MessageQueue.Processing;

// The transport seam (ADR 0004): the only thing a provider implements for consuming. One instance per delivery.
// The dispatcher calls exactly one of these per delivery, or none when the handler was cancelled by shutdown
// (the transport then leaves the message to be redelivered).
interface IDeliveryActions
{
    // Remove the message from the queue (acknowledge / complete).
    Task CompleteAsync(CancellationToken cancellationToken);

    // Redeliver the message after `delay` as delivery attempt `nextAttempt`, and remove the current delivery.
    Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken);

    // Move the message to the dead-letter store. Only called when dead-lettering is enabled.
    Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken);
}

// Code is one of DeadLetterReasons; Detail is diagnostic, e.g. an exception type name. Never exception text (S6).
readonly record struct DeadLetterReason(string Code, string? Detail = null);

enum DispatchOutcome
{
    Completed,
    Rescheduled,
    DeadLettered,
    Dropped
}
