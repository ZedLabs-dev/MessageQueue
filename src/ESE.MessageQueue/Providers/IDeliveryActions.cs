using ESE.MessageQueue.Processing;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// The transport seam (ADR 0004): what a provider implements to settle one delivery. Create one instance per delivery
/// and pass it to <see cref="MessageDispatcher{T}.DispatchAsync"/>. The dispatcher calls at most one of these methods,
/// or none when the handler was cancelled by the provider's stopping token (leave the message to be redelivered then).
/// A method that throws makes <c>DispatchAsync</c> throw; the provider decides how to hand the delivery back.
/// </summary>
public interface IDeliveryActions
{
    /// <summary>Removes the message from the queue (acknowledge or complete).</summary>
    Task CompleteAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Redelivers the message after <paramref name="delay"/> as delivery attempt <paramref name="nextAttempt"/>
    /// (1-based; <c>MessageContext.DeliveryAttempt</c> of that delivery) and removes the current delivery.
    /// </summary>
    Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken);

    /// <summary>Moves the message to the dead-letter store. Called only when <c>EnableDeadLetterQueue</c> is true.</summary>
    Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken);
}

/// <summary>Why a delivery is dead-lettered. <c>default</c> reads as <see cref="DeadLetterReasons.DeliveryFailed"/>.</summary>
public readonly record struct DeadLetterReason
{
    readonly string? code;

    /// <summary>Creates a reason.</summary>
    /// <param name="code">One of <see cref="DeadLetterReasons"/>; bounded, safe to store on the message and tag metrics with.</param>
    /// <param name="detail">Diagnostic detail such as an exception type name; never exception text (S6).</param>
    /// <exception cref="ArgumentException"><paramref name="code"/> is null or blank.</exception>
    public DeadLetterReason(string code, string? detail = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        this.code = code;
        Detail = detail;
    }

    /// <summary>One of <see cref="DeadLetterReasons"/>; never null.</summary>
    public string Code => code ?? DeadLetterReasons.DeliveryFailed;

    /// <summary>Diagnostic detail such as an exception type name; never exception text (S6).</summary>
    public string? Detail { get; }
}

/// <summary>What <see cref="MessageDispatcher{T}"/> did with a delivery.</summary>
public enum DispatchOutcome
{
    /// <summary>The handler acknowledged it; <see cref="IDeliveryActions.CompleteAsync"/> was called.</summary>
    Completed,

    /// <summary>It is retried later; <see cref="IDeliveryActions.RescheduleAsync"/> was called.</summary>
    Rescheduled,

    /// <summary>It was dead-lettered; <see cref="IDeliveryActions.DeadLetterAsync"/> was called.</summary>
    DeadLettered,

    /// <summary>It would have been dead-lettered but dead-lettering is disabled, so it was completed and logged.</summary>
    Dropped
}
