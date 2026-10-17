using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;

namespace ESE.MessageQueue.Tests.Processing;

// A transport seam that records what the dispatcher asked it to do.
sealed class FakeDeliveryActions : IDeliveryActions
{
    public List<string> Calls { get; } = [];
    public TimeSpan? RescheduleDelay { get; private set; }
    public int? NextAttempt { get; private set; }
    public DeadLetterReason? DeadLetterReason { get; private set; }

    public Task CompleteAsync(CancellationToken cancellationToken)
    {
        Calls.Add("Complete");
        return Task.CompletedTask;
    }

    public Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken)
    {
        Calls.Add("Reschedule");
        RescheduleDelay = delay;
        NextAttempt = nextAttempt;
        return Task.CompletedTask;
    }

    public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken)
    {
        Calls.Add("DeadLetter");
        DeadLetterReason = reason;
        return Task.CompletedTask;
    }
}
