namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>What one <see cref="IDeadLetterQueue{T}.ReplayAsync"/> call did.</summary>
public sealed record DeadLetterReplayResult
{
    /// <summary>True when the call only counted (<see cref="DeadLetterReplayOptions{T}.DryRun"/>).</summary>
    public bool DryRun { get; init; }

    /// <summary>Dead letters the options selected: replayed plus failed, or in a dry run the ones that would be replayed.</summary>
    public int Matched { get; init; }

    /// <summary>Dead letters moved back to the queue and removed from the dead letters.</summary>
    public int Replayed { get; init; }

    /// <summary>Dead letters examined but not selected (or whose message id was already replayed by this call); left in place.</summary>
    public int Skipped { get; init; }

    /// <summary>Selected dead letters that could not be replayed; they stay dead letters.</summary>
    public int Failed { get; init; }

    /// <summary>The first <see cref="DeadLetterLimits.MaxReportedFailures"/> failures.</summary>
    public IReadOnlyList<DeadLetterReplayFailure> Failures { get; init; } = [];
}
