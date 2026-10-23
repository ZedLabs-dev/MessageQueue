namespace ESE.MessageQueue.Abstractions;

/// <summary>Bounds of the dead-letter operations (<see cref="IDeadLetterQueue{T}"/>).</summary>
public static class DeadLetterLimits
{
    /// <summary>The most dead letters one <see cref="IDeadLetterQueue{T}.PeekAsync"/> call returns.</summary>
    public const int MaxPeekCount = 1000;

    /// <summary>The most dead letters one replay or purge call examines, matching or not; call again for more.</summary>
    public const int MaxScanCount = 10_000;

    /// <summary>The most per-message failures one <see cref="DeadLetterReplayResult"/> lists; it still counts them all.</summary>
    public const int MaxReportedFailures = 100;
}
