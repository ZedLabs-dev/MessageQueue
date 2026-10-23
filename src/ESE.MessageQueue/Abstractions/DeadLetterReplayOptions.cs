namespace ESE.MessageQueue.Abstractions;

/// <summary>
/// Which dead letters <see cref="IDeadLetterQueue{T}.ReplayAsync"/> moves back to the queue. A dead letter is replayed
/// when its reason code is selected (<see cref="ReasonCodes"/>) and <see cref="Filter"/>, if set, accepts it.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public sealed class DeadLetterReplayOptions<T> where T : class
{
    /// <summary>The most dead letters to replay (at least 1).</summary>
    public required int MaxCount { get; init; }

    /// <summary>
    /// The reason codes to replay (matched exactly). When null, every reason except
    /// <see cref="Processing.DeadLetterReasons.MessageTooLarge"/> and <see cref="Processing.DeadLetterReasons.DeserializationFailed"/>,
    /// which would only fail again: name them here to replay them anyway (for example after raising the size limit or
    /// fixing the message type). Must not be empty.
    /// </summary>
    public IReadOnlyCollection<string>? ReasonCodes { get; init; }

    /// <summary>An extra condition on the snapshot, for example on a header or the message; null accepts every dead letter.</summary>
    public Func<DeadLetterMessage<T>, bool>? Filter { get; init; }

    /// <summary>When true, only counts what would be replayed (<see cref="DeadLetterReplayResult.Matched"/>); nothing is changed.</summary>
    public bool DryRun { get; init; }
}
